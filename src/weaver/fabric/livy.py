"""Manage Weaver execution inside Fabric Spark sessions.

Livy sessions are reused across related work to avoid repeated startup cost.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any

from ..errors import WeaverError
from .auth import FABRIC_SCOPE, token_source
from .client import (
    CONNECTION_ATTEMPTS,
    FABRIC_API,
    TRANSIENT_STATUSES,
    retry_delay,
    send,
)

DEFAULT_LIVY_API_VERSION = "2023-12-01"
DEFAULT_POLL_INTERVAL = 3.0
DEFAULT_SESSION_TIMEOUT = 600.0
DEFAULT_STATEMENT_TIMEOUT = 900.0
#: How long a close waits for the session to actually release its capacity slot.
#: Shorter than a start, because a session being torn down has no work to finish
#: and a caller should not be held up by one that will not admit it has gone.
DEFAULT_CLOSE_TIMEOUT = 120.0

#: Wrapped around returned values so a result can be told from printed output.
RESULT_PREFIX = "__weaver_result__"


class LivyError(WeaverError):
    """Raised when a Livy session fails: it would not start, or it has died."""


class LivyStatementError(LivyError):
    """Raised when a statement failed. The session that ran it is fine.

    A remote ``ModuleNotFoundError`` says the submitted program was wrong and
    nothing about the Spark session, which is still up and still costs a minute
    to replace. Treating the two alike is how one bad command ends a console.

    A subclass, so every existing ``except LivyError`` still catches it.
    """

    def __init__(self, message: str, *, ename=None, evalue=None) -> None:
        super().__init__(message)
        #: The remote exception's class name, where Livy reported one.
        self.ename = ename
        #: Its message, likewise.
        self.evalue = evalue


@dataclass(frozen=True)
class StatementResult:
    """What one submitted statement produced."""

    text: str
    payload: Any = None

    @property
    def returned(self) -> bool:
        return self.payload is not None


@dataclass(frozen=True)
class LivySessionInfo:
    """One entry returned by Fabric's Lakehouse sessions collection."""

    id: str
    name: str | None = None
    submitter_id: str | None = None
    submitter_name: str | None = None
    artifact_id: str | None = None
    scheduler_state: str | None = None
    plugin_state: str | None = None
    livy_state: str | None = None
    submitted_at: str | None = None
    started_at: str | None = None
    ended_at: str | None = None
    result: str | None = None
    cancellation_reason: str | None = None
    tags: tuple[str, ...] = ()

    @classmethod
    def from_mapping(cls, value: dict[str, Any]) -> "LivySessionInfo":
        tags = value.get("tags") or ()
        if isinstance(tags, str):
            tags = (tags,)
        return cls(
            id=str(value.get("id") or value.get("livyId") or ""),
            name=_optional_text(value.get("name")),
            submitter_id=_optional_text(value.get("submitterId")),
            submitter_name=_optional_text(value.get("submitterName")),
            artifact_id=_optional_text(value.get("artifactId")),
            scheduler_state=_optional_text(value.get("schedulerState")),
            plugin_state=_optional_text(value.get("pluginState")),
            livy_state=_optional_text(value.get("livyState") or value.get("state")),
            submitted_at=_optional_text(value.get("submittedAt")),
            started_at=_optional_text(value.get("startedAt")),
            ended_at=_optional_text(value.get("endedAt")),
            result=_optional_text(value.get("result")),
            cancellation_reason=_optional_text(value.get("cancellationReason")),
            tags=tuple(str(tag) for tag in tags),
        )

    @property
    def active(self) -> bool:
        """Whether this session still occupies, or waits for, a capacity slot."""

        if self.scheduler_state:
            return self.scheduler_state.casefold() != "ended"
        if self.livy_state:
            return self.livy_state.casefold() not in {
                "dead",
                "error",
                "killed",
                "success",
                "shutting_down",
            }
        return False


@dataclass(frozen=True)
class WorkspaceLivySession:
    """A Livy collection entry with the Lakehouse whose collection owns it."""

    lakehouse_id: str
    lakehouse_name: str
    session: LivySessionInfo

    @property
    def active(self) -> bool:
        return self.session.active


def _spark_home(workspace):
    """The Lakehouse a Spark session attaches to, from the workspace's own.

    The fallback for a caller that named none. The first configured one, by
    name, so a workspace answers the same way twice. Which it is does not affect
    where work lands, because every generated statement names its Lakehouse, so
    a stable answer is all that is wanted.
    """

    from ..errors import CommandError
    from ..targets import ItemRef

    configured = sorted(getattr(workspace, "lakehouses", ()) or ())
    if not configured:
        raise CommandError(
            "starting a Spark session needs a Lakehouse to attach to, and none "
            "was named. Give the operation a Lakehouse target, or add one to "
            "`lakehouses` in workspace configuration. Warehouse-only work needs "
            "no Spark session."
        )
    return ItemRef(configured[0])


def sessions_url(
    workspace_id: str,
    lakehouse_id: str,
    *,
    api_base_url: str = FABRIC_API,
    api_version: str = DEFAULT_LIVY_API_VERSION,
) -> str:
    base = api_base_url.rstrip("/")
    return (
        f"{base}/workspaces/{workspace_id}"
        f"/lakehouses/{lakehouse_id}"
        f"/livyapi/versions/{api_version}/sessions"
    )


def list_livy_sessions(
    workspace_id: str,
    lakehouse_id: str,
    *,
    client=None,
) -> tuple[LivySessionInfo, ...]:
    """List the Spark sessions Fabric records for one Lakehouse.

    This is read-only. In particular, it never cancels a stale session: the
    caller that owns a session remains the only thing entitled to end it.
    """

    from .client import FabricClient

    client = client or FabricClient()
    payload = client.get_json(
        sessions_url(workspace_id, lakehouse_id, api_base_url=client.api_base_url)
    )
    return tuple(
        LivySessionInfo.from_mapping(item) for item in payload.get("items", ())
    )


def list_workspace_livy_sessions(
    workspace,
    *,
    client=None,
    active_only: bool = False,
) -> tuple[WorkspaceLivySession, ...]:
    """List sessions across every Lakehouse in a Fabric workspace.

    A Fabric capacity can apply a session limit across the workspace while the
    API exposes collections per Lakehouse, so looking only at Weaver's own
    Lakehouse would miss a notebook occupying the same slot.
    """

    from .client import FabricClient
    from .resolution import FabricResolver
    from .resources import LAKEHOUSE, list_items

    client = client or FabricClient()
    resolver = FabricResolver(workspace, client=client)
    found = tuple(
        WorkspaceLivySession(lakehouse.id, lakehouse.name, session)
        for lakehouse in list_items(
            resolver.workspace, item_type=LAKEHOUSE, client=client
        )
        for session in list_livy_sessions(
            resolver.workspace.id, lakehouse.id, client=client
        )
    )
    if active_only:
        found = tuple(entry for entry in found if entry.active)
    return tuple(
        sorted(found, key=lambda entry: (entry.lakehouse_name, entry.session.id))
    )


def _optional_text(value: Any) -> str | None:
    return None if value is None or value == "" else str(value)


def _call(
    method: str,
    url: str,
    token: str,
    payload: Any = None,
    expected: tuple[int, ...] = (200, 201, 202),
) -> dict:
    import requests

    # Livy runs over the same REST front door, and it refuses a call the same way
    # while a capacity is busy. Polling a statement for minutes meets that often.
    for attempt in range(1, CONNECTION_ATTEMPTS + 1):
        try:
            response = send(
                method,
                url,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                },
                data=json.dumps(payload) if payload is not None else None,
                timeout=120,
            )
        except requests.exceptions.RequestException as exc:
            raise LivyError(f"{method} {url} could not be reached: {exc}") from exc
        if response.status_code in expected:
            return response.json() if response.content else {}
        if response.status_code in TRANSIENT_STATUSES and attempt < CONNECTION_ATTEMPTS:
            time.sleep(retry_delay(response, attempt))
            continue
        raise LivyError(
            f"{method} {url} returned {response.status_code}: "
            f"{response.text.strip()[:400] or 'no body'}"
        )
    raise LivyError(f"{method} {url} did not settle")


class LivySession:
    """One Fabric Spark session, held open for a batch of statements."""

    def __init__(
        self,
        workspace_id: str,
        lakehouse_id: str,
        *,
        token: str | None = None,
        environment_id: str | None = None,
        api_base_url: str = FABRIC_API,
        poll_interval: float = DEFAULT_POLL_INTERVAL,
        bootstrap: str | None = None,
    ) -> None:
        self._token_source = token_source(token, scope=FABRIC_SCOPE)
        self.base = sessions_url(workspace_id, lakehouse_id, api_base_url=api_base_url)
        self.environment_id = environment_id
        self.poll_interval = poll_interval
        self.bootstrap = bootstrap
        self.session_url: str | None = None
        self._weaver_asserted = False

    @property
    def token(self) -> str:
        """A currently-valid bearer, renewed when it is close to expiring.

        A session is held open across a whole suite, so a snapshotted token
        expires mid-run and every statement after it fails with ``401``.
        """

        return self._token_source()

    @classmethod
    def for_workspace(
        cls, workspace, *, resolver=None, lakehouse=None, **kwargs
    ) -> "LivySession":
        """A session attached to one of the workspace's Lakehouses.

        Fabric creates a Spark session against a Lakehouse, whose id is in the
        Livy URL, so a session needs one to live in. Which one does not affect
        where work lands: every statement Weaver generates names the Lakehouse
        it is about, in full. The attachment is a home, not a destination.

        The home is named by the caller, from the physical Lakehouses the
        operation was actually asked for, and falls back to the workspace's own
        configured Lakehouses. An operation that names neither is Warehouse-only
        and has no reason to start Spark at all, which is what the error says.

        The workspace's ``environment`` is attached where it names one, so a
        body that imports Weaver finds what ``weaver fabric environment publish`` published.
        Nothing is copied into the workspace.

        An Environment is not required to start a session. Submitting Spark
        to a workspace and running the installed package are two different
        needs, and only the second waits on a wheel publish: a build's
        statements are Spark SQL that imports nothing, so it runs on the
        workspace's default runtime. :meth:`ensure_weaver` is where the other
        need is stated, and where a missing Environment is refused.
        """

        from ..targets import ItemRef
        from .resolution import FabricResolver
        from .resources import LAKEHOUSE

        resolver = resolver or FabricResolver(workspace)
        home_item = lakehouse if lakehouse is not None else _spark_home(workspace)
        if isinstance(home_item, str):
            home_item = ItemRef(home_item)
        home = resolver.resolve(home_item, item_type=LAKEHOUSE)

        environment_id = kwargs.pop("environment_id", None)
        if environment_id is None and getattr(workspace, "environment", None):
            environment_id = _resolve_environment_id(workspace, resolver)

        return cls(
            resolver.workspace.id,
            home.id,
            environment_id=environment_id,
            bootstrap=emit_source(),
            **kwargs,
        )

    def ensure_weaver(self) -> None:
        """Assert this session can ``import weaver``, once.

        Called by whatever submits a body that imports Weaver, so a session that
        only carries Spark SQL never waits on a wheel publish. The import stays
        loaded afterwards, so the cost is one statement per session.

        This is also where a missing Environment is refused, because this is
        where one is needed: an installed Weaver is what an Environment carries,
        and a session without one runs the default runtime perfectly well until
        something tries to import from it.
        """

        if self._weaver_asserted:
            return
        if not self.environment_id:
            from ..errors import CommandError

            raise CommandError(missing_environment())
        self.run(environment_bootstrap())
        self._weaver_asserted = True

    def __enter__(self) -> "LivySession":
        self.start()
        return self

    def __exit__(self, *exc) -> bool:
        self.close()
        return False

    def start(self, *, timeout: float = DEFAULT_SESSION_TIMEOUT) -> None:
        self._weaver_asserted = False
        payload: dict[str, Any] = {"name": "weaver"}
        if self.environment_id:
            # Fabric attaches an Environment to a Livy session through a Spark
            # conf, not a top-level field. The published libraries, Weaver and its
            # dependencies, are loaded only when this is set.
            payload["conf"] = {
                "spark.fabric.environmentDetails": json.dumps(
                    {"id": self.environment_id}
                )
            }
        created = _call("POST", self.base, self.token, payload)
        session_id = created.get("id") or created.get("livyId")
        if session_id is None:
            raise LivyError(f"Livy did not return a session id: {created}")
        self.session_url = f"{self.base}/{session_id}"
        self._await("idle", timeout=timeout)
        if self.bootstrap:
            self.run(self.bootstrap)

    def _await(self, wanted: str, *, timeout: float) -> dict:
        deadline = time.time() + timeout
        while time.time() < deadline:
            state = _call("GET", self.session_url, self.token, expected=(200,))
            current = (state.get("state") or "").lower()
            if current == wanted:
                return state
            if current in {"error", "dead", "killed", "shutting_down"}:
                raise LivyError(f"Livy session entered state {current!r}")
            time.sleep(self.poll_interval)
        raise LivyError(f"Livy session did not reach {wanted!r} within {int(timeout)}s")

    def run(
        self, code: str, *, timeout: float = DEFAULT_STATEMENT_TIMEOUT
    ) -> StatementResult:
        """Run code in the session and return what it printed.

        A statement that needs to return something calls :func:`emit`, which
        prints a tagged JSON line, so printed output and returned values stay
        distinguishable and a result survives whatever else was logged.
        """

        if self.session_url is None:
            raise LivyError("The Livy session has not been started.")

        submitted = _call(
            "POST",
            f"{self.session_url}/statements",
            self.token,
            {"code": code, "kind": "pyspark"},
        )
        statement_url = f"{self.session_url}/statements/{submitted['id']}"

        deadline = time.time() + timeout
        while time.time() < deadline:
            statement = _call("GET", statement_url, self.token, expected=(200,))
            if (statement.get("state") or "").lower() in {
                "available",
                "error",
                "cancelled",
            }:
                return _result(statement)
            time.sleep(self.poll_interval)
        raise LivyError(f"Livy statement did not finish within {int(timeout)}s")

    def close(self, *, timeout: float = DEFAULT_CLOSE_TIMEOUT) -> None:
        """Ask Fabric to end the session, and wait until it has.

        The waiting matters: a capacity limits concurrent Spark sessions, often
        to one, and `DELETE` returns when the request is accepted rather than
        when the slot is released. Closing and immediately opening another would
        queue the new session behind the old one's slot.

        A close that cannot be confirmed is reported rather than raised: the
        session is abandoned either way, and a teardown problem must not mask
        what the caller was doing.
        """

        if self.session_url is None:
            return
        url = self.session_url
        try:
            _call("DELETE", url, self.token, expected=(200, 202, 204, 404))
            self._await_release(url, timeout=timeout)
        finally:
            self.session_url = None

    def _await_release(self, url: str, *, timeout: float) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                state = _call("GET", url, self.token, expected=(200, 404))
            except LivyError:  # gone, or no longer ours to ask about
                return
            if not state:  # 404, so the session is no longer there
                return
            if (state.get("state") or "").lower() in {
                "dead",
                "killed",
                "success",
                "error",
            }:
                return
            time.sleep(self.poll_interval)
        print(
            f"warning: Livy session {url.rsplit('/', 1)[-1]} did not report itself "
            f"released within {int(timeout)}s; a capacity limited to one session "
            "may refuse the next one"
        )


def _result(statement: dict) -> StatementResult:
    output = statement.get("output") or {}
    if output.get("status") and output["status"] != "ok":
        traceback = "\n".join(output.get("traceback") or [])
        raise LivyStatementError(
            f"{output.get('ename')}: {output.get('evalue')}"
            + (f"\n{traceback}" if traceback else ""),
            ename=output.get("ename"),
            evalue=output.get("evalue"),
        )
    text = (output.get("data") or {}).get("text/plain", "")
    return StatementResult(text=text, payload=_payload(text))


def _payload(text: str) -> Any:
    for line in reversed((text or "").splitlines()):
        if line.startswith(RESULT_PREFIX):
            try:
                return json.loads(line[len(RESULT_PREFIX) :])
            except json.JSONDecodeError:
                return None
    return None


def emit_source() -> str:
    """The helper a submitted program uses to return a value."""

    return (
        "import json as _json\n"
        f"def emit(value):\n"
        f"    print({RESULT_PREFIX!r} + _json.dumps(value, default=str))\n"
    )


def _resolve_environment_id(workspace, resolver) -> str:
    """The item id of the workspace's named Environment.

    Resolved by type, so a same-named Lakehouse or Warehouse cannot be picked up
    by mistake. Identity is ``workspace + type + name``.
    """

    from .resources import ENVIRONMENT, find_item

    item = find_item(
        resolver.workspace,
        workspace.environment,
        item_type=ENVIRONMENT,
        client=resolver.client,
    )
    return item.id


def missing_environment(workspace=None) -> str:
    """Explain how to configure the Environment required by a Fabric run."""

    name = getattr(workspace, "workspace", None)
    where = f" for workspace {name!r}" if name else ""
    return (
        f"No Fabric Environment is configured{where}. Running Weaver in Fabric "
        "requires a Fabric Environment with Weaver installed. Pass "
        "--environment <name>, or set environment in workspace configuration."
    )


def environment_bootstrap() -> str:
    """The bootstrap for a body whose Weaver comes from an Environment.

    A plain ``import weaver``: no source copied, no ``sys.path`` change. An
    Environment with no usable Weaver fails naming the fix rather than falling
    back to a shipped copy.
    """

    return (
        "try:\n"
        "    import weaver\n"
        "except ImportError as _exc:\n"
        "    raise ImportError(\n"
        "        'this body imports Weaver, and the attached Fabric Environment '\n"
        "        'has no usable Weaver install; run '\n"
        "        'weaver fabric environment publish <env> --workspace <ws>'\n"
        "    ) from _exc\n"
    )
