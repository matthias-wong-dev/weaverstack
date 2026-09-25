"""Run Weaver work in reusable Fabric Spark sessions through Livy."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any, Mapping

from ..errors import WeaverError
from .auth import FABRIC_SCOPE, token_source
from .client import (
    CONNECTION_ATTEMPTS,
    FABRIC_API,
    TRANSIENT_STATUSES,
    _response_message,
    retry_delay,
    send,
)

DEFAULT_LIVY_API_VERSION = "2023-12-01"
DEFAULT_POLL_INTERVAL = 3.0
DEFAULT_SESSION_TIMEOUT = 600.0
DEFAULT_STATEMENT_TIMEOUT = 900.0
#: A close waits for Fabric to release the session's capacity slot.
DEFAULT_CLOSE_TIMEOUT = 120.0

#: Wrapped around returned values so a result can be told from printed output.
RESULT_PREFIX = "__weaver_result__"


class LivyError(WeaverError):
    """A Livy session could not start or has died."""

    executor = "Livy"


class LivyStatementError(LivyError):
    """A statement failed, but its Spark session remains usable."""

    def __init__(self, message: str, *, ename=None, evalue=None) -> None:
        super().__init__(message)
        self.ename = ename
        self.evalue = evalue


@dataclass(frozen=True)
class StatementResult:
    text: str
    payload: Any = None

    @property
    def returned(self) -> bool:
        return self.payload is not None


@dataclass(frozen=True)
class LivySessionInfo:
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
    lakehouse_id: str
    lakehouse_name: str
    session: LivySessionInfo

    @property
    def active(self) -> bool:
        return self.session.active


def _spark_home(workspace):
    """Choose a stable attachment when the caller names no Lakehouse."""

    from ..errors import CommandError
    from ..targets import ItemRef

    configured = getattr(workspace, "configured_lakehouses", ()) or ()
    if not configured:
        raise CommandError(
            "starting a Spark session needs a Lakehouse to attach to, and none "
            "was named. Give the operation a Lakehouse target, or add a Lakehouse "
            "entry to `targets` in workspace configuration. Warehouse-only work "
            "needs no Spark session."
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
    *,
    retry_transient: bool = True,
) -> dict:
    import requests

    # Long polls encounter the same transient capacity responses as other Fabric
    # REST calls.
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
        if (
            retry_transient
            and response.status_code in TRANSIENT_STATUSES
            and attempt < CONNECTION_ATTEMPTS
        ):
            time.sleep(retry_delay(response, attempt))
            continue
        raise LivyError(
            f"{method} {url} returned {response.status_code}: "
            f"{_response_message(response)}"
        )
    raise LivyError(f"{method} {url} did not settle")


class LivySession:
    """A Fabric Spark session held open for a batch of statements."""

    def __init__(
        self,
        workspace_id: str,
        lakehouse_id: str,
        *,
        token: str | None = None,
        environment_id: str | None = None,
        environment_reference: str | None = None,
        workspace_name: str | None = None,
        api_base_url: str = FABRIC_API,
        poll_interval: float = DEFAULT_POLL_INTERVAL,
        bootstrap: str | None = None,
        weaver_bootstrap: str | None = None,
    ) -> None:
        self._token_source = token_source(token, scope=FABRIC_SCOPE)
        self.base = sessions_url(workspace_id, lakehouse_id, api_base_url=api_base_url)
        self.environment_id = environment_id
        self.environment_reference = environment_reference
        self.workspace_name = workspace_name
        self.poll_interval = poll_interval
        self.bootstrap = bootstrap
        self.weaver_bootstrap = weaver_bootstrap
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
        """Create a session attached to a workspace Lakehouse.

        The attachment hosts the session; fully qualified statements still name
        their destinations. An Environment is attached when configured but is
        required only by work that imports Weaver.
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
        environment_reference = kwargs.pop("environment_reference", None)
        if environment_reference is None and getattr(workspace, "environment", None):
            environment_reference = str(workspace.environment)

        # Custom start-up code must provide `emit` for returned values.
        kwargs.setdefault("bootstrap", emit_source())
        return cls(
            resolver.workspace.id,
            home.id,
            environment_id=environment_id,
            environment_reference=environment_reference,
            workspace_name=workspace.workspace,
            **kwargs,
        )

    def ensure_weaver(self) -> None:
        """Verify once that the session can import Weaver.

        Spark SQL does not require an Environment. Imported Weaver does, unless
        ``weaver_bootstrap`` supplies the package, as the pytest harness does.
        """

        if self._weaver_asserted:
            return
        if self.weaver_bootstrap is not None:
            self.run(self.weaver_bootstrap)
            self._weaver_asserted = True
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
            # Fabric attaches published Environment libraries through Spark
            # configuration, not a top-level Livy field.
            payload["conf"] = {
                "spark.fabric.environmentDetails": json.dumps(
                    {"id": self.environment_id}
                )
            }
        try:
            created = _call("POST", self.base, self.token, payload)
            session_id = created.get("id") or created.get("livyId")
            if session_id is None:
                raise LivyError(f"Livy did not return a session id: {created}")
            self.session_url = f"{self.base}/{session_id}"
            self._await("idle", timeout=timeout)
        except LivyError as exc:
            if self.environment_reference and self.workspace_name:
                raise LivyError(
                    f"Spark session in workspace {self.workspace_name!r} could not "
                    f"attach Environment {self.environment_reference!r}: {exc}"
                ) from exc
            raise
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
                raise LivyError(
                    f"Livy session entered state {current!r}"
                    + _session_state_detail(state)
                )
            time.sleep(self.poll_interval)
        raise LivyError(f"Livy session did not reach {wanted!r} within {int(timeout)}s")

    def run(
        self,
        code: str,
        *,
        timeout: float = DEFAULT_STATEMENT_TIMEOUT,
        retry_submission: bool = True,
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
            retry_transient=retry_submission,
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
        """End the session and wait for Fabric to release its capacity slot.

        ``DELETE`` returns before the slot is released. An unconfirmed close is
        reported rather than raised so teardown does not mask the caller's work.
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
        session_id = url.rsplit("/", 1)[-1]
        while time.time() < deadline:
            try:
                state = _call("GET", url, self.token, expected=(200, 404))
            except LivyError:  # Gone, or no longer ours to query.
                return
            if not state:  # 404: the session no longer exists.
                return
            if (state.get("state") or "").lower() in {
                "dead",
                "killed",
                "success",
                "error",
            }:
                # Livy reaches a terminal state before Fabric's scheduler frees
                # the capacity slot. The Lakehouse collection is the authority
                # for that second transition.
                try:
                    collection = _call("GET", self.base, self.token, expected=(200,))
                except LivyError:
                    time.sleep(self.poll_interval)
                    continue
                matching = next(
                    (
                        LivySessionInfo.from_mapping(item)
                        for item in collection.get("items", ())
                        if str(item.get("id") or item.get("livyId") or "") == session_id
                    ),
                    None,
                )
                if matching is None or not matching.active:
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


def _session_state_detail(state: Mapping[str, Any]) -> str:
    """Distinguish capacity and Environment failures that both end as ``dead``."""

    info = state.get("fabricSessionStateInfo") or {}
    message = str(info.get("errorMessage") or "").strip()
    return f": {message}" if message else ""


def emit_source() -> str:
    return (
        "import json as _json\n"
        f"def emit(value):\n"
        f"    print({RESULT_PREFIX!r} + _json.dumps(value, default=str))\n"
    )


def _resolve_environment_id(workspace, resolver) -> str:
    """Resolve the workspace's Environment without changing the Livy workspace.

    A qualified Environment may belong to another workspace.
    """

    from ..workspaces import EnvironmentRef
    from .resources import ENVIRONMENT, find_item, find_workspace

    reference = EnvironmentRef.parse(workspace.environment)
    owner_name = reference.owner(workspace.workspace)
    owner = (
        resolver.workspace
        if owner_name == workspace.workspace
        else find_workspace(owner_name, client=resolver.client)
    )

    item = find_item(
        owner,
        reference.name,
        item_type=ENVIRONMENT,
        client=resolver.client,
    )
    return item.id


def missing_environment(workspace=None) -> str:
    name = getattr(workspace, "workspace", None)
    where = f" for workspace {name!r}" if name else ""
    return (
        f"No Fabric Environment is configured{where}. Running Weaver in Fabric "
        "requires a Fabric Environment with Weaver installed. Pass "
        "--environment <Environment | Workspace/Environment>, or set environment "
        "in workspace configuration."
    )


def environment_bootstrap() -> str:
    """Import Weaver from the Environment without a source-copy fallback."""

    return (
        "try:\n"
        "    import weaver\n"
        "except ImportError as _exc:\n"
        "    raise ImportError(\n"
        "        'this body imports Weaver, and the attached Fabric Environment '\n"
        "        'has no usable Weaver install; run '\n"
        "        'weaver fabric environment publish <Environment | Workspace/Environment>'\n"
        "    ) from _exc\n"
    )
