"""Running Weaver code inside a Fabric Spark session.

This is the third execution position — not Weaver reaching into a workspace over
HTTP, but Weaver *running there*. It is the position the product claim rests on,
and the only one that proves a notebook user could do the same thing.

A session is expensive to start and cheap to reuse, so callers should hold one
open across a batch of work rather than paying for it per statement.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any

from ..errors import WeaverError
from .auth import FABRIC_SCOPE, get_token
from .client import FABRIC_API

DEFAULT_LIVY_API_VERSION = "2023-12-01"
DEFAULT_POLL_INTERVAL = 3.0
DEFAULT_SESSION_TIMEOUT = 600.0
DEFAULT_STATEMENT_TIMEOUT = 900.0

#: Wrapped around returned values so a result can be told from printed output.
RESULT_PREFIX = "__weaver_result__"


class LivyError(WeaverError):
    """Raised when a Livy session or statement fails."""


@dataclass(frozen=True)
class StatementResult:
    """What one submitted statement produced."""

    text: str
    payload: Any = None

    @property
    def returned(self) -> bool:
        return self.payload is not None


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


def _call(method: str, url: str, token: str, payload: Any = None,
          expected: tuple[int, ...] = (200, 201, 202)) -> dict:
    import requests

    response = requests.request(
        method,
        url,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        data=json.dumps(payload) if payload is not None else None,
        timeout=120,
    )
    if response.status_code not in expected:
        raise LivyError(
            f"{method} {url} returned {response.status_code}: "
            f"{response.text.strip()[:400] or 'no body'}"
        )
    return response.json() if response.content else {}


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
        self.token = token or get_token(FABRIC_SCOPE)
        self.base = sessions_url(workspace_id, lakehouse_id, api_base_url=api_base_url)
        self.environment_id = environment_id
        self.poll_interval = poll_interval
        self.bootstrap = bootstrap
        self.session_url: str | None = None

    @classmethod
    def for_host(cls, host, *, resolver=None, **kwargs) -> "LivySession":
        """A session against a host's Weaver Lakehouse, ready to ``import weaver``.

        The session is created against the Weaver Lakehouse, so that is its
        default and the shipped package is reachable. Destination Lakehouses
        need no attachment — they are addressed by explicit ``abfss`` roots.

        The bootstrap runs once when the session starts, so callers submit their
        work and nothing else.
        """

        from ..targets import ItemRef
        from .resolution import FabricResolver
        from .runtime import abfss_package_root, bootstrap_source

        resolver = resolver or FabricResolver(host)
        home = resolver.resolve(ItemRef(host.weaver_lakehouse))
        return cls(
            resolver.workspace.id,
            home.id,
            environment_id=kwargs.pop("environment_id", None),
            bootstrap=bootstrap_source(abfss_package_root(host, resolver)) + emit_source(),
            **kwargs,
        )

    def __enter__(self) -> "LivySession":
        self.start()
        return self

    def __exit__(self, *exc) -> bool:
        self.close()
        return False

    def start(self, *, timeout: float = DEFAULT_SESSION_TIMEOUT) -> None:
        payload: dict[str, Any] = {"name": "weaver"}
        if self.environment_id:
            payload["environmentId"] = self.environment_id
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

    def run(self, code: str, *, timeout: float = DEFAULT_STATEMENT_TIMEOUT) -> StatementResult:
        """Run code in the session and return what it printed.

        A statement that wants to return something calls :func:`emit`, which
        prints a tagged JSON line — printed output and returned values are then
        distinguishable, and a result survives whatever else was logged.
        """

        if self.session_url is None:
            raise LivyError("the Livy session has not been started")

        submitted = _call(
            "POST", f"{self.session_url}/statements", self.token,
            {"code": code, "kind": "pyspark"},
        )
        statement_url = f"{self.session_url}/statements/{submitted['id']}"

        deadline = time.time() + timeout
        while time.time() < deadline:
            statement = _call("GET", statement_url, self.token, expected=(200,))
            if (statement.get("state") or "").lower() in {"available", "error", "cancelled"}:
                return _result(statement)
            time.sleep(self.poll_interval)
        raise LivyError(f"Livy statement did not finish within {int(timeout)}s")

    def close(self) -> None:
        if self.session_url is None:
            return
        try:
            _call("DELETE", self.session_url, self.token, expected=(200, 202, 204, 404))
        finally:
            self.session_url = None


def _result(statement: dict) -> StatementResult:
    output = statement.get("output") or {}
    if output.get("status") and output["status"] != "ok":
        traceback = "\n".join(output.get("traceback") or [])
        raise LivyError(
            f"{output.get('ename')}: {output.get('evalue')}"
            + (f"\n{traceback}" if traceback else "")
        )
    text = (output.get("data") or {}).get("text/plain", "")
    return StatementResult(text=text, payload=_payload(text))


def _payload(text: str) -> Any:
    for line in reversed((text or "").splitlines()):
        if line.startswith(RESULT_PREFIX):
            try:
                return json.loads(line[len(RESULT_PREFIX):])
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
