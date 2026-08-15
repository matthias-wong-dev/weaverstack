"""Fabric REST transport.

Thin on purpose: a token, a base URL, and enough error translation that a
failure says what failed rather than surfacing a bare HTTP status.
"""

from __future__ import annotations

import json
import time
from typing import Any

from ..errors import WeaverError
from .auth import FABRIC_SCOPE, token_source

#: Generic technical defaults, not environment-specific.
FABRIC_API = "https://api.fabric.microsoft.com/v1"
ONELAKE_DFS = "https://onelake.dfs.fabric.microsoft.com"
DEFAULT_TIMEOUT = 60.0
DEFAULT_OPERATION_TIMEOUT = 900.0
DEFAULT_OPERATION_POLL_INTERVAL = 2.0


#: How many times a request is retried when the transport fails, and how long to
#: wait between attempts. A desktop operation talks to Fabric for as long as the
#: operation takes, and over ten minutes a single connection is likely to be
#: refused outright while the work it is watching carries on unaffected.
CONNECTION_ATTEMPTS = 4
CONNECTION_BACKOFF = 2.0


class FabricError(WeaverError):
    """Raised when a Fabric API call fails."""

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


def never_sent(exc: BaseException) -> bool:
    """Whether a transport failure happened before the request left this machine.

    A connection that was never established carries nothing: Fabric has not seen
    the request, so sending it again cannot create a second item or run a
    statement twice. Once the request is on the wire that is no longer knowable.
    """

    try:
        from urllib3.exceptions import ConnectTimeoutError, NewConnectionError
    except ImportError:  # pragma: no cover - requests vendors urllib3
        return False
    # requests reports the cause several ways: chained, as the argument it was
    # constructed with, or as a MaxRetryError's `reason`. All three are walked,
    # because which one appears depends on where urllib3 gave up.
    frontier = [exc]
    seen: set[int] = set()
    while frontier:
        current = frontier.pop()
        if current is None or id(current) in seen:
            continue
        seen.add(id(current))
        if isinstance(current, (NewConnectionError, ConnectTimeoutError)):
            return True
        frontier.extend([current.__cause__, current.__context__])
        frontier.extend(arg for arg in current.args if isinstance(arg, BaseException))
        reason = getattr(current, "reason", None)
        if isinstance(reason, BaseException):
            frontier.append(reason)
    return False


def send(method: str, url: str, **kwargs):
    """One HTTP request, retried while the failure is safe to repeat.

    A read can always be repeated. Anything else only when the connection was
    never established, since Fabric cannot have acted on a request it never
    received. The last failure is raised as it came, for the caller to translate
    into its own error.
    """

    import requests

    for attempt in range(1, CONNECTION_ATTEMPTS + 1):
        try:
            return requests.request(method, url, **kwargs)
        except requests.exceptions.RequestException as exc:
            repeatable = method == "GET" or never_sent(exc)
            if not repeatable or attempt == CONNECTION_ATTEMPTS:
                raise
            time.sleep(CONNECTION_BACKOFF * attempt)


class FabricClient:
    """Authenticated access to the Fabric REST API."""

    def __init__(
        self,
        *,
        api_base_url: str = FABRIC_API,
        token: str | None = None,
        timeout: float = DEFAULT_TIMEOUT,
    ) -> None:
        self.api_base_url = api_base_url.rstrip("/")
        self.timeout = timeout
        self._token_source = token_source(token, scope=FABRIC_SCOPE)

    @property
    def token(self) -> str:
        """A currently-valid bearer, renewed when it is close to expiring.

        Read per request rather than cached: a client outlives its token, and a
        stale one surfaces as ``401`` in whatever call happens to be next.
        """

        return self._token_source()

    def request(
        self,
        method: str,
        path: str,
        *,
        payload: Any = None,
        expected: tuple[int, ...] = (200, 201, 202),
    ):
        import requests

        url = path if path.startswith("http") else f"{self.api_base_url}/{path.lstrip('/')}"
        try:
            response = send(
                method,
                url,
                headers={
                    "Authorization": f"Bearer {self.token}",
                    "Content-Type": "application/json",
                },
                data=json.dumps(payload) if payload is not None else None,
                timeout=self.timeout,
            )
        except requests.exceptions.RequestException as exc:
            raise FabricError(f"{method} {url} could not be reached: {exc}") from exc
        if response.status_code not in expected:
            raise FabricError(
                f"{method} {url} returned {response.status_code}: "
                f"{response.text.strip()[:400] or 'no body'}",
                status_code=response.status_code,
            )
        return response

    def get_json(self, path: str) -> dict:
        response = self.request("GET", path, expected=(200,))
        return response.json() if response.content else {}

    def paged(self, path: str, *, key: str = "value") -> list[dict]:
        """Every item across a paged listing."""

        items: list[dict] = []
        next_path: str | None = path
        while next_path:
            payload = self.get_json(next_path)
            items.extend(payload.get(key, []))
            next_path = payload.get("continuationUri")
        return items

    def wait_for_operation(
        self,
        response,
        *,
        timeout: float = DEFAULT_OPERATION_TIMEOUT,
        poll_interval: float = DEFAULT_OPERATION_POLL_INTERVAL,
    ) -> dict:
        """Wait for a Fabric long-running-operation response to settle."""

        if response.status_code != 202:
            return response.json() if response.content else {}

        location = response.headers.get("Location")
        operation_id = response.headers.get("x-ms-operation-id")
        if not location and operation_id:
            location = f"operations/{operation_id}"
        if not location:
            raise FabricError(
                "Fabric accepted a long-running operation without a polling location"
            )

        deadline = time.monotonic() + timeout
        current = response
        while time.monotonic() < deadline:
            retry_after = current.headers.get("Retry-After")
            try:
                delay = float(retry_after) if retry_after is not None else poll_interval
            except (TypeError, ValueError):
                delay = poll_interval
            time.sleep(max(0.0, delay))
            current = self.request("GET", location, expected=(200,))
            body = current.json() if current.content else {}
            status = str(body.get("status") or "").casefold()
            if status == "succeeded":
                return body
            if status in {"failed", "cancelled", "canceled"}:
                error = body.get("error") or {}
                message = error.get("message") if isinstance(error, dict) else None
                raise FabricError(
                    f"Fabric operation {operation_id or location} {status}"
                    + (f": {message}" if message else "")
                )
            location = current.headers.get("Location") or location

        raise FabricError(
            f"Fabric operation {operation_id or location} did not finish within "
            f"{int(timeout)}s"
        )
