"""Fabric REST transport.

Thin on purpose: a token, a base URL, and enough error translation that a
failure says what failed rather than surfacing a bare HTTP status.
"""

from __future__ import annotations

import json
import time
from contextlib import nullcontext
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

#: Statuses where Fabric answered "not now". It refused the request rather than
#: acting on it, so repeating it is safe whatever the method. A 500 is not here:
#: it can mean the work was done and the reply was not.
TRANSIENT_STATUSES = frozenset({429, 502, 503, 504})


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


def retry_delay(response, attempt: int) -> float:
    """How long to wait: what ``Retry-After`` asked for, or a widening gap."""

    asked = response.headers.get("Retry-After")
    if asked:
        try:
            return max(0.0, float(asked))
        except ValueError:
            pass
    return CONNECTION_BACKOFF * attempt


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
        telemetry=None,
    ) -> None:
        self.api_base_url = api_base_url.rstrip("/")
        self.timeout = timeout
        self.telemetry = telemetry
        self._token_source = token_source(token, scope=FABRIC_SCOPE)

    def authenticate(self) -> dict:
        """Acquire the REST token and return non-secret authentication metadata."""

        self._token_source()
        return dict(
            getattr(self._token_source, "diagnostic", {"path": "Session identity"})
        )

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
        """One Fabric call, repeated while Fabric answers "not now"."""

        import requests

        url = (
            path
            if path.startswith("http")
            else f"{self.api_base_url}/{path.lstrip('/')}"
        )
        observation = (
            self.telemetry.external("rest", method.lower(), detail=path)
            if self.telemetry is not None
            else nullcontext()
        )
        with observation:
            for attempt in range(1, CONNECTION_ATTEMPTS + 1):
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
                    raise FabricError(
                        f"{method} {url} could not be reached: {exc}"
                    ) from exc
                if response.status_code in expected:
                    return response
                if (
                    response.status_code in TRANSIENT_STATUSES
                    and attempt < CONNECTION_ATTEMPTS
                ):
                    time.sleep(retry_delay(response, attempt))
                    continue
                raise FabricError(
                    f"{method} {url} returned {response.status_code}: "
                    f"{response.text.strip()[:400] or 'no body'}",
                    status_code=response.status_code,
                )

    def get_json(self, path: str) -> dict:
        response = self.request("GET", path, expected=(200,))
        return response.json() if response.content else {}

    def paged(
        self,
        path: str,
        *,
        key: str = "value",
        not_found_empty: bool = False,
    ) -> list[dict]:
        """Every item across a listing, optionally accepting an absent first page."""

        items: list[dict] = []
        next_path: str | None = path
        first = True
        while next_path:
            try:
                payload = self.get_json(next_path)
            except FabricError as exc:
                if first and not_found_empty and exc.status_code == 404:
                    return []
                raise
            items.extend(payload.get(key, []))
            next_path = payload.get("continuationUri")
            first = False
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
