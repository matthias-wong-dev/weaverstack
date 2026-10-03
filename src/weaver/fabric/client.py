"""Fabric REST transport with authentication and error translation."""

from __future__ import annotations

import json
import time
from contextlib import nullcontext
from dataclasses import dataclass
from typing import Any

from ..errors import OutcomeUnknown, WeaverError, reported_message
from .auth import FABRIC_SCOPE, token_source

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

#: Methods that change nothing, so any failure of one is safe to repeat.
READ_METHODS = frozenset({"GET", "HEAD"})


class FabricError(WeaverError):
    executor = "REST"

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class FabricOutcomeUnknown(FabricError, OutcomeUnknown):
    """The request may have been acted on, and its response was lost."""


def outcome_unknown(method: str, status_code: int) -> bool:
    """Whether a mutation's failed reply leaves open that Fabric acted on it."""

    return method not in READ_METHODS and status_code == 500


def _response_message(response) -> str:
    try:
        payload = response.json() if response.content else None
    except (TypeError, ValueError):
        payload = None
    return reported_message(payload) or response.text.strip()[:400] or "no body"


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
    asked = response.headers.get("Retry-After")
    if asked:
        try:
            return max(0.0, float(asked))
        except ValueError:
            pass
    return CONNECTION_BACKOFF * attempt


def _remaining(deadline: float) -> float:
    from requests.exceptions import Timeout

    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise Timeout("REST operation deadline expired")
    return remaining


def _backoff(delay: float, deadline: float | None) -> None:
    time.sleep(delay if deadline is None else min(delay, _remaining(deadline)))


def send(method: str, url: str, *, deadline: float | None = None, **kwargs):
    """One HTTP request, retried while the failure is safe to repeat.

    A read can always be repeated. Anything else only when the connection was
    never established, since Fabric cannot have acted on a request it never
    received. The last failure is raised as it came, for the caller to translate
    into its own error.
    """

    import requests

    for attempt in range(1, CONNECTION_ATTEMPTS + 1):
        options = kwargs
        if deadline is not None:
            remaining = _remaining(deadline)
            timeout = kwargs.get("timeout")
            options = {
                **kwargs,
                "timeout": remaining if timeout is None else min(timeout, remaining),
            }
        try:
            return requests.request(method, url, **options)
        except requests.exceptions.RequestException as exc:
            repeatable = method in READ_METHODS or never_sent(exc)
            if not repeatable or attempt == CONNECTION_ATTEMPTS:
                raise
            _backoff(CONNECTION_BACKOFF * attempt, deadline)


def send_until_answered(
    method: str,
    url: str,
    *,
    expected: tuple[int, ...],
    retry_transient: bool = True,
    deadline: float | None = None,
    **kwargs,
):
    """:func:`send`, also repeating a transient refusal while attempts remain.

    Returns the last response whatever its status, for the caller to judge.
    """

    for attempt in range(1, CONNECTION_ATTEMPTS + 1):
        response = send(method, url, deadline=deadline, **kwargs)
        if (
            retry_transient
            and response.status_code not in expected
            and response.status_code in TRANSIENT_STATUSES
            and attempt < CONNECTION_ATTEMPTS
        ):
            _backoff(retry_delay(response, attempt), deadline)
            continue
        return response


class FabricClient:
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
        """Return a current bearer token; a client can outlive one token."""

        return self._token_source()

    def request(
        self,
        method: str,
        path: str,
        *,
        payload: Any = None,
        expected: tuple[int, ...] = (200, 201, 202),
        retry_transient: bool = True,
        timeout: float | None = None,
        deadline: float | None = None,
    ):
        """Issue one REST request with bounded transport retries."""

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
            try:
                response = send_until_answered(
                    method,
                    url,
                    expected=expected,
                    retry_transient=retry_transient,
                    deadline=deadline,
                    headers={
                        "Authorization": f"Bearer {self.token}",
                        "Content-Type": "application/json",
                    },
                    data=json.dumps(payload) if payload is not None else None,
                    timeout=self.timeout if timeout is None else timeout,
                )
            except requests.exceptions.RequestException as exc:
                error = FabricError if never_sent(exc) else FabricOutcomeUnknown
                raise error(f"{method} {url} could not be reached: {exc}") from exc
            if response.status_code in expected:
                return response
            error = (
                FabricOutcomeUnknown
                if outcome_unknown(method, response.status_code)
                else FabricError
            )
            raise error(
                f"{method} {url} returned {response.status_code}: "
                f"{_response_message(response)}",
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
        if response.status_code != 202:
            return response.json() if response.content else {}

        operation = accepted_operation(response, poll_interval=poll_interval)
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            time.sleep(operation.retry_after)
            operation = self.poll_operation(operation, poll_interval=poll_interval)
            if operation.done:
                return operation.body
        raise FabricError(
            f"Fabric operation {operation.name} did not finish within {int(timeout)}s"
        )

    def poll_operation(
        self,
        operation: "Operation",
        *,
        poll_interval: float = DEFAULT_OPERATION_POLL_INTERVAL,
    ) -> "Operation":
        """Observe a long-running operation once; raise if it failed."""

        current = self.request("GET", operation.location, expected=(200,))
        body = current.json() if current.content else {}
        status = str(body.get("status") or "").casefold()
        if status in {"failed", "cancelled", "canceled"}:
            error = body.get("error") or {}
            message = error.get("message") if isinstance(error, dict) else None
            raise FabricError(
                f"Fabric operation {operation.name} {status}"
                + (f": {message}" if message else "")
            )
        return Operation(
            location=current.headers.get("Location") or operation.location,
            operation_id=operation.operation_id,
            retry_after=_retry_after(current, poll_interval),
            done=status == "succeeded",
            body=body,
        )


@dataclass(frozen=True)
class Operation:
    """A Fabric long-running operation and its latest observation."""

    location: str
    operation_id: str | None = None
    retry_after: float = DEFAULT_OPERATION_POLL_INTERVAL
    done: bool = False
    body: Any = None

    @property
    def name(self) -> str:
        return self.operation_id or self.location


def accepted_operation(
    response, *, poll_interval: float = DEFAULT_OPERATION_POLL_INTERVAL
) -> Operation:
    """The operation behind a ``202 Accepted`` response."""

    location = response.headers.get("Location")
    operation_id = response.headers.get("x-ms-operation-id")
    if not location and operation_id:
        location = f"operations/{operation_id}"
    if not location:
        raise FabricError(
            "Fabric accepted a long-running operation without a polling location"
        )
    return Operation(
        location=location,
        operation_id=operation_id,
        retry_after=_retry_after(response, poll_interval),
    )


def _retry_after(response, default: float) -> float:
    try:
        return max(0.0, float(response.headers.get("Retry-After")))
    except (TypeError, ValueError):
        return default
