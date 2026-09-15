"""Shared Session resources with explicit, bounded recovery.

Concurrent callers share one acquisition. A failed operation does not mark its
resource failed; callers explicitly reacquire resources after acquisition or
transport failures.
"""

from __future__ import annotations

import threading
from concurrent.futures import Executor, Future
from enum import Enum
from typing import Callable, Generic, TypeVar

from ..errors import WeaverError
from .telemetry import SessionTelemetry, TelemetryContext

T = TypeVar("T")


class ResourceState(str, Enum):
    NOT_STARTED = "not_started"
    STARTING = "starting"
    READY = "ready"
    FAILED = "failed"
    CLOSED = "closed"


class ResourceError(WeaverError):
    pass


class Resource(Generic[T]):
    """A lazily acquired, shared resource owned by a Session.

    ``acquire`` is called at most once per attempt and never concurrently.
    ``release`` is called only for a value this resource actually acquired, so a
    Session never closes what it was given.
    """

    def __init__(
        self,
        name: str,
        acquire: Callable[[], T],
        *,
        executor: Executor,
        release: Callable[[T], None] | None = None,
        telemetry: SessionTelemetry | None = None,
        telemetry_resource: str | None = None,
        max_attempts: int = 2,
        close_timeout: float = 120.0,
    ) -> None:
        self.name = name
        self._acquire = acquire
        self._release = release
        self._executor = executor
        self._telemetry = telemetry
        self._telemetry_resource = telemetry_resource
        self._max_attempts = max_attempts
        self._close_timeout = close_timeout
        self._lock = threading.Lock()
        self._state = ResourceState.NOT_STARTED
        self._future: Future | None = None
        self._attempts = 0
        self._error: BaseException | None = None

    # --- state --------------------------------------------------------------

    @property
    def state(self) -> ResourceState:
        with self._lock:
            return self._state

    @property
    def ready(self) -> bool:
        return self.state is ResourceState.READY

    @property
    def acquired(self) -> bool:
        return self.state in {ResourceState.READY, ResourceState.STARTING}

    @property
    def attempts(self) -> int:
        with self._lock:
            return self._attempts

    # --- acquisition --------------------------------------------------------

    def start(self, *, speculative: bool = False) -> Future:
        """Begin acquiring without waiting, returning the one acquisition.

        Concurrent callers receive the same in-flight acquisition.

        A failed ``speculative`` acquisition leaves the resource unstarted. The
        first required acquisition retries and reports any failure.
        """

        with self._lock:
            if self._state is ResourceState.CLOSED:
                raise ResourceError(f"The {self.name} resource is closed.")
            if self._state is ResourceState.FAILED:
                raise ResourceError(
                    f"The {self.name} resource failed and has not been "
                    f"reacquired: {self._error}"
                ) from self._error
            if self._future is None:
                self._attempts += 1
                self._state = ResourceState.STARTING
                context = (
                    self._telemetry.capture_context()
                    if self._telemetry is not None
                    else None
                )
                self._future = self._executor.submit(
                    self._acquire_once, context, speculative=speculative
                )
            return self._future

    def get(self, *, timeout: float | None = None) -> T:
        return self.start().result(timeout)

    def _acquire_once(
        self, context: TelemetryContext | None, *, speculative: bool = False
    ) -> T:
        try:
            if self._telemetry is not None:
                with self._telemetry.use_context(context or TelemetryContext()):
                    if self._telemetry_resource is None:
                        with self._telemetry.timing(f"{self.name}.acquire"):
                            value = self._acquire()
                    else:
                        with self._telemetry.external(
                            self._telemetry_resource, "acquire", detail=self.name
                        ):
                            value = self._acquire()
            else:
                value = self._acquire()
        except BaseException as exc:
            with self._lock:
                if speculative and self._state is not ResourceState.CLOSED:
                    self._state = ResourceState.NOT_STARTED
                    self._future = None
                    self._attempts -= 1
                else:
                    self._state = ResourceState.FAILED
                self._error = exc
            raise
        with self._lock:
            # A concurrent close owns the acquired value and releases it.
            if self._state is ResourceState.CLOSED:
                self._release_value(value)
                raise ResourceError(
                    f"The {self.name} resource was closed while starting."
                )
            self._state = ResourceState.READY
        return value

    def fail(self, error: BaseException | None = None) -> None:
        """Mark an acquisition or transport failure for later reacquisition.

        A failed SQL statement does not imply a failed connection.
        """

        with self._lock:
            if self._state is ResourceState.CLOSED:
                return
            self._state = ResourceState.FAILED
            self._error = error
            future, self._future = self._future, None
        if self._telemetry is not None:
            self._telemetry.count(f"{self.name}.failed")
        self._close_future(future)

    def reacquire(self) -> None:
        """Permit another acquisition within the configured attempt limit.

        Bounded: a resource that has exhausted its attempts stays failed and
        says so.
        """

        with self._lock:
            if self._state is ResourceState.CLOSED:
                raise ResourceError(f"The {self.name} resource is closed.")
            if self._state is not ResourceState.FAILED:
                return
            if self._attempts >= self._max_attempts:
                raise ResourceError(
                    f"The {self.name} resource failed {self._attempts} times and "
                    f"cannot be acquired again: {self._error}"
                ) from self._error
            self._state = ResourceState.NOT_STARTED
            self._future = None
            self._error = None

    # --- teardown -----------------------------------------------------------

    def close(self) -> None:
        """Wait for an in-flight acquisition, then release its value.

        A close arriving mid-acquisition waits for it. A small capacity permits
        one Spark session, and an abandoned starting one holds that slot until
        Fabric reaps it, so the next run queues behind a session nobody is
        using.

        The wait is bounded by ``close_timeout``.
        """

        with self._lock:
            if self._state is ResourceState.CLOSED:
                return
            previous, self._state = self._state, ResourceState.CLOSED
            future, self._future = self._future, None
        if previous in {ResourceState.STARTING, ResourceState.READY}:
            self._close_future(future)

    def _close_future(self, future: Future | None) -> None:
        if future is None:
            return
        try:
            value = future.result(self._close_timeout)
        except TimeoutError:
            # Fabric will eventually reap an acquisition that outlives the
            # bounded close wait.
            if self._telemetry is not None:
                self._telemetry.count(f"{self.name}.abandoned")
            return
        except BaseException:
            return  # acquisition failed; there is no value to release
        self._release_value(value)

    def _release_value(self, value: T) -> None:
        if self._release is None:
            return
        try:
            self._release(value)
        except Exception:  # noqa: BLE001 - teardown must not mask the real work
            if self._telemetry is not None:
                self._telemetry.count(f"{self.name}.close_failed")


__all__ = ["Resource", "ResourceError", "ResourceState"]
