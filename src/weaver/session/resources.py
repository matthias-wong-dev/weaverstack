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
from .telemetry import SessionTelemetry

T = TypeVar("T")


class ResourceState(str, Enum):
    """Where one resource is in its life."""

    NOT_STARTED = "not_started"
    STARTING = "starting"
    READY = "ready"
    FAILED = "failed"
    CLOSED = "closed"


class ResourceError(WeaverError):
    """A resource could not be acquired, or is no longer usable."""


class Resource(Generic[T]):
    """A lazily acquired, shared, closable thing owned by a Session.

    ``acquire`` is called at most once per attempt and never concurrently.
    ``release`` is called only for a value this resource actually acquired —
    a Session never closes what it was given.
    """

    def __init__(
        self,
        name: str,
        acquire: Callable[[], T],
        *,
        executor: Executor,
        release: Callable[[T], None] | None = None,
        telemetry: SessionTelemetry | None = None,
        max_attempts: int = 2,
        close_timeout: float = 120.0,
    ) -> None:
        self.name = name
        self._acquire = acquire
        self._release = release
        self._executor = executor
        self._telemetry = telemetry
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
        """Whether this Session actually holds the thing — so must close it."""

        return self.state in {ResourceState.READY, ResourceState.STARTING}

    @property
    def attempts(self) -> int:
        with self._lock:
            return self._attempts

    # --- acquisition --------------------------------------------------------

    def start(self, *, speculative: bool = False) -> Future:
        """Begin acquiring without waiting, returning the one acquisition.

        Called again while an acquisition is in flight, this returns that
        acquisition rather than starting another, so a background warm-up and a
        foreground caller share one Livy session.

        ``speculative`` marks an acquisition nobody asked for. One that fails
        leaves the resource unstarted rather than failed, so the first caller
        that needs it tries again and reports in its own terms.
        """

        with self._lock:
            if self._state is ResourceState.CLOSED:
                raise ResourceError(f"the {self.name} resource is closed")
            if self._state is ResourceState.FAILED:
                raise ResourceError(
                    f"the {self.name} resource failed and has not been "
                    f"reacquired: {self._error}"
                ) from self._error
            if self._future is None:
                self._attempts += 1
                self._state = ResourceState.STARTING
                self._future = self._executor.submit(
                    self._acquire_once, speculative=speculative
                )
            return self._future

    def get(self, *, timeout: float | None = None) -> T:
        """The resource, waiting for whichever acquisition is under way."""

        return self.start().result(timeout)

    def _acquire_once(self, *, speculative: bool = False) -> T:
        try:
            if self._telemetry is not None:
                with self._telemetry.timing(f"{self.name}.acquire"):
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
            # A close that landed while this was still starting wins: the value
            # is released rather than handed to a caller of a closed Session.
            if self._state is ResourceState.CLOSED:
                self._release_value(value)
                raise ResourceError(f"the {self.name} resource was closed while starting")
            self._state = ResourceState.READY
        return value

    def fail(self, error: BaseException | None = None) -> None:
        """Declare the resource dead — the caller has seen it is unusable.

        For a resource fault, not a statement fault: a failed SQL statement
        leaves a healthy connection.
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
        """Permit one further acquisition of a failed resource.

        Bounded: a resource that has exhausted its attempts stays failed and
        says so.
        """

        with self._lock:
            if self._state is ResourceState.CLOSED:
                raise ResourceError(f"the {self.name} resource is closed")
            if self._state is not ResourceState.FAILED:
                return
            if self._attempts >= self._max_attempts:
                raise ResourceError(
                    f"the {self.name} resource failed {self._attempts} times and "
                    f"will not be acquired again: {self._error}"
                ) from self._error
            self._state = ResourceState.NOT_STARTED
            self._future = None
            self._error = None

    # --- teardown -----------------------------------------------------------

    def close(self) -> None:
        """Release what this resource acquired, if it acquired anything.

        A close arriving mid-acquisition waits for it. A small capacity permits
        one Spark session, and an abandoned starting one holds that slot until
        Fabric reaps it, so the next run queues behind a session nobody is
        using.

        The wait is bounded: a resource that never finishes starting is
        abandoned rather than holding the process open.
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
            # Still starting, and out of patience. Abandoned deliberately: an
            # exit that cannot complete is worse than a resource the platform
            # will reap on its own.
            if self._telemetry is not None:
                self._telemetry.count(f"{self.name}.abandoned")
            return
        except BaseException:
            return  # never acquired, so there is nothing to release
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
