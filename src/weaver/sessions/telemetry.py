"""Session reporting context and external-resource telemetry.

Reporting frames say why work is happening. This module records the external
resource, operation, elapsed time, and outcome.
"""

from __future__ import annotations

import threading
import time
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any, Iterator, Mapping

RESOURCES = frozenset({"tds", "livy", "onelake", "rest"})


@dataclass(frozen=True)
class TelemetryContext:
    task: str | None = None
    step: str | None = None
    substep: str | None = None


@dataclass(frozen=True)
class TelemetryEvent:
    resource: str
    operation: str
    seconds: float
    task: str | None = None
    step: str | None = None
    substep: str | None = None
    failed: bool = False
    detail: str | None = None

    def to_mapping(self) -> dict[str, Any]:
        return {
            "resource": self.resource,
            "operation": self.operation,
            "seconds": round(self.seconds, 3),
            "task": self.task,
            "step": self.step,
            "substep": self.substep,
            "failed": self.failed,
            "detail": self.detail,
        }


@dataclass
class Measure:
    name: str
    calls: int = 0
    seconds: float = 0.0
    failures: int = 0

    def to_mapping(self) -> dict:
        return {
            "name": self.name,
            "calls": self.calls,
            "seconds": round(self.seconds, 3),
            "failures": self.failures,
        }


class SessionTelemetry:
    """A Session-owned ledger of external cost and reporting attribution.

    ``timing`` and ``measures`` remain for callers that need the existing
    low-level diagnostics. External operations additionally become immutable
    :class:`TelemetryEvent` values, which is the API used by test reporting.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._measures: dict[str, Measure] = {}
        self._counters: dict[str, int] = {}
        self._events: list[TelemetryEvent] = []
        self._context: ContextVar[TelemetryContext] = ContextVar(
            "weaver_telemetry_context", default=TelemetryContext()
        )
        self._started = time.monotonic()

    # --- reporting context ------------------------------------------------

    @property
    def context(self) -> TelemetryContext:
        return self._context.get()

    def capture_context(self) -> TelemetryContext:
        return self.context

    @contextmanager
    def use_context(self, context: TelemetryContext) -> Iterator[None]:
        token = self._context.set(context)
        try:
            yield
        finally:
            self._context.reset(token)

    def set_frames(self, frames) -> None:
        names = {"task": None, "step": None, "substep": None}
        for frame in frames:
            names[frame.kind] = frame.name
        self._context.set(TelemetryContext(**names))

    # --- recording ---------------------------------------------------------

    @contextmanager
    def timing(self, name: str) -> Iterator[None]:
        started = time.monotonic()
        try:
            yield
        except BaseException:
            self.record(name, time.monotonic() - started, failed=True)
            raise
        self.record(name, time.monotonic() - started)

    @contextmanager
    def external(
        self,
        resource: str,
        operation: str,
        *,
        detail: str | None = None,
        measure: str | None = None,
    ) -> Iterator[None]:
        """Time one external operation, retaining failures before re-raising."""

        if resource not in RESOURCES:
            raise ValueError(f"Unknown Session telemetry resource: {resource!r}.")
        started = time.monotonic()
        try:
            yield
        except BaseException:
            self.record_event(
                resource,
                operation,
                time.monotonic() - started,
                detail=detail,
                failed=True,
                measure=measure,
            )
            raise
        self.record_event(
            resource,
            operation,
            time.monotonic() - started,
            detail=detail,
            measure=measure,
        )

    def record_event(
        self,
        resource: str,
        operation: str,
        seconds: float,
        *,
        detail: str | None = None,
        failed: bool = False,
        measure: str | None = None,
    ) -> None:
        if resource not in RESOURCES:
            raise ValueError(f"Unknown Session telemetry resource: {resource!r}.")
        context = self.context
        event = TelemetryEvent(
            resource=resource,
            operation=operation,
            seconds=seconds,
            task=context.task,
            step=context.step,
            substep=context.substep,
            failed=failed,
            detail=detail,
        )
        with self._lock:
            self._events.append(event)
        self.record(measure or f"{resource}.{operation}", seconds, failed=failed)

    def record(self, name: str, seconds: float, *, failed: bool = False) -> None:
        with self._lock:
            measure = self._measures.get(name)
            if measure is None:
                measure = self._measures[name] = Measure(name)
            measure.calls += 1
            measure.seconds += seconds
            if failed:
                measure.failures += 1

    def count(self, name: str, amount: int = 1) -> None:
        with self._lock:
            self._counters[name] = self._counters.get(name, 0) + amount

    # --- reading -----------------------------------------------------------

    @property
    def lifetime(self) -> float:
        return time.monotonic() - self._started

    @property
    def measures(self) -> Mapping[str, Measure]:
        with self._lock:
            return {
                name: Measure(**vars(measure))
                for name, measure in self._measures.items()
            }

    @property
    def counters(self) -> Mapping[str, int]:
        with self._lock:
            return dict(self._counters)

    def events(self) -> tuple[TelemetryEvent, ...]:
        with self._lock:
            return tuple(self._events)

    def resources_used(self) -> frozenset[str]:
        return frozenset(event.resource for event in self.events())

    def by_resource(self) -> Mapping[str, Measure]:
        return self._aggregate(lambda event: event.resource)

    def by_task(self) -> Mapping[str, Measure]:
        return self._aggregate(lambda event: event.task or "<unattributed>")

    def by_step(self) -> Mapping[tuple[str, str], Measure]:
        return self._aggregate(
            lambda event: (event.task or "<unattributed>", event.step or "<none>")
        )

    def by_resource_and_task(self) -> Mapping[tuple[str, str], Measure]:
        return self._aggregate(
            lambda event: (event.resource, event.task or "<unattributed>")
        )

    def total_external_seconds(self) -> float:
        return sum(event.seconds for event in self.events())

    def _aggregate(self, key) -> Mapping[Any, Measure]:
        grouped: dict[Any, Measure] = {}
        for event in self.events():
            name = key(event)
            measure = grouped.setdefault(name, Measure(str(name)))
            measure.calls += 1
            measure.seconds += event.seconds
            measure.failures += int(event.failed)
        return grouped

    def to_mapping(self) -> dict:
        return {
            "lifetime": round(self.lifetime, 3),
            "measures": [
                measure.to_mapping()
                for measure in sorted(
                    self.measures.values(), key=lambda measure: measure.name
                )
            ],
            "events": [event.to_mapping() for event in self.events()],
            "counters": dict(sorted(self.counters.items())),
        }

    def report(self) -> str:
        """Return a human-readable diagnostic summary."""

        lines = [f"session lifetime {self.lifetime:.1f}s"]
        for measure in sorted(self.measures.values(), key=lambda one: -one.seconds):
            failed = f", {measure.failures} failed" if measure.failures else ""
            lines.append(
                f"  {measure.name:<24} {measure.calls:>4} calls {measure.seconds:>8.1f}s{failed}"
            )
        return "\n".join(lines)


__all__ = [
    "Measure",
    "RESOURCES",
    "SessionTelemetry",
    "TelemetryContext",
    "TelemetryEvent",
]
