"""What a Session spent, recorded where the spending happens.

Every expensive thing Weaver does crosses a Session: acquiring a token, starting
Livy, submitting a statement, opening TDS, asking a workspace what a name means.
Timing them anywhere else means timing them twice or not at all, so the ledger
lives on the Session and every capability records into it.

What it is for is the shape of a run rather than a total: nine minutes of Livy
startup and one of execution is a different problem from ten of execution, and a
wall clock cannot tell them apart.

.. code-block:: text

    livy.start          1 call     42.1s
    livy.submit        38 calls    12.4s
    resolve.item       11 calls     3.2s   (cache hits: 96)
    tds.execute        22 calls     8.7s

Thread-safe because a Session acquires resources in the background: the console
prompt returns while auth and Livy are still starting, and both record here.
"""

from __future__ import annotations

import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Iterator, Mapping


@dataclass
class Measure:
    """One named thing, how often it happened and how long it took."""

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
    """A Session's own ledger of transport and resource cost.

    Counters that are not timings — cache hits, in particular — are recorded
    separately because a cache hit is the *absence* of a call, and adding it to
    the call count of the thing it avoided would hide exactly the improvement it
    represents.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._measures: dict[str, Measure] = {}
        self._counters: dict[str, int] = {}
        self._started = time.monotonic()

    # --- recording ----------------------------------------------------------

    @contextmanager
    def timing(self, name: str) -> Iterator[None]:
        """Time one call, recording it as a failure if it raises."""

        started = time.monotonic()
        try:
            yield
        except BaseException:
            self.record(name, time.monotonic() - started, failed=True)
            raise
        self.record(name, time.monotonic() - started)

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

    # --- reading ------------------------------------------------------------

    @property
    def lifetime(self) -> float:
        """Seconds since this Session's telemetry began — its own lifetime."""

        return time.monotonic() - self._started

    @property
    def measures(self) -> Mapping[str, Measure]:
        with self._lock:
            return {name: Measure(**vars(measure)) for name, measure in self._measures.items()}

    @property
    def counters(self) -> Mapping[str, int]:
        with self._lock:
            return dict(self._counters)

    def to_mapping(self) -> dict:
        return {
            "lifetime": round(self.lifetime, 3),
            "measures": [measure.to_mapping() for measure in sorted(
                self.measures.values(), key=lambda measure: measure.name
            )],
            "counters": dict(sorted(self.counters.items())),
        }

    def report(self) -> str:
        """A short human-readable summary, for diagnostics rather than output."""

        lines = [f"session lifetime {self.lifetime:.1f}s"]
        for measure in sorted(self.measures.values(), key=lambda one: -one.seconds):
            failed = f", {measure.failures} failed" if measure.failures else ""
            lines.append(
                f"  {measure.name:<24} {measure.calls:>4} calls {measure.seconds:>8.1f}s{failed}"
            )
        for name, value in sorted(self.counters.items()):
            lines.append(f"  {name:<24} {value:>4}")
        return "\n".join(lines)


__all__ = ["Measure", "SessionTelemetry"]
