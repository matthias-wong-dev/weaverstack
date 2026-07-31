"""Counting Livy round trips, because they are what the Fabric suite costs.

A statement submitted to a Fabric session costs seconds — enough that the number
of round trips, not the work inside them, sets the runtime of `-m fabric`. That
cost is invisible in ordinary pytest output: a test making six calls and a test
making one look identical until someone times the run by hand.

So every submission goes through :class:`CountedLivySession`, which records it
against the test that caused it, and the suite prints a breakdown at the end.
The point is to make the expensive thing *countable*, not to hide it — a helper
that quietly batched calls would remove the number this exists to show.

Nothing here asserts a budget. A count that has to be edited whenever a probe
legitimately changes teaches the suite to raise the number rather than ask why,
and the summary already puts the regression in front of whoever caused it.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field

#: What the ledger attributes a call to when nothing is running — session-scoped
#: fixture setup before the first test, and teardown after the last.
OUTSIDE_A_TEST = "<session fixtures>"


@dataclass(frozen=True)
class LivyCall:
    """One statement submitted to a Fabric session."""

    nodeid: str
    #: What the caller was doing — "generate", "install", "query". Labels are how
    #: a breakdown says *which* round trip was expensive, not merely that one was.
    label: str
    seconds: float
    code_bytes: int
    #: Characters of output Livy returned, payload included. Large output is a
    #: transport cost of its own, and a consolidated payload is meant to grow
    #: this while shrinking the call count.
    returned_bytes: int
    failed: bool = False


@dataclass
class LivyLedger:
    """Every Livy submission in one pytest run, attributed to its test."""

    calls: list[LivyCall] = field(default_factory=list)
    #: The test currently running, maintained by the conftest's runtest hooks.
    nodeid: str = OUTSIDE_A_TEST
    #: Session startup, which is a Livy cost but not a submission — reported
    #: separately so it is never mistaken for something a test could avoid.
    startup_seconds: float = 0.0

    def record(
        self, *, label: str, seconds: float, code_bytes: int,
        returned_bytes: int, failed: bool = False,
    ) -> None:
        self.calls.append(
            LivyCall(
                nodeid=self.nodeid, label=label, seconds=seconds,
                code_bytes=code_bytes, returned_bytes=returned_bytes, failed=failed,
            )
        )

    @property
    def seconds(self) -> float:
        return sum(call.seconds for call in self.calls)

    def by_test(self) -> list[tuple[str, int, float]]:
        """``(nodeid, calls, seconds)``, most expensive first."""

        totals: dict[str, list[float]] = {}
        for call in self.calls:
            entry = totals.setdefault(call.nodeid, [0, 0.0])
            entry[0] += 1
            entry[1] += call.seconds
        return sorted(
            ((nodeid, int(n), secs) for nodeid, (n, secs) in totals.items()),
            key=lambda row: row[2],
            reverse=True,
        )

    def by_label(self) -> list[tuple[str, int, float]]:
        """``(label, calls, seconds)``, most expensive first."""

        totals: dict[str, list[float]] = {}
        for call in self.calls:
            entry = totals.setdefault(call.label, [0, 0.0])
            entry[0] += 1
            entry[1] += call.seconds
        return sorted(
            ((label, int(n), secs) for label, (n, secs) in totals.items()),
            key=lambda row: row[2],
            reverse=True,
        )

    def report(self, *, limit: int = 12) -> list[str]:
        """The lines pytest prints in its summary. Empty when nothing ran."""

        if not self.calls:
            return []
        lines = [
            f"Livy calls: {len(self.calls)}",
            f"Livy elapsed: {self.seconds:.1f}s"
            + (
                f" (plus {self.startup_seconds:.1f}s session startup)"
                if self.startup_seconds
                else ""
            ),
            "",
            "By phase:",
        ]
        lines.extend(
            f"  {label}: {n} calls / {secs:.1f}s" for label, n, secs in self.by_label()
        )
        lines.extend(["", "Top callers:"])
        lines.extend(
            f"  {nodeid}: {n} calls / {secs:.1f}s"
            for nodeid, n, secs in self.by_test()[:limit]
        )
        return lines


#: The run's ledger. One per pytest process, like the Livy session it measures.
LEDGER = LivyLedger()


class CountedLivySession:
    """A :class:`~weaver.fabric.LivySession` that records what it is asked to run.

    A proxy rather than a subclass, because the session is constructed by
    ``LivySession.for_workspace`` and the suite has no business changing how
    production builds one. Everything but ``run`` passes straight through.
    """

    def __init__(self, session, ledger: LivyLedger = LEDGER) -> None:
        object.__setattr__(self, "_session", session)
        object.__setattr__(self, "_ledger", ledger)

    def run(self, code: str, *, label: str = "statement", **kwargs):
        started = time.monotonic()
        failed = False
        result = None
        try:
            result = self._session.run(code, **kwargs)
            return result
        except BaseException:
            failed = True
            raise
        finally:
            self._ledger.record(
                label=label,
                seconds=time.monotonic() - started,
                code_bytes=len(code),
                returned_bytes=len(getattr(result, "text", "") or ""),
                failed=failed,
            )

    # The proxy is deliberately transparent: fixtures set attributes on the
    # session (`weaver_startup_seconds`), and `close()` must reach the real one.
    def __getattr__(self, name):
        return getattr(self._session, name)

    def __setattr__(self, name, value):
        setattr(self._session, name, value)
