"""What the harness spends standing an estate up, measured rather than inferred.

Provisioning an estate, staging the repository, generating a bundle, installing
it, is the largest single cost in the Fabric suite, and it happens in fixtures.
Before this it was visible only as wall-clock time nobody attributed, so an
argument about reducing it had nothing but arithmetic behind it.

These crossings are the *harness's*, not Weaver's. A test's declared resources
are compared with what its own Sessions crossed in its claim body (see
``tests/support/weaver_test.py``), and putting fixture plumbing into that ledger
would make every assertion-heavy test declare a resource its subject never
touched. So they are recorded here and reported separately.
"""

from __future__ import annotations

import time
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass, field

#: The phases an estate goes through, in the order they run. Named rather than
#: free text, so the summary groups the same work the same way every run.
STAGE_REPOSITORY = "stage the repository"
GENERATE = "generate the bundle"
INSTALL = "install the bundle"
RESET = "reset the target"


@dataclass
class Phase:
    """One provisioning phase, how often it ran and how long it took."""

    name: str
    runs: int = 0
    seconds: float = 0.0
    resources: dict = field(default_factory=lambda: defaultdict(float))


_phases: dict[str, Phase] = {}


@contextmanager
def measured(name: str, *, resource: str):
    """Time one provisioning phase and record what it crossed."""

    started = time.monotonic()
    try:
        yield
    finally:
        record(name, time.monotonic() - started, resource=resource)


def record(name: str, seconds: float, *, resource: str) -> None:
    """Add one phase's cost to the ledger."""

    phase = _phases.setdefault(name, Phase(name=name))
    phase.runs += 1
    phase.seconds += seconds
    phase.resources[resource] += seconds


def ledger() -> tuple[Phase, ...]:
    """Every phase recorded, most expensive first."""

    return tuple(sorted(_phases.values(), key=lambda one: one.seconds, reverse=True))


def total_seconds() -> float:
    return sum(phase.seconds for phase in _phases.values())


def reset() -> None:
    """Forget everything recorded. For a test about this ledger."""

    _phases.clear()


__all__ = [
    "GENERATE",
    "INSTALL",
    "RESET",
    "STAGE_REPOSITORY",
    "Phase",
    "ledger",
    "measured",
    "record",
    "reset",
    "total_seconds",
]
