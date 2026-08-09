"""RunRequest — what was asked for, and under what policy.

One request type for every kind of runtime execution, because ``weaver load``
and ``weaver test`` are two kinds of the same thing: run installed work against
an installed estate, in dependency order, and say what happened. They differ in
which nodes are selected, not in how a run behaves — so they differ here, in a
field, rather than in two orchestration engines that must be kept in agreement.

.. code-block:: text

    weaver load   → RunRequest.load(...)   ┐
    weaver test   → RunRequest.test(...)   ├→ Runner
    a later kind  → RunRequest.<kind>(...) ┘

That last line is the point of the shape. Semantic-model refresh, or any other
installed runtime work, becomes a selection rule and a primitive — not a second
Runner, a second report model and a second set of ordering bugs.

The public commands stay ``weaver load`` and ``weaver test``: one internal model
does not mean one user-facing output, and each renders the projection its
readers expect.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

#: Run every loadable object installed in the requested targets.
LOAD = "load"
#: Run the installed Tests and Assumptions in the requested targets.
TEST = "test"


@dataclass(frozen=True)
class RunRequest:
    """The requested scope and the policy the run is executed under."""

    kind: str
    targets: tuple
    #: One installed node by name, where the caller asked for exactly one.
    name: str | None = None
    #: A source file compiled and run without being installed. ``test`` only.
    file: str | None = None
    #: Continue independent branches after a node fails, and report.
    fault_tolerant: bool = False
    #: Plan, resolve and report without dispatching anything.
    dry_run: bool = False
    #: Whether resolution should require the estate to be there before running.
    #:
    #: A load is about to *write*, so a missing target or an uninstalled
    #: artefact is a reason not to start — and saying which of the two it was is
    #: the point of resolving ahead of dispatching. A validation *reads*: if
    #: what it reads is not there, its own dispatch fails with a message about
    #: the thing that was missing, which is more precise than anything an
    #: inventory could say ahead of time.
    verifies_estate: bool = True

    def __post_init__(self) -> None:
        from ..errors import CommandError

        if not self.targets:
            raise CommandError(f"{self.kind} needs at least one target")
        if self.name is not None and self.file is not None:
            raise CommandError(
                "a run selects name= or file=, not both — one names something "
                "the estate has and the other something it may not"
            )

    @classmethod
    def load(cls, targets: Sequence, **policy) -> "RunRequest":
        return cls(kind=LOAD, targets=tuple(targets), **policy)

    @classmethod
    def test(cls, targets: Sequence, **policy) -> "RunRequest":
        policy.setdefault("verifies_estate", False)
        return cls(kind=TEST, targets=tuple(targets), **policy)

    @property
    def selection(self) -> str | None:
        """What was selected within the targets, for a report to record."""

        return self.file if self.file is not None else self.name

    def to_mapping(self) -> dict:
        return {
            "kind": self.kind,
            "targets": [str(target) for target in self.targets],
            "name": self.name,
            "file": self.file,
            "fault_tolerant": self.fault_tolerant,
            "dry_run": self.dry_run,
            # Behaviourally significant, so it is in the handover. A request
            # that crossed a boundary without it would arrive meaning something
            # else — preflighting an estate the caller said not to.
            "verifies_estate": self.verifies_estate,
        }


__all__ = ["LOAD", "TEST", "RunRequest"]
