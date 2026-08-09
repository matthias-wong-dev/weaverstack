"""RunResult — the canonical, in-memory answer to "what happened?".

One result model for every kind of run. ``weaver load`` and ``weaver test`` were
two report models describing the same events — a node ran, a node was blocked, a
node was skipped, the run as a whole came to a status — kept in agreement by
hand, which meant a fix to one was a bug waiting in the other.

This is the *internal* model, and it is authoritative in memory. A physical task
log is downstream of it:

.. code-block:: text

    Runner
      ├── events
      ├── node results
      └── RunResult
                ↓  optional boundary
             _/Log

Ordinary correctness must not require a log, which is what lets a run-cycle test
execute a whole Runner with no storage at all. Production still writes one.

**One internal model does not mean one user-facing shape.** ``weaver load
--json`` and ``weaver test --json`` render projections of this, because their
readers are asking different questions — a load reader wants rows moved, a test
reader wants which checks disagreed and by how much.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

# --- node statuses ------------------------------------------------------------
#
# What became of one node. Kept identical to the load statuses they replace, so
# a persisted report from before this refactor still reads.

SUCCEEDED = "succeeded"
SUCCEEDED_WITH_REJECTS = "succeeded_with_rejects"
FAILED = "failed"
BLOCKED = "blocked"
SKIPPED = "skipped"
PENDING = "pending"
#: The node could not be resolved, so nothing ran. Not the same as a primitive
#: that ran and reported failure, and a reader asking which is asking this.
INVALID = "invalid"
#: A dry run's outcome: resolved and ready, having executed nothing.
VALIDATED = "validated"

# --- run statuses -------------------------------------------------------------

RUN_SUCCEEDED = "succeeded"
RUN_SUCCEEDED_WITH_REJECTS = "succeeded_with_rejects"
RUN_PARTIALLY_SUCCEEDED = "partially_succeeded"
RUN_FAILED = "failed"
#: A dry run in which something could not be resolved. A dry run has no
#: successes to be partial about — it either proved the run could happen or it
#: found a reason it could not.
RUN_INVALID = "invalid"


@dataclass(frozen=True)
class RunNodeResult:
    """What became of one node of the graph.

    ``executed`` is separate from ``status`` and has to be: a dry run reports
    ``validated`` having executed nothing, and a real run reports ``blocked``
    having executed nothing either. A reader asking "did this touch the target?"
    is asking about ``executed``, and no status answers it on its own.
    """

    node_id: str
    physical_target: str
    primitive_kind: str
    status: str
    logical_id: str | None = None
    dispatch_location: str | None = None
    executed: bool = False
    messages: tuple = ()
    result: Any = None
    started_at: str | None = None
    finished_at: str | None = None
    #: Evidence a caller asked for by name. Never persisted and never compared:
    #: diagnostic rows carry whatever a check selected, and a durable record of
    #: them would put data into the estate's own evidence.
    diagnostics: Any = field(default=None, compare=False, repr=False)

    @property
    def succeeded(self) -> bool:
        return self.status in (SUCCEEDED, VALIDATED)

    def to_mapping(self) -> dict[str, Any]:
        return {
            "node_id": self.node_id,
            "logical_id": self.logical_id,
            "physical_target": self.physical_target,
            "primitive_kind": self.primitive_kind,
            "dispatch_location": self.dispatch_location,
            "status": self.status,
            "executed": self.executed,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "rows": None if self.result is None else self.result.as_row(),
            "messages": [
                message.to_mapping() if hasattr(message, "to_mapping") else str(message)
                for message in self.messages
            ],
        }


@dataclass(frozen=True)
class RunResult:
    """One Runner execution, whole.

    The same shape whether or not anything ran, which is what lets a dry run be
    inspected exactly as a real run is. ``task_log`` is absent for a dry run,
    and its absence is the result saying so — dry runs write no evidence, so
    there is none to point at.
    """

    kind: str
    requested: tuple[str, ...]
    status: str
    dry_run: bool = False
    fault_tolerant: bool = False
    nodes: tuple[RunNodeResult, ...] = ()
    edges: tuple[tuple[str, str], ...] = ()
    order: tuple[str, ...] = ()
    messages: tuple = ()
    selection: str | None = None
    task_id: str | None = None
    task_log: str | None = None
    started_at: str | None = None
    finished_at: str | None = None
    workspace: str | None = None

    @property
    def succeeded(self) -> bool:
        return self.status in (RUN_SUCCEEDED, RUN_SUCCEEDED_WITH_REJECTS)

    @property
    def by_node(self) -> Mapping[str, RunNodeResult]:
        return {node.node_id: node for node in self.nodes}

    @property
    def executed(self) -> tuple[RunNodeResult, ...]:
        return tuple(node for node in self.nodes if node.executed)

    def to_mapping(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "requested": list(self.requested),
            "status": self.status,
            "dry_run": self.dry_run,
            "fault_tolerant": self.fault_tolerant,
            "selection": self.selection,
            "workspace": self.workspace,
            "task_id": self.task_id,
            "task_log": self.task_log,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "order": list(self.order),
            "edges": [list(edge) for edge in self.edges],
            "nodes": [node.to_mapping() for node in self.nodes],
            "messages": [
                message.to_mapping() if hasattr(message, "to_mapping") else str(message)
                for message in self.messages
            ],
        }


def run_status(nodes, *, dry_run: bool = False) -> str:
    """The worst thing that happened, which is what a run's status means.

    Not a count and not a majority: a caller asking whether a run succeeded is
    asking whether anything did not, so one failure among fifty successes is a
    failed run — and one that merely rejected rows is neither a success nor a
    failure and says so in its own word.
    """

    statuses = {node.status for node in nodes}
    if dry_run:
        # Nothing ran, so "partially succeeded" would be a claim about work that
        # did not happen. A dry run either proved the run could happen or found
        # a reason it could not.
        if not statuses:
            return RUN_INVALID
        return (
            RUN_INVALID
            if statuses & {INVALID, BLOCKED}
            else RUN_SUCCEEDED
        )
    if not statuses:
        return RUN_SUCCEEDED
    if FAILED in statuses or BLOCKED in statuses or INVALID in statuses:
        succeeded = {SUCCEEDED, SUCCEEDED_WITH_REJECTS, VALIDATED, SKIPPED}
        return (
            RUN_PARTIALLY_SUCCEEDED
            if statuses & succeeded
            else RUN_FAILED
        )
    if SUCCEEDED_WITH_REJECTS in statuses:
        return RUN_SUCCEEDED_WITH_REJECTS
    return RUN_SUCCEEDED


__all__ = [
    "BLOCKED",
    "FAILED",
    "INVALID",
    "PENDING",
    "RUN_FAILED",
    "RUN_INVALID",
    "RUN_PARTIALLY_SUCCEEDED",
    "RUN_SUCCEEDED",
    "RUN_SUCCEEDED_WITH_REJECTS",
    "SKIPPED",
    "SUCCEEDED",
    "SUCCEEDED_WITH_REJECTS",
    "VALIDATED",
    "RunNodeResult",
    "RunResult",
    "run_status",
]
