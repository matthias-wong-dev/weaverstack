"""What a load run reports: statuses, messages and the shape returned to a caller.

The vocabulary layer of load orchestration, and it is deliberately the only one
that has no behaviour. Planning, resolution, execution and logging all produce
these values; nothing here produces anything.

**A message stream, not an error string.** A node can fail for several reasons at
once — its target is missing *and* the module it would import cannot be located —
and an orchestrator that flattened those into one string would be choosing which
half of the answer to keep. So every finding, whether the primitive returned it
or the orchestrator caught it, enters one stream of :class:`LoadMessage`, and a
node carries as many as it has.

**Dry-run statuses are not execution statuses.** A validated node has not run,
and calling it ``succeeded`` would put a claim in a report that nothing performed.
The two sets are kept apart here so the distinction cannot be lost downstream —
see :data:`VALIDATION_STATUSES` and :data:`EXECUTION_STATUSES`.

:class:`weaver.runtime.load_result.LoadResult` is re-exported rather than
redefined. A primitive already returns that shape on all four engines, and a
second dataclass meaning the same thing is exactly the drift the runtime module
exists to prevent.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from .runtime.load_result import LoadResult

# --- statuses ----------------------------------------------------------------

#: Planned but not started.
PENDING = "pending"
#: Dispatch began.
RUNNING = "running"
#: Completed successfully.
SUCCEEDED = "succeeded"
#: Valid work completed, but the primitive reported rejected rows.
SUCCEEDED_WITH_REJECTS = "succeeded_with_rejects"
#: The primitive or the dispatch around it failed.
FAILED = "failed"
#: An upstream dependency failed or could not be validated.
BLOCKED = "blocked"
#: Deliberately omitted by a planner or execution policy. Never used for failure
#: propagation — a node whose dependency failed is ``blocked``, and conflating
#: the two would make "nothing was wrong" and "something upstream broke" read the
#: same in a log.
SKIPPED = "skipped"

EXECUTION_STATUSES = (
    PENDING,
    RUNNING,
    SUCCEEDED,
    SUCCEEDED_WITH_REJECTS,
    FAILED,
    BLOCKED,
    SKIPPED,
)

#: A dry-run node's dispatch target and prerequisites were all resolved.
VALIDATED = "validated"
#: A dry-run node's own primitive or target could not be resolved.
INVALID = "invalid"

VALIDATION_STATUSES = (VALIDATED, INVALID, BLOCKED)

# --- final task statuses ------------------------------------------------------

#: Every executable step succeeded without rejects.
TASK_SUCCEEDED = "succeeded"
#: Every executable branch completed, but a primitive reported rejects.
TASK_SUCCEEDED_WITH_REJECTS = "succeeded_with_rejects"
#: At least one branch succeeded and at least one failed or was blocked.
TASK_PARTIALLY_SUCCEEDED = "partially_succeeded"
#: No requested branch completed, or fail-fast stopped the task.
TASK_FAILED = "failed"
#: A dry run could not resolve a valid executable plan.
TASK_INVALID = "invalid"

# --- messages -----------------------------------------------------------------
#
# Owned by the run package, because they are runtime vocabulary rather than load
# vocabulary: a load, a validation and whatever runtime work comes next all
# report through them. Re-exported here under the name this module's public
# report has always used, so a reader of a LoadRunReport sees one message type.

from .run.result import (  # noqa: E402 - the vocabulary this report projects
    CATALOGUE_BINDING_INVALID,
    DAG_CYCLE,
    DEPENDENCY_BLOCKED,
    DEPENDENCY_EXTERNAL,
    DEPENDENCY_UNRESOLVED,
    DISPATCH_EXCEPTION,
    DISPATCH_LOCATION_MISSING,
    ENDPOINT_REFRESH_FAILURE,
    MODULE_IMPORT_FAILURE,
    PRIMITIVE_FAILURE,
    PRIMITIVE_REJECTS,
    RESULT_CONTRACT_INVALID,
    SEVERITY_ERROR,
    SEVERITY_INFO,
    SEVERITY_WARNING,
    TARGET_MISSING,
    RunMessage as LoadMessage,
    error,
    info,
    warning,
)


@dataclass(frozen=True)
class LoadNodeReport:
    """What became of one planned node.

    ``executed`` is separate from ``status`` and has to be: a dry run reports
    ``validated`` having executed nothing, and a real run reports ``blocked``
    having executed nothing either. A reader asking "did this touch the target?"
    is asking about ``executed``, and no status answers it on its own.
    """

    node_id: str
    logical_id: str | None
    physical_target: str
    primitive_kind: str
    dispatch_location: str | None
    status: str
    executed: bool = False
    messages: tuple[LoadMessage, ...] = ()
    result: LoadResult | None = None
    started_at: str | None = None
    finished_at: str | None = None

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
            "messages": [message.to_mapping() for message in self.messages],
        }

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> "LoadNodeReport":
        rows = payload.get("rows")
        return cls(
            node_id=payload["node_id"],
            logical_id=payload.get("logical_id"),
            physical_target=payload["physical_target"],
            primitive_kind=payload["primitive_kind"],
            dispatch_location=payload.get("dispatch_location"),
            status=payload["status"],
            executed=bool(payload.get("executed", False)),
            messages=tuple(
                LoadMessage.from_mapping(one) for one in payload.get("messages") or ()
            ),
            result=None if rows is None else LoadResult.from_row(rows),
            started_at=payload.get("started_at"),
            finished_at=payload.get("finished_at"),
        )


@dataclass(frozen=True)
class LoadRunReport:
    """One ``weaver.load(...)`` invocation, whole.

    The same shape whether or not anything ran, which is what lets a dry run be
    inspected exactly as a real run is. ``task_id`` and ``task_log`` are absent
    for a dry run, and their absence is the report saying so — dry runs write no
    evidence, so there is none to point at.
    """

    requested: tuple[str, ...]
    status: str
    dry_run: bool
    fault_tolerant: bool
    nodes: tuple[LoadNodeReport, ...] = ()
    edges: tuple[tuple[str, str], ...] = ()
    order: tuple[str, ...] = ()
    messages: tuple[LoadMessage, ...] = ()
    task_id: str | None = None
    task_log: str | None = None
    started_at: str | None = None
    finished_at: str | None = None
    workspace: str | None = None

    @property
    def succeeded(self) -> bool:
        return self.status in (TASK_SUCCEEDED, TASK_SUCCEEDED_WITH_REJECTS)

    @property
    def by_node(self) -> Mapping[str, LoadNodeReport]:
        return {node.node_id: node for node in self.nodes}

    def to_mapping(self) -> dict[str, Any]:
        return {
            "requested": list(self.requested),
            "status": self.status,
            "dry_run": self.dry_run,
            "fault_tolerant": self.fault_tolerant,
            "workspace": self.workspace,
            "task_id": self.task_id,
            "task_log": self.task_log,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "order": list(self.order),
            "edges": [list(edge) for edge in self.edges],
            "nodes": [node.to_mapping() for node in self.nodes],
            "messages": [message.to_mapping() for message in self.messages],
        }

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> "LoadRunReport":
        """Reconstruct a report that crossed a process boundary.

        The desktop CLI runs a load *inside* Fabric and gets its report back as
        JSON, and what it renders has to be the same object an in-session caller
        holds — otherwise the two would drift into two renderers and the CLI
        would quietly become the second place that knows what a load report
        means.
        """

        return cls(
            requested=tuple(payload.get("requested") or ()),
            status=payload["status"],
            dry_run=bool(payload.get("dry_run", False)),
            fault_tolerant=bool(payload.get("fault_tolerant", False)),
            nodes=tuple(
                LoadNodeReport.from_mapping(one) for one in payload.get("nodes") or ()
            ),
            edges=tuple(
                (edge[0], edge[1]) for edge in payload.get("edges") or ()
            ),
            order=tuple(payload.get("order") or ()),
            messages=tuple(
                LoadMessage.from_mapping(one) for one in payload.get("messages") or ()
            ),
            task_id=payload.get("task_id"),
            task_log=payload.get("task_log"),
            started_at=payload.get("started_at"),
            finished_at=payload.get("finished_at"),
            workspace=payload.get("workspace"),
        )


def final_status(nodes: tuple[LoadNodeReport, ...], *, dry_run: bool) -> str:
    """The task status these node reports add up to.

    Derived rather than accumulated, so the summary cannot disagree with the
    nodes it summarises — the failure mode of a status advanced by hand as the
    run proceeds.
    """

    if not nodes:
        return TASK_INVALID if dry_run else TASK_SUCCEEDED
    statuses = [node.status for node in nodes]
    if dry_run:
        return TASK_INVALID if any(
            status in (INVALID, BLOCKED) for status in statuses
        ) else TASK_SUCCEEDED
    bad = [status for status in statuses if status in (FAILED, BLOCKED)]
    good = [
        status
        for status in statuses
        if status in (SUCCEEDED, SUCCEEDED_WITH_REJECTS, SKIPPED)
    ]
    if not bad:
        return (
            TASK_SUCCEEDED_WITH_REJECTS
            if SUCCEEDED_WITH_REJECTS in statuses
            else TASK_SUCCEEDED
        )
    if good:
        return TASK_PARTIALLY_SUCCEEDED
    return TASK_FAILED


__all__ = [
    "BLOCKED",
    "DAG_CYCLE",
    "DEPENDENCY_BLOCKED",
    "DEPENDENCY_EXTERNAL",
    "DEPENDENCY_UNRESOLVED",
    "DISPATCH_EXCEPTION",
    "DISPATCH_LOCATION_MISSING",
    "ENDPOINT_REFRESH_FAILURE",
    "EXECUTION_STATUSES",
    "FAILED",
    "INVALID",
    "CATALOGUE_BINDING_INVALID",
    "LoadMessage",
    "LoadNodeReport",
    "LoadResult",
    "LoadRunReport",
    "MODULE_IMPORT_FAILURE",
    "PENDING",
    "PRIMITIVE_FAILURE",
    "PRIMITIVE_REJECTS",
    "RESULT_CONTRACT_INVALID",
    "RUNNING",
    "SEVERITY_ERROR",
    "SEVERITY_INFO",
    "SEVERITY_WARNING",
    "SKIPPED",
    "SUCCEEDED",
    "SUCCEEDED_WITH_REJECTS",
    "TARGET_MISSING",
    "TASK_FAILED",
    "TASK_INVALID",
    "TASK_PARTIALLY_SUCCEEDED",
    "TASK_SUCCEEDED",
    "TASK_SUCCEEDED_WITH_REJECTS",
    "VALIDATED",
    "VALIDATION_STATUSES",
    "error",
    "final_status",
    "info",
    "warning",
]
