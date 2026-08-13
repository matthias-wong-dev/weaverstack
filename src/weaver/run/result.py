"""Common result, status, and message types for runtime operations.

Run results record whether each node succeeded. Operation-specific report
renderers project the common model into their own public output.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from ..errors import WeaverError


# --- the contract a result must meet ------------------------------------------





class RunError(WeaverError):
    """An error raised when a run cannot proceed.

    ``result`` retains operation-specific evidence when it is available.
    """

    def __init__(self, message: str, *, result: object | None = None) -> None:
        super().__init__(message)
        self.result = result


def reports_outcome(result: object) -> bool:
    """Return whether a result reports a success outcome."""

    return hasattr(result, "succeeded")


def represent(result: object) -> dict | None:
    """Serialise a result without requiring an operation-specific type.

    All results report ``succeeded``; richer result types provide their own
    mapping or row representation.
    """

    if result is None:
        return None
    for name in ("to_mapping", "as_row"):
        describe = getattr(result, name, None)
        if callable(describe):
            return describe()
    return {
        "succeeded": bool(getattr(result, "succeeded", False)),
        "error_message": getattr(result, "error_message", None),
    }


@dataclass(frozen=True)
class RunFailure:
    """Represent a dispatch failure that has no operation-specific result."""

    error_message: str
    succeeded: bool = False

    def as_row(self) -> dict:
        return {"succeeded": False, "error_message": self.error_message}


# --- what a run says about a node ---------------------------------------------



# --- severity -----------------------------------------------------------------

SEVERITY_ERROR = "error"
SEVERITY_WARNING = "warning"
SEVERITY_INFO = "info"

# --- what a run can find ------------------------------------------------------

#: The primitive ran and refused rows.
PRIMITIVE_REJECTS = "primitive_rejects"
#: The primitive ran and reported failure in its own result.
PRIMITIVE_FAILURE = "primitive_failure"
#: Dispatch raised something the primitive did not normalise.
DISPATCH_EXCEPTION = "dispatch_exception"
#: The installed primitive could not be located.
DISPATCH_LOCATION_MISSING = "dispatch_location_missing"
#: The physical target this node runs against is not there.
TARGET_MISSING = "target_missing"
#: A deployed Python module could not be imported, or carries no expected class.
MODULE_IMPORT_FAILURE = "module_import_failure"
#: A primitive returned something that does not report whether it succeeded.
RESULT_CONTRACT_INVALID = "result_contract_invalid"
#: The endpoint refresh could not be performed.
ENDPOINT_REFRESH_FAILURE = "endpoint_refresh_failure"
#: An upstream node failed or could not be resolved, so this one may not run.
DEPENDENCY_BLOCKED = "dependency_blocked"
#: The catalogue's physical binding is missing, ambiguous or malformed.
CATALOGUE_BINDING_INVALID = "catalogue_binding_invalid"
#: The planned graph contains a cycle.
DAG_CYCLE = "dag_cycle"
#: A dependency named in the catalogue could not be resolved to anything.
DEPENDENCY_UNRESOLVED = "dependency_unresolved"
#: A reference Weaver deliberately does not follow — a fully qualified physical
#: read that names something outside the estate's own logical graph.
DEPENDENCY_EXTERNAL = "dependency_external"


@dataclass(frozen=True)
class RunMessage:
    """One finding about one node, or about the run as a whole."""

    severity: str
    code: str
    message: str
    detail: str | None = None
    source: str | None = None

    def to_mapping(self) -> dict[str, Any]:
        return {
            "severity": self.severity,
            "code": self.code,
            "message": self.message,
            "detail": self.detail,
            "source": self.source,
        }

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> "RunMessage":
        return cls(
            severity=payload["severity"],
            code=payload["code"],
            message=payload["message"],
            detail=payload.get("detail"),
            source=payload.get("source"),
        )


def error(code: str, message: str, **extra: str | None) -> RunMessage:
    return RunMessage(SEVERITY_ERROR, code, message, **extra)


def warning(code: str, message: str, **extra: str | None) -> RunMessage:
    return RunMessage(SEVERITY_WARNING, code, message, **extra)


def info(code: str, message: str, **extra: str | None) -> RunMessage:
    return RunMessage(SEVERITY_INFO, code, message, **extra)





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
    #: What this node was for, where one graph carries more than one kind — a
    #: Test and an Assumption are both validations and are reported apart.
    role: str | None = None
    #: Whether the dispatch threw rather than producing a result. A node that
    #: raised was never evaluated, which a reader asking "did this check run?"
    #: needs to know.
    raised: bool = False
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
            "role": self.role,
            "executed": self.executed,
            # Whether anything was evaluated at all. Without it a reader cannot
            # tell a check that could not run from one that ran and failed.
            "raised": self.raised,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            # As narrow as the contract that admitted it: a result describes
            # itself if it can, and otherwise answers only what every result must.
            "rows": represent(self.result),
            "messages": [
                message.to_mapping() if hasattr(message, "to_mapping") else str(message)
                for message in self.messages
            ],
        }


@dataclass(frozen=True)
class RunResult:
    """One Runner execution, whole.

    The same shape whether or not anything ran, which is what lets a dry run be
    inspected exactly as a real run is.

    **Where the evidence was written is not here.** A run is correct without a
    log — that is what lets a whole Runner execute in a test with no storage at
    all — so the location of a physical record belongs to the sink that wrote
    it and to the public report that points at it, not to the canonical
    in-memory result. Production still writes one.
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
    selection: str | tuple[str, ...] | None = None
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
            "selection": (
                list(self.selection)
                if isinstance(self.selection, tuple)
                else self.selection
            ),
            "workspace": self.workspace,
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
    """Return the overall run status from node statuses.

    A rejection result has its own status and does not make the run fail.
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
