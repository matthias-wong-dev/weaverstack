"""Shared result, status and message types for runtime operations."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from ..errors import WeaverError

# --- the contract a result must meet ------------------------------------------


class RunError(WeaverError):
    """A run failure, with operation-specific evidence when available."""

    def __init__(self, message: str, *, result: object | None = None) -> None:
        super().__init__(message)
        self.result = result


def reports_outcome(result: object) -> bool:

    return hasattr(result, "succeeded")


def represent(result: object) -> dict | None:
    """Serialise any result that follows the runtime result contract."""

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
    """A dispatch failure without an operation-specific result."""

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

PRIMITIVE_REJECTS = "primitive_rejects"
PRIMITIVE_FAILURE = "primitive_failure"
DISPATCH_EXCEPTION = "dispatch_exception"
DISPATCH_LOCATION_MISSING = "dispatch_location_missing"
MODULE_IMPORT_FAILURE = "module_import_failure"
RESULT_CONTRACT_INVALID = "result_contract_invalid"
ENDPOINT_REFRESH_FAILURE = "endpoint_refresh_failure"
DEPENDENCY_BLOCKED = "dependency_blocked"
CATALOGUE_BINDING_INVALID = "catalogue_binding_invalid"
DAG_CYCLE = "dag_cycle"
DEPENDENCY_UNRESOLVED = "dependency_unresolved"
#: A reference Weaver does not follow: a fully qualified physical read that names
#: something outside the estate's own logical graph.
DEPENDENCY_EXTERNAL = "dependency_external"


@dataclass(frozen=True)
class RunMessage:
    """A finding about one node or the run as a whole."""

    severity: str
    code: str
    message: str
    detail: str | None = None
    source: str | None = None
    executor: str | None = None

    def to_mapping(self) -> dict[str, Any]:
        mapping = {
            "severity": self.severity,
            "code": self.code,
            "message": self.message,
            "detail": self.detail,
            "source": self.source,
        }
        if self.executor is not None:
            mapping["executor"] = self.executor
        return mapping

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> "RunMessage":
        return cls(
            severity=payload["severity"],
            code=payload["code"],
            message=payload["message"],
            detail=payload.get("detail"),
            source=payload.get("source"),
            executor=payload.get("executor"),
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
#: Resolution failed before dispatch; the primitive did not run.
INVALID = "invalid"
#: A dry run's outcome: resolved and ready, having executed nothing.
VALIDATED = "validated"

# --- run statuses -------------------------------------------------------------

RUN_SUCCEEDED = "succeeded"
RUN_SUCCEEDED_WITH_REJECTS = "succeeded_with_rejects"
RUN_PARTIALLY_SUCCEEDED = "partially_succeeded"
RUN_FAILED = "failed"
#: A dry run in which something could not be resolved. A dry run has no
#: successes to be partial about. It either proved the run could happen or it
#: found a reason it could not.
RUN_INVALID = "invalid"


@dataclass(frozen=True)
class RunNodeResult:
    """A node outcome.

    ``executed`` is independent of status because validated and blocked nodes do
    not touch their targets.
    """

    node_id: str
    physical_target: str
    primitive_kind: str
    status: str
    logical_id: str | None = None
    dispatch_location: str | None = None
    #: What this node was for, where one graph carries more than one kind. A Test
    #: and an Assumption are both validations and are reported apart.
    role: str | None = None
    #: Distinguishes a check that could not run from one that found a failure.
    raised: bool = False
    #: Distinguishes a named refusal from an unexpected dispatch error.
    refused: bool = False
    executed: bool = False
    messages: tuple = ()
    result: Any = None
    started_at: str | None = None
    finished_at: str | None = None
    target_type: str | None = field(default=None, compare=False, repr=False)
    target_name: str | None = field(default=None, compare=False, repr=False)
    schema_name: str | None = field(default=None, compare=False, repr=False)
    object_name: str | None = field(default=None, compare=False, repr=False)
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
            "raised": self.raised,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "rows": represent(self.result),
            "messages": [
                message.to_mapping() if hasattr(message, "to_mapping") else str(message)
                for message in self.messages
            ],
        }


@dataclass(frozen=True)
class RunResult:
    """A run outcome, whether planned or executed.

    The recording sink owns the location of durable evidence; this in-memory
    result remains independent of storage.
    """

    kind: str
    requested: tuple[str, ...]
    status: str
    dry_run: bool = False
    fault_tolerant: bool = False
    #: Whether the run reconstructed each selected table from zero.
    reload: bool = False
    #: Whether the run waived the declared stability limits.
    ignore_stability_threshold: bool = False
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
            "reload": self.reload,
            "ignore_stability_threshold": self.ignore_stability_threshold,
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
    """Derive the run status from node statuses."""

    statuses = {node.status for node in nodes}
    if not statuses:
        # A plan may select nothing, as a stale load of a green estate does.
        return RUN_SUCCEEDED
    if dry_run:
        # Nothing ran, so "partially succeeded" would be a claim about work that
        # did not happen. A dry run either proved the run could happen or found
        # a reason it could not.
        return RUN_INVALID if statuses & {INVALID, BLOCKED} else RUN_SUCCEEDED
    if FAILED in statuses or BLOCKED in statuses or INVALID in statuses:
        succeeded = {SUCCEEDED, SUCCEEDED_WITH_REJECTS, VALIDATED, SKIPPED}
        return RUN_PARTIALLY_SUCCEEDED if statuses & succeeded else RUN_FAILED
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
