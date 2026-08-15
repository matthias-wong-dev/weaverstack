"""Serializable reports for validation runs.

Mappings retain counts and statuses. Targeted diagnostics remain local to the
report object because they may be large or contain sensitive data.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Sequence

#: The validation ran and found nothing.
PASSED = "passed"
#: The validation ran and found evidence — discrepancies, or violations.
FAILED = "failed"
#: The validation could not be evaluated. Not a failure *of* the data.
INVALID = "invalid"
#: Planned and not run, because the caller asked what would run.
PLANNED = "planned"

STATUSES = (PASSED, FAILED, INVALID, PLANNED)


@dataclass(frozen=True)
class ValidationNodeReport:
    """What became of one validation."""

    logical_id: str
    kind: str
    physical_target: str
    primitive_kind: str
    dispatch_location: str | None
    status: str
    executed: bool = False
    messages: tuple[str, ...] = ()
    result: Any = None
    started_at: str | None = None
    finished_at: str | None = None
    #: The rows, when a caller asked for one validation by name or by file.
    #: Excluded from :meth:`to_mapping`; diagnostics are not durable report data.
    diagnostics: Any = field(default=None, compare=False, repr=False)

    @property
    def succeeded(self) -> bool:
        return self.status in (PASSED, PLANNED)

    @classmethod
    def from_mapping(cls, payload: dict[str, Any]) -> "ValidationNodeReport":
        """Read one node report from a transport mapping.

        Transported reports include durable counts but not diagnostic rows.
        """

        from .runtime.validation_result import AssumptionResult, TestResult

        kind = str(payload.get("kind") or "").strip()
        result: Any = None
        if "violation_count" in payload:
            result = AssumptionResult(
                violation_count=int(payload["violation_count"] or 0),
                error_message=payload.get("error_message"),
            )
        elif "missing_count" in payload:
            result = TestResult(
                missing_count=int(payload["missing_count"] or 0),
                unexpected_count=int(payload["unexpected_count"] or 0),
                error_message=payload.get("error_message"),
            )
        return cls(
            logical_id=payload["logical_id"],
            kind=kind.title(),
            physical_target=payload["physical_target"],
            primitive_kind=payload["primitive_kind"],
            dispatch_location=payload.get("dispatch_location"),
            status=payload["status"],
            executed=bool(payload.get("executed")),
            messages=tuple(payload.get("messages") or ()),
            result=result,
            started_at=payload.get("started_at"),
            finished_at=payload.get("finished_at"),
        )

    def to_mapping(self) -> dict[str, Any]:
        mapping: dict[str, Any] = {
            "logical_id": self.logical_id,
            "kind": self.kind.casefold(),
            "physical_target": self.physical_target,
            "primitive_kind": self.primitive_kind,
            "dispatch_location": self.dispatch_location,
            "status": self.status,
            "executed": self.executed,
            "messages": list(self.messages),
            "started_at": self.started_at,
            "finished_at": self.finished_at,
        }
        if self.result is not None:
            # A validation that could not be evaluated carries a dispatch
            # failure rather than a validation result, and a dispatch failure
            # answers with a row rather than a mapping. Both are serialisable,
            # so the report says what happened either way — an invalid node
            # used to crash the serialiser instead.
            shape = getattr(self.result, "to_mapping", None) or self.result.as_row
            mapping.update(shape())
        return mapping


@dataclass(frozen=True)
class ValidationRunReport:
    """One whole run: every validation, and what the run as a whole did."""

    status: str
    nodes: tuple[ValidationNodeReport, ...] = ()
    workflow_id: str | None = None
    started_at: str | None = None
    finished_at: str | None = None

    @property
    def succeeded(self) -> bool:
        """Return whether no validation failed or was invalid.

        A planned dry run is successful because it dispatches no validation.
        """

        return self.status in (PASSED, PLANNED)

    @property
    def failed_nodes(self) -> tuple[ValidationNodeReport, ...]:
        return tuple(node for node in self.nodes if node.status == FAILED)

    @property
    def invalid_nodes(self) -> tuple[ValidationNodeReport, ...]:
        return tuple(node for node in self.nodes if node.status == INVALID)

    def node(self, name: str) -> ValidationNodeReport:
        """One node by its logical ``Schema.Object``, for an interactive caller."""

        for node in self.nodes:
            if node.logical_id.rsplit("/", 1)[-1].casefold() == name.casefold():
                return node
        raise KeyError(name)

    def totals(self) -> dict[str, int]:
        """Return the report's physical discrepancy and violation counts."""

        missing = unexpected = violations = 0
        for node in self.nodes:
            result = node.result
            if result is None:
                continue
            missing += getattr(result, "missing_count", 0)
            unexpected += getattr(result, "unexpected_count", 0)
            violations += getattr(result, "violation_count", 0)
        return {
            "planned": len(self.nodes),
            "executed": sum(1 for node in self.nodes if node.executed),
            "passed": sum(1 for node in self.nodes if node.status == PASSED),
            "failed": len(self.failed_nodes),
            "invalid": len(self.invalid_nodes),
            "missing_count": missing,
            "unexpected_count": unexpected,
            "violation_count": violations,
        }

    @classmethod
    def from_mapping(cls, payload: dict[str, Any]) -> "ValidationRunReport":
        return cls(
            status=payload["status"],
            nodes=tuple(
                ValidationNodeReport.from_mapping(node)
                for node in payload.get("nodes") or ()
            ),
            workflow_id=payload.get("workflow_id"),
            started_at=payload.get("started_at"),
            finished_at=payload.get("finished_at"),
        )

    def to_mapping(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "nodes": [node.to_mapping() for node in self.nodes],
            "totals": self.totals(),
            "workflow_id": self.workflow_id,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
        }


def run_status(nodes: Sequence[ValidationNodeReport]) -> str:
    """Return the run status from node statuses, with invalid taking priority."""

    statuses = {node.status for node in nodes}
    if INVALID in statuses:
        return INVALID
    if FAILED in statuses:
        return FAILED
    if statuses == {PLANNED}:
        return PLANNED
    return PASSED


__all__ = [
    "FAILED",
    "INVALID",
    "PASSED",
    "PLANNED",
    "STATUSES",
    "ValidationNodeReport",
    "ValidationRunReport",
    "run_status",
]
