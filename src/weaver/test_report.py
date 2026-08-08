"""What a validation run reports.

The sibling of :mod:`weaver.load_report`, and shaped like it so a reader who
knows one knows the other: a node report per validation, a run report over them,
and everything serialisable because the CLI prints it and the task log persists
it.

**Counts, never rows.** A node carries its scalar result and, only when a caller
explicitly asked for one validation, the diagnostic rows alongside it — held on
the report object for that caller and excluded from every mapping. Diagnostic
rows may be large and may carry sensitive business data; they are interactive
evidence, not a record.

**Failing and unrunnable are different statuses.** A Test that found
discrepancies did its job. A Test whose key repeats, or whose primitive was
never installed, did not run at all, and reporting the two alike would let a
broken estate read as a passing one.
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
    #: Deliberately absent from :meth:`to_mapping` — see the module docstring.
    diagnostics: Any = field(default=None, compare=False, repr=False)

    @property
    def succeeded(self) -> bool:
        return self.status in (PASSED, PLANNED)

    @classmethod
    def from_mapping(cls, payload: dict[str, Any]) -> "ValidationNodeReport":
        """Read one node back, whichever side of a transport wrote it.

        Diagnostics are never carried: they are not in the mapping, so a report
        that crossed a boundary has counts and no rows — which is what the
        design says a durable or transported record holds.
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
            mapping.update(self.result.to_mapping())
        return mapping


@dataclass(frozen=True)
class ValidationRunReport:
    """One whole run: every validation, and what the run as a whole did."""

    status: str
    nodes: tuple[ValidationNodeReport, ...] = ()
    task_log: str | None = None
    started_at: str | None = None
    finished_at: str | None = None

    @property
    def succeeded(self) -> bool:
        return self.status == PASSED

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
        """What a completion document aggregates.

        Physical counts throughout — the symmetric difference's own rows — and
        no manufactured logical "changed row" count, which would need a key that
        a Test is not required to have.
        """

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
            task_log=payload.get("task_log"),
            started_at=payload.get("started_at"),
            finished_at=payload.get("finished_at"),
        )

    def to_mapping(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "nodes": [node.to_mapping() for node in self.nodes],
            "totals": self.totals(),
            "task_log": self.task_log,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
        }


def run_status(nodes: Sequence[ValidationNodeReport]) -> str:
    """The run's status, from its nodes, worst first.

    A run with something unrunnable in it is invalid even if every validation
    that *did* run passed: the estate could not answer the question it was
    asked, and reporting that as a pass is the failure mode the whole status
    vocabulary exists to prevent.
    """

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
