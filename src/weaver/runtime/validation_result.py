"""Validation counts and errors that can cross runtime boundaries.

Diagnostic rows remain with interactive callers and are not persisted.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TestResult:
    """A Test's discrepancy counts or execution error."""

    #: Not a pytest test class. Weaver's Test is a data validation, and pytest's
    #: collector recognises only the name, so it would warn about every module
    #: that imports this one into a test. The same opt-out
    #: :class:`weaver.objects.Test` carries.
    __test__ = False

    missing_count: int = 0
    unexpected_count: int = 0
    error_message: str | None = None

    @property
    def failure_count(self) -> int:

        return self.missing_count + self.unexpected_count

    @property
    def succeeded(self) -> bool:
        return self.error_message is None and self.failure_count == 0

    @classmethod
    def failed_to_run(cls, message: str) -> "TestResult":

        return cls(error_message=message)

    def to_mapping(self) -> dict:
        return {
            "missing_count": self.missing_count,
            "unexpected_count": self.unexpected_count,
            "failure_count": self.failure_count,
            "error_message": self.error_message,
        }

    @classmethod
    def from_mapping(cls, mapping) -> "TestResult":

        return cls(
            missing_count=mapping.get("missing_count", 0),
            unexpected_count=mapping.get("unexpected_count", 0),
            error_message=mapping.get("error_message"),
        )


@dataclass(frozen=True)
class AssumptionResult:
    """An Assumption's violation count or execution error."""

    violation_count: int = 0
    error_message: str | None = None

    @property
    def succeeded(self) -> bool:
        return self.error_message is None and self.violation_count == 0

    @classmethod
    def failed_to_run(cls, message: str) -> "AssumptionResult":
        return cls(error_message=message)

    def to_mapping(self) -> dict:
        return {
            "violation_count": self.violation_count,
            "error_message": self.error_message,
        }

    @classmethod
    def from_mapping(cls, mapping) -> "AssumptionResult":
        return cls(
            violation_count=mapping.get("violation_count", 0),
            error_message=mapping.get("error_message"),
        )


def result_from_rows(frame, *, kind: str, collect: bool = False):
    """Build a validation result without evaluating its rows twice.

    Return diagnostics only when requested; they may contain selected data and
    must not enter durable evidence.
    """

    from ..declaration.metadata import ASSUMPTION
    from .test_compare import ACTUAL, EXPECTED, SIDE_COLUMN

    if kind == ASSUMPTION:
        if collect:
            rows = _collected(frame)
            return AssumptionResult(violation_count=len(rows)), rows
        return AssumptionResult(violation_count=int(frame.count())), None

    if collect:
        rows = _collected(frame)
        sides = [str(row[SIDE_COLUMN]) for row in rows]
        return (
            TestResult(
                missing_count=sum(1 for side in sides if side == EXPECTED),
                unexpected_count=sum(1 for side in sides if side == ACTUAL),
            ),
            rows,
        )

    # One row per side at most, so this collects counts and never evidence.
    by_side = {
        str(row[SIDE_COLUMN]): int(row["count"])
        for row in frame.groupBy(SIDE_COLUMN).count().collect()
    }
    return (
        TestResult(
            missing_count=by_side.get(EXPECTED, 0),
            unexpected_count=by_side.get(ACTUAL, 0),
        ),
        None,
    )


def _collected(frame) -> tuple:
    return tuple(
        row.asDict() if hasattr(row, "asDict") else dict(row) for row in frame.collect()
    )


__all__ = ["AssumptionResult", "TestResult", "result_from_rows"]
