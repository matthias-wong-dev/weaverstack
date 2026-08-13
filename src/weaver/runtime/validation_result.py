"""Transport-neutral scalar results for validation runs.

Diagnostic rows remain with interactive callers; durable reports carry counts
and an error when a validation could not be evaluated.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TestResult:
    """One Test's outcome: how much disagreed, and whether it ran at all."""

    #: Not a pytest test class. Weaver's Test is a data validation, and pytest's
    #: collector recognises only the name — so it would warn about every module
    #: that imports this one into a test. The same opt-out
    #: :class:`weaver.objects.Test` carries.
    __test__ = False

    missing_count: int = 0
    unexpected_count: int = 0
    error_message: str | None = None

    @property
    def failure_count(self) -> int:
        """Return the number of physical discrepancy rows."""

        return self.missing_count + self.unexpected_count

    @property
    def succeeded(self) -> bool:
        return self.error_message is None and self.failure_count == 0

    @classmethod
    def failed_to_run(cls, message: str) -> "TestResult":
        """A Test that could not be evaluated, which is not a Test that passed."""

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
        """Rebuild a result from its serialised counts and error."""

        return cls(
            missing_count=mapping.get("missing_count", 0),
            unexpected_count=mapping.get("unexpected_count", 0),
            error_message=mapping.get("error_message"),
        )


@dataclass(frozen=True)
class AssumptionResult:
    """One Assumption's outcome: how many rows contradicted it."""

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


__all__ = ["AssumptionResult", "TestResult"]
