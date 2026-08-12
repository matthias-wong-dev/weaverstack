"""What a validation run reports, as numbers.

Deliberately scalar and transport-neutral. A Test's evidence is rows, and rows
are what a person looks at; what a *run* records is how many there were. The two
are separated here because they have different lifetimes and different risks —
diagnostic rows may be large and may carry sensitive business data, so they are
returned to an interactive caller and never persisted, while the counts are
small, safe and the whole of what a task log needs.

.. code-block:: text

    TestResult         missing_count + unexpected_count = failure_count
    AssumptionResult   violation_count

**Zero is not the only way to succeed, and it is not the only way to be
finished.** A validation that could not be evaluated has no counts at all, and
saying it found nothing would report a broken Test as a passing one. So
``error_message`` is what separates them, and ``succeeded`` is false whenever it
is set, whatever the counts say.
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
        """The physical discrepancy rows, which is what a Test counts.

        One changed entity contributes two — an expected-side row and an
        actual-side row — deliberately. See :mod:`weaver.runtime.test_compare`.
        """

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
        """The same judgement, rebuilt where the run is being decided.

        ``failure_count`` is derived and deliberately not read back: it is in
        the mapping for a reader, and taking it as input would let a result
        arrive disagreeing with its own two counts.
        """

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
