"""Orchestration decisions, made without a session, a target or a session.

Everything a validation run *decides* rather than executes: which primitive to
reach for, what a failure means, what reaches a durable record. A fake dispatch
surface answers, so these are microseconds and cover the cases a real estate
would be tedious to put into — a duplicate key, a missing primitive, a procedure
that must be executed exactly once.

Two of these assert what the code does **not** do, and they are the ones worth
keeping honest. A suppressed run must never materialise a diagnostic row, so the
fake frame's ``collect`` raises. And a targeted run must not execute a Test
twice to get both counts and rows, so the fake executor counts its calls.
"""

from __future__ import annotations

import pytest

from weaver.declaration.metadata import ObjectId
from weaver.declaration.model import WeaverItemId
from weaver.load_plan import PhysicalTargetRef
from weaver.runtime.validation_result import AssumptionResult, TestResult
from weaver.test_execution import (
    PYTHON_VALIDATION,
    WAREHOUSE_PROCEDURE,
    execute_validation,
    execute_validations,
    primitive_kind,
)
from weaver.test_plan import InstalledValidation
from weaver.test_report import (
    FAILED,
    INVALID,
    PASSED,
    PLANNED,
    ValidationNodeReport,
    ValidationRunReport,
    run_status,
)

LAKEHOUSE = WeaverItemId.parse("Lakehouse/Sales")
WAREHOUSE = WeaverItemId.parse("Warehouse/Reporting")
LAKEHOUSE_TARGET = PhysicalTargetRef(kind="lakehouse", name="Sales_LH")
WAREHOUSE_TARGET = PhysicalTargetRef(kind="warehouse", name="Reporting_WH")


def _validation(kind="Test", *, item=WAREHOUSE, target=WAREHOUSE_TARGET, installed=True):
    from weaver.etl import validation_artefact_id

    object_id = ObjectId(schema="Sales", object="OrdersReconcile")
    from weaver.declaration.model import WeaverDocumentId

    return InstalledValidation(
        logical=WeaverDocumentId(item, object_id),
        kind=kind,
        target=target,
        artefact=validation_artefact_id(item, kind, object_id),
        object_type=("stored_procedure" if item == WAREHOUSE else "file")
        if installed
        else None,
        primary_key=("OrderId",),
    )


class _Executor:
    """A Warehouse that answers, and counts how often it was asked."""

    def __init__(self, outputs, result_sets=()):
        self.outputs = outputs
        self.result_sets = tuple(result_sets)
        self.calls: list[tuple[str, tuple]] = []

    def call_procedure(self, procedure, *, inputs=(), outputs=()):
        self.calls.append((procedure, tuple(inputs)))
        return dict(self.outputs)

    def call_procedure_with_results(self, procedure, *, inputs=(), outputs=()):
        from weaver.sql.execution import ProcedureResult

        self.calls.append((procedure, tuple(inputs)))
        return ProcedureResult(outputs=dict(self.outputs), result_sets=self.result_sets)


class _Environment:
    def __init__(self, executor=None):
        self._executor = executor
        self.spark = None
        self.resolver = None

    def sql_for(self, target):
        return self._executor


# --- which primitive ----------------------------------------------------------


def test_a_warehouse_validation_is_reached_as_a_procedure():
    assert primitive_kind(_validation()) == WAREHOUSE_PROCEDURE


def test_a_lakehouse_validation_is_reached_as_a_module():
    validation = _validation(item=LAKEHOUSE, target=LAKEHOUSE_TARGET)

    assert primitive_kind(validation) == PYTHON_VALIDATION


# --- what a result means ------------------------------------------------------


def test_a_test_with_no_discrepancies_passes():
    executor = _Executor({"missing_count": 0, "unexpected_count": 0})

    node = execute_validation(_validation(), environment=_Environment(executor))

    assert node.status == PASSED
    assert node.result.failure_count == 0


def test_a_test_with_discrepancies_fails_and_carries_both_counts():
    executor = _Executor({"missing_count": 2, "unexpected_count": 3})

    node = execute_validation(_validation(), environment=_Environment(executor))

    assert node.status == FAILED
    assert (node.result.missing_count, node.result.unexpected_count) == (2, 3)
    assert node.result.failure_count == 5


def test_an_assumption_reads_its_own_count():
    executor = _Executor({"violation_count": 4})

    node = execute_validation(
        _validation("Assumption"), environment=_Environment(executor)
    )

    assert node.status == FAILED
    assert node.result.violation_count == 4


# --- failing is not the same as not running -----------------------------------


def test_a_missing_primitive_is_invalid_rather_than_passing():
    """A Test that was never installed must not read as a Test that found nothing."""

    node = execute_validation(
        _validation(installed=False), environment=_Environment(_Executor({}))
    )

    assert node.status == INVALID
    assert node.result.failure_count == 0
    assert node.result.error_message
    assert not node.result.succeeded


def test_an_execution_failure_is_invalid_and_says_why():
    class _Broken(_Executor):
        def call_procedure(self, procedure, *, inputs=(), outputs=()):
            raise RuntimeError("the declared Primary key repeats")

    node = execute_validation(_validation(), environment=_Environment(_Broken({})))

    assert node.status == INVALID
    assert "repeats" in node.result.error_message
    assert node.messages


def test_no_sql_capability_is_reported_against_the_validation():
    node = execute_validation(_validation(), environment=_Environment(None))

    assert node.status == INVALID
    assert "no SQL capability" in node.result.error_message


# --- one failure does not stop the rest ---------------------------------------


def test_every_validation_reports_even_after_one_fails():
    class _Alternating(_Executor):
        def __init__(self):
            super().__init__({"missing_count": 0, "unexpected_count": 0})
            self.seen = 0

        def call_procedure(self, procedure, *, inputs=(), outputs=()):
            self.seen += 1
            if self.seen == 1:
                raise RuntimeError("nope")
            return {"missing_count": 0, "unexpected_count": 0}

    nodes = execute_validations(
        (_validation(), _validation()), environment=_Environment(_Alternating())
    )

    assert [node.status for node in nodes] == [INVALID, PASSED]


# --- suppression --------------------------------------------------------------


def test_a_whole_target_run_asks_the_procedure_to_stay_quiet():
    executor = _Executor({"missing_count": 0, "unexpected_count": 0})

    execute_validation(_validation(), environment=_Environment(executor))

    assert executor.calls[0][1] == (("suppress_result_set", 1),)


def test_a_targeted_run_asks_for_the_rows():
    executor = _Executor(
        {"missing_count": 1, "unexpected_count": 0},
        result_sets=(({"_weaver_side": "expected", "OrderId": 1},),),
    )

    node = execute_validation(
        _validation(), environment=_Environment(executor), collect_diagnostics=True
    )

    assert executor.calls[0][1] == (("suppress_result_set", 0),)
    assert node.diagnostics == ({"_weaver_side": "expected", "OrderId": 1},)


def test_a_targeted_run_executes_the_procedure_exactly_once():
    """Twice would compare data that could have changed in between."""

    executor = _Executor(
        {"missing_count": 1, "unexpected_count": 0},
        result_sets=(({"_weaver_side": "expected", "OrderId": 1},),),
    )

    execute_validation(
        _validation(), environment=_Environment(executor), collect_diagnostics=True
    )

    assert len(executor.calls) == 1


def test_a_suppressed_spark_run_never_materialises_a_row():
    """The claim is about what is *not* done, so the frame refuses to be collected."""

    from weaver.test_execution import _dispatch_python

    class _Frame:
        def __init__(self, n):
            self.n = n

        def count(self):
            return self.n

        def where(self, _predicate):
            return _Frame(1)

        def collect(self):
            raise AssertionError("a suppressed run must not collect diagnostic rows")

    class _Class:
        def __init__(self, spark, lakehouse=None):
            pass

        def read(self):
            return _Frame(2)

    class _Module:
        Sales__OrdersReconcile = _Class

    import weaver.test_execution as execution

    validation = _validation(item=LAKEHOUSE, target=LAKEHOUSE_TARGET)
    environment = _Environment()
    environment.spark = object()

    class _Scope:
        def context_for(self, **_kwargs):
            return object()

    environment.runtime_scope = _Scope()

    class _Lakehouse:
        def files_root(self):
            return "/tmp/files"

    original_lakehouse = execution.__dict__.get("lakehouse_for")
    import weaver.lakehouse as lakehouse_module
    import weaver.runtime.python_context as context_module

    real_lakehouse_for = lakehouse_module.lakehouse_for
    real_import = context_module.import_deployed_module
    lakehouse_module.lakehouse_for = lambda *_a, **_k: _Lakehouse()
    context_module.import_deployed_module = lambda *_a, **_k: _Module()
    try:
        result, diagnostics = _dispatch_python(validation, environment, False)
    finally:
        lakehouse_module.lakehouse_for = real_lakehouse_for
        context_module.import_deployed_module = real_import

    assert result.failure_count == 2
    assert diagnostics is None


# --- dry run ------------------------------------------------------------------


def test_a_dry_run_dispatches_nothing():
    executor = _Executor({"missing_count": 9, "unexpected_count": 9})

    node = execute_validation(
        _validation(), environment=_Environment(executor), dry_run=True
    )

    assert node.status == PLANNED
    assert not node.executed
    assert executor.calls == []


# --- the run's own status -----------------------------------------------------


def _node(status, result=None):
    return ValidationNodeReport(
        logical_id="Lakehouse/Sales/Sales.X",
        kind="Test",
        physical_target="Lakehouse/Sales_LH",
        primitive_kind=PYTHON_VALIDATION,
        dispatch_location="d",
        status=status,
        executed=status != PLANNED,
        result=result,
    )


@pytest.mark.parametrize(
    "statuses,expected",
    [
        ([PASSED, PASSED], PASSED),
        ([PASSED, FAILED], FAILED),
        ([PASSED, INVALID], INVALID),
        # Worst first: an estate that could not answer is not an estate that passed.
        ([FAILED, INVALID], INVALID),
        ([PLANNED, PLANNED], PLANNED),
    ],
)
def test_the_run_takes_the_worst_status(statuses, expected):
    assert run_status([_node(status) for status in statuses]) == expected


def test_the_totals_aggregate_physical_counts():
    report = ValidationRunReport(
        status=FAILED,
        nodes=(
            _node(FAILED, TestResult(missing_count=2, unexpected_count=3)),
            _node(FAILED, AssumptionResult(violation_count=4)),
            _node(PASSED, TestResult()),
        ),
    )

    assert report.totals() == {
        "planned": 3,
        "executed": 3,
        "passed": 1,
        "failed": 2,
        "invalid": 0,
        "missing_count": 2,
        "unexpected_count": 3,
        "violation_count": 4,
    }


def test_a_node_mapping_carries_counts_and_never_rows():
    """What a task log and a transported report are allowed to hold."""

    node = ValidationNodeReport(
        logical_id="Lakehouse/Sales/Sales.X",
        kind="Test",
        physical_target="Lakehouse/Sales_LH",
        primitive_kind=PYTHON_VALIDATION,
        dispatch_location="d",
        status=FAILED,
        executed=True,
        result=TestResult(missing_count=1, unexpected_count=1),
        diagnostics=({"_weaver_side": "expected", "_weaver_sk": 1, "OrderId": 7},),
    )

    mapping = node.to_mapping()

    assert mapping["missing_count"] == 1
    assert mapping["failure_count"] == 2
    assert "diagnostics" not in mapping
    assert "_weaver_sk" not in str(mapping)


def test_a_report_survives_a_transport_round_trip_without_its_rows():
    node = ValidationNodeReport(
        logical_id="Lakehouse/Sales/Sales.X",
        kind="Assumption",
        physical_target="Lakehouse/Sales_LH",
        primitive_kind=PYTHON_VALIDATION,
        dispatch_location="d",
        status=FAILED,
        executed=True,
        result=AssumptionResult(violation_count=3),
        diagnostics=({"OrderId": 7},),
    )
    report = ValidationRunReport(status=FAILED, nodes=(node,))

    back = ValidationRunReport.from_mapping(report.to_mapping())

    assert back.nodes[0].kind == "Assumption"
    assert back.nodes[0].result.violation_count == 3
    assert back.nodes[0].diagnostics is None
