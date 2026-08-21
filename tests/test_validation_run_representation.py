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
from support.weaver_test import weaver_test

from weaver.declaration.metadata import ObjectId
from weaver.declaration.model import WeaverItemId
from weaver.load_plan import PhysicalTargetRef
from weaver.runtime.validation_result import AssumptionResult, TestResult
from weaver.test_execution import (
    PYTHON_VALIDATION,
    WAREHOUSE_PROCEDURE,
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


def _validation(
    kind="Test", *, item=WAREHOUSE, target=WAREHOUSE_TARGET, installed=True
):
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


class _Session:
    """The Session capabilities a validation reaches its engine through."""

    def __init__(self, executor=None):
        self._executor = executor

    def sql_executor(self, target, workspace=None):
        return self._executor

    def resolver(self, workspace=None):
        return None

    def spark(self, workspace=None):
        return None


def _run(validation, executor=None, *, collect=False):
    """One installed validation, run the way the Runner dispatches one.

    What comes back is the judgement the validation made. Turning that into a
    status is the Runner's, and turning *that* into `passed` or `failed` is the
    test operation's projection — three steps that used to be one function.
    """

    from weaver.test_execution import run_installed_validation

    return run_installed_validation(
        validation, session=_Session(executor), collect_diagnostics=collect
    )


def _graph_of(validation):
    """A one-node graph over an installed validation, as selection would build."""

    from weaver.run.graph import RunGraph, RunNode

    return RunGraph(
        nodes=(
            RunNode(
                node_id=str(validation.logical),
                physical_target=validation.target,
                primitive_kind=primitive_kind(validation),
                logical_id=validation.logical,
                role=validation.kind,
                installed=validation,
            ),
        ),
        requested=(validation.target,),
    )


def _ran(validation, executor=None, *, collect=False):
    """The same run, rendered as a validation node — status vocabulary and all."""

    from types import SimpleNamespace

    from weaver.operations.test import _as_validation_node
    from weaver.run.outcome import settle

    node = SimpleNamespace(
        node_id=str(validation.logical),
        primitive_kind=primitive_kind(validation),
        physical_target=validation.target,
        logical_id=validation.logical,
        role=validation.kind,
        installed=validation,
    )
    try:
        returned = _run(validation, executor, collect=collect)
    except Exception as exc:  # noqa: BLE001 - a failed validation is a result
        outcome = settle(node, raised=exc)
    else:
        outcome = settle(node, returned=returned)
    return _as_validation_node(
        SimpleNamespace(
            node_id=node.node_id,
            logical_id=str(validation.logical),
            physical_target=str(validation.target),
            primitive_kind=node.primitive_kind,
            dispatch_location=str(validation.artefact),
            role=validation.kind,
            status=outcome.status,
            raised=outcome.raised,
            executed=True,
            messages=outcome.messages,
            result=outcome.result,
            started_at=None,
            finished_at=None,
        )
    )


@weaver_test()
def test_a_lakehouse_run_requires_an_environment_before_reading_the_catalogue():
    """A desktop run cannot dispatch Lakehouse code without an Environment."""

    from weaver.errors import CommandError
    from weaver.operations.test import run_test
    from weaver.sessions.testing import TestSession
    from weaver.workspaces import Workspace

    workspace = Workspace(workspace="Analytics")
    with TestSession(workspace=workspace) as session:
        with pytest.raises(
            CommandError,
            match="Lakehouse/Sales_LH requires a Fabric Environment with Weaver installed",
        ):
            run_test(
                session,
                workspace=workspace,
                requested=(LAKEHOUSE_TARGET,),
            )

        assert session.calls == []


@pytest.mark.parametrize(
    ("kind", "result_type"),
    [("Test", TestResult), ("Assumption", AssumptionResult)],
)
@weaver_test()
def test_a_dispatch_error_is_projected_into_the_validation_result_kind(
    kind, result_type
):
    """A report of an invalid validation always retains its own result shape."""

    from types import SimpleNamespace

    from weaver.operations.test import _as_validation_node
    from weaver.run.result import FAILED as RUN_FAILED
    from weaver.run.result import RunFailure

    node = _as_validation_node(
        SimpleNamespace(
            logical_id="Lakehouse/Sales/Sales.OrdersReconcile",
            physical_target="Lakehouse/Sales_LH",
            primitive_kind=PYTHON_VALIDATION,
            dispatch_location="remote",
            role=kind,
            status=RUN_FAILED,
            raised=True,
            executed=True,
            messages=(),
            result=RunFailure("LivyError: session unavailable"),
            started_at=None,
            finished_at=None,
        )
    )

    assert node.status == INVALID
    assert isinstance(node.result, result_type)
    assert node.result.error_message == "LivyError: session unavailable"


# --- which primitive ----------------------------------------------------------


@weaver_test()
def test_a_warehouse_validation_is_reached_as_a_procedure():
    assert primitive_kind(_validation()) == WAREHOUSE_PROCEDURE


@weaver_test()
def test_a_lakehouse_validation_is_reached_as_a_module():
    validation = _validation(item=LAKEHOUSE, target=LAKEHOUSE_TARGET)

    assert primitive_kind(validation) == PYTHON_VALIDATION


# --- what a result means ------------------------------------------------------


@weaver_test()
def test_a_test_with_no_discrepancies_passes():
    executor = _Executor({"missing_count": 0, "unexpected_count": 0})

    node = _ran(_validation(), executor)

    assert node.status == PASSED
    assert node.result.failure_count == 0


@weaver_test()
def test_a_test_with_discrepancies_fails_and_carries_both_counts():
    executor = _Executor({"missing_count": 2, "unexpected_count": 3})

    node = _ran(_validation(), executor)

    assert node.status == FAILED
    assert (node.result.missing_count, node.result.unexpected_count) == (2, 3)
    assert node.result.failure_count == 5


@weaver_test()
def test_an_assumption_reads_its_own_count():
    executor = _Executor({"violation_count": 4})

    node = _ran(_validation("Assumption"), executor)

    assert node.status == FAILED
    assert node.result.violation_count == 4


# --- failing is not the same as not running -----------------------------------


@weaver_test()
def test_a_missing_primitive_is_invalid_rather_than_passing():
    """A Test that was never installed must not read as a Test that found nothing."""

    node = _ran(_validation(installed=False), _Executor({}))

    assert node.status == INVALID
    assert node.result.failure_count == 0
    assert node.result.error_message
    assert not node.result.succeeded


@weaver_test()
def test_an_execution_failure_is_invalid_and_says_why():
    class _Broken(_Executor):
        def call_procedure(self, procedure, *, inputs=(), outputs=()):
            raise RuntimeError("the declared Primary key repeats")

    node = _ran(_validation(), _Broken({}))

    assert node.status == INVALID
    assert "repeats" in node.result.error_message
    assert node.messages


@weaver_test()
def test_no_sql_capability_is_reported_against_the_validation():
    node = _ran(_validation(), None)

    assert node.status == INVALID
    assert "no SQL capability" in node.result.error_message


# --- one failure does not stop the rest ---------------------------------------


@weaver_test()
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

    alternating = _Alternating()
    nodes = tuple(_ran(_validation(), alternating) for _ in range(2))

    assert [node.status for node in nodes] == [INVALID, PASSED]


# --- suppression --------------------------------------------------------------


@weaver_test()
def test_a_whole_target_run_asks_the_procedure_to_stay_quiet():
    executor = _Executor({"missing_count": 0, "unexpected_count": 0})

    _ran(_validation(), executor)

    assert executor.calls[0][1] == (("suppress_result_set", 1),)


@weaver_test()
def test_a_targeted_run_asks_for_the_rows():
    executor = _Executor(
        {"missing_count": 1, "unexpected_count": 0},
        result_sets=(({"_weaver_side": "expected", "OrderId": 1},),),
    )

    node = _ran(_validation(), executor, collect=True)

    assert executor.calls[0][1] == (("suppress_result_set", 0),)
    assert node.diagnostics == ({"_weaver_side": "expected", "OrderId": 1},)


@weaver_test()
def test_a_targeted_run_executes_the_procedure_exactly_once():
    """Twice would compare data that could have changed in between."""

    executor = _Executor(
        {"missing_count": 1, "unexpected_count": 0},
        result_sets=(({"_weaver_side": "expected", "OrderId": 1},),),
    )

    _ran(_validation(), executor, collect=True)

    assert len(executor.calls) == 1


class _CountingFrame:
    """A frame that answers an aggregation and refuses to hand over rows.

    Two claims in one object, and both are about what the code must *not* do.
    ``collect`` on the frame itself raises, so a suppressed run that
    materialised diagnostic rows fails loudly rather than quietly pulling a
    million of them to the driver. And every action is counted, so a run that
    evaluated the comparison twice — once per side — fails too: two actions can
    observe different data if the tables move between them, and the two halves
    of one Test's answer would then describe different estates.
    """

    def __init__(self, by_side):
        self.by_side = dict(by_side)
        self.actions = 0

    def groupBy(self, column):  # noqa: N802 - Spark's own spelling
        assert column == "_weaver_side"
        return _Grouped(self)

    def count(self):
        self.actions += 1
        return sum(self.by_side.values())

    def where(self, _predicate):
        raise AssertionError(
            "a suppressed run must aggregate by side, not count each side"
        )

    def collect(self):
        raise AssertionError("a suppressed run must not collect diagnostic rows")


class _Grouped:
    def __init__(self, frame):
        self.frame = frame

    def count(self):
        return _Aggregated(self.frame)


class _Aggregated:
    def __init__(self, frame):
        self.frame = frame

    def collect(self):
        self.frame.actions += 1
        return [
            {"_weaver_side": side, "count": n} for side, n in self.frame.by_side.items()
        ]


@weaver_test()
def test_a_suppressed_spark_run_never_materialises_a_row():
    """The claim is about what is *not* done, so the frame refuses to be collected."""

    from weaver.test_execution import _dispatch_python

    _Frame = _CountingFrame
    frame = _CountingFrame({"expected": 1, "actual": 1})

    class _Class:
        def __init__(self, spark, lakehouse=None):
            pass

        def read(self):
            return frame

    class _Module:
        Sales__OrdersReconcile = _Class

    import weaver.test_execution as execution

    validation = _validation(item=LAKEHOUSE, target=LAKEHOUSE_TARGET)

    class _Scope:
        def context_for(self, **_kwargs):
            return object()

    from types import SimpleNamespace

    # The capabilities a Python validation reaches through, as the Session
    # supplies them: this run's Spark, and this run's runtime scope.
    environment = SimpleNamespace(spark=object(), resolver=None, runtime_scope=_Scope())

    class _Lakehouse:
        def files_root(self):
            return "/tmp/files"

    execution.__dict__.get("lakehouse_for")
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

    assert result.missing_count == 1
    assert result.unexpected_count == 1
    assert diagnostics is None
    # One action for the whole comparison, not one per side.
    assert frame.actions == 1


# --- dry run ------------------------------------------------------------------


@weaver_test()
def test_a_dry_run_dispatches_nothing():
    executor = _Executor({"missing_count": 9, "unexpected_count": 9})

    from weaver.catalogue.state import Catalogue
    from weaver.run import Runner, RunRequest, RunState

    validation = _validation()
    runner = Runner(
        RunState(catalogue=Catalogue(rows={})),
        RunRequest.test((validation.target,), dry_run=True),
    )
    runner._graph = _graph_of(validation)

    from weaver.operations.test import _as_validation_node

    result = runner.run()
    node = _as_validation_node(result.nodes[0])

    assert node.status == PLANNED
    assert not node.executed
    assert executor.calls == [], "a dry run asks the engine nothing"


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
@weaver_test()
def test_the_run_takes_the_worst_status(statuses, expected):
    assert run_status([_node(status) for status in statuses]) == expected


@weaver_test()
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


@weaver_test()
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


@weaver_test()
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


# --- running a source file, without installing it ------------------------------


class _BatchExecutor:
    """A Warehouse batch that returns evidence *and then* its projection.

    Which is what the generated validation batch does, and the shape that made
    the first implementation wrong: it read the first result set, found a
    diagnostic row with no count column on it, and reported a failing Test as
    passing.
    """

    def __init__(self, result_sets):
        self.result_sets = tuple(result_sets)
        self.statements: list[str] = []

    def query(self, statement, parameters=None):
        raise AssertionError(
            "a batch returning evidence and counts must not be read one set at a time"
        )

    def query_result_sets(self, statement, parameters=None):
        self.statements.append(statement)
        return self.result_sets


class _FileSession:
    """The Session capabilities a source-file run reaches its engine through."""

    def __init__(self, executor):
        self._executor = executor

    def sql_executor(self, target, workspace=None):
        return self._executor

    def spark(self, workspace=None):
        return None

    def resolver(self, workspace=None):
        return None


def _source(tmp_path, name, text):
    path = tmp_path / name
    path.write_text(text, encoding="utf-8")
    return path


FILE_TEST = """/*
Test ID: Sales.OrdersReconcile

Description: A Test run from source.

Primary key: OrderId
*/
select OrderId from Sales.Expected;

select OrderId from Sales.Orders;
"""


def _file_node(tmp_path, executor, **kwargs):
    from datetime import datetime, timezone

    from weaver.test_file import source_file_node

    return source_file_node(
        _FileSession(executor),
        requested=[WAREHOUSE_TARGET],
        path=_source(tmp_path, "Sales.OrdersReconcile.sql", FILE_TEST),
        started=datetime.now(timezone.utc),
        **kwargs,
    )


@weaver_test()
def test_a_failing_source_test_reports_its_counts_not_a_diagnostic_row(tmp_path):
    """The regression: counts come from the projection, never the evidence."""

    executor = _BatchExecutor(
        (
            # The diagnostics the batch emits first...
            (
                {"_weaver_side": "expected", "_weaver_sk": 1, "OrderId": 7},
                {"_weaver_side": "actual", "_weaver_sk": 2, "OrderId": 9},
            ),
            # ...and the projection of its locals, last.
            ({"missing_count": 1, "unexpected_count": 1},),
        )
    )

    node = _file_node(tmp_path, executor)

    assert node.status == FAILED
    assert node.result.missing_count == 1
    assert node.result.unexpected_count == 1


@weaver_test()
def test_a_source_run_returns_the_evidence_it_produced(tmp_path):
    executor = _BatchExecutor(
        (
            ({"_weaver_side": "expected", "_weaver_sk": 1, "OrderId": 7},),
            ({"missing_count": 1, "unexpected_count": 0},),
        )
    )

    node = _file_node(tmp_path, executor)

    assert [row["OrderId"] for row in node.diagnostics] == [7]


@weaver_test()
def test_a_passing_source_test_has_only_its_projection(tmp_path):
    executor = _BatchExecutor((({"missing_count": 0, "unexpected_count": 0},),))

    node = _file_node(tmp_path, executor)

    assert node.status == PASSED
    assert node.diagnostics == ()


@weaver_test()
def test_a_source_dry_run_compiles_and_dispatches_nothing(tmp_path):
    """What *would* run — so the file is still compiled, and nothing executed."""

    executor = _BatchExecutor(())

    node = _file_node(tmp_path, executor, dry_run=True)

    assert node.status == PLANNED
    assert not node.executed
    assert node.logical_id == "Sales.OrdersReconcile"
    assert executor.statements == []
