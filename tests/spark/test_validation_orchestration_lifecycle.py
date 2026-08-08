"""``weaver.test`` end to end: build, load, then validate what was loaded.

The whole path, through a real session against real Delta — build the estate,
load it, run every installed validation, and read the counts and the task log
back. Nothing is faked: the catalogue is the one the build wrote, the modules
are the ones it deployed, and the rows are the ones the load put there.

What this proves that the layers below cannot:

- validation runs against **loaded** data, so a passing Test means the load
  agreed with the Test rather than that both were empty
- a run reads the *installed* catalogue and never reopens the repository
- the task log records counts and no discrepancy row ever reaches it
"""

from __future__ import annotations

import json

import pytest

from weaver.build_bundle import (
    InstallationEnvironment,
    ItemBinding,
    ItemBindings,
    LakehouseBinding,
    build_uploaded_item_repository,
    effective_item_bindings,
)
from weaver.declaration.model import WeaverItemId
from weaver.load import LoadSession
from weaver.load_plan import PhysicalTargetRef
from weaver.locations import Location
from weaver.spark import SparkCatalogue
from weaver.test import run_test

pytestmark = pytest.mark.spark

ITEM = "Lakehouse/Sales"

SCHEMA = "Schema ID: Sales\nDescription: Sales objects.\n"

SEED = '''"""
Folder ID: Sales.Seed

Description: The source rows.

Lineage: A source system.

File key: "*.csv"
"""
from weaver import Folder


class Sales__Seed(Folder):
    def read(self):
        with self.staging_folder() as staging:
            (staging.path / "orders.csv").write_text(
                "OrderId,Amount\\n1,100\\n2,200\\n", encoding="utf-8"
            )
        return staging, []
'''

ORDER = '''"""
Table ID: Sales.Order

Description: One row per order.

Lineage: The seeded order export.

Primary key: OrderId

Schema:
  OrderId: int
  Amount: int
"""
from weaver import Folder, Table

from Files.Sales__Seed import Sales__Seed


class Sales__Order(Table):
    def read(self):
        frame = self.spark.read.option("header", True).csv(
            str(Sales__Seed(self).path())
        )
        return frame.selectExpr("cast(OrderId as int) OrderId", "cast(Amount as int) Amount"), None
'''

RECONCILE = '''"""
Test ID: Sales.OrdersReconcile

Description: The loaded orders are exactly the two the source carries.

Primary key: OrderId
"""
from weaver import Test

from Sales__Order import Sales__Order


class Sales__OrdersReconcile(Test):
    def expected(self):
        return self.spark.createDataFrame(
            [(1, 100), (2, 200)], "OrderId int, Amount int"
        )

    def actual(self):
        return Sales__Order(self).dataframe().selectExpr("OrderId", "Amount")
'''

POSITIVE = """/*
Assumption ID: Sales.AmountsArePositive

Description: Every order carries a positive amount.
*/
select OrderId, Amount from Sales.Order where Amount <= 0;
"""

FAILING = '''"""
Test ID: Sales.OrdersMiscount

Description: A Test the loaded data does not satisfy, on purpose.

Primary key: OrderId
"""
from weaver import Test

from Sales__Order import Sales__Order


class Sales__OrdersMiscount(Test):
    def expected(self):
        return self.spark.createDataFrame(
            [(1, 100), (3, 300)], "OrderId int, Amount int"
        )

    def actual(self):
        return Sales__Order(self).dataframe().selectExpr("OrderId", "Amount")
'''


def _write(root, relative: str, text: str) -> None:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _failures(report) -> str:
    return "\n".join(
        f"{action.action_id}: {action.error_type}: {action.error_message}"
        for sequence in report.sequences
        for action in sequence.actions
        if action.status == "failed"
    )


@pytest.fixture
def validated(tmp_path, lakehouses, spark, weaver_catalogue):
    """An estate built, loaded, and ready to be asked whether it holds up."""

    root = tmp_path / "Estate"
    _write(root, "Lakehouse/Sales/schemas/Sales.yml", SCHEMA)
    _write(root, "Lakehouse/Sales/Files/Sales__Seed.py", SEED)
    _write(root, "Lakehouse/Sales/Sales__Order.py", ORDER)
    _write(root, "Lakehouse/Sales/tests/Sales__OrdersReconcile.py", RECONCILE)
    _write(root, "Lakehouse/Sales/tests/Sales__OrdersMiscount.py", FAILING)
    _write(root, "Lakehouse/Sales/assumptions/Sales.AmountsArePositive.sql", POSITIVE)

    target = SparkCatalogue(
        spark, lakehouses.resolver.spark_destination(lakehouses.target)
    )
    selected = ItemBindings(
        (ItemBinding(WeaverItemId.parse(ITEM), LakehouseBinding(lakehouses.target)),)
    )
    try:
        built = build_uploaded_item_repository(
            Location(str(root)),
            bindings=effective_item_bindings(
                selected, weaver_lakehouse=lakehouses.weaver.name
            ),
            environment=InstallationEnvironment(
                store=lakehouses.store,
                resolver=lakehouses.resolver,
                spark=spark,
                workspace=lakehouses.workspace,
            ),
            control_lakehouse=LakehouseBinding(lakehouses.weaver),
        )
        assert built.report.status == "succeeded", _failures(built.report)

        requested = (PhysicalTargetRef("lakehouse", lakehouses.target.name),)
        from weaver.load import run_load

        with LoadSession(
            lakehouses.workspace, requested, spark=spark, store=lakehouses.store
        ) as session:
            loaded = run_load(session, requested=requested)
        assert loaded.status in ("succeeded", "succeeded_with_rejects"), loaded.status

        yield lakehouses, requested
    finally:
        spark.sql(f"DROP SCHEMA IF EXISTS {target.qualified_schema('Sales')} CASCADE")


def _test(validated, **kwargs):
    lakehouses, requested = validated
    with LoadSession(
        lakehouses.workspace, requested, spark=lakehouses.spark, store=lakehouses.store
    ) as session:
        return run_test(session, requested=requested, **kwargs)


@pytest.fixture
def lakehouses_with_spark(lakehouses, spark):
    lakehouses.spark = spark
    return lakehouses


def run(validated, spark, **kwargs):
    lakehouses, requested = validated
    with LoadSession(
        lakehouses.workspace, requested, spark=spark, store=lakehouses.store
    ) as session:
        return run_test(session, requested=requested, **kwargs)


# --- the whole target ---------------------------------------------------------


def test_every_installed_validation_runs(validated, spark):
    report = run(validated, spark)

    assert sorted(node.logical_id.rsplit("/", 1)[-1] for node in report.nodes) == [
        "Sales.AmountsArePositive",
        "Sales.OrdersMiscount",
        "Sales.OrdersReconcile",
    ]


def test_a_test_that_agrees_with_the_load_passes(validated, spark):
    """Against loaded rows, so passing means agreement rather than emptiness."""

    report = run(validated, spark)
    node = report.node("Sales.OrdersReconcile")

    assert node.status == "passed", node.messages
    assert node.result.failure_count == 0
    assert node.executed


def test_an_assumption_that_holds_passes(validated, spark):
    node = run(validated, spark).node("Sales.AmountsArePositive")

    assert node.status == "passed"
    assert node.result.violation_count == 0


def test_a_test_the_data_does_not_satisfy_fails_with_its_counts(validated, spark):
    node = run(validated, spark).node("Sales.OrdersMiscount")

    assert node.status == "failed"
    # Order 2 is unexpected, order 3 is missing; order 1 agrees.
    assert node.result.missing_count == 1
    assert node.result.unexpected_count == 1
    assert node.result.failure_count == 2


def test_one_failure_does_not_stop_the_others(validated, spark):
    """A validation is read-only, so there is nothing to protect by stopping."""

    report = run(validated, spark)

    assert all(node.executed for node in report.nodes)
    assert {node.status for node in report.nodes} == {"passed", "failed"}


def test_the_run_status_is_the_worst_node(validated, spark):
    assert run(validated, spark).status == "failed"


def test_a_whole_target_run_transfers_no_diagnostic_rows(validated, spark):
    """Suppression is about size and sensitivity, not speed."""

    report = run(validated, spark)

    assert all(node.diagnostics is None for node in report.nodes)


# --- one by name --------------------------------------------------------------


def test_naming_one_runs_only_it(validated, spark):
    report = run(validated, spark, name="Sales.OrdersMiscount")

    assert [node.logical_id.rsplit("/", 1)[-1] for node in report.nodes] == [
        "Sales.OrdersMiscount"
    ]


def test_naming_one_returns_its_evidence(validated, spark):
    """The rows, and the counts, from the same execution."""

    node = run(validated, spark, name="Sales.OrdersMiscount").nodes[0]

    assert node.result.failure_count == 2
    rows = sorted(
        (row["_weaver_side"], row["OrderId"]) for row in node.diagnostics
    )
    assert rows == [("actual", 2), ("expected", 3)]


def test_the_diagnostic_rows_carry_the_correlation_key(validated, spark):
    node = run(validated, spark, name="Sales.OrdersMiscount").nodes[0]

    assert {"_weaver_side", "_weaver_sk"} <= set(node.diagnostics[0])


def test_naming_something_absent_is_an_error(validated, spark):
    from weaver.errors import ValidationError

    with pytest.raises(ValidationError, match="no validation named"):
        run(validated, spark, name="Sales.NotThere")


# --- what a run leaves behind -------------------------------------------------


def test_the_run_writes_a_test_task_log(validated, spark):
    lakehouses, _requested = validated
    report = run(validated, spark)

    assert report.task_log
    written = sorted(
        entry.location.value.rsplit("/", 1)[-1]
        for entry in lakehouses.store.list(Location(report.task_log))
        if not entry.is_directory
    )

    assert "plan.json" in written
    # One immutable step document per validation, and one completion.
    assert sum(1 for name in written if "_test_" in name) == 2
    assert sum(1 for name in written if "_assumption_" in name) == 1
    assert sum(1 for name in written if "_complete_" in name) == 1


def test_the_task_log_records_counts_and_no_rows(validated, spark):
    """Diagnostic rows are interactive evidence, never a durable record."""

    lakehouses, _requested = validated
    report = run(validated, spark, name="Sales.OrdersMiscount")

    written = [
        json.loads(lakehouses.store.read(entry.location).decode("utf-8"))
        for entry in lakehouses.store.list(Location(report.task_log))
        if not entry.is_directory
    ]
    text = json.dumps(written)

    assert '"missing_count": 1' in text
    assert "_weaver_sk" not in text
    assert "_weaver_side" not in text


def test_the_completion_document_aggregates_the_run(validated, spark):
    lakehouses, _requested = validated
    report = run(validated, spark)

    completion = json.loads(
        lakehouses.store.read(
            next(
                entry.location
                for entry in lakehouses.store.list(Location(report.task_log))
                if "_complete_" in entry.location.value
            )
        ).decode("utf-8")
    )

    assert completion["planned"] == 3
    assert completion["passed"] == 2
    assert completion["failed"] == 1
    assert completion["missing_count"] == 1


def test_a_dry_run_dispatches_nothing_and_writes_nothing(validated, spark):
    report = run(validated, spark, dry_run=True)

    assert report.status == "planned"
    assert all(not node.executed for node in report.nodes)
    assert report.task_log is None
