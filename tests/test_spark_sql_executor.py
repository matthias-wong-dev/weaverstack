"""The Spark SQL executor applies Weaver's exact-case identity contract."""

from __future__ import annotations

import pytest

from weaver.build_bundle.executors.base import InstallationContext, ResolvedTarget
from weaver.build_bundle.executors.spark_sql import SparkSqlExecutor
from weaver.build_bundle.executors.spark_sql_batch import SparkSqlBatchExecutor
from weaver.build_bundle.models import InstallAction
from weaver.build_bundle.targets import BoundTarget
from weaver.errors import InstallError
from weaver.spark import fabric_destination, local_destination
from weaver.targets import ItemRef


class _Conf:
    def __init__(self, value: str = "false") -> None:
        self.value = value
        self.changes: list[str] = []

    def get(self, key: str) -> str:
        assert key == "spark.sql.caseSensitive"
        return self.value

    def set(self, key: str, value: str) -> None:
        assert key == "spark.sql.caseSensitive"
        self.value = str(value)
        self.changes.append(self.value)


class _Spark:
    def __init__(self, *, fail: bool = False) -> None:
        self.conf = _Conf()
        self.fail = fail
        self.executed: list[tuple[str, str]] = []

    def sql(self, statement: str):
        self.executed.append((statement, self.conf.value))
        if self.fail:
            raise RuntimeError("view failed")


def _run(spark, destination):
    action = InstallAction(
        id="view-Sales.ActiveCustomer",
        kind="build_view",
        resource_node_id="delta:Sales.ActiveCustomer",
        executor="spark_sql",
        payload="payload/view.spark.sql",
        payload_sha256="x",
    )
    target = ResolvedTarget(
        bound=BoundTarget(id="lakehouse-Sales", kind="lakehouse", item_id="Sales"),
        lakehouse=ItemRef("Sales"),
        destination=destination,
    )
    context = InstallationContext(
        spark=spark, resolver=None, store=None, target=target
    )
    return SparkSqlExecutor().execute(
        action,
        b"CREATE OR REPLACE VIEW {{object:Sales.ActiveCustomer}} AS "
        b"SELECT * FROM {{object:Sales.CustomerEnriched}}",
        context,
    )


def test_fabric_view_analysis_uses_exact_case_and_restores_session():
    spark = _Spark()
    destination = fabric_destination(workspace="Analytics", lakehouse="Sales")

    details = _run(spark, destination)

    statement, case_sensitive = spark.executed[0]
    assert case_sensitive == "true"
    assert "`CustomerEnriched`" in statement
    assert spark.conf.value == "false"
    assert spark.conf.changes == ["true", "false"]
    assert details["destination"] == "Sales"


def test_fabric_view_failure_still_restores_session():
    import pytest

    spark = _Spark(fail=True)
    destination = fabric_destination(workspace="Analytics", lakehouse="Sales")

    with pytest.raises(RuntimeError, match="view failed"):
        _run(spark, destination)

    assert spark.conf.value == "false"


def test_local_view_uses_the_emulator_session_policy_without_mutating_it():
    spark = _Spark()
    destination = local_destination(item="Sales", tables_root="/tmp/Sales/Tables")
    spark.conf.value = "true"

    _run(spark, destination)

    assert spark.executed[0][1] == "true"
    # Constructing the local catalogue establishes the emulator's session-wide
    # exact-case policy; the executor does not toggle it back.
    assert spark.conf.changes == ["true"]


def test_catalogue_batch_executes_each_statement_in_payload_order():
    spark = _Spark()
    destination = fabric_destination(workspace="Analytics", lakehouse="Control")
    action = InstallAction(
        id="publish-catalogue",
        kind="publish_catalogue",
        resource_node_id=None,
        executor="spark_sql_batch",
        payload="payload/catalogue.spark-sql-batch.json",
        payload_sha256="x",
    )
    target = ResolvedTarget(
        bound=BoundTarget(id="control", kind="lakehouse", item_id="Control"),
        lakehouse=ItemRef("Control"),
        destination=destination,
    )
    context = InstallationContext(
        spark=spark, resolver=None, store=None, target=target
    )

    details = SparkSqlBatchExecutor().execute(
        action,
        b'["DELETE FROM {{object:_.Registry}}", "MERGE INTO {{object:_.Registry}}"]',
        context,
    )

    assert [statement.split()[0] for statement, _case in spark.executed] == [
        "DELETE",
        "MERGE",
    ]
    assert all(case == "true" for _statement, case in spark.executed)
    assert details["statement_count"] == 2
    assert spark.conf.value == "false"


def _batch_context(spark, *, epoch=None):
    return InstallationContext(
        spark=spark,
        resolver=None,
        store=None,
        target=ResolvedTarget(
            bound=BoundTarget(id="control", kind="lakehouse", item_id="Control"),
            lakehouse=ItemRef("Control"),
            destination=fabric_destination(workspace="Analytics", lakehouse="Control"),
        ),
        epoch=epoch,
    )


def _batch_action():
    return InstallAction(
        id="publish-registry",
        kind="publish_registry",
        resource_node_id=None,
        executor="spark_sql_batch",
        payload="payload/registry.spark-sql-batch.json",
        payload_sha256="x",
    )


def test_every_statement_in_a_batch_gets_the_same_epoch():
    """The reason the epoch is an installation value rather than a clock call.

    One build publishes Registry rows for several items in several statements.
    Were each to read the clock, an alias and the source it points at could be
    dated milliseconds apart and then order against each other on the next build
    — which is exactly the false staleness the epoch exists to prevent.
    """

    spark = _Spark()
    payload = (
        b'["INSERT INTO {{object:_.Registry}} VALUES (CAST(\'{{epoch}}\' AS TIMESTAMP))",'
        b' "INSERT INTO {{object:_.Registry}} VALUES (CAST(\'{{epoch}}\' AS TIMESTAMP))"]'
    )

    SparkSqlBatchExecutor().execute(
        _batch_action(), payload, _batch_context(spark, epoch="2026-07-31 09:00:00.000000")
    )

    dated = [statement for statement, _case in spark.executed]
    assert len(dated) == 2
    assert all("2026-07-31 09:00:00.000000" in statement for statement in dated)
    assert dated[0] == dated[1]
    assert all("{{epoch}}" not in statement for statement in dated)


def test_a_statement_needing_an_epoch_without_one_says_so():
    """Rather than reaching ``expand``, which would report it as an unresolvable
    name and say nothing about the missing value."""

    spark = _Spark()

    with pytest.raises(InstallError, match="supplied none"):
        SparkSqlBatchExecutor().execute(
            _batch_action(),
            b'["INSERT INTO {{object:_.Registry}} VALUES (\'{{epoch}}\')"]',
            _batch_context(spark, epoch=None),
        )

    assert spark.executed == []


def test_a_batch_naming_no_epoch_runs_without_one():
    """Only Registry publication carries the token; every other batch is
    unaffected by its absence."""

    spark = _Spark()
    SparkSqlBatchExecutor().execute(
        _batch_action(), b'["DELETE FROM {{object:_.Registry}}"]', _batch_context(spark)
    )

    assert [statement.split()[0] for statement, _case in spark.executed] == ["DELETE"]
