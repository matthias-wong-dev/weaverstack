"""The Spark SQL executor applies Weaver's exact-case identity contract."""

from __future__ import annotations

from weaver.build_bundle.executors.base import InstallationContext, ResolvedTarget
from weaver.build_bundle.executors.spark_sql import SparkSqlExecutor
from weaver.build_bundle.models import BuildAction
from weaver.build_bundle.targets import BoundTarget
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
    action = BuildAction(
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
        spark=spark, resolver=None, store=None, snapshot=None, target=target
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
