"""SQL-backed table build, end to end against the local Lakehouse.

This is the decisive first checkpoint of the SQL-table feature: it builds a real
bundle from an SES repository and installs it into the local Delta emulator
through the *actual* generated bundle — no hand-maintained ``build.spark.sql``.
It proves both Spark schema modes:

- ``Sales.InferredCustomer`` — no declared schema, shape taken from its query;
- ``Sales.DeclaredCustomer`` — a declared schema, wider than the query infers.

Both build only their main table, both carry Weaver's audit columns, and neither
loads a row — the base ``Sales.Customer`` table's ``read()`` raises if called
(build-philosophy §7, §15).
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from weaver import ItemRef, Location, RepositoryRef
from weaver.build_bundle import generate_build_bundle, install_bundle

pytestmark = pytest.mark.spark

FIXTURE = Path(__file__).parent.parent / "fixtures" / "sql-table-build"
REPO_NAME = "SqlTables"


@pytest.fixture
def clean_sales_catalog(spark):
    """Keep the shared session's catalog free of a stale ``Sales`` database.

    The session is process-wide, so a database this test registers must not leak
    into the next. Each test owns its own Lakehouse directories, so the catalog
    is the only shared surface to tidy.
    """

    spark.sql("DROP SCHEMA IF EXISTS Sales CASCADE")
    yield
    spark.sql("DROP SCHEMA IF EXISTS Sales CASCADE")


def _generate(lakehouses, spark, tmp_path, *, output: str = "bundle", copy: bool = True):
    from weaver.build_bundle import LakehouseBinding, TargetBindings

    if copy:
        destination = lakehouses.resolver.repository(RepositoryRef(REPO_NAME))
        shutil.copytree(FIXTURE, destination.path)

    return generate_build_bundle(
        weaver_lakehouse=lakehouses.weaver,
        repository_name=REPO_NAME,
        targets=TargetBindings(lakehouse=LakehouseBinding(lakehouse=lakehouses.target)),
        output=Location(str(tmp_path / output)),
        host=lakehouses.host,
        store=lakehouses.store,
        spark=spark,
    )


def _install(lakehouses, spark, bundle):
    from weaver.build_bundle import InstallationEnvironment

    environment = InstallationEnvironment(
        store=lakehouses.store, resolver=lakehouses.resolver, spark=spark
    )
    return install_bundle(bundle, environment=environment)


def _build_and_install(lakehouses, spark, tmp_path):
    bundle = _generate(lakehouses, spark, tmp_path)
    return bundle, _install(lakehouses, spark, bundle)


def _schema_of(spark, qualified: str) -> dict[str, str]:
    return {field.name: field.dataType.simpleString() for field in spark.table(qualified).schema}


def _nullable_of(spark, qualified: str) -> dict[str, bool]:
    return {field.name: field.nullable for field in spark.table(qualified).schema}


AUDIT = ("Row_insert_datetime", "Row_update_datetime", "Row_delete_datetime")


def test_both_spark_schema_modes_build_only_the_main_table(
    lakehouses, spark, tmp_path, clean_sales_catalog
):
    _, report = _build_and_install(lakehouses, spark, tmp_path)
    assert report.status == "succeeded", report.to_yaml()

    inferred = _schema_of(spark, "Sales.InferredCustomer")
    declared = _schema_of(spark, "Sales.DeclaredCustomer")

    # Business columns are present in both, and there is no view, no *_Current and
    # no empty *_History — only the authored main table exists.
    for schema in (inferred, declared):
        assert [name for name in schema if name not in AUDIT] == [
            "CustomerId",
            "CustomerName",
        ]
    tables = {
        row.tableName.lower()
        for row in spark.sql("SHOW TABLES IN Sales").collect()
    }
    assert "customer_history" not in tables
    assert "inferredcustomer_current" not in tables


def test_inferred_types_come_from_the_query_and_declared_from_the_declaration(
    lakehouses, spark, tmp_path, clean_sales_catalog
):
    _build_and_install(lakehouses, spark, tmp_path)

    # The base table types CustomerId as int; the inferred table follows the query.
    assert _schema_of(spark, "Sales.InferredCustomer")["CustomerId"] == "int"
    # The declared table asked for a wider bigint, and the declaration wins even
    # though the query still yields int.
    assert _schema_of(spark, "Sales.DeclaredCustomer")["CustomerId"] == "bigint"


def test_every_built_table_carries_the_delta_audit_columns_all_not_null(
    lakehouses, spark, tmp_path, clean_sales_catalog
):
    _build_and_install(lakehouses, spark, tmp_path)

    for qualified in ("Sales.Customer", "Sales.InferredCustomer", "Sales.DeclaredCustomer"):
        schema = _schema_of(spark, qualified)
        nullable = _nullable_of(spark, qualified)
        for audit in AUDIT:
            assert schema.get(audit) == "timestamp", (qualified, audit)
            # Weaver populates all three on every loaded row, so all are not null.
            assert nullable.get(audit) is False, (qualified, audit)


def test_primary_key_columns_are_not_null_in_both_schema_modes(
    lakehouses, spark, tmp_path, clean_sales_catalog
):
    _build_and_install(lakehouses, spark, tmp_path)

    # CustomerId is the primary key of both tables, so it is not null whether the
    # shape is inferred or declared; CustomerName is not keyed, so it stays nullable.
    for qualified in ("Sales.InferredCustomer", "Sales.DeclaredCustomer"):
        nullable = _nullable_of(spark, qualified)
        assert nullable["CustomerId"] is False, qualified
        assert nullable["CustomerName"] is True, qualified


def test_the_build_does_not_load_any_rows(
    lakehouses, spark, tmp_path, clean_sales_catalog
):
    """The base table's read() raises; a successful build proves it never ran,
    and every built table is empty."""

    _build_and_install(lakehouses, spark, tmp_path)

    for qualified in ("Sales.Customer", "Sales.InferredCustomer", "Sales.DeclaredCustomer"):
        assert spark.table(qualified).count() == 0


def test_dependency_order_places_the_base_table_before_its_readers(
    lakehouses, spark, tmp_path, clean_sales_catalog
):
    bundle, _ = _build_and_install(lakehouses, spark, tmp_path)

    built_at = {
        action.resource_node_id: sequence.number
        for sequence, _, action in bundle.plan.actions()
        if action.resource_node_id is not None
    }
    customer = next(node for node in built_at if node.endswith("Sales.Customer"))
    inferred = next(node for node in built_at if node.endswith("Sales.InferredCustomer"))
    declared = next(node for node in built_at if node.endswith("Sales.DeclaredCustomer"))
    assert built_at[customer] < built_at[inferred]
    assert built_at[customer] < built_at[declared]


def test_rebuilding_produces_the_same_plan_and_succeeds_again(
    lakehouses, spark, tmp_path, clean_sales_catalog
):
    first = _generate(lakehouses, spark, tmp_path, output="bundle-1")
    assert _install(lakehouses, spark, first).status == "succeeded"

    # Regenerate from the same repository: the same source yields the same
    # semantic bundle — determinism is a safety property (§10) — and installing
    # it again over the existing tables is idempotent (CREATE OR REPLACE).
    second = _generate(lakehouses, spark, tmp_path, output="bundle-2", copy=False)
    assert first.plan.bundle_id == second.plan.bundle_id
    assert _install(lakehouses, spark, second).status == "succeeded"
