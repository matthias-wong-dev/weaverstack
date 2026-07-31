"""One declared document, actually built — does the engine agree with the DDL?

`test_document_action.py` proves a declaration renders the statement Weaver
intends. It cannot prove that statement is *valid*, or that the object it makes
has the shape the declaration asked for. Only an engine answers that, and it is
the narrowest thing worth paying an engine for.

So: render, execute, and inspect the object. No repository beyond one item, no
catalogue, no bundle, no installer.
"""

from __future__ import annotations

import pytest
from factories import (
    folder_document,
    lakehouse_table,
    single_document_repository,
    spark_view,
)

pytestmark = pytest.mark.spark

TARGET = "Sales_LH"
AUDIT = {"row_insert_datetime", "row_update_datetime", "row_delete_datetime"}


def columns_of(spark, lakehouses, schema: str, name: str) -> dict:
    destination = lakehouses.resolver.spark_destination(lakehouses.target)
    fields = spark.table(destination.qualify(schema, name)).schema
    return {field.name.casefold(): field for field in fields}


def test_a_declared_table_is_built_with_its_declared_columns(
    tmp_path, build_item, lakehouses, spark
):
    repository = single_document_repository(
        tmp_path / "repo",
        documents={
            "DWG__Customer.py": lakehouse_table(
                "DWG.Customer",
                columns={
                    "CustomerId": "string",
                    "CustomerName": "string",
                    "IsActive": "boolean",
                },
            )
        },
    )

    build_item(repository, target=TARGET)

    columns = columns_of(spark, lakehouses, "DWG", "Customer")
    assert {"customerid", "customername", "isactive"} <= set(columns)


def test_declared_types_survive_into_the_physical_table(
    tmp_path, build_item, lakehouses, spark
):
    """The types are the declaration's promise, and the engine is the arbiter."""

    repository = single_document_repository(
        tmp_path / "repo",
        documents={
            "DWG__Customer.py": lakehouse_table(
                "DWG.Customer",
                columns={"CustomerId": "string", "Score": "double", "IsActive": "boolean"},
            )
        },
    )

    build_item(repository, target=TARGET)

    columns = columns_of(spark, lakehouses, "DWG", "Customer")
    assert columns["customerid"].dataType.simpleString() == "string"
    assert columns["score"].dataType.simpleString() == "double"
    assert columns["isactive"].dataType.simpleString() == "boolean"


def test_weaver_adds_its_audit_columns_to_every_table(
    tmp_path, build_item, lakehouses, spark
):
    repository = single_document_repository(
        tmp_path / "repo",
        documents={"DWG__Customer.py": lakehouse_table("DWG.Customer")},
    )

    build_item(repository, target=TARGET)

    assert AUDIT <= set(columns_of(spark, lakehouses, "DWG", "Customer"))


def test_the_primary_key_is_physically_not_nullable(
    tmp_path, build_item, lakehouses, spark
):
    """A nullability the engine does not enforce is a promise Weaver did not keep."""

    repository = single_document_repository(
        tmp_path / "repo",
        documents={
            "DWG__Customer.py": lakehouse_table(
                "DWG.Customer",
                columns={"CustomerId": "string", "CustomerName": "string"},
            )
        },
    )

    build_item(repository, target=TARGET)

    columns = columns_of(spark, lakehouses, "DWG", "Customer")
    assert columns["customerid"].nullable is False


def test_a_table_is_built_empty(tmp_path, build_item, lakehouses, spark):
    """Build creates structure; only a load puts rows in it."""

    repository = single_document_repository(
        tmp_path / "repo",
        documents={"DWG__Customer.py": lakehouse_table("DWG.Customer")},
    )

    build_item(repository, target=TARGET)

    destination = lakehouses.resolver.spark_destination(lakehouses.target)
    assert spark.table(destination.qualify("DWG", "Customer")).count() == 0


def test_a_declared_view_is_built_over_the_table_it_reads(
    tmp_path, build_item, lakehouses, spark
):
    """The view's tokens must resolve, and the result must be queryable.

    A view exists only as a name, so an unresolved token here is not a bad path
    but a statement the engine rejects — which is why it needs an engine.
    """

    repository = single_document_repository(
        tmp_path / "repo",
        documents={
            "DWG__Customer.py": lakehouse_table("DWG.Customer"),
            "DWG.ActiveCustomer.sql": spark_view(
                "DWG.ActiveCustomer", depends_on="DWG.Customer"
            ),
        },
    )

    build_item(repository, target=TARGET)

    destination = lakehouses.resolver.spark_destination(lakehouses.target)
    assert spark.table(destination.qualify("DWG", "ActiveCustomer")).count() == 0


def test_a_declared_folder_is_created_as_a_directory(
    tmp_path, build_item, lakehouses
):
    """A folder has no statement and no catalogue name — it is a path."""

    from weaver import FolderTarget

    repository = single_document_repository(
        tmp_path / "repo",
        schemas=("DWG", "Raw"),
        documents={"Files/Raw__CustomerCsv.py": folder_document("Raw.CustomerCsv")},
    )

    build_item(repository, target=TARGET)

    location = lakehouses.resolver.folder_object(
        FolderTarget(lakehouse=lakehouses.target), "Raw", "CustomerCsv"
    )
    assert lakehouses.store.exists(location)


def test_a_rebuild_drops_the_object_and_creates_it_again(
    tmp_path, build_item, lakehouses, spark
):
    """How Weaver rebuilds, proven against a real engine.

    A Lakehouse table's generated DDL is `CREATE TABLE`, not `CREATE OR
    REPLACE` — so rebuilding is not one idempotent statement, it is a drop stage
    clearing the way for a build stage. That ordering is asserted in pure Python
    (`test_item_plan.py`); this asserts the engine accepts the pair, which is the
    part only an engine can say.

    The second plan reads the inventory back first, as a real second build does.
    Planning against an empty inventory would ask for the schema to be created
    twice, which the planner never does — asserting against that would be testing
    the harness.
    """

    from weaver.build_bundle.prune import read_lakehouse_inventory
    from factories import bound_target

    repository = single_document_repository(
        tmp_path / "repo",
        documents={"DWG__Customer.py": lakehouse_table("DWG.Customer")},
    )

    build_item(repository, target=TARGET)
    installed = read_lakehouse_inventory(
        bound_target(id="target-1", item_id=TARGET),
        resolver=lakehouses.resolver,
        store=lakehouses.store,
        spark=spark,
    )
    results = build_item(
        repository, target=TARGET, inventory=installed, rebuild=True
    )

    failures = {r.action_id: r.error_message for r in results if r.status == "failed"}
    assert not failures, failures
    kinds = [r.action_id for r in results]
    assert any(name.startswith("managed-drop-") for name in kinds), kinds
    assert any(name.startswith("object-") for name in kinds), kinds
    # And the rebuilt table is readable, so the pair left a working object.
    destination = lakehouses.resolver.spark_destination(lakehouses.target)
    assert spark.table(destination.qualify("DWG", "Customer")).count() == 0
