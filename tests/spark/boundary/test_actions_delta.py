"""Every action that changes a Lakehouse, executed, with the result inspected.

`test_document_action.py` proves a declaration renders the statement Weaver
intends. It cannot prove that statement is *valid*, or that the object it makes
has the shape the declaration asked for. Only an engine answers that, and it is
the narrowest thing worth paying an engine for.

So: render, execute, and inspect the object. No repository beyond one item, no
catalogue, no bundle, no installer.

**The names are the checklist.** One `test_<kind>_action_<what it proves>` per
action kind, so `pytest --collect-only -q -k _action_` lists what is actually
checked rather than what someone remembers checking. `test_action_checklist.py`
holds this file to that list — add an action kind and it names the test that does
not exist yet.

Each test starts from a **Weaver document** and runs the action Weaver generates
from it. The subject is never "can Spark create a view" but "the view this
document declares is the view that appears".
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


def test_build_table_action_creates_the_declared_columns(
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


def test_build_table_action_uses_the_declared_types(
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


def test_build_table_action_adds_the_audit_columns(
    tmp_path, build_item, lakehouses, spark
):
    repository = single_document_repository(
        tmp_path / "repo",
        documents={"DWG__Customer.py": lakehouse_table("DWG.Customer")},
    )

    build_item(repository, target=TARGET)

    assert AUDIT <= set(columns_of(spark, lakehouses, "DWG", "Customer"))


def test_build_table_action_makes_the_primary_key_not_nullable(
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


def test_build_table_action_creates_an_empty_table(tmp_path, build_item, lakehouses, spark):
    """Build creates structure; only a load puts rows in it."""

    repository = single_document_repository(
        tmp_path / "repo",
        documents={"DWG__Customer.py": lakehouse_table("DWG.Customer")},
    )

    build_item(repository, target=TARGET)

    destination = lakehouses.resolver.spark_destination(lakehouses.target)
    assert spark.table(destination.qualify("DWG", "Customer")).count() == 0


def test_build_view_action_creates_a_view_over_the_table_it_reads(
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


def test_build_view_action_creates_a_view_over_another_view(
    tmp_path, build_item, lakehouses, spark
):
    """A view whose dependency is itself a view, built in one pass.

    The chain is what makes the ordering claim bite: the outer view's statement
    is invalid until the inner one exists, so a plan that got the layers wrong
    fails here rather than producing something subtly unqueryable. Re-homed from
    a hand-written Spark spike that proved the engine *could* do this before
    Weaver did — the claim is now made through real documents and real actions.
    """

    from factories import single_document_repository

    repository = single_document_repository(
        tmp_path / "repo",
        documents={
            "DWG__Customer.py": lakehouse_table("DWG.Customer"),
            "DWG.ActiveCustomer.sql": spark_view(
                "DWG.ActiveCustomer", depends_on="DWG.Customer"
            ),
            "DWG.ActiveSummary.sql": spark_view(
                "DWG.ActiveSummary", depends_on="DWG.ActiveCustomer"
            ),
        },
    )

    results = build_item(repository, target=TARGET)

    failures = {r.action_id: r.error_message for r in results if r.status == "failed"}
    assert not failures, failures
    destination = lakehouses.resolver.spark_destination(lakehouses.target)
    # The outer view resolves through the inner one, so querying it exercises
    # both — an outer view over a copied-out query would read the same.
    assert spark.table(destination.qualify("DWG", "ActiveSummary")).count() == 0
    views = {name.casefold() for name in _views_in(spark, destination, "DWG")}
    assert {"activecustomer", "activesummary"} <= views


def _views_in(spark, destination, schema: str) -> set:
    rows = spark.sql(f"SHOW VIEWS IN {destination.qualified_schema(schema)}").collect()
    return {row["viewName"] for row in rows}


def test_build_folder_action_creates_the_directory(
    tmp_path, build_item, lakehouses
):
    """A folder has no statement and no catalogue name — it is a path."""

    from weaver.targets import FolderTarget

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


def test_drop_table_action_clears_the_way_for_a_rebuild(
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


def test_build_table_action_infers_the_shape_of_a_query_shaped_table(
    tmp_path, build_item, lakehouses, spark
):
    """The other way a Lakehouse table is built, and the one only Spark can check.

    A Python document declares its columns, so its DDL is a plain `CREATE TABLE`.
    A Spark SQL document declares a *query*, and the `spark_table` executor runs
    it, reads the result's schema and creates the table from that. Everything
    about that is deferred to an engine, and until now it was proven only against
    a fake session — which cannot say whether Spark agrees about the types.
    """

    from factories import single_document_repository

    body = (
        "/*\nTable ID: DWG.Summary\n\n"
        "Description: A query-shaped table.\n\n"
        "Lineage: Declared for a test.\n\n"
        "Dependencies: []\n\n"
        "Schema:\n  CustomerId: string\n  Score: double\n*/\n"
        "select cast(null as string) as CustomerId\n"
        "     , cast(null as double) as Score\n"
        " where 1 = 0\n"
    )
    repository = single_document_repository(
        tmp_path / "repo", documents={"DWG.Summary.sql": body}
    )

    results = build_item(repository, target=TARGET)

    failures = {r.action_id: r.error_message for r in results if r.status == "failed"}
    assert not failures, failures
    assert any(r.executor == "spark_table" for r in results), [
        r.executor for r in results
    ]
    columns = columns_of(spark, lakehouses, "DWG", "Summary")
    assert columns["customerid"].dataType.simpleString() == "string"
    assert columns["score"].dataType.simpleString() == "double"
    assert AUDIT <= set(columns)


def test_create_schema_action_creates_the_schema(tmp_path, build_item, lakehouses, spark):
    """A schema is a namespace with no object in it, so nothing else proves it.

    Every other test here reaches its schema by creating something inside it. If
    the schema action were wrong the object would fail too, and the failure would
    name the object — so the namespace gets its own claim.
    """

    from factories import single_document_repository
    from weaver.spark import SparkCatalogue

    repository = single_document_repository(
        tmp_path / "repo",
        documents={"DWG__Customer.py": lakehouse_table("DWG.Customer")},
    )

    build_item(repository, target=TARGET)

    catalogue = SparkCatalogue(
        spark, lakehouses.resolver.spark_destination(lakehouses.target)
    )
    assert catalogue.schema_exists("DWG")


def test_drop_folder_action_removes_the_directory(
    tmp_path, build_item, lakehouses
):
    """The destructive half of a folder's lifecycle, executed.

    `build_folder` is strict and `drop_folder` is too — neither tolerates the
    target being in the state it is trying to reach — so a rebuild of a folder is
    a real pair of statements against a real filesystem.
    """

    from factories import bound_target, single_document_repository
    from weaver.targets import FolderTarget
    from weaver.build_bundle.prune import read_lakehouse_inventory

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

    installed = read_lakehouse_inventory(
        bound_target(id="target-1", item_id=TARGET),
        resolver=lakehouses.resolver,
        store=lakehouses.store,
    )
    results = build_item(
        repository, target=TARGET, inventory=installed, rebuild=True
    )

    failures = {r.action_id: r.error_message for r in results if r.status == "failed"}
    assert not failures, failures
    assert any(r.action_id.startswith("managed-drop-") for r in results)
    # Dropped and remade, so it is there — and the drop really ran, which the
    # strictness of the create that followed it proves.
    assert lakehouses.store.exists(location)


def test_prune_table_action_removes_an_object_nothing_declares(
    tmp_path, build_item, lakehouses, spark
):
    """Prune, actually executed against Delta rather than asserted to be empty.

    Everything else about prune is proven in pure Python, and the fidelity tests
    say a correct estate produces no prune at all. Neither runs the statement. A
    frozen `DROP TABLE` that the engine rejects would satisfy both.
    """

    from factories import bound_target, single_document_repository
    from weaver.spark import SparkCatalogue

    repository = single_document_repository(
        tmp_path / "repo",
        documents={"DWG__Customer.py": lakehouse_table("DWG.Customer")},
    )
    build_item(repository, target=TARGET)

    destination = lakehouses.resolver.spark_destination(lakehouses.target)
    catalogue = SparkCatalogue(spark, destination)
    catalogue.sql(
        "CREATE TABLE {{object:DWG.Orphan}} (CustomerId string) USING DELTA"
    )
    assert catalogue.exists("DWG", "Orphan")

    installed = read_back(lakehouses, spark)
    results = build_item(
        repository, target=TARGET, inventory=installed, build=False
    )

    failures = {r.action_id: r.error_message for r in results if r.status == "failed"}
    assert not failures, failures
    assert any("prune" in r.action_id for r in results), [r.action_id for r in results]
    assert not catalogue.exists("DWG", "Orphan")
    assert catalogue.exists("DWG", "Customer"), "prune took a declared object"


def read_back(lakehouses, spark):
    from factories import bound_target
    from weaver.build_bundle.prune import read_lakehouse_inventory

    return read_lakehouse_inventory(
        bound_target(id="target-1", item_id=TARGET),
        resolver=lakehouses.resolver,
        store=lakehouses.store,
        spark=spark,
    )
