"""Executing catalogue reconciliation against local Delta.

The rendering tests assert what the statements *say*; these assert what they
*do* — against real Delta tables built by the ordinary build path, read back
through the tolerant reader. Between them the loop is closed: projection to
statements to rows to a read that compares equal to the projection.

The property that matters most is the last one here: a Lakehouse build's
statements, executed in full, leave a Warehouse installation's rows exactly as
they were.
"""

from __future__ import annotations

import pytest

from weaver import LocalStore, Location
from weaver.catalogue import (
    AUDIT_COLUMN_NAMES,
    CATALOGUE_TABLES,
    DEPENDENCY,
    INSTALLATION,
    REGISTRY,
    TABLE_DICTIONARY,
    InstallationScope,
    target_type_for_ses_target,
)
from weaver.catalogue.projection import project_installation
from weaver.catalogue.reader import read_installation, read_table
from weaver.catalogue.reconcile import (
    compare,
    prune_installation,
    prune_repository,
    reconcile,
    summarise,
)
from weaver.ses import read_repository
from weaver.spark import SparkCatalogue

pytestmark = pytest.mark.spark

FIXTURE = "tests/fixtures/catalogue-estate"
LAKEHOUSE = InstallationScope(repository="catalogue-estate", target_type="lakehouse")
WAREHOUSE = InstallationScope(repository="catalogue-estate", target_type="warehouse")


@pytest.fixture
def catalogue(lakehouses, spark):
    """Empty catalogue tables of exactly the declared shape.

    Created directly from the representation rather than through a build, and the
    distinction matters for what this file is testing. That the *ordinary build
    path* produces these tables is proved by ``test_catalogue_builtin_build`` and
    ``test_catalogue_setup``; here the subject is what the DML does to tables of
    that shape. Going through a full bundle per test cost about forty Spark
    statements each and pushed the suite past its timeout, for no assertion these
    tests make.

    ``test_projected_rows_survive_a_round_trip_through_delta`` still pins the shape
    against the representation, so a table created here that did not match what a
    build produces would not go unnoticed.
    """

    catalogue = SparkCatalogue(
        spark, lakehouses.resolver.spark_destination(lakehouses.weaver)
    )
    catalogue.create_schema("_")
    for table in CATALOGUE_TABLES:
        columns = ", ".join(
            f"`{column.name}` {column.type}{' NOT NULL' if column.not_null else ''}"
            for column in table.columns
        ) + ", " + ", ".join(f"`{name}` timestamp NOT NULL" for name in AUDIT_COLUMN_NAMES)
        catalogue.sql(
            f"CREATE OR REPLACE TABLE {{{{object:_.{table.name}}}}} ({columns}) USING delta "
            "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')"
        )
    try:
        yield catalogue
    finally:
        spark.sql(f"DROP SCHEMA IF EXISTS {catalogue.qualified_schema('_')} CASCADE")


@pytest.fixture(scope="module")
def estate():
    return read_repository(
        Location(value=FIXTURE), store=LocalStore(), name="catalogue-estate"
    )


def _projection(estate, scope: InstallationScope, *, target_name: str = "Sales_LH"):
    retained = [
        document.node_id
        for document in estate.documents
        if target_type_for_ses_target(document.target_kind) == scope.target_type
    ]
    return project_installation(
        estate,
        retained=retained,
        scope=scope,
        target_name=target_name,
        weaver_version="9.9.9",
    )


def _run(catalogue, statements) -> None:
    for statement in statements:
        catalogue.sql(statement)


# --- the round trip -----------------------------------------------------------


def test_projected_rows_survive_a_round_trip_through_delta(catalogue, estate):
    """Projection, statements, rows, read — and the read equals the projection.

    Every column of every table, compared by value. This is the assertion that
    would catch a column written into the wrong position, a boolean stored as a
    string, or a null that arrived as the word "None".
    """

    projection = _projection(estate, LAKEHOUSE)
    _run(catalogue, reconcile(projection).statements)

    for table in CATALOGUE_TABLES:
        expected = {
            tuple(row.get(name) for name in table.column_names)
            for row in projection.for_table(table)
        }
        actual = {
            tuple(row.get(name) for name in table.column_names)
            for row in read_table(catalogue, table, scope=LAKEHOUSE)
        }
        assert actual == expected, table.name


def test_a_null_round_trips_as_a_null_not_as_prose(catalogue, estate):
    _run(catalogue, reconcile(_projection(estate, LAKEHOUSE)).statements)
    (row,) = [
        row
        for row in read_table(catalogue, TABLE_DICTIONARY, scope=LAKEHOUSE)
        if row["object_name"] == "Region"
    ]
    assert row["identity_column"] is None
    assert row["description_reference"] is None


def test_booleans_round_trip_as_booleans(catalogue, estate):
    _run(catalogue, reconcile(_projection(estate, LAKEHOUSE)).statements)
    rows = {
        row["object_name"]: row
        for row in read_table(catalogue, TABLE_DICTIONARY, scope=LAKEHOUSE)
    }
    assert rows["Region"]["is_static"] is True
    assert rows["Customer"]["is_static"] is False


def test_the_audit_columns_are_stamped_on_insert(catalogue, estate):
    _run(catalogue, reconcile(_projection(estate, LAKEHOUSE)).statements)
    rows = catalogue.sql(
        "SELECT row_insert_datetime, row_update_datetime, row_delete_datetime "
        "FROM {{object:_.Registry}}"
    ).collect()
    assert rows
    for row in rows:
        assert row["row_insert_datetime"] is not None
        assert row["row_update_datetime"] is not None
        # A live row's delete datetime is the sentinel maximum, never null.
        assert row["row_delete_datetime"].year == 9999


# --- insert, update, no-op ----------------------------------------------------


def test_running_the_same_reconciliation_twice_changes_nothing(catalogue, estate):
    """The guard makes an unchanged row a genuine no-op, not a rewrite.

    Asserted through ``row_update_datetime``: if the merge wrote, the stamp would
    move. This is what makes rebuilding unchanged SES cheap and, more importantly,
    non-destructive of history.
    """

    statements = reconcile(_projection(estate, LAKEHOUSE)).statements
    _run(catalogue, statements)
    before = {
        (row["schema_name"], row["object_name"]): row["row_update_datetime"]
        for row in catalogue.sql(
            "SELECT schema_name, object_name, row_update_datetime FROM {{object:_.Registry}}"
        ).collect()
    }
    _run(catalogue, statements)
    after = {
        (row["schema_name"], row["object_name"]): row["row_update_datetime"]
        for row in catalogue.sql(
            "SELECT schema_name, object_name, row_update_datetime FROM {{object:_.Registry}}"
        ).collect()
    }
    assert before == after


def test_a_changed_row_is_updated_in_place_and_keeps_its_insert_stamp(catalogue, estate):
    _run(catalogue, reconcile(_projection(estate, LAKEHOUSE)).statements)
    (original,) = catalogue.sql(
        "SELECT row_insert_datetime FROM {{object:_.Installation}}"
    ).collect()

    # Rebinding to a different Lakehouse: same key, different target_name.
    rebound = _projection(estate, LAKEHOUSE, target_name="Sales_LH_v2")
    _run(catalogue, reconcile(rebound).statements)

    rows = catalogue.sql(
        "SELECT target_name, row_insert_datetime FROM {{object:_.Installation}}"
    ).collect()
    assert len(rows) == 1, "rebinding must update the installation, not add one"
    assert rows[0]["target_name"] == "Sales_LH_v2"
    assert rows[0]["row_insert_datetime"] == original["row_insert_datetime"]


def test_a_row_no_longer_projected_is_deleted(catalogue, estate):
    projection = _projection(estate, LAKEHOUSE)
    _run(catalogue, reconcile(projection).statements)
    assert len(read_table(catalogue, REGISTRY, scope=LAKEHOUSE)) == 5

    # A repository that now declares fewer objects.
    reduced = project_installation(
        estate,
        retained=["delta:Sales.Region", "folder:Sales.CustomerCsv"],
        scope=LAKEHOUSE,
        target_name="Sales_LH",
        weaver_version="9.9.9",
    )
    _run(catalogue, reconcile(reduced).statements)
    assert {row["object_name"] for row in read_table(catalogue, REGISTRY, scope=LAKEHOUSE)} == {
        "Region",
        "CustomerCsv",
    }


def test_child_rows_of_a_removed_object_go_with_it(catalogue, estate):
    """No cascade mechanism needed: each table's delete is scoped the same way."""

    _run(catalogue, reconcile(_projection(estate, LAKEHOUSE)).statements)
    reduced = project_installation(
        estate,
        retained=["delta:Sales.Region"],
        scope=LAKEHOUSE,
        target_name="Sales_LH",
        weaver_version="9.9.9",
    )
    _run(catalogue, reconcile(reduced).statements)
    read = read_installation(catalogue, scope=LAKEHOUSE)
    assert {row["object_name"] for row in read["ColumnDictionary"]} == {"Region"}
    assert read["ForeignKeyDictionary"] == ()
    assert read["Alias"] == ()


# --- installation isolation ---------------------------------------------------


def test_a_lakehouse_reconciliation_leaves_the_warehouse_rows_untouched(catalogue, estate):
    """The property the whole installation model exists for.

    Both installations are written, then the Lakehouse side is reconciled again
    from a *reduced* projection — the case that deletes. Every Warehouse row must
    survive byte for byte, including the row for ``Sales.Customer``, which exists
    in both.
    """

    _run(catalogue, reconcile(_projection(estate, LAKEHOUSE)).statements)
    _run(
        catalogue,
        reconcile(_projection(estate, WAREHOUSE, target_name="Sales_WH")).statements,
    )

    def warehouse_rows():
        return {
            name: tuple(sorted(map(repr, rows)))
            for name, rows in read_installation(catalogue, scope=WAREHOUSE).items()
        }

    before = warehouse_rows()
    assert before["Registry"], "the warehouse side must actually have rows"

    reduced = project_installation(
        estate,
        retained=["delta:Sales.Region"],
        scope=LAKEHOUSE,
        target_name="Sales_LH",
        weaver_version="9.9.9",
    )
    _run(catalogue, reconcile(reduced).statements)

    assert warehouse_rows() == before


def test_the_same_object_name_coexists_in_both_installations(catalogue, estate):
    _run(catalogue, reconcile(_projection(estate, LAKEHOUSE)).statements)
    _run(
        catalogue,
        reconcile(_projection(estate, WAREHOUSE, target_name="Sales_WH")).statements,
    )
    rows = catalogue.sql(
        "SELECT target_type FROM {{object:_.Registry}} "
        "WHERE schema_name = 'Sales' AND object_name = 'Customer' ORDER BY target_type"
    ).collect()
    assert [row["target_type"] for row in rows] == ["lakehouse", "warehouse"]


def test_a_dependency_that_leaves_the_repository_is_stored_as_such(catalogue, estate):
    _run(
        catalogue,
        reconcile(_projection(estate, WAREHOUSE, target_name="Sales_WH")).statements,
    )
    external = [
        row
        for row in read_table(catalogue, DEPENDENCY, scope=WAREHOUSE)
        if row["is_within_repository"] is False
    ]
    assert [
        (row["dependency_repository"], row["dependency_object_name"]) for row in external
    ] == [("Sales_LH", "Customer")]


# --- the summary a reviewer reads ---------------------------------------------


def test_the_summary_reports_inserts_then_no_ops(catalogue, estate):
    projection = _projection(estate, LAKEHOUSE)

    first = summarise(projection, read_installation(catalogue, scope=LAKEHOUSE))
    assert sum(change.inserted for change in first) == projection.total
    assert all(change.updated == 0 and change.deleted == 0 for change in first)

    _run(catalogue, reconcile(projection).statements)

    second = summarise(projection, read_installation(catalogue, scope=LAKEHOUSE))
    assert all(change.is_noop for change in second)
    assert sum(change.unchanged for change in second) == projection.total


def test_the_summary_reports_an_update_when_only_a_non_key_column_changed(
    catalogue, estate
):
    _run(catalogue, reconcile(_projection(estate, LAKEHOUSE)).statements)
    rebound = _projection(estate, LAKEHOUSE, target_name="Sales_LH_v2")
    change = compare(
        INSTALLATION,
        rebound.for_table(INSTALLATION),
        read_table(catalogue, INSTALLATION, scope=LAKEHOUSE),
    )
    assert (change.inserted, change.updated, change.deleted) == (0, 1, 0)


def test_the_summary_reports_deletes_without_the_statements_depending_on_it(
    catalogue, estate
):
    """The statements are correct against any prior state; the summary is for review.

    A build that derived its deletes from a read would have its deletion scope
    widened by a failed read. Here nothing is derived from the read, so a wrong
    summary is a wrong report and never a wrong mutation.
    """

    _run(catalogue, reconcile(_projection(estate, LAKEHOUSE)).statements)
    reduced = project_installation(
        estate,
        retained=["delta:Sales.Region"],
        scope=LAKEHOUSE,
        target_name="Sales_LH",
        weaver_version="9.9.9",
    )
    change = compare(
        REGISTRY, reduced.for_table(REGISTRY), read_table(catalogue, REGISTRY, scope=LAKEHOUSE)
    )
    assert change.deleted == 4

    # The delete names only what is *kept*, and removes the complement within the
    # scope — so a row being removed appears nowhere in it. Asserted on the delete
    # statement rather than on the whole set, because a retained row may mention a
    # removed object perfectly legitimately: Sales.Region's lineage is
    # `$Sales.CustomerCsv`, and that is a description, not a key.
    delete = reconcile(reduced).registry.delete
    assert "CustomerCsv" not in delete
    assert "`object_name` <=> CAST('Region' AS STRING)" in delete


# --- explicit prune scopes ----------------------------------------------------


def test_installation_prune_removes_one_scope_and_leaves_the_other(catalogue, estate):
    _run(catalogue, reconcile(_projection(estate, LAKEHOUSE)).statements)
    _run(
        catalogue,
        reconcile(_projection(estate, WAREHOUSE, target_name="Sales_WH")).statements,
    )

    _run(catalogue, prune_installation(LAKEHOUSE))

    assert all(rows == () for rows in read_installation(catalogue, scope=LAKEHOUSE).values())
    warehouse = read_installation(catalogue, scope=WAREHOUSE)
    assert warehouse["Registry"]
    assert warehouse["Installation"]


def test_repository_prune_removes_both_scopes(catalogue, estate):
    _run(catalogue, reconcile(_projection(estate, LAKEHOUSE)).statements)
    _run(
        catalogue,
        reconcile(_projection(estate, WAREHOUSE, target_name="Sales_WH")).statements,
    )

    _run(catalogue, prune_repository("catalogue-estate"))

    for scope in (LAKEHOUSE, WAREHOUSE):
        assert all(rows == () for rows in read_installation(catalogue, scope=scope).values())


def test_pruning_one_repository_leaves_another_alone(catalogue, estate):
    _run(catalogue, reconcile(_projection(estate, LAKEHOUSE)).statements)
    other = InstallationScope(repository="OtherRepo", target_type="lakehouse")
    catalogue.sql(
        "INSERT INTO {{object:_.Registry}} VALUES ('OtherRepo', 'lakehouse', 'X', 'Y', "
        "'table', 'data', 'sig', current_timestamp(), current_timestamp(), "
        "current_timestamp())"
    )

    _run(catalogue, prune_repository("catalogue-estate"))

    assert len(read_table(catalogue, REGISTRY, scope=other)) == 1


def test_prune_uncertifies_before_it_removes_the_descriptions(catalogue):
    """Registry first, so nothing is left certified while what described it is gone."""

    order = [statement.splitlines()[0] for statement in prune_installation(LAKEHOUSE)]
    assert "{{object:_.Registry}}" in order[0]
    assert "{{object:_.Installation}}" in order[1]
    assert order[-1].endswith("{{object:_.SchemaDictionary}}")


def test_partial_dictionary_state_is_repaired_by_the_next_successful_build(
    catalogue, estate
):
    """Why the dictionaries need no all-or-nothing transaction.

    A build that failed midway through the dictionaries leaves some tables written
    and others not. The next successful build converges them by ordinary row
    comparison — there is nothing to roll back, and nothing was certified, because
    Registry never ran.
    """

    projection = _projection(estate, LAKEHOUSE)
    plan = reconcile(projection)

    # Simulate the interrupted build: the first two dictionary tables only.
    for reconciliation in plan.dictionaries[:2]:
        _run(catalogue, reconciliation.statements)

    read = read_installation(catalogue, scope=LAKEHOUSE)
    assert read[plan.dictionaries[0].table.name]
    assert read["Registry"] == (), "nothing may be certified by a failed build"
    assert read["Installation"] == ()

    # And now a successful build.
    _run(catalogue, plan.statements)

    for table in CATALOGUE_TABLES:
        expected = {
            tuple(row.get(name) for name in table.column_names)
            for row in projection.for_table(table)
        }
        actual = {
            tuple(row.get(name) for name in table.column_names)
            for row in read_table(catalogue, table, scope=LAKEHOUSE)
        }
        assert actual == expected, table.name
