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

from weaver import LocalStore, Location, RepositoryRef
from weaver.build_bundle.bundle import load_bundle
from weaver.build_bundle.installer import InstallationEnvironment, install_bundle
from weaver.build_bundle.planner import generate_build_bundle
from weaver.build_bundle.targets import LakehouseBinding, TargetBindings
from weaver.catalogue import (
    CATALOGUE_REPOSITORY,
    CATALOGUE_TABLES,
    DEPENDENCY,
    INSTALLATION,
    REGISTRY,
    TABLE_DICTIONARY,
    InstallationScope,
    target_type_for_ses_target,
)
from weaver.catalogue.builtin import repository_files
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

pytestmark = pytest.mark.spark

FIXTURE = "tests/fixtures/catalogue-estate"
LAKEHOUSE = InstallationScope(repository="catalogue-estate", target_type="lakehouse")
WAREHOUSE = InstallationScope(repository="catalogue-estate", target_type="warehouse")


@pytest.fixture
def catalogue(lakehouses, spark):
    """A real, empty catalogue, built by the ordinary build path."""

    resolver, store = lakehouses.resolver, lakehouses.store
    repository = resolver.repository(RepositoryRef(CATALOGUE_REPOSITORY))
    for relative, data in repository_files().items():
        store.write(repository.join(*relative.split("/")), data)
    bundle = generate_build_bundle(
        weaver_lakehouse=lakehouses.weaver,
        repository_name=CATALOGUE_REPOSITORY,
        targets=TargetBindings(lakehouse=LakehouseBinding(lakehouse=lakehouses.weaver)),
        output=resolver.build_bundle("bootstrap"),
        host=lakehouses.host,
        store=store,
        prune=False,
        spark=spark,
    )
    report = install_bundle(
        load_bundle(bundle.location, store=store),
        environment=InstallationEnvironment(store=store, resolver=resolver, spark=spark),
    )
    assert report.status == "succeeded"
    try:
        yield spark
    finally:
        spark.sql("DROP DATABASE IF EXISTS `_` CASCADE")


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


def _run(spark, statements) -> None:
    for statement in statements:
        spark.sql(statement)


# --- the round trip -----------------------------------------------------------


def test_projected_rows_survive_a_round_trip_through_delta(catalogue, estate):
    """Projection, statements, rows, read — and the read equals the projection.

    Every column of every table, compared by value. This is the assertion that
    would catch a column written into the wrong position, a boolean stored as a
    string, or a null that arrived as the word "None".
    """

    spark = catalogue
    projection = _projection(estate, LAKEHOUSE)
    _run(spark, reconcile(projection).statements)

    for table in CATALOGUE_TABLES:
        expected = {
            tuple(row.get(name) for name in table.column_names)
            for row in projection.for_table(table)
        }
        actual = {
            tuple(row.get(name) for name in table.column_names)
            for row in read_table(spark, table, scope=LAKEHOUSE)
        }
        assert actual == expected, table.name


def test_a_null_round_trips_as_a_null_not_as_prose(catalogue, estate):
    spark = catalogue
    _run(spark, reconcile(_projection(estate, LAKEHOUSE)).statements)
    (row,) = [
        row
        for row in read_table(spark, TABLE_DICTIONARY, scope=LAKEHOUSE)
        if row["object_name"] == "Region"
    ]
    assert row["identity_column"] is None
    assert row["description_reference"] is None


def test_booleans_round_trip_as_booleans(catalogue, estate):
    spark = catalogue
    _run(spark, reconcile(_projection(estate, LAKEHOUSE)).statements)
    rows = {
        row["object_name"]: row
        for row in read_table(spark, TABLE_DICTIONARY, scope=LAKEHOUSE)
    }
    assert rows["Region"]["is_static"] is True
    assert rows["Customer"]["is_static"] is False


def test_the_audit_columns_are_stamped_on_insert(catalogue, estate):
    spark = catalogue
    _run(spark, reconcile(_projection(estate, LAKEHOUSE)).statements)
    rows = spark.sql(
        "SELECT row_insert_datetime, row_update_datetime, row_delete_datetime "
        "FROM `_`.`Registry`"
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

    spark = catalogue
    statements = reconcile(_projection(estate, LAKEHOUSE)).statements
    _run(spark, statements)
    before = {
        (row["schema_name"], row["object_name"]): row["row_update_datetime"]
        for row in spark.sql(
            "SELECT schema_name, object_name, row_update_datetime FROM `_`.`Registry`"
        ).collect()
    }
    _run(spark, statements)
    after = {
        (row["schema_name"], row["object_name"]): row["row_update_datetime"]
        for row in spark.sql(
            "SELECT schema_name, object_name, row_update_datetime FROM `_`.`Registry`"
        ).collect()
    }
    assert before == after


def test_a_changed_row_is_updated_in_place_and_keeps_its_insert_stamp(catalogue, estate):
    spark = catalogue
    _run(spark, reconcile(_projection(estate, LAKEHOUSE)).statements)
    (original,) = spark.sql(
        "SELECT row_insert_datetime FROM `_`.`Installation`"
    ).collect()

    # Rebinding to a different Lakehouse: same key, different target_name.
    rebound = _projection(estate, LAKEHOUSE, target_name="Sales_LH_v2")
    _run(spark, reconcile(rebound).statements)

    rows = spark.sql(
        "SELECT target_name, row_insert_datetime FROM `_`.`Installation`"
    ).collect()
    assert len(rows) == 1, "rebinding must update the installation, not add one"
    assert rows[0]["target_name"] == "Sales_LH_v2"
    assert rows[0]["row_insert_datetime"] == original["row_insert_datetime"]


def test_a_row_no_longer_projected_is_deleted(catalogue, estate):
    spark = catalogue
    projection = _projection(estate, LAKEHOUSE)
    _run(spark, reconcile(projection).statements)
    assert len(read_table(spark, REGISTRY, scope=LAKEHOUSE)) == 5

    # A repository that now declares fewer objects.
    reduced = project_installation(
        estate,
        retained=["delta:Sales.Region", "folder:Sales.CustomerCsv"],
        scope=LAKEHOUSE,
        target_name="Sales_LH",
        weaver_version="9.9.9",
    )
    _run(spark, reconcile(reduced).statements)
    assert {row["object_name"] for row in read_table(spark, REGISTRY, scope=LAKEHOUSE)} == {
        "Region",
        "CustomerCsv",
    }


def test_child_rows_of_a_removed_object_go_with_it(catalogue, estate):
    """No cascade mechanism needed: each table's delete is scoped the same way."""

    spark = catalogue
    _run(spark, reconcile(_projection(estate, LAKEHOUSE)).statements)
    reduced = project_installation(
        estate,
        retained=["delta:Sales.Region"],
        scope=LAKEHOUSE,
        target_name="Sales_LH",
        weaver_version="9.9.9",
    )
    _run(spark, reconcile(reduced).statements)
    read = read_installation(spark, scope=LAKEHOUSE)
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

    spark = catalogue
    _run(spark, reconcile(_projection(estate, LAKEHOUSE)).statements)
    _run(
        spark,
        reconcile(_projection(estate, WAREHOUSE, target_name="Sales_WH")).statements,
    )

    def warehouse_rows():
        return {
            name: tuple(sorted(map(repr, rows)))
            for name, rows in read_installation(spark, scope=WAREHOUSE).items()
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
    _run(spark, reconcile(reduced).statements)

    assert warehouse_rows() == before


def test_the_same_object_name_coexists_in_both_installations(catalogue, estate):
    spark = catalogue
    _run(spark, reconcile(_projection(estate, LAKEHOUSE)).statements)
    _run(
        spark,
        reconcile(_projection(estate, WAREHOUSE, target_name="Sales_WH")).statements,
    )
    rows = spark.sql(
        "SELECT target_type FROM `_`.`Registry` "
        "WHERE schema_name = 'Sales' AND object_name = 'Customer' ORDER BY target_type"
    ).collect()
    assert [row["target_type"] for row in rows] == ["lakehouse", "warehouse"]


def test_a_dependency_that_leaves_the_repository_is_stored_as_such(catalogue, estate):
    spark = catalogue
    _run(
        spark,
        reconcile(_projection(estate, WAREHOUSE, target_name="Sales_WH")).statements,
    )
    external = [
        row
        for row in read_table(spark, DEPENDENCY, scope=WAREHOUSE)
        if row["is_within_repository"] is False
    ]
    assert [
        (row["dependency_repository"], row["dependency_object_name"]) for row in external
    ] == [("Sales_LH", "Customer")]


# --- the summary a reviewer reads ---------------------------------------------


def test_the_summary_reports_inserts_then_no_ops(catalogue, estate):
    spark = catalogue
    projection = _projection(estate, LAKEHOUSE)

    first = summarise(projection, read_installation(spark, scope=LAKEHOUSE))
    assert sum(change.inserted for change in first) == projection.total
    assert all(change.updated == 0 and change.deleted == 0 for change in first)

    _run(spark, reconcile(projection).statements)

    second = summarise(projection, read_installation(spark, scope=LAKEHOUSE))
    assert all(change.is_noop for change in second)
    assert sum(change.unchanged for change in second) == projection.total


def test_the_summary_reports_an_update_when_only_a_non_key_column_changed(
    catalogue, estate
):
    spark = catalogue
    _run(spark, reconcile(_projection(estate, LAKEHOUSE)).statements)
    rebound = _projection(estate, LAKEHOUSE, target_name="Sales_LH_v2")
    change = compare(
        INSTALLATION,
        rebound.for_table(INSTALLATION),
        read_table(spark, INSTALLATION, scope=LAKEHOUSE),
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

    spark = catalogue
    _run(spark, reconcile(_projection(estate, LAKEHOUSE)).statements)
    reduced = project_installation(
        estate,
        retained=["delta:Sales.Region"],
        scope=LAKEHOUSE,
        target_name="Sales_LH",
        weaver_version="9.9.9",
    )
    change = compare(
        REGISTRY, reduced.for_table(REGISTRY), read_table(spark, REGISTRY, scope=LAKEHOUSE)
    )
    assert change.deleted == 4
    # The rendered statements mention no key that is being removed — they name only
    # what is kept, and delete the complement within the scope.
    statements = "\n".join(reconcile(reduced).statements)
    assert "CustomerCsv" not in statements


# --- explicit prune scopes ----------------------------------------------------


def test_installation_prune_removes_one_scope_and_leaves_the_other(catalogue, estate):
    spark = catalogue
    _run(spark, reconcile(_projection(estate, LAKEHOUSE)).statements)
    _run(
        spark,
        reconcile(_projection(estate, WAREHOUSE, target_name="Sales_WH")).statements,
    )

    _run(spark, prune_installation(LAKEHOUSE))

    assert all(rows == () for rows in read_installation(spark, scope=LAKEHOUSE).values())
    warehouse = read_installation(spark, scope=WAREHOUSE)
    assert warehouse["Registry"]
    assert warehouse["Installation"]


def test_repository_prune_removes_both_scopes(catalogue, estate):
    spark = catalogue
    _run(spark, reconcile(_projection(estate, LAKEHOUSE)).statements)
    _run(
        spark,
        reconcile(_projection(estate, WAREHOUSE, target_name="Sales_WH")).statements,
    )

    _run(spark, prune_repository("catalogue-estate"))

    for scope in (LAKEHOUSE, WAREHOUSE):
        assert all(rows == () for rows in read_installation(spark, scope=scope).values())


def test_pruning_one_repository_leaves_another_alone(catalogue, estate):
    spark = catalogue
    _run(spark, reconcile(_projection(estate, LAKEHOUSE)).statements)
    other = InstallationScope(repository="OtherRepo", target_type="lakehouse")
    spark.sql(
        "INSERT INTO `_`.`Registry` VALUES ('OtherRepo', 'lakehouse', 'X', 'Y', "
        "'table', 'data', 'sig', current_timestamp(), current_timestamp(), "
        "current_timestamp())"
    )

    _run(spark, prune_repository("catalogue-estate"))

    assert len(read_table(spark, REGISTRY, scope=other)) == 1


def test_prune_uncertifies_before_it_removes_the_descriptions(catalogue):
    """Registry first, so nothing is left certified while what described it is gone."""

    order = [statement.splitlines()[0] for statement in prune_installation(LAKEHOUSE)]
    assert "`Registry`" in order[0]
    assert "`Installation`" in order[1]
    assert order[-1].endswith("`_`.`SchemaDictionary`")
