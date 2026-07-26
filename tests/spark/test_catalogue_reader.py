"""Reading the catalogue tolerantly, without hiding a real failure.

Two absences are ordinary and must read as data: a table that does not exist yet
(bootstrap — the build that writes the catalogue is the build that creates it) and
a column an older Weaver never wrote (upgrade). A third case, an unexpected extra
column, is the mirror of the second: an older Weaver must not choke on a
catalogue a newer one extended.

Everything else must propagate. A permission error or a corrupt log read as "no
rows" would make the next build conclude that everything is new — and once drop
policy lands, that everything the catalogue no longer mentions may be removed.
That asymmetry is the point of the module and the point of these tests.
"""

from __future__ import annotations

import pytest

from weaver.catalogue import REGISTRY, TABLE_DICTIONARY, InstallationScope
from weaver.catalogue.reader import read_installation, read_table

pytestmark = pytest.mark.spark

SCOPE = InstallationScope(repository="SalesRepo", target_type="lakehouse")
OTHER = InstallationScope(repository="SalesRepo", target_type="warehouse")


@pytest.fixture
def catalogue_schema(spark, tmp_path):
    """An empty schema `_` pinned to this test's own directory."""

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `_` LOCATION '{tmp_path}/Tables/_'")
    try:
        yield
    finally:
        spark.sql("DROP SCHEMA IF EXISTS `_` CASCADE")


def _create(spark, name: str, columns: str) -> None:
    spark.sql(f"CREATE OR REPLACE TABLE `_`.`{name}` ({columns}) USING delta")


def _registry_columns(spark, extra: str = "") -> None:
    _create(
        spark,
        "Registry",
        "repository string, target_type string, schema_name string, "
        "object_name string, object_type string, object_role string, "
        f"signature string, row_insert_datetime timestamp, "
        f"row_update_datetime timestamp, row_delete_datetime timestamp{extra}",
    )


# --- absence -----------------------------------------------------------------


def test_a_missing_table_reads_as_no_rows(spark, catalogue_schema):
    """Bootstrap: the build that writes the catalogue is the one that creates it."""

    assert read_table(spark, REGISTRY, scope=SCOPE) == ()


def test_a_missing_schema_reads_as_no_rows(spark):
    """Before the very first setup, schema `_` does not exist either."""

    spark.sql("DROP SCHEMA IF EXISTS `_` CASCADE")
    assert read_table(spark, REGISTRY, scope=SCOPE) == ()


def test_every_table_reads_as_no_rows_before_the_catalogue_exists(spark):
    spark.sql("DROP SCHEMA IF EXISTS `_` CASCADE")
    read = read_installation(spark, scope=SCOPE)
    assert set(read) == {"SchemaDictionary", "FolderDictionary", "TableDictionary",
                         "ColumnDictionary", "IndexDictionary", "ForeignKeyDictionary",
                         "Dependency", "Alias", "Installation", "Registry"}
    assert all(rows == () for rows in read.values())


def test_an_empty_table_reads_as_no_rows(spark, catalogue_schema):
    _registry_columns(spark)
    assert read_table(spark, REGISTRY, scope=SCOPE) == ()


# --- shape tolerance ---------------------------------------------------------


def test_a_missing_column_reads_as_a_typed_null(spark, catalogue_schema):
    """Upgrade: newer Weaver comparing against a table an older one created.

    ``object_role`` is the column this Weaver added. A row written before it
    existed must still read, so the next build can repair it by ordinary
    comparison rather than failing.
    """

    _create(
        spark,
        "Registry",
        "repository string, target_type string, schema_name string, "
        "object_name string, object_type string, signature string",
    )
    spark.sql(
        "INSERT INTO `_`.`Registry` VALUES "
        "('SalesRepo', 'lakehouse', 'Sales', 'Customer', 'table', 'abc')"
    )
    (row,) = read_table(spark, REGISTRY, scope=SCOPE)
    assert row["object_role"] is None
    assert set(row) == set(REGISTRY.column_names)


def test_an_unexpected_extra_column_is_ignored(spark, catalogue_schema):
    """The mirror case: an older Weaver reading a catalogue a newer one extended."""

    _registry_columns(spark, extra=", future_column string")
    spark.sql(
        "INSERT INTO `_`.`Registry` VALUES ('SalesRepo', 'lakehouse', 'Sales', "
        "'Customer', 'table', 'data', 'abc', current_timestamp(), "
        "current_timestamp(), current_timestamp(), 'ignored')"
    )
    (row,) = read_table(spark, REGISTRY, scope=SCOPE)
    assert "future_column" not in row
    assert row["object_type"] == "table"


def test_a_column_of_the_wrong_type_is_cast_to_the_expected_one(spark, catalogue_schema):
    """An older catalogue may have stored a boolean as a string.

    Left uncast, a projected boolean would never compare equal to it and every
    build would rewrite an unchanged row.
    """

    # Every column declared as a string, including the three booleans — the shape
    # a catalogue written before those columns were typed would have.
    _create(
        spark,
        "TableDictionary",
        ", ".join(f"`{column.name}` string" for column in TABLE_DICTIONARY.columns),
    )
    stored = {
        "repository": "SalesRepo",
        "target_type": "lakehouse",
        "is_incremental": "true",
        "is_static": "true",
        "prohibit_rebuild": "false",
    }
    values = ", ".join(
        f"'{stored.get(column.name, column.name)}'" for column in TABLE_DICTIONARY.columns
    )
    spark.sql(f"INSERT INTO `_`.`TableDictionary` VALUES ({values})")
    (row,) = read_table(spark, TABLE_DICTIONARY, scope=SCOPE)
    assert row["is_static"] is True
    assert row["is_incremental"] is True
    assert row["prohibit_rebuild"] is False


def test_the_projected_row_has_exactly_the_expected_columns_in_order(spark, catalogue_schema):
    _registry_columns(spark)
    spark.sql(
        "INSERT INTO `_`.`Registry` VALUES ('SalesRepo', 'lakehouse', 'Sales', "
        "'Customer', 'table', 'data', 'abc', current_timestamp(), "
        "current_timestamp(), current_timestamp())"
    )
    (row,) = read_table(spark, REGISTRY, scope=SCOPE)
    assert list(row) == list(REGISTRY.column_names)


# --- scope -------------------------------------------------------------------


def test_a_read_is_narrowed_to_one_installation(spark, catalogue_schema):
    """A build compares and writes within one installation and sees no other."""

    _registry_columns(spark)
    for target_type in ("lakehouse", "warehouse"):
        spark.sql(
            f"INSERT INTO `_`.`Registry` VALUES ('SalesRepo', '{target_type}', 'Sales', "
            "'Customer', 'table', 'data', 'abc', current_timestamp(), "
            "current_timestamp(), current_timestamp())"
        )
    assert [row["target_type"] for row in read_table(spark, REGISTRY, scope=SCOPE)] == [
        "lakehouse"
    ]
    assert [row["target_type"] for row in read_table(spark, REGISTRY, scope=OTHER)] == [
        "warehouse"
    ]


def test_another_repository_s_rows_are_not_read(spark, catalogue_schema):
    _registry_columns(spark)
    for repository in ("SalesRepo", "OtherRepo"):
        spark.sql(
            f"INSERT INTO `_`.`Registry` VALUES ('{repository}', 'lakehouse', 'Sales', "
            "'Customer', 'table', 'data', 'abc', current_timestamp(), "
            "current_timestamp(), current_timestamp())"
        )
    rows = read_table(spark, REGISTRY, scope=SCOPE)
    assert [row["repository"] for row in rows] == ["SalesRepo"]


def test_omitting_the_scope_reads_every_installation(spark, catalogue_schema):
    # Used by repository-wide operations, never by a build.
    _registry_columns(spark)
    for target_type in ("lakehouse", "warehouse"):
        spark.sql(
            f"INSERT INTO `_`.`Registry` VALUES ('SalesRepo', '{target_type}', 'Sales', "
            "'Customer', 'table', 'data', 'abc', current_timestamp(), "
            "current_timestamp(), current_timestamp())"
        )
    assert len(read_table(spark, REGISTRY)) == 2


# --- real failures propagate -------------------------------------------------


def test_a_syntax_or_infrastructure_failure_is_not_read_as_an_empty_catalogue(
    spark, catalogue_schema
):
    """The case that would be catastrophic if swallowed.

    A session that fails for any reason other than "not created yet" must raise.
    Reading it as no rows would tell the next build that nothing is catalogued.
    """

    class Failing:
        def table(self, name):
            raise PermissionError("storage account access denied")

    with pytest.raises(PermissionError):
        read_table(Failing(), REGISTRY, scope=SCOPE)


def test_an_analysis_error_that_is_not_absence_still_propagates(spark, catalogue_schema):
    from pyspark.errors import AnalysisException

    class Failing:
        def table(self, name):
            raise AnalysisException("DELTA_LOG_CORRUPTED: checkpoint is unreadable")

    with pytest.raises(AnalysisException):
        read_table(Failing(), REGISTRY, scope=SCOPE)


def test_reading_without_a_session_is_refused_rather_than_treated_as_absence(spark):
    """The Weaver Lakehouse is always present, so no session is a caller's error."""

    with pytest.raises(ValueError, match="needs a Spark session"):
        read_table(None, REGISTRY, scope=SCOPE)
