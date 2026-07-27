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

from weaver.catalogue.legacy import REGISTRY, TABLE_DICTIONARY, InstallationScope
from weaver.catalogue.reader import read_installation, read_table
from weaver.spark import SparkCatalogue, local_destination

pytestmark = pytest.mark.spark

SCOPE = InstallationScope(repository="SalesRepo", target_type="lakehouse")
OTHER = InstallationScope(repository="SalesRepo", target_type="warehouse")


@pytest.fixture
def absent(spark, lakehouses):
    """A Weaver Lakehouse whose catalogue schema has never been created."""

    catalogue = SparkCatalogue(
        spark, lakehouses.resolver.spark_destination(lakehouses.weaver)
    )
    spark.sql(f"DROP SCHEMA IF EXISTS {catalogue.qualified_schema('_')} CASCADE")
    return catalogue


def _bound(session):
    """One arbitrary destination, so a fake session can still be addressed."""

    return SparkCatalogue(session, local_destination(item="Weaver", tables_root="/x"))


class _Conf:
    """The minimum session policy surface a deliberately failing fake needs."""

    def set(self, _key, _value):
        pass


def _create(catalogue, name: str, columns: str) -> None:
    catalogue.sql(
        f"CREATE OR REPLACE TABLE {{{{object:_.{name}}}}} ({columns}) USING delta"
    )


def _registry_columns(catalogue, extra: str = "") -> None:
    _create(
        catalogue,
        "Registry",
        "repository string, target_type string, schema_name string, "
        "object_name string, object_type string, object_role string, "
        f"signature string, row_insert_datetime timestamp, "
        f"row_update_datetime timestamp, row_delete_datetime timestamp{extra}",
    )


# --- absence -----------------------------------------------------------------


def test_a_missing_table_reads_as_no_rows(weaver_catalogue):
    """Bootstrap: the build that writes the catalogue is the one that creates it."""

    assert read_table(weaver_catalogue, REGISTRY, scope=SCOPE) == ()


def test_a_missing_schema_reads_as_no_rows(absent):
    """Before the very first setup, schema `_` does not exist either."""

    assert read_table(absent, REGISTRY, scope=SCOPE) == ()


def test_every_table_reads_as_no_rows_before_the_catalogue_exists(absent):
    read = read_installation(absent, scope=SCOPE)
    assert set(read) == {"SchemaDictionary", "FolderDictionary", "TableDictionary",
                         "ColumnDictionary", "IndexDictionary", "ForeignKeyDictionary",
                         "Dependency", "Alias", "Installation", "Registry"}
    assert all(rows == () for rows in read.values())


def test_an_empty_table_reads_as_no_rows(weaver_catalogue):
    _registry_columns(weaver_catalogue)
    assert read_table(weaver_catalogue, REGISTRY, scope=SCOPE) == ()


# --- shape tolerance ---------------------------------------------------------


def test_a_missing_column_reads_as_a_typed_null(weaver_catalogue):
    """Upgrade: newer Weaver comparing against a table an older one created.

    ``object_role`` is the column this Weaver added. A row written before it
    existed must still read, so the next build can repair it by ordinary
    comparison rather than failing.
    """

    _create(
        weaver_catalogue,
        "Registry",
        "repository string, target_type string, schema_name string, "
        "object_name string, object_type string, signature string",
    )
    weaver_catalogue.sql(
        "INSERT INTO {{object:_.Registry}} VALUES "
        "('SalesRepo', 'lakehouse', 'Sales', 'Customer', 'table', 'abc')"
    )
    (row,) = read_table(weaver_catalogue, REGISTRY, scope=SCOPE)
    assert row["object_role"] is None
    assert set(row) == set(REGISTRY.column_names)


def test_an_unexpected_extra_column_is_ignored(weaver_catalogue):
    """The mirror case: an older Weaver reading a catalogue a newer one extended."""

    _registry_columns(weaver_catalogue, extra=", future_column string")
    weaver_catalogue.sql(
        "INSERT INTO {{object:_.Registry}} VALUES ('SalesRepo', 'lakehouse', 'Sales', "
        "'Customer', 'table', 'data', 'abc', current_timestamp(), "
        "current_timestamp(), current_timestamp(), 'ignored')"
    )
    (row,) = read_table(weaver_catalogue, REGISTRY, scope=SCOPE)
    assert "future_column" not in row
    assert row["object_type"] == "table"


def test_a_column_of_the_wrong_type_is_cast_to_the_expected_one(weaver_catalogue):
    """An older catalogue may have stored a boolean as a string.

    Left uncast, a projected boolean would never compare equal to it and every
    build would rewrite an unchanged row.
    """

    # Every column declared as a string, including the three booleans — the shape
    # a catalogue written before those columns were typed would have.
    _create(
        weaver_catalogue,
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
    weaver_catalogue.sql(f"INSERT INTO {{{{object:_.TableDictionary}}}} VALUES ({values})")
    (row,) = read_table(weaver_catalogue, TABLE_DICTIONARY, scope=SCOPE)
    assert row["is_static"] is True
    assert row["is_incremental"] is True
    assert row["prohibit_rebuild"] is False


def test_the_projected_row_has_exactly_the_expected_columns_in_order(weaver_catalogue):
    _registry_columns(weaver_catalogue)
    weaver_catalogue.sql(
        "INSERT INTO {{object:_.Registry}} VALUES ('SalesRepo', 'lakehouse', 'Sales', "
        "'Customer', 'table', 'data', 'abc', current_timestamp(), "
        "current_timestamp(), current_timestamp())"
    )
    (row,) = read_table(weaver_catalogue, REGISTRY, scope=SCOPE)
    assert list(row) == list(REGISTRY.column_names)


# --- scope -------------------------------------------------------------------


def test_a_read_is_narrowed_to_one_installation(weaver_catalogue):
    """A build compares and writes within one installation and sees no other."""

    _registry_columns(weaver_catalogue)
    for target_type in ("lakehouse", "warehouse"):
        weaver_catalogue.sql(
            f"INSERT INTO {{{{object:_.Registry}}}} VALUES ('SalesRepo', '{target_type}', 'Sales', "
            "'Customer', 'table', 'data', 'abc', current_timestamp(), "
            "current_timestamp(), current_timestamp())"
        )
    assert [row["target_type"] for row in read_table(weaver_catalogue, REGISTRY, scope=SCOPE)] == [
        "lakehouse"
    ]
    assert [row["target_type"] for row in read_table(weaver_catalogue, REGISTRY, scope=OTHER)] == [
        "warehouse"
    ]


def test_another_repository_s_rows_are_not_read(weaver_catalogue):
    _registry_columns(weaver_catalogue)
    for repository in ("SalesRepo", "OtherRepo"):
        weaver_catalogue.sql(
            f"INSERT INTO {{{{object:_.Registry}}}} VALUES ('{repository}', 'lakehouse', 'Sales', "
            "'Customer', 'table', 'data', 'abc', current_timestamp(), "
            "current_timestamp(), current_timestamp())"
        )
    rows = read_table(weaver_catalogue, REGISTRY, scope=SCOPE)
    assert [row["repository"] for row in rows] == ["SalesRepo"]


def test_omitting_the_scope_reads_every_installation(weaver_catalogue):
    # Used by repository-wide operations, never by a build.
    _registry_columns(weaver_catalogue)
    for target_type in ("lakehouse", "warehouse"):
        weaver_catalogue.sql(
            f"INSERT INTO {{{{object:_.Registry}}}} VALUES ('SalesRepo', '{target_type}', 'Sales', "
            "'Customer', 'table', 'data', 'abc', current_timestamp(), "
            "current_timestamp(), current_timestamp())"
        )
    assert len(read_table(weaver_catalogue, REGISTRY)) == 2


# --- real failures propagate -------------------------------------------------


def test_a_syntax_or_infrastructure_failure_is_not_read_as_an_empty_catalogue(weaver_catalogue):
    """The case that would be catastrophic if swallowed.

    A session that fails for any reason other than "not created yet" must raise.
    Reading it as no rows would tell the next build that nothing is catalogued.
    """

    class Failing:
        catalog = None
        conf = _Conf()

        def table(self, name):
            raise PermissionError("storage account access denied")

    with pytest.raises(PermissionError):
        read_table(_bound(Failing()), REGISTRY, scope=SCOPE)


def test_an_analysis_error_that_is_not_absence_still_propagates(weaver_catalogue):
    from pyspark.errors import AnalysisException

    class Failing:
        catalog = None
        conf = _Conf()

        def table(self, name):
            raise AnalysisException("DELTA_LOG_CORRUPTED: checkpoint is unreadable")

    with pytest.raises(AnalysisException):
        read_table(_bound(Failing()), REGISTRY, scope=SCOPE)


def test_reading_without_a_catalogue_is_refused_rather_than_treated_as_absence():
    """A session alone does not say which Lakehouse holds the catalogue.

    Refusing is the point: the alternative is reading the *attached* Lakehouse and
    reporting an empty catalogue that is merely somewhere else.
    """

    with pytest.raises(ValueError, match="bound to the Weaver Lakehouse"):
        read_table(None, REGISTRY, scope=SCOPE)
