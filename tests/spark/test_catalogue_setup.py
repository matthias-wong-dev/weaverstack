"""Bootstrapping the Weaver Lakehouse end to end, on local Delta.

This is the checkpoint's proof: Weaver installs its own catalogue through the
ordinary build path, and the catalogue ends up describing itself. If that works,
a catalogue table is an ordinary Weaver object — which is the claim everything
later in the design leans on.

One bundle does the whole bootstrap, because the barriers already order it: the
schema, then the tables, then the catalogue's own DML writing into the tables the
same bundle just created. Generation reads nothing, so an absent catalogue is not
a special case — it is the ordinary one.
"""

from __future__ import annotations

import pytest

from weaver import ItemRef, RepositoryRef
from weaver.catalogue import (
    CATALOGUE_REPOSITORY,
    CATALOGUE_TABLES,
    DEPENDENCY,
    INSTALLATION,
    REGISTRY,
    SCHEMA_DICTIONARY,
    TABLE_DICTIONARY,
    InstallationScope,
)
from weaver.catalogue.reader import read_installation, read_table
from weaver.setup import BUNDLE_NAME, initialise_weaver_lakehouse

pytestmark = pytest.mark.spark

SCOPE = InstallationScope(repository=CATALOGUE_REPOSITORY, target_type="lakehouse")


@pytest.fixture
def setup(lakehouses, spark):
    result = initialise_weaver_lakehouse(
        weaver_lakehouse=lakehouses.weaver,
        host=lakehouses.host,
        store=lakehouses.store,
        spark=spark,
    )
    try:
        yield result
    finally:
        spark.sql("DROP DATABASE IF EXISTS `_` CASCADE")


def _failures(report) -> str:
    return "\n".join(
        f"{action.action_id}: {action.error_type}: {action.error_message}"
        for sequence in report.sequences
        for action in sequence.actions
        if action.status == "failed"
    )


# --- it installs --------------------------------------------------------------


def test_setup_succeeds(setup):
    assert setup.succeeded, _failures(setup.report)


def test_the_repository_is_materialised_into_the_weaver_lakehouse(setup, lakehouses):
    root = lakehouses.resolver.repository(RepositoryRef(CATALOGUE_REPOSITORY))
    assert lakehouses.store.exists(root.join("_schemas", "_.yml"))
    for table in CATALOGUE_TABLES:
        assert lakehouses.store.exists(root.join(f"{table.qualified}.spark.sql"))
    assert "_schemas/_.yml" in setup.materialised


def test_the_bundle_is_kept_where_bundles_belong(setup, lakehouses):
    expected = lakehouses.resolver.build_bundle(BUNDLE_NAME)
    assert setup.bundle.location.value == expected.value


def test_every_catalogue_table_exists_and_matches_the_representation(setup, spark):
    for table in CATALOGUE_TABLES:
        fields = spark.table(f"`_`.`{table.name}`").schema.fields
        assert [field.name for field in fields] == list(table.physical_columns), table.name


# --- and then describes itself -------------------------------------------------


def test_the_catalogue_registers_its_own_tables(setup, spark):
    """The recursion, made visible: ten tables, each certified in its own Registry."""

    rows = read_table(spark, REGISTRY, scope=SCOPE)
    assert {row["object_name"] for row in rows} == {
        table.name for table in CATALOGUE_TABLES
    }
    for row in rows:
        assert row["schema_name"] == "_"
        assert row["object_type"] == "table"
        assert row["object_role"] == "data"
        assert row["signature"]


def test_the_catalogue_records_its_own_installation(setup, spark):
    (row,) = read_table(spark, INSTALLATION, scope=SCOPE)
    assert row["target_name"] == "Weaver"
    assert row["weaver_version"]
    assert row["signature"]


def test_the_catalogue_describes_its_own_schema(setup, spark):
    (row,) = read_table(spark, SCHEMA_DICTIONARY, scope=SCOPE)
    assert row["schema_name"] == "_"
    assert "control plane" in row["description"]


def test_the_catalogue_describes_its_own_tables_and_their_keys(setup, spark):
    rows = {
        row["object_name"]: row for row in read_table(spark, TABLE_DICTIONARY, scope=SCOPE)
    }
    for table in CATALOGUE_TABLES:
        row = rows[table.name]
        assert row["object_type"] == "table"
        assert row["primary_key"] == ", ".join(table.key)
        # Every catalogue table is Static and prohibits rebuild — which is what will
        # stop an ordinary build treating it as disposable once drop policy lands.
        assert row["is_static"] is True
        assert row["prohibit_rebuild"] is True
        assert row["is_incremental"] is False


def test_the_catalogue_describes_every_one_of_its_own_columns(setup, spark):
    from weaver.catalogue import COLUMN_DICTIONARY

    rows = read_table(spark, COLUMN_DICTIONARY, scope=SCOPE)
    described = {(row["object_name"], row["column_name"]) for row in rows}
    for table in CATALOGUE_TABLES:
        for column in table.columns:
            assert (table.name, column.name) in described, f"{table.name}.{column.name}"


def test_the_catalogue_records_its_own_logical_keys(setup, spark):
    from weaver.catalogue import INDEX_DICTIONARY

    rows = read_table(spark, INDEX_DICTIONARY, scope=SCOPE)
    keys = {(row["object_name"], row["column_set"]) for row in rows}
    for table in CATALOGUE_TABLES:
        assert (table.name, ", ".join(table.key)) in keys, table.name
    assert {row["index_type"] for row in rows} == {"primary_key"}


def test_the_catalogue_declares_no_dependencies_and_records_none(setup, spark):
    """`Dependencies: []` all the way through — the bodies are literals."""

    assert read_table(spark, DEPENDENCY, scope=SCOPE) == ()


def test_no_alias_or_relationship_rows_are_invented(setup, spark):
    read = read_installation(spark, scope=SCOPE)
    assert read["Alias"] == ()
    assert read["ForeignKeyDictionary"] == ()


def test_no_folder_rows_since_the_catalogue_has_no_folders(setup, spark):
    read = read_installation(spark, scope=SCOPE)
    assert read["FolderDictionary"] == ()


# --- ordering, as installed ----------------------------------------------------


def test_registry_is_published_after_the_dictionaries_and_the_installation(setup):
    numbers = [sequence.number for sequence in setup.report.sequences]
    assert numbers == sorted(numbers)
    assert numbers[-1] == 9020
    statuses = {sequence.number: sequence.status for sequence in setup.report.sequences}
    assert all(status == "succeeded" for status in statuses.values())


def test_setup_prunes_nothing(setup):
    """The Weaver Lakehouse belongs to the installation, not to this repository."""

    kinds = {
        action.action_id
        for sequence in setup.report.sequences
        for action in sequence.actions
    }
    assert not [name for name in kinds if name.startswith("prune-")]


def test_a_users_own_schema_in_the_weaver_lakehouse_survives_setup(lakehouses, spark):
    """The consequence of not pruning, asserted rather than assumed."""

    tables_root = lakehouses.resolver.tables_root(ItemRef("Weaver")).value
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `Scratch` LOCATION '{tables_root}/Scratch'")
    spark.sql("CREATE OR REPLACE TABLE `Scratch`.`Notes` (x int) USING delta")
    try:
        result = initialise_weaver_lakehouse(
            weaver_lakehouse=lakehouses.weaver,
            host=lakehouses.host,
            store=lakehouses.store,
            spark=spark,
        )
        assert result.succeeded, _failures(result.report)
        assert spark.table("`Scratch`.`Notes`").count() == 0  # still there
    finally:
        spark.sql("DROP DATABASE IF EXISTS `Scratch` CASCADE")
        spark.sql("DROP DATABASE IF EXISTS `_` CASCADE")


# --- re-running ----------------------------------------------------------------


def test_re_running_setup_produces_the_same_bundle_and_the_same_rows(
    setup, lakehouses, spark
):
    """Idempotent in shape: same package, same bundle identity, same catalogue.

    Not yet idempotent in *rows* for anything else — build still emits
    ``CREATE OR REPLACE TABLE``, so a re-run empties the tables before
    repopulating this repository's own rows. Only setup does that, and only until
    drop policy reads the signatures this catalogue now holds.
    """

    before = {
        name: tuple(sorted(map(repr, rows)))
        for name, rows in read_installation(spark, scope=SCOPE).items()
    }

    again = initialise_weaver_lakehouse(
        weaver_lakehouse=lakehouses.weaver,
        host=lakehouses.host,
        store=lakehouses.store,
        spark=spark,
    )
    assert again.succeeded, _failures(again.report)
    assert again.bundle.plan.bundle_id == setup.bundle.plan.bundle_id

    after = {
        name: tuple(sorted(map(repr, rows)))
        for name, rows in read_installation(spark, scope=SCOPE).items()
    }
    assert after == before


def test_the_result_serialises_for_a_cli_without_owning_any_semantics(setup):
    mapping = setup.to_mapping()
    assert mapping["repository"] == CATALOGUE_REPOSITORY
    assert mapping["weaver_lakehouse"] == "Weaver"
    assert mapping["status"] == "succeeded"
    assert len(mapping["tables"]) == 10
    assert mapping["bundle_id"]
