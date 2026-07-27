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

from dataclasses import dataclass

import pytest

from weaver import ItemRef
from weaver.catalogue.item_tables import (
    CATALOGUE_TABLES,
    DEPENDENCY,
    INSTALLATION,
    REGISTRY,
    SCHEMA_DICTIONARY,
    TABLE_DICTIONARY,
    ItemInstallationScope,
)
from weaver.catalogue.reader import read_installation, read_table
from weaver.spark import SparkCatalogue, local_destination
from weaver.setup import BUNDLE_NAME, initialise_weaver_lakehouse

pytestmark = pytest.mark.spark

SCOPE = ItemInstallationScope(item_type="Lakehouse", item_name="_weaver")


@dataclass(frozen=True)
class Bootstrapped:
    """One completed bootstrap, with the environment it ran against."""

    result: object
    host: object
    resolver: object
    store: object
    #: How this bootstrap's Weaver Lakehouse is addressed in the shared session.
    catalogue: object


def _environment(root):
    """A disposable Weaver Lakehouse skeleton at one root."""

    from weaver import ItemRef, LocalHost, LocalResolver, LocalStore

    host = LocalHost(root=root, weaver_lakehouse="Weaver")
    store = LocalStore()
    resolver = LocalResolver(host)
    store.make_directory(resolver.files_root(ItemRef("Weaver")))
    store.make_directory(resolver.tables_root(ItemRef("Weaver")))
    store.make_directory(resolver.weaver_items_root)
    return host, resolver, store


def _bootstrap(spark, root) -> Bootstrapped:
    host, resolver, store = _environment(root)
    result = initialise_weaver_lakehouse(
        weaver_lakehouse=ItemRef("Weaver"), host=host, store=store, spark=spark
    )
    return Bootstrapped(
        result=result,
        host=host,
        resolver=resolver,
        store=store,
        catalogue=SparkCatalogue(spark, resolver.spark_destination(ItemRef("Weaver"))),
    )


def _drop_catalogue_schema(spark) -> None:
    """Forget the shared session's registration of a Weaver Lakehouse now gone.

    Harness isolation, not product behaviour: production has one Weaver Lakehouse
    for the life of the session, while this file presents a succession of
    temporary directories under that one logical name.
    """

    destination = local_destination(item="Weaver", tables_root="/unused")
    spark.sql(f"DROP SCHEMA IF EXISTS {destination.qualified_schema('_')} CASCADE")


@pytest.fixture(scope="module")
def setup(spark, tmp_path_factory):
    """One bootstrap, shared by every read-only assertion in this file.

    Module-scoped on purpose. A full bootstrap is about twenty seconds of Spark, and
    almost everything here only *reads* the catalogue it produced — so paying for it
    once is the difference between this file taking a minute and taking five. The
    two tests that mutate anything run their own setup instead.
    """

    _drop_catalogue_schema(spark)
    try:
        yield _bootstrap(spark, tmp_path_factory.mktemp("weaver-setup"))
    finally:
        _drop_catalogue_schema(spark)


def _failures(report) -> str:
    return "\n".join(
        f"{action.action_id}: {action.error_type}: {action.error_message}"
        for sequence in report.sequences
        for action in sequence.actions
        if action.status == "failed"
    )


# --- it installs --------------------------------------------------------------


def test_setup_succeeds(setup):
    assert setup.result.succeeded, _failures(setup.result.report)


def test_the_repository_is_materialised_into_the_weaver_lakehouse(setup):
    root = setup.resolver.weaver_items_root
    assert setup.store.exists(root.join("Lakehouse", "_weaver", "schemas", "_.yml"))
    for table in CATALOGUE_TABLES:
        assert setup.store.exists(
            root.join("Lakehouse", "_weaver", f"{table.qualified}.sql")
        )
    assert "Lakehouse/_weaver/schemas/_.yml" in setup.result.materialised


def test_the_bundle_is_kept_where_bundles_belong(setup):
    expected = setup.resolver.build_bundle(BUNDLE_NAME)
    assert setup.result.bundle.location.value == expected.value


def test_every_catalogue_table_exists_and_matches_the_representation(setup, spark):
    for table in CATALOGUE_TABLES:
        fields = spark.table(setup.catalogue.qualify("_", table.name)).schema.fields
        assert [field.name for field in fields] == list(table.physical_columns), table.name


# --- and then describes itself -------------------------------------------------


def test_the_catalogue_registers_its_own_tables(setup, spark):
    """The recursion, made visible: ten tables, each certified in its own Registry."""

    rows = read_table(setup.catalogue, REGISTRY, scope=SCOPE)
    assert {row["object_name"] for row in rows} == {
        table.name for table in CATALOGUE_TABLES
    }
    for row in rows:
        assert row["schema_name"] == "_"
        assert row["object_type"] == "table"
        assert row["object_role"] == "data"
        assert row["signature"]


def test_the_catalogue_records_its_own_installation(setup, spark):
    (row,) = read_table(setup.catalogue, INSTALLATION, scope=SCOPE)
    assert row["target_name"] == "Weaver"
    assert row["weaver_version"]
    assert row["signature"]


def test_the_catalogue_describes_its_own_schema(setup, spark):
    (row,) = read_table(setup.catalogue, SCHEMA_DICTIONARY, scope=SCOPE)
    assert row["schema_name"] == "_"
    assert "control plane" in row["description"]


def test_the_catalogue_describes_its_own_tables_and_their_keys(setup, spark):
    rows = {
        row["object_name"]: row for row in read_table(setup.catalogue, TABLE_DICTIONARY, scope=SCOPE)
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
    from weaver.catalogue.item_tables import COLUMN_DICTIONARY

    rows = read_table(setup.catalogue, COLUMN_DICTIONARY, scope=SCOPE)
    described = {(row["object_name"], row["column_name"]) for row in rows}
    for table in CATALOGUE_TABLES:
        for column in table.columns:
            assert (table.name, column.name) in described, f"{table.name}.{column.name}"


def test_the_catalogue_records_its_own_logical_keys(setup, spark):
    from weaver.catalogue.item_tables import INDEX_DICTIONARY

    rows = read_table(setup.catalogue, INDEX_DICTIONARY, scope=SCOPE)
    keys = {(row["object_name"], row["column_set"]) for row in rows}
    for table in CATALOGUE_TABLES:
        assert (table.name, ", ".join(table.key)) in keys, table.name
    assert {row["index_type"] for row in rows} == {"primary_key"}


def test_the_catalogue_declares_no_dependencies_and_records_none(setup, spark):
    """`Dependencies: []` all the way through — the bodies are literals."""

    assert read_table(setup.catalogue, DEPENDENCY, scope=SCOPE) == ()


def test_no_alias_or_relationship_rows_are_invented(setup, spark):
    read = read_installation(setup.catalogue, scope=SCOPE, tables=CATALOGUE_TABLES)
    assert read["Alias"] == ()
    assert read["ForeignKeyDictionary"] == ()


def test_no_folder_rows_since_the_catalogue_has_no_folders(setup, spark):
    read = read_installation(setup.catalogue, scope=SCOPE, tables=CATALOGUE_TABLES)
    assert read["FolderDictionary"] == ()


# --- ordering, as installed ----------------------------------------------------


def test_registry_is_published_after_the_dictionaries_and_the_installation(setup):
    numbers = [sequence.number for sequence in setup.result.report.sequences]
    assert numbers == sorted(numbers)
    assert numbers[-1] == 9020
    statuses = {
        sequence.number: sequence.status for sequence in setup.result.report.sequences
    }
    assert all(status == "succeeded" for status in statuses.values())


def test_setup_prunes_nothing(setup):
    """The Weaver Lakehouse belongs to the installation, not to this repository."""

    kinds = {
        action.action_id
        for sequence in setup.result.report.sequences
        for action in sequence.actions
    }
    assert not [name for name in kinds if name.startswith("prune-")]


# --- tests that own schema `_` themselves --------------------------------------
#
# These two bootstrap their own catalogue rather than sharing the module's, because
# each needs a *pristine* one: schema `_` is a fixed name in one shared Spark
# catalog, so a test that creates or drops it takes the module's with it. They must
# therefore stay last in the file — anything ordered after them that reads the
# shared catalogue would find it gone. Only a test needing no Spark may follow.


def test_a_users_own_schema_in_the_weaver_lakehouse_survives_setup(spark, tmp_path):
    """The consequence of not pruning, asserted rather than assumed."""

    _drop_catalogue_schema(spark)
    host, resolver, store = _environment(tmp_path)
    weaver = SparkCatalogue(spark, resolver.spark_destination(ItemRef("Weaver")))
    weaver.create_schema("Scratch")
    weaver.sql("CREATE OR REPLACE TABLE {{object:Scratch.Notes}} (x int) USING delta")
    try:
        result = initialise_weaver_lakehouse(
            weaver_lakehouse=ItemRef("Weaver"), host=host, store=store, spark=spark
        )
        assert result.succeeded, _failures(result.report)
        assert weaver.sql("SELECT * FROM {{object:Scratch.Notes}}").count() == 0
    finally:
        spark.sql(f"DROP SCHEMA IF EXISTS {weaver.qualified_schema('Scratch')} CASCADE")
        _drop_catalogue_schema(spark)


def test_re_running_setup_produces_the_same_bundle_and_the_same_rows(spark, tmp_path):
    """Idempotent in shape: same package, same bundle identity, same catalogue.

    Not yet idempotent in *rows* for anything else — build still emits
    ``CREATE OR REPLACE TABLE``, so a re-run empties the tables before
    repopulating this repository's own rows. Only setup does that, and only until
    drop policy reads the signatures this catalogue now holds.
    """

    _drop_catalogue_schema(spark)
    try:
        first = _bootstrap(spark, tmp_path)
        assert first.result.succeeded, _failures(first.result.report)
        before = {
            name: tuple(sorted(map(repr, rows)))
            for name, rows in read_installation(
                first.catalogue, scope=SCOPE, tables=CATALOGUE_TABLES
            ).items()
        }
        assert before["Registry"], "the first run must have catalogued something"

        again = initialise_weaver_lakehouse(
            weaver_lakehouse=ItemRef("Weaver"),
            host=first.host,
            store=first.store,
            spark=spark,
        )
        assert again.succeeded, _failures(again.report)
        assert again.bundle.plan.bundle_id == first.result.bundle.plan.bundle_id

        after = {
            name: tuple(sorted(map(repr, rows)))
            for name, rows in read_installation(
                first.catalogue, scope=SCOPE, tables=CATALOGUE_TABLES
            ).items()
        }
        assert after == before
    finally:
        _drop_catalogue_schema(spark)


def test_the_result_serialises_for_a_cli_without_owning_any_semantics(setup):
    mapping = setup.result.to_mapping()
    assert mapping["item"] == "Lakehouse/_weaver"
    assert mapping["weaver_lakehouse"] == "Weaver"
    assert mapping["status"] == "succeeded"
    assert len(mapping["tables"]) == 10
    assert mapping["bundle_id"]
