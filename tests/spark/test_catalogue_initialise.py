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

from weaver.targets import ItemRef
from weaver.catalogue import (
    CATALOGUE_TABLES,
    DEPENDENCY,
    INSTALLATION,
    REGISTRY,
    SCHEMA_DICTIONARY,
    TABLE_DICTIONARY,
    InstallationScope,
)
from weaver.catalogue.reader import read_installation, read_table
from weaver.spark import SparkCatalogue, local_destination
from weaver.initialise import initialise_weaver_lakehouse

pytestmark = pytest.mark.spark

SCOPE = InstallationScope(item_type="Lakehouse", item_name="_weaver")


@dataclass(frozen=True)
class Bootstrapped:
    """One completed bootstrap, with the environment it ran against."""

    result: object
    workspace: object
    resolver: object
    store: object
    #: How this bootstrap's Weaver Lakehouse is addressed in the shared session.
    catalogue: object


def _environment(root):
    """A disposable Weaver Lakehouse skeleton at one root."""

    from weaver.targets import ItemRef
    from weaver.workspaces import LocalWorkspace
    from weaver.resolution import LocalResolver
    from weaver.store import LocalStore

    workspace = LocalWorkspace(workspace=root, weaver_lakehouse="Weaver")
    store = LocalStore()
    resolver = LocalResolver(workspace)
    store.make_directory(resolver.files_root(ItemRef("Weaver")))
    store.make_directory(resolver.tables_root(ItemRef("Weaver")))
    return workspace, resolver, store


def _bootstrap(spark, root) -> Bootstrapped:
    workspace, resolver, store = _environment(root)
    result = initialise_weaver_lakehouse(
        weaver_lakehouse=ItemRef("Weaver"), workspace=workspace, store=store, spark=spark
    )
    return Bootstrapped(
        result=result,
        workspace=workspace,
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
def initialised(spark, tmp_path_factory):
    """One bootstrap, shared by every read-only assertion in this file.

    Module-scoped on purpose. A full bootstrap is about twenty seconds of Spark, and
    almost everything here only *reads* the catalogue it produced — so paying for it
    once is the difference between this file taking a minute and taking five. The
    two tests that mutate anything run their own initialisation instead.
    """

    _drop_catalogue_schema(spark)
    try:
        yield _bootstrap(spark, tmp_path_factory.mktemp("weaver-initialise"))
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


def test_initialisation_succeeds(initialised):
    assert initialised.result.succeeded, _failures(initialised.result.report)


def test_the_package_owned_item_is_not_written_into_authored_source(initialised):
    root = initialised.resolver.weaver_items_root
    assert not initialised.store.exists(root.join("Lakehouse", "_weaver"))
    assert not initialised.store.exists(root)


def test_the_bootstrap_bundle_is_driver_local_and_temporary(initialised):
    assert initialised.result.plan.bundle_id
    assert not initialised.store.exists(initialised.resolver.build_bundles_root)


def test_every_catalogue_table_exists_and_matches_the_representation(initialised, spark):
    for table in CATALOGUE_TABLES:
        fields = spark.table(initialised.catalogue.qualify("_", table.name)).schema.fields
        assert [field.name for field in fields] == list(table.physical_columns), table.name


# --- and then describes itself -------------------------------------------------


def test_the_catalogue_registers_its_own_tables(initialised, spark):
    """The recursion, made visible: ten tables, each certified in its own Registry."""

    rows = [
        row
        for row in read_table(initialised.catalogue, REGISTRY, scope=SCOPE)
        if row["object_type"] == "table"
    ]
    assert {row["object_name"] for row in rows} == {
        table.name for table in CATALOGUE_TABLES
    }
    for row in rows:
        assert row["schema_name"] == "_"
        assert row["object_role"] == "data"
        assert row["signature"]


def test_the_catalogue_registers_the_task_log_folder_it_declares(initialised, spark):
    """The control plane owns one Folder as well as its tables, and certifies it."""

    rows = [
        row
        for row in read_table(initialised.catalogue, REGISTRY, scope=SCOPE)
        if row["object_type"] == "folder"
    ]

    assert [(row["schema_name"], row["object_name"]) for row in rows] == [
        ("Files/_", "Log")
    ]
    assert rows[0]["object_role"] == "data"
    assert rows[0]["signature"]


def test_the_catalogue_records_its_own_installation(initialised, spark):
    (row,) = read_table(initialised.catalogue, INSTALLATION, scope=SCOPE)
    assert row["target_name"] == "Weaver"
    assert row["weaver_version"]
    assert row["signature"]


def test_the_catalogue_describes_its_own_schema(initialised, spark):
    """Twice, because ``_`` names two namespaces — a Delta one and a Files one.

    The same asymmetry every Lakehouse item has once it owns a Folder: a schema
    holding tables and a directory holding folders are different places wearing
    one declared name, and the catalogue records both.
    """

    rows = {
        row["schema_name"]: row
        for row in read_table(initialised.catalogue, SCHEMA_DICTIONARY, scope=SCOPE)
    }

    assert set(rows) == {"_", "Files/_"}
    assert all("control plane" in row["description"] for row in rows.values())


def test_the_catalogue_describes_its_own_tables_and_their_keys(initialised, spark):
    rows = {
        row["object_name"]: row
        for row in read_table(
            initialised.catalogue, TABLE_DICTIONARY, scope=SCOPE
        )
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


def test_the_catalogue_describes_every_one_of_its_own_columns(initialised, spark):
    from weaver.catalogue.tables import COLUMN_DICTIONARY

    rows = read_table(initialised.catalogue, COLUMN_DICTIONARY, scope=SCOPE)
    described = {(row["object_name"], row["column_name"]) for row in rows}
    for table in CATALOGUE_TABLES:
        for column in table.columns:
            assert (table.name, column.name) in described, f"{table.name}.{column.name}"


def test_the_catalogue_records_its_own_logical_keys(initialised, spark):
    from weaver.catalogue.tables import INDEX_DICTIONARY

    rows = read_table(initialised.catalogue, INDEX_DICTIONARY, scope=SCOPE)
    keys = {(row["object_name"], row["column_set"]) for row in rows}
    for table in CATALOGUE_TABLES:
        assert (table.name, ", ".join(table.key)) in keys, table.name
    assert {row["index_type"] for row in rows} == {"primary_key"}


def test_the_catalogue_declares_no_dependencies_and_records_none(initialised, spark):
    """`Dependencies: []` all the way through — the bodies are literals."""

    assert read_table(initialised.catalogue, DEPENDENCY, scope=SCOPE) == ()


def test_no_alias_or_relationship_rows_are_invented(initialised, spark):
    read = read_installation(initialised.catalogue, scope=SCOPE, tables=CATALOGUE_TABLES)
    assert read["Alias"] == ()
    assert read["ForeignKeyDictionary"] == ()


def test_the_one_folder_the_catalogue_owns_is_described_like_any_other(
    initialised, spark
):
    """The task log, and nothing else. The control plane is otherwise tables."""

    read = read_installation(initialised.catalogue, scope=SCOPE, tables=CATALOGUE_TABLES)

    (row,) = read["FolderDictionary"]
    assert (row["schema_name"], row["object_name"]) == ("Files/_", "Log")
    assert row["file_key"] == "**/*"
    # Nothing loads into it: a task writes its own evidence, exactly as a Folder
    # object's authored code writes into its destination.
    assert row["is_static"] is True
    assert row["is_incremental"] is False


# --- ordering, as installed ----------------------------------------------------


def test_registry_is_published_before_the_local_control_refresh(initialised):
    numbers = [sequence.number for sequence in initialised.result.report.sequences]
    assert numbers == sorted(numbers)
    # Sequence numbers describe the assembled plan now, so the barriers are named
    # by what they are rather than by a reserved number.
    descriptions = [
        sequence.description for sequence in initialised.result.report.sequences
    ]
    assert descriptions[-2:] == [
        "publish item registry last",
        "refresh the Weaver Lakehouse SQL endpoint after catalogue DML",
    ]
    by_description = {
        sequence.description: sequence
        for sequence in initialised.result.report.sequences
    }
    assert by_description["publish item registry last"].status == "succeeded"
    # The emulator has no SQL analytics endpoint, so its refresh is explicitly
    # skipped rather than pretended.
    assert (
        by_description[
            "refresh the Weaver Lakehouse SQL endpoint after catalogue DML"
        ].status
        == "skipped"
    )
    # Everything that was not a refresh really ran. Both refreshes are skipped:
    # the built-in item's own, closing its Delta work, and the control plane's
    # after catalogue DML — the emulator has no endpoint for either.
    by_kind = {
        "refresh": [
            sequence
            for sequence in initialised.result.report.sequences
            if "refresh" in sequence.description
        ],
        "other": [
            sequence
            for sequence in initialised.result.report.sequences
            if "refresh" not in sequence.description
        ],
    }
    assert all(sequence.status == "succeeded" for sequence in by_kind["other"])
    assert by_kind["refresh"]
    assert all(sequence.status == "skipped" for sequence in by_kind["refresh"])


def test_initialisation_has_no_undeclared_catalogue_objects_to_prune(initialised):
    """The reserved catalogue scope already matches its built-in declaration."""

    kinds = {
        action.action_id
        for sequence in initialised.result.report.sequences
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


def test_a_users_own_schema_in_the_weaver_lakehouse_survives_initialisation(spark, tmp_path):
    """The built-in item's authoritative prune is restricted to schema `_`."""

    _drop_catalogue_schema(spark)
    workspace, resolver, store = _environment(tmp_path)
    weaver = SparkCatalogue(spark, resolver.spark_destination(ItemRef("Weaver")))
    weaver.create_schema("Scratch")
    weaver.sql("CREATE TABLE IF NOT EXISTS {{object:Scratch.Notes}} (x int) USING delta")
    try:
        result = initialise_weaver_lakehouse(
            weaver_lakehouse=ItemRef("Weaver"), workspace=workspace, store=store, spark=spark
        )
        assert result.succeeded, _failures(result.report)
        assert weaver.sql("SELECT * FROM {{object:Scratch.Notes}}").count() == 0
    finally:
        spark.sql(f"DROP SCHEMA IF EXISTS {weaver.qualified_schema('Scratch')} CASCADE")
        _drop_catalogue_schema(spark)


def test_re_running_initialisation_preserves_the_same_rows(spark, tmp_path):
    """An unchanged incremental plan preserves the published catalogue."""

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
            workspace=first.workspace,
            store=first.store,
            spark=spark,
        )
        assert again.succeeded, _failures(again.report)
        after = {
            name: tuple(sorted(map(repr, rows)))
            for name, rows in read_installation(
                first.catalogue, scope=SCOPE, tables=CATALOGUE_TABLES
            ).items()
        }
        assert after == before
    finally:
        _drop_catalogue_schema(spark)


def test_the_result_serialises_for_a_cli_without_owning_any_semantics(initialised):
    mapping = initialised.result.to_mapping()
    assert mapping["item"] == "Lakehouse/_weaver"
    assert mapping["weaver_lakehouse"] == "Weaver"
    assert mapping["status"] == "succeeded"
    assert len(mapping["tables"]) == 10
    assert mapping["bundle_id"]
