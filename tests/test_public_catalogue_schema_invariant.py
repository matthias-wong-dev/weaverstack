"""The public ``_`` schema, spelled out.

``_`` is a public interface: people write queries and reports against it, so its
table names, physical column names and stored value vocabularies are a contract
rather than a rendering of whatever Python happens to call things.

Every name below is written out rather than derived. A test that asked the code
what it produces would agree with any rename, which is the one thing this must
not do.
"""

from __future__ import annotations

import pytest

from weaver.catalogue.tables import (
    CATALOGUE_TABLES,
    KEY_TYPE_VOCABULARY,
    LOG,
    OBJECT_ROLE_VOCABULARY,
    OBJECT_TYPE_VOCABULARY,
    RESULT_VOCABULARY,
    TEST_TYPE_VOCABULARY,
    public_column_name,
)

AUDIT = ("Row insert datetime", "Row update datetime", "Row delete datetime")

#: Every catalogue table and the physical columns it publishes, in order.
PUBLIC_SCHEMA: dict[str, tuple[str, ...]] = {
    "Installation": (
        "Item type",
        "Item name",
        "Target name",
        "Weaver version",
        "Signature",
        *AUDIT,
    ),
    "Registry": (
        "Item type",
        "Item name",
        "Schema name",
        "Object name",
        "Object type",
        "Object role",
        "Signature",
        "Build datetime",
        *AUDIT,
    ),
    "SchemaDictionary": (
        "Item type",
        "Item name",
        "Schema name",
        "Description",
        "Description reference",
        "Signature",
        *AUDIT,
    ),
    "TableDictionary": (
        "Item type",
        "Item name",
        "Schema name",
        "Object name",
        "Object type",
        "Description",
        "Description reference",
        "Lineage",
        "Lineage reference",
        "Primary key",
        "Not null columns",
        "Identity column",
        "Comparison columns",
        "Is incremental",
        "Is static",
        "Prohibit rebuild",
        "Signature",
        *AUDIT,
    ),
    "FolderDictionary": (
        "Item type",
        "Item name",
        "Schema name",
        "Object name",
        "Description",
        "Description reference",
        "Lineage",
        "Lineage reference",
        "File key",
        "Is incremental",
        "Is static",
        "Prohibit rebuild",
        "Signature",
        *AUDIT,
    ),
    "ColumnDictionary": (
        "Item type",
        "Item name",
        "Schema name",
        "Object name",
        "Column name",
        "Description",
        "Description reference",
        "Is identity",
        "Signature",
        *AUDIT,
    ),
    "KeyDictionary": (
        "Item type",
        "Item name",
        "Schema name",
        "Object name",
        "Key type",
        "Column set",
        "Signature",
        *AUDIT,
    ),
    "ForeignKeyDictionary": (
        "Item type",
        "Item name",
        "Foreign schema name",
        "Foreign object name",
        "Foreign column set",
        "Primary item type",
        "Primary item name",
        "Primary schema name",
        "Primary object name",
        "Primary column set",
        "Signature",
        *AUDIT,
    ),
    "TestDictionary": (
        "Item type",
        "Item name",
        "Schema name",
        "Object name",
        "Test type",
        "Description",
        "Description reference",
        "Primary key",
        "Signature",
        *AUDIT,
    ),
    "Dependency": (
        "Item type",
        "Item name",
        "Referencing schema name",
        "Referencing object name",
        "Dependency reference",
        "Referenced item type",
        "Referenced item name",
        "Referenced schema name",
        "Referenced object name",
        "Signature",
        *AUDIT,
    ),
    "Alias": (
        "Item type",
        "Item name",
        "Destination schema name",
        "Destination object name",
        "Source item type",
        "Source item name",
        "Source schema name",
        "Source object name",
        "Signature",
        *AUDIT,
    ),
}

LOG_COLUMNS = (
    "Log SK",
    "Workflow ID",
    "Task type",
    "Target type",
    "Target name",
    "Schema name",
    "Object name",
    "Result",
    "Started datetime",
    "Completed datetime",
    "Duration milliseconds",
    "Message",
    "Details",
    # Weaver's audit trio, appended to every table it builds. Only the insert
    # datetime means anything for an append-oriented table.
    *AUDIT,
)


def test_the_catalogue_publishes_exactly_these_tables():
    assert {table.name for table in CATALOGUE_TABLES} == set(PUBLIC_SCHEMA)


@pytest.mark.parametrize("name", sorted(PUBLIC_SCHEMA))
def test_every_public_column_has_its_frozen_name(name):
    table = next(table for table in CATALOGUE_TABLES if table.name == name)

    assert table.public_columns == PUBLIC_SCHEMA[name]


def test_the_log_publishes_its_frozen_columns():
    assert LOG.public_columns == LOG_COLUMNS


def test_the_log_is_not_a_catalogue_dictionary():
    """It records what happened, not what is installed, so nothing reconciles it."""

    assert LOG.name not in {table.name for table in CATALOGUE_TABLES}


def test_internal_keys_stay_snake_case():
    """The mapping is a persistence boundary, not a rename of Python."""

    for table in (*CATALOGUE_TABLES, LOG):
        for column in table.columns:
            assert column.name == column.name.lower(), column.name
            assert " " not in column.name, column.name


def test_index_dictionary_is_gone():
    """``KeyDictionary`` records logical keys; nothing builds an index."""

    assert "IndexDictionary" not in PUBLIC_SCHEMA
    assert "IndexDictionary" not in {table.name for table in CATALOGUE_TABLES}


def test_build_datetime_replaced_the_public_build_epoch():
    registry = next(table for table in CATALOGUE_TABLES if table.name == "Registry")

    assert "Build datetime" in registry.public_columns
    assert not any("epoch" in column.lower() for column in registry.public_columns)


# --- stored value vocabularies ------------------------------------------------


def test_object_type_vocabulary_is_frozen():
    assert OBJECT_TYPE_VOCABULARY == {
        "folder": "Folder",
        "table": "Table",
        "view": "View",
        "file": "File",
        "stored_procedure": "Stored procedure",
    }


def test_object_role_vocabulary_is_frozen():
    assert OBJECT_ROLE_VOCABULARY == {
        "data": "Data",
        "load": "Load",
        "test": "Test",
        "assumption": "Assumption",
    }


def test_key_type_vocabulary_is_frozen():
    assert KEY_TYPE_VOCABULARY == {"primary_key": "Primary key", "unique": "Unique"}


def test_test_type_vocabulary_is_frozen():
    assert TEST_TYPE_VOCABULARY == {"test": "Test", "assumption": "Assumption"}


def test_result_vocabulary_is_frozen():
    assert RESULT_VOCABULARY == {
        "succeeded": "Succeeded",
        "failed": "Failed",
        "skipped": "Skipped",
        "blocked": "Blocked",
    }


def test_a_stored_value_round_trips_through_its_vocabulary():
    registry = next(table for table in CATALOGUE_TABLES if table.name == "Registry")
    column = registry.column("object_type")

    assert column.to_public("stored_procedure") == "Stored procedure"
    assert column.from_public("Stored procedure") == "stored_procedure"
    assert column.to_public(None) is None


def test_an_unknown_internal_value_is_refused_rather_than_written():
    registry = next(table for table in CATALOGUE_TABLES if table.name == "Registry")

    with pytest.raises(ValueError, match="object_type does not accept"):
        registry.column("object_type").to_public("sproc")


def test_an_unknown_stored_value_reads_as_written():
    """A newer catalogue may hold a value this Weaver has no name for."""

    registry = next(table for table in CATALOGUE_TABLES if table.name == "Registry")

    assert registry.column("object_type").from_public("Materialised view") == (
        "Materialised view"
    )


# --- the derivation itself ----------------------------------------------------


@pytest.mark.parametrize(
    "internal, public",
    [
        ("item_type", "Item type"),
        ("schema_name", "Schema name"),
        ("is_incremental", "Is incremental"),
        ("row_insert_datetime", "Row insert datetime"),
        ("workflow_id", "Workflow ID"),
        ("log_sk", "Log SK"),
        ("duration_milliseconds", "Duration milliseconds"),
        ("description_reference", "Description reference"),
    ],
)
def test_public_names_follow_the_sentence_case_rules(internal, public):
    assert public_column_name(internal) == public


# --- every node status has a public Result -----------------------------------


def test_every_node_status_maps_to_a_frozen_result():
    """A status with no mapping fails the run at its last step, in Fabric.

    Weaver has two node vocabularies — a load's and a validation's — and
    `_.Log` records both. When they were mapped by hand one of them was
    forgotten, and the first thing that noticed was a real workspace.
    """

    from weaver.run.evidence import RESULT_FOR_STATUS
    from weaver.run.result import (
        BLOCKED,
        FAILED,
        INVALID,
        PENDING,
        SKIPPED,
        SUCCEEDED,
        SUCCEEDED_WITH_REJECTS,
        VALIDATED,
    )
    from weaver.test_report import STATUSES as VALIDATION_STATUSES

    every = {
        SUCCEEDED,
        SUCCEEDED_WITH_REJECTS,
        FAILED,
        BLOCKED,
        SKIPPED,
        PENDING,
        INVALID,
        VALIDATED,
        *VALIDATION_STATUSES,
    }

    assert every <= set(RESULT_FOR_STATUS)
    assert set(RESULT_FOR_STATUS.values()) <= set(RESULT_VOCABULARY)
