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
from support.weaver_test import weaver_test

from weaver.catalogue.tables import (
    BOOKMARK,
    BOOKMARK_SENTINEL,
    BOOKMARK_SENTINEL_TEXT,
    PROJECTED_TABLES,
    KEY_TYPE_VOCABULARY,
    LOG,
    OBJECT_ROLE_VOCABULARY,
    OBJECT_TYPE_VOCABULARY,
    REGISTRY,
    RESULT_VOCABULARY,
    RUNTIME_TABLES,
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
    "Shortcut": (
        "Item type",
        "Item name",
        "Shortcut ID",
        "Schema name",
        "Object name",
        "Shortcut type",
        "Target type",
        "Target item type",
        "Target item name",
        "Target schema name",
        "Target object name",
        "Target workspace name",
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


BOOKMARK_COLUMNS = (
    "Item type",
    "Item name",
    "Schema name",
    "Object name",
    "Bookmark datetime",
    *AUDIT,
)


@weaver_test()
def test_the_catalogue_publishes_exactly_these_tables():
    assert {table.name for table in PROJECTED_TABLES} == set(PUBLIC_SCHEMA)


# --- the runtime-maintained tables --------------------------------------------


@weaver_test()
def test_the_bookmark_publishes_its_frozen_columns():
    assert BOOKMARK.public_columns == BOOKMARK_COLUMNS


@weaver_test()
def test_a_bookmark_is_keyed_by_the_identity_the_registry_uses():
    """One object seen twice, so the two rows have to agree about what it is.

    Not "the same four names" — the same four *columns*: a Registry row and a
    Bookmark row are the same installed object, and a key that drifted would
    leave a bookmark standing for something else.
    """

    assert BOOKMARK.key == REGISTRY.key[:4]
    assert BOOKMARK.key == ("item_type", "item_name", "schema_name", "object_name")
    assert BOOKMARK.public_columns[: len(BOOKMARK.key)] == (
        "Item type",
        "Item name",
        "Schema name",
        "Object name",
    )


@weaver_test()
def test_a_bookmark_datetime_is_microsecond_precision_and_not_null():
    """A bookmark is compared with source timestamps, so precision is contract."""

    column = BOOKMARK.column("bookmark_datetime")

    assert column.warehouse_type == "datetime2(6)"
    assert column.not_null


@weaver_test()
def test_the_sentinel_is_one_instant_spelled_two_ways():
    """T-SQL renders the text and Python compares the value; they must agree."""

    from datetime import datetime, timezone

    assert BOOKMARK_SENTINEL == datetime(1900, 1, 1, tzinfo=timezone.utc)
    assert BOOKMARK_SENTINEL_TEXT == "1900-01-01 00:00:00.000000"
    assert BOOKMARK_SENTINEL == datetime.fromisoformat(BOOKMARK_SENTINEL_TEXT).replace(
        tzinfo=timezone.utc
    )


@weaver_test()
def test_the_runtime_tables_are_not_catalogue_dictionaries():
    """Nothing projects them, so nothing reconciles them against a declaration."""

    projected = {table.name for table in PROJECTED_TABLES}

    assert {table.name for table in RUNTIME_TABLES} == {"Log", "Bookmark"}
    assert not projected & {table.name for table in RUNTIME_TABLES}


@pytest.mark.parametrize("name", sorted(PUBLIC_SCHEMA))
@weaver_test()
def test_every_public_column_has_its_frozen_name(name):
    table = next(table for table in PROJECTED_TABLES if table.name == name)

    assert table.public_columns == PUBLIC_SCHEMA[name]


@weaver_test()
def test_the_log_publishes_its_frozen_columns():
    assert LOG.public_columns == LOG_COLUMNS


@weaver_test()
def test_the_log_is_not_a_catalogue_dictionary():
    """It records what happened, not what is installed, so nothing reconciles it."""

    assert LOG.name not in {table.name for table in PROJECTED_TABLES}


@weaver_test()
def test_internal_keys_stay_snake_case():
    """The mapping is a persistence boundary, not a rename of Python."""

    for table in (*PROJECTED_TABLES, *RUNTIME_TABLES):
        for column in table.columns:
            assert column.name == column.name.lower(), column.name
            assert " " not in column.name, column.name


@weaver_test()
def test_index_dictionary_is_gone():
    """``KeyDictionary`` records logical keys; nothing builds an index."""

    assert "IndexDictionary" not in PUBLIC_SCHEMA
    assert "IndexDictionary" not in {table.name for table in PROJECTED_TABLES}


@weaver_test()
def test_build_datetime_replaced_the_public_build_epoch():
    registry = next(table for table in PROJECTED_TABLES if table.name == "Registry")

    assert "Build datetime" in registry.public_columns
    assert not any("epoch" in column.lower() for column in registry.public_columns)


# --- stored value vocabularies ------------------------------------------------


@weaver_test()
def test_object_type_vocabulary_is_frozen():
    assert OBJECT_TYPE_VOCABULARY == {
        "folder": "Folder",
        "table": "Table",
        "view": "View",
        "file": "File",
        "stored_procedure": "Stored procedure",
        "schema": "Schema",
    }


@weaver_test()
def test_object_role_vocabulary_is_frozen():
    assert OBJECT_ROLE_VOCABULARY == {
        "data": "Data",
        "load": "Load",
        "test": "Test",
        "assumption": "Assumption",
        "shortcut": "Shortcut",
    }


@weaver_test()
def test_key_type_vocabulary_is_frozen():
    assert KEY_TYPE_VOCABULARY == {"primary_key": "Primary key", "unique": "Unique"}


@weaver_test()
def test_test_type_vocabulary_is_frozen():
    assert TEST_TYPE_VOCABULARY == {"test": "Test", "assumption": "Assumption"}


@weaver_test()
def test_result_vocabulary_is_frozen():
    assert RESULT_VOCABULARY == {
        "succeeded": "Succeeded",
        "failed": "Failed",
        "skipped": "Skipped",
        "blocked": "Blocked",
    }


@weaver_test()
def test_a_stored_value_round_trips_through_its_vocabulary():
    registry = next(table for table in PROJECTED_TABLES if table.name == "Registry")
    column = registry.column("object_type")

    assert column.to_public("stored_procedure") == "Stored procedure"
    assert column.from_public("Stored procedure") == "stored_procedure"
    assert column.to_public(None) is None


@weaver_test()
def test_an_unknown_internal_value_is_refused_rather_than_written():
    registry = next(table for table in PROJECTED_TABLES if table.name == "Registry")

    with pytest.raises(ValueError, match="object_type does not accept"):
        registry.column("object_type").to_public("sproc")


@weaver_test()
def test_an_unknown_stored_value_reads_as_written():
    """A newer catalogue may hold a value this Weaver has no name for."""

    registry = next(table for table in PROJECTED_TABLES if table.name == "Registry")

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
@weaver_test()
def test_public_names_follow_the_sentence_case_rules(internal, public):
    assert public_column_name(internal) == public


# --- every node status has a public Result -----------------------------------


@weaver_test()
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
