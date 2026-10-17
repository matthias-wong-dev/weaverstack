"""Physical ``_`` schemas, as a catalogue connection reports them.

``RELEASED_0_9_0`` is the catalogue Weaver 0.9.0 builds, recorded from that
release's declarations. ``current`` derives this version's from its own.
"""

from __future__ import annotations

from weaver.catalogue.fork import copied_tables
from weaver.catalogue.tables import AUDIT_COLUMN_NAMES, public_column_name

_AUDIT = tuple(public_column_name(name) for name in AUDIT_COLUMN_NAMES)

#: ``table: (column, ...)`` without the audit columns.
RELEASED_0_9_0 = {
    "SchemaDictionary": ("Item type", "Item name", "Schema name", "Description", "Description reference", "Signature"),
    "FolderDictionary": ("Item type", "Item name", "Schema name", "Object name", "Description", "Description reference", "Lineage", "Lineage reference", "File key", "Is incremental", "Is static", "Prohibit rebuild", "Signature"),
    "TableDictionary": ("Item type", "Item name", "Schema name", "Object name", "Object type", "Description", "Description reference", "Lineage", "Lineage reference", "Primary key", "Not null columns", "Identity column", "Comparison columns", "Is incremental", "Is static", "Prohibit rebuild", "Signature"),
    "ColumnDictionary": ("Item type", "Item name", "Schema name", "Object name", "Column name", "Description", "Description reference", "Is identity", "Signature"),
    "KeyDictionary": ("Item type", "Item name", "Schema name", "Object name", "Key type", "Column set", "Signature"),
    "ForeignKeyDictionary": ("Item type", "Item name", "Foreign schema name", "Foreign object name", "Foreign column set", "Primary item type", "Primary item name", "Primary schema name", "Primary object name", "Primary column set", "Signature"),
    "TestDictionary": ("Item type", "Item name", "Schema name", "Object name", "Test type", "Description", "Description reference", "Primary key", "Signature"),
    "Dependency": ("Item type", "Item name", "Referencing schema name", "Referencing object name", "Dependency reference", "Referenced item type", "Referenced item name", "Referenced schema name", "Referenced object name", "Signature"),
    "Shortcut": ("Item type", "Item name", "Shortcut ID", "Schema name", "Object name", "Shortcut type", "Target type", "Target item type", "Target item name", "Target schema name", "Target object name", "Target workspace name", "Signature"),
    "Installation": ("Item type", "Item name", "Target name", "Weaver version", "Signature"),
    "Registry": ("Item type", "Item name", "Schema name", "Object name", "Object type", "Object role", "Signature", "Build datetime"),
    "Bookmark": ("Item type", "Item name", "Schema name", "Object name", "Bookmark datetime"),
    "LoadStatus": ("Item type", "Item name", "Schema name", "Object name", "Workflow ID", "Result", "Started datetime", "Completed datetime", "Duration milliseconds"),
    "TestStatus": ("Item type", "Item name", "Schema name", "Object name", "Test type", "Workflow ID", "Result", "Started datetime", "Completed datetime", "Duration milliseconds", "Failure count"),
}  # fmt: skip

#: ``_.Mirror`` as 0.9.0 creates it when something is mirrored into a catalogue.
RELEASED_0_9_0_MIRROR = ("Item type", "Item name", "Schema name", "Object name", "Source workspace name", "Source target name", "Source schema name", "Source object name", "Physical type")  # fmt: skip

#: Installation between 0.9.0 and Fabric GUIDs: ``Item ID`` held the target name.
INTERIM_INSTALLATION = ("Item type", "Item name", "Target name", "Weaver version", "Workspace ID", "Item ID", "Signature")  # fmt: skip


def shape(tables) -> dict:
    """``{table: (column, ...)}`` as ``CatalogueConnection.shape`` returns it."""

    return {
        table.casefold(): {name.casefold(): name for name in (*columns, *_AUDIT)}
        for table, columns in tables.items()
    }


def rows(tables) -> list[dict]:
    """The same tables as the information-schema rows that shape is read from."""

    return [
        {"TABLE_NAME": table, "COLUMN_NAME": name, "IS_NULLABLE": "NO"}
        for table, columns in tables.items()
        for name in (*columns, *_AUDIT)
    ]


def current(*, borrowed: bool = False) -> dict:
    """This version's copied tables, as its declarations build them."""

    return {
        table.name: tuple(
            table.public_name_of(name)
            for name in table.physical_columns
            if name not in AUDIT_COLUMN_NAMES
        )
        for table in copied_tables(borrowed=borrowed)
    }


def released(*, borrowed: bool = False) -> dict:
    found = dict(RELEASED_0_9_0)
    if borrowed:
        found["Mirror"] = RELEASED_0_9_0_MIRROR
    return found
