"""Physical ``_`` schemas as ``SOURCE_SHAPE_QUERY`` returns them.

``RELEASED_0_9_0`` is the catalogue Weaver 0.9.0 builds, recorded from that
release's declarations. ``current`` derives this version's from its own.
"""

from __future__ import annotations

import re

from weaver.catalogue.fork import copied_tables
from weaver.catalogue.tables import AUDIT_COLUMN_NAMES, public_column_name

_AUDIT = tuple(
    (public_column_name(name), "datetime2(6)", True) for name in AUDIT_COLUMN_NAMES
)

#: ``table: ((column, type, not null), ...)`` without the audit columns.
RELEASED_0_9_0 = {
    "SchemaDictionary": (("Item type", "varchar(128)", True), ("Item name", "varchar(128)", True), ("Schema name", "varchar(128)", True), ("Description", "varchar(4000)", False), ("Description reference", "varchar(128)", False), ("Signature", "varchar(128)", True),),
    "FolderDictionary": (("Item type", "varchar(128)", True), ("Item name", "varchar(128)", True), ("Schema name", "varchar(128)", True), ("Object name", "varchar(128)", True), ("Description", "varchar(4000)", False), ("Description reference", "varchar(128)", False), ("Lineage", "varchar(4000)", False), ("Lineage reference", "varchar(128)", False), ("File key", "varchar(1000)", False), ("Is incremental", "bit", False), ("Is static", "bit", False), ("Prohibit rebuild", "bit", False), ("Signature", "varchar(128)", True),),
    "TableDictionary": (("Item type", "varchar(128)", True), ("Item name", "varchar(128)", True), ("Schema name", "varchar(128)", True), ("Object name", "varchar(128)", True), ("Object type", "varchar(128)", True), ("Description", "varchar(4000)", False), ("Description reference", "varchar(128)", False), ("Lineage", "varchar(4000)", False), ("Lineage reference", "varchar(128)", False), ("Primary key", "varchar(1000)", False), ("Not null columns", "varchar(1000)", False), ("Identity column", "varchar(128)", False), ("Comparison columns", "varchar(1000)", False), ("Is incremental", "bit", False), ("Is static", "bit", False), ("Prohibit rebuild", "bit", False), ("Signature", "varchar(128)", True),),
    "ColumnDictionary": (("Item type", "varchar(128)", True), ("Item name", "varchar(128)", True), ("Schema name", "varchar(128)", True), ("Object name", "varchar(128)", True), ("Column name", "varchar(128)", True), ("Description", "varchar(4000)", False), ("Description reference", "varchar(128)", False), ("Is identity", "bit", False), ("Signature", "varchar(128)", True),),
    "KeyDictionary": (("Item type", "varchar(128)", True), ("Item name", "varchar(128)", True), ("Schema name", "varchar(128)", True), ("Object name", "varchar(128)", True), ("Key type", "varchar(128)", True), ("Column set", "varchar(1000)", True), ("Signature", "varchar(128)", True),),
    "ForeignKeyDictionary": (("Item type", "varchar(128)", True), ("Item name", "varchar(128)", True), ("Foreign schema name", "varchar(128)", True), ("Foreign object name", "varchar(128)", True), ("Foreign column set", "varchar(1000)", True), ("Primary item type", "varchar(128)", True), ("Primary item name", "varchar(128)", True), ("Primary schema name", "varchar(128)", True), ("Primary object name", "varchar(128)", True), ("Primary column set", "varchar(1000)", True), ("Signature", "varchar(128)", True),),
    "TestDictionary": (("Item type", "varchar(128)", True), ("Item name", "varchar(128)", True), ("Schema name", "varchar(128)", True), ("Object name", "varchar(128)", True), ("Test type", "varchar(128)", True), ("Description", "varchar(4000)", False), ("Description reference", "varchar(128)", False), ("Primary key", "varchar(1000)", False), ("Signature", "varchar(128)", True),),
    "Dependency": (("Item type", "varchar(128)", True), ("Item name", "varchar(128)", True), ("Referencing schema name", "varchar(128)", True), ("Referencing object name", "varchar(128)", True), ("Dependency reference", "varchar(1000)", True), ("Referenced item type", "varchar(128)", False), ("Referenced item name", "varchar(128)", False), ("Referenced schema name", "varchar(128)", False), ("Referenced object name", "varchar(128)", False), ("Signature", "varchar(128)", True),),
    "Shortcut": (("Item type", "varchar(128)", True), ("Item name", "varchar(128)", True), ("Shortcut ID", "varchar(128)", True), ("Schema name", "varchar(128)", True), ("Object name", "varchar(128)", False), ("Shortcut type", "varchar(128)", True), ("Target type", "varchar(128)", True), ("Target item type", "varchar(128)", True), ("Target item name", "varchar(128)", True), ("Target schema name", "varchar(128)", True), ("Target object name", "varchar(128)", False), ("Target workspace name", "varchar(128)", False), ("Signature", "varchar(128)", True),),
    "Installation": (("Item type", "varchar(128)", True), ("Item name", "varchar(128)", True), ("Target name", "varchar(128)", True), ("Weaver version", "varchar(128)", True), ("Signature", "varchar(128)", True),),
    "Registry": (("Item type", "varchar(128)", True), ("Item name", "varchar(128)", True), ("Schema name", "varchar(128)", True), ("Object name", "varchar(128)", True), ("Object type", "varchar(128)", True), ("Object role", "varchar(128)", True), ("Signature", "varchar(128)", True), ("Build datetime", "datetime2(6)", False),),
    "Bookmark": (("Item type", "varchar(128)", True), ("Item name", "varchar(128)", True), ("Schema name", "varchar(128)", True), ("Object name", "varchar(128)", True), ("Bookmark datetime", "datetime2(6)", True),),
    "LoadStatus": (("Item type", "varchar(128)", True), ("Item name", "varchar(128)", True), ("Schema name", "varchar(128)", True), ("Object name", "varchar(128)", True), ("Workflow ID", "varchar(128)", False), ("Result", "varchar(128)", True), ("Started datetime", "datetime2(6)", False), ("Completed datetime", "datetime2(6)", False), ("Duration milliseconds", "bigint", False),),
    "TestStatus": (("Item type", "varchar(128)", True), ("Item name", "varchar(128)", True), ("Schema name", "varchar(128)", True), ("Object name", "varchar(128)", True), ("Test type", "varchar(128)", False), ("Workflow ID", "varchar(128)", False), ("Result", "varchar(128)", True), ("Started datetime", "datetime2(6)", False), ("Completed datetime", "datetime2(6)", False), ("Duration milliseconds", "bigint", False), ("Failure count", "bigint", False),),
}  # fmt: skip

#: ``_.Mirror`` as 0.9.0 creates it when something is mirrored into a catalogue.
RELEASED_0_9_0_MIRROR = (("Item type", "varchar(128)", True), ("Item name", "varchar(128)", True), ("Schema name", "varchar(128)", True), ("Object name", "varchar(128)", True), ("Source workspace name", "varchar(128)", True), ("Source target name", "varchar(128)", True), ("Source schema name", "varchar(128)", True), ("Source object name", "varchar(128)", True), ("Physical type", "varchar(128)", True),)  # fmt: skip

#: Installation between 0.9.0 and Fabric GUIDs: ``Item ID`` held the target name.
INTERIM_INSTALLATION = (
    ("Item type", "varchar(128)", True),
    ("Item name", "varchar(128)", True),
    ("Target name", "varchar(128)", True),
    ("Weaver version", "varchar(128)", True),
    ("Workspace ID", "varchar(128)", False),
    ("Item ID", "varchar(128)", False),
    ("Signature", "varchar(128)", True),
)


def rows(tables, *, audited: bool = True) -> list[dict]:
    """``{table: ((column, type, not null), ...)}`` as information-schema rows."""

    found = []
    for table, columns in tables.items():
        for name, spelled, not_null in (*columns, *(_AUDIT if audited else ())):
            data_type, length = _parsed(spelled)
            found.append(
                {
                    "TABLE_NAME": table,
                    "COLUMN_NAME": name,
                    "IS_NULLABLE": "NO" if not_null else "YES",
                    "DATA_TYPE": data_type,
                    "CHARACTER_MAXIMUM_LENGTH": length,
                }
            )
    return found


def current(*, borrowed: bool = False) -> dict:
    """This version's copied tables, as its declarations build them."""

    found = {}
    for table in copied_tables(borrowed=borrowed):
        found[table.name] = tuple(
            (
                table.public_name_of(name),
                table.column(name).warehouse_type,
                table.column(name).not_null,
            )
            for name in table.physical_columns
            if name not in AUDIT_COLUMN_NAMES
        )
    return found


def released(*, borrowed: bool = False) -> dict:
    found = dict(RELEASED_0_9_0)
    if borrowed:
        found["Mirror"] = RELEASED_0_9_0_MIRROR
    return found


def _parsed(spelled: str) -> tuple[str, int | None]:
    match = re.fullmatch(r"(\w+)(?:\((max|\d+)\))?", spelled)
    assert match, spelled
    data_type, size = match.groups()
    if data_type not in {"varchar", "nvarchar", "char", "nchar", "varbinary"}:
        return data_type, None
    return data_type, -1 if size == "max" else int(size)
