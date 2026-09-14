"""Copy one catalogue's installed state into another.

History stays where it happened, so ``_.Log`` and ``_.LoadStatistic`` remain in
the source. The destination keeps the ``Warehouse/_weaver`` rows from its own
build. ``_.Mirror`` moves only when present because no document declares it.

The copy is server-side: Fabric spells another Warehouse in the same workspace
three-part, so each table moves in one statement.

See ``design/catalogue.md``.
"""

from __future__ import annotations

from typing import Sequence

from ..declaration.model import WAREHOUSE
from .builtin import BUILTIN_ITEM
from .tables import (
    AUDIT_COLUMN_NAMES,
    CATALOGUE_SCHEMA,
    CATALOGUE_TABLES,
    CURRENT_STATE_TABLES,
    HISTORY_TABLES,
    MIRROR,
    PROJECTED_TABLES,
    SCOPE_ITEM_NAME,
    SCOPE_ITEM_TYPE,
)
from .tsql import identifier, literal

#: Current installed state, excluding history owned by the source estate.
FORKED_TABLES = PROJECTED_TABLES + CURRENT_STATE_TABLES

_AUDIT_TYPE = "datetime2(6)"


def source_relation(catalogue_name: str, table) -> str:
    """Name a same-workspace source table from the destination connection."""

    return ".".join(
        identifier(part) for part in (catalogue_name, CATALOGUE_SCHEMA, table.name)
    )


def local_relation(table) -> str:
    return f"{identifier(CATALOGUE_SCHEMA)}.{identifier(table.name)}"


def create_statement(table) -> str:
    """Create ``_.Mirror`` when absent; no build document declares it."""

    definitions = ",\n    ".join(
        f"{identifier(table.public_name_of(name))} {_definition(table, name)}"
        for name in table.physical_columns
    )
    return (
        f"if object_id(N'{CATALOGUE_SCHEMA}.{table.name}', N'U') is null\n"
        f"create table {local_relation(table)} (\n    {definitions}\n);"
    )


def _definition(table, name: str) -> str:
    # Audit columns are outside each table contract and are always written.
    if name in AUDIT_COLUMN_NAMES:
        return f"{_AUDIT_TYPE} not null"
    column = table.column(name)
    return column.warehouse_type + (" not null" if column.not_null else "")


def copy_statement(table, *, source_catalogue: str) -> str:
    """Copy one table's rows from ``source_catalogue`` into this catalogue.

    Named columns make the copy independent of declaration order. All values,
    including ``build_datetime``, retain their source values.
    """

    columns = ", ".join(identifier(name) for name in table.public_columns)
    return (
        f"insert into {local_relation(table)} ({columns})\n"
        f"select {columns}\n"
        f"  from {source_relation(source_catalogue, table)}\n"
        f" where {_excluding_builtin(table)};"
    )


def _excluding_builtin(table) -> str:
    # Both scope columns are non-null, so the negated conjunction is exact.
    item_type = identifier(table.public_name_of(SCOPE_ITEM_TYPE))
    item_name = identifier(table.public_name_of(SCOPE_ITEM_NAME))
    return (
        f"not ({item_type} = {literal(WAREHOUSE)}"
        f" and {item_name} = {literal(BUILTIN_ITEM.item_name)})"
    )


def fork_statements(
    *, source_catalogue: str, borrowed: bool = False
) -> tuple[str, ...]:
    """Return the state-copy statements, including ``_.Mirror`` when present."""

    statements = [
        copy_statement(table, source_catalogue=source_catalogue)
        for table in FORKED_TABLES
    ]
    if borrowed:
        statements.append(create_statement(MIRROR))
        statements.append(copy_statement(MIRROR, source_catalogue=source_catalogue))
    return tuple(statements)


def copied_tables(*, borrowed: bool = False) -> tuple:
    """Return copied tables in order, with an existing ``_.Mirror`` last."""
    return FORKED_TABLES + (MIRROR,) if borrowed else FORKED_TABLES


def forked_table_names(*, borrowed: bool = False) -> tuple[str, ...]:
    return tuple(table.name for table in copied_tables(borrowed=borrowed))


def uncopied_table_names() -> tuple[str, ...]:
    return tuple(table.name for table in HISTORY_TABLES)


def _every_declared_table_is_accounted_for() -> bool:
    return {table.name for table in FORKED_TABLES} | {
        table.name for table in HISTORY_TABLES
    } == {table.name for table in CATALOGUE_TABLES}


__all__: Sequence[str] = [
    "FORKED_TABLES",
    "copied_tables",
    "copy_statement",
    "create_statement",
    "fork_statements",
    "forked_table_names",
    "local_relation",
    "source_relation",
    "uncopied_table_names",
]
