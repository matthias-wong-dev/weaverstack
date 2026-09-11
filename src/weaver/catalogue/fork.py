"""The statements that copy one catalogue's installed state into another.

Three rules decide what moves. History stays where it happened, so ``_.Log`` and
``_.LoadStatistic`` are left. The destination's own ``Warehouse/_weaver`` rows
are its build's, so the source's are excluded. ``_.Mirror`` moves only where the
source has one, since nothing declares that table.

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

#: The declared catalogue tables a fork copies. Everything but history, which
#: belongs to the estate whose runs produced it.
FORKED_TABLES = PROJECTED_TABLES + CURRENT_STATE_TABLES

#: What the three audit columns are physically, as every catalogue table
#: carries them.
_AUDIT_TYPE = "datetime2(6)"


def source_relation(catalogue_name: str, table) -> str:
    """One source catalogue table, three-part.

    Three parts because the connection is open against the destination
    Warehouse, and three-part is how a Fabric Warehouse reaches another item in
    the same workspace. See :func:`weaver.build_bundle.shortcuts.view_statement`,
    which spells a Warehouse's ``_`` surface views the same way.
    """

    return ".".join(
        identifier(part) for part in (catalogue_name, CATALOGUE_SCHEMA, table.name)
    )


def local_relation(table) -> str:
    """One table of this catalogue, as a statement running against it names it."""

    return f"{identifier(CATALOGUE_SCHEMA)}.{identifier(table.name)}"


def create_statement(table) -> str:
    """The table, created where this catalogue has none.

    For ``_.Mirror`` alone, which no document declares and no build makes.
    """

    definitions = ",\n    ".join(
        f"{identifier(table.public_name_of(name))} {_definition(table, name)}"
        for name in table.physical_columns
    )
    return (
        f"if object_id(N'{CATALOGUE_SCHEMA}.{table.name}', N'U') is null\n"
        f"create table {local_relation(table)} (\n    {definitions}\n);"
    )


def _definition(table, name: str) -> str:
    """One column as the Warehouse declares it: its type, and whether it is null.

    The audit trio is not among a table's own columns, so its type comes from
    the one every catalogue table carries. All three are written on every row,
    so none has a valid null state.
    """

    if name in AUDIT_COLUMN_NAMES:
        return f"{_AUDIT_TYPE} not null"
    column = table.column(name)
    return column.warehouse_type + (" not null" if column.not_null else "")


def copy_statement(table, *, source_catalogue: str) -> str:
    """Copy one table's rows from ``source_catalogue`` into this one.

    Columns are named rather than starred, so the statement says which value
    lands where and does not depend on two catalogues declaring their columns in
    one order. Every value is the source's, ``build_datetime`` included: a
    forked Registry is the installed history this estate inherits, and when a
    mirror physically established something is what ``_.Mirror`` records.
    """

    columns = ", ".join(identifier(name) for name in table.public_columns)
    return (
        f"insert into {local_relation(table)} ({columns})\n"
        f"select {columns}\n"
        f"  from {source_relation(source_catalogue, table)}\n"
        f" where {_excluding_builtin(table)};"
    )


def _excluding_builtin(table) -> str:
    """Keep every row but the ones scoped to ``Warehouse/_weaver``.

    Both scope columns are not null on every forked table, so a plain negated
    conjunction is exact here and needs no null-safe form.
    """

    item_type = identifier(table.public_name_of(SCOPE_ITEM_TYPE))
    item_name = identifier(table.public_name_of(SCOPE_ITEM_NAME))
    return (
        f"not ({item_type} = {literal(WAREHOUSE)}"
        f" and {item_name} = {literal(BUILTIN_ITEM.item_name)})"
    )


def fork_statements(
    *, source_catalogue: str, borrowed: bool = False
) -> tuple[str, ...]:
    """Every statement that copies one catalogue's state into this one.

    ``borrowed`` says the source holds a ``_.Mirror``. Set, the destination is
    given one and its rows come across.
    """

    statements = [
        copy_statement(table, source_catalogue=source_catalogue)
        for table in FORKED_TABLES
    ]
    if borrowed:
        statements.append(create_statement(MIRROR))
        statements.append(copy_statement(MIRROR, source_catalogue=source_catalogue))
    return tuple(statements)


def copied_tables(*, borrowed: bool = False) -> tuple:
    """What a fork copies, in the order it copies them.

    ``_.Mirror`` is last, and only where the source has one.
    """

    return FORKED_TABLES + (MIRROR,) if borrowed else FORKED_TABLES


def forked_table_names(*, borrowed: bool = False) -> tuple[str, ...]:
    """What a fork copies, by table name, for a caller reporting the work."""

    return tuple(table.name for table in copied_tables(borrowed=borrowed))


def uncopied_table_names() -> tuple[str, ...]:
    """What a fork leaves behind, by table name."""

    return tuple(table.name for table in HISTORY_TABLES)


def _every_declared_table_is_accounted_for() -> bool:
    """Whether the two halves cover the declared catalogue. Held by a test."""

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
