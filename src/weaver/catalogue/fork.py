"""Copying one catalogue's installed state into another.

A fork gives a destination catalogue the installed state of a source catalogue,
so an estate can be worked on without touching the estate it came from. The
destination Warehouse is emptied by an ordinary wipe and its ``_`` schema is
rebuilt by an ordinary build, so the tables the rows land in carry the declared
shape. This renders the copy that follows.

Two rules decide what moves.

**History stays where it happened.** ``_.Log`` and ``_.LoadStatistic`` record
what a run did to the source estate, and the fork did not do it. Everything else
moves: the projected tables say what is installed and where, and the
current-state tables say how far each object has been loaded and validated, both
of which the fork inherits.

**The destination's own ``_weaver`` rows are its build's, not the source's.**
The build that made the ``_`` schema published its own Installation, dictionary
and Registry rows for ``Warehouse/_weaver``, and those name the destination
Warehouse. Copying the source's would overwrite that with the address of a
catalogue this one is not.

The copy is server-side. Fabric spells another Warehouse in the same workspace
three-part, which is the same reach a built Warehouse's ``_`` surface views use,
so each table moves in one statement and no row passes through this process.
"""

from __future__ import annotations

from typing import Sequence

from ..declaration.model import WAREHOUSE
from .builtin import BUILTIN_ITEM
from .tables import (
    CATALOGUE_SCHEMA,
    CATALOGUE_TABLES,
    CURRENT_STATE_TABLES,
    HISTORY_TABLES,
    PROJECTED_TABLES,
    SCOPE_ITEM_NAME,
    SCOPE_ITEM_TYPE,
)
from .tsql import identifier, literal

#: The catalogue tables a fork copies. Everything but history, which belongs to
#: the estate whose runs produced it.
FORKED_TABLES = PROJECTED_TABLES + CURRENT_STATE_TABLES


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


def copy_statement(table, *, source_catalogue: str) -> str:
    """Copy one table's rows from ``source_catalogue`` into this one.

    Columns are named rather than starred, so the statement says which value
    lands where and does not depend on two catalogues declaring their columns in
    one order.
    """

    columns = ", ".join(identifier(name) for name in table.public_columns)
    return (
        f"insert into {identifier(CATALOGUE_SCHEMA)}.{identifier(table.name)} "
        f"({columns})\n"
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


def fork_statements(*, source_catalogue: str) -> tuple[str, ...]:
    """Every statement that copies one catalogue's state into this one.

    Projected tables first and current state after, which is the order they
    describe an estate in: what is installed, then how far it has been run.
    """

    return tuple(
        copy_statement(table, source_catalogue=source_catalogue)
        for table in FORKED_TABLES
    )


def forked_table_names() -> tuple[str, ...]:
    """What a fork copies, by table name, for a caller reporting the work."""

    return tuple(table.name for table in FORKED_TABLES)


def uncopied_table_names() -> tuple[str, ...]:
    """What a fork leaves behind, by table name."""

    return tuple(table.name for table in HISTORY_TABLES)


def _every_table_is_accounted_for() -> bool:
    """Whether the two halves cover the catalogue. Held by an invariant test."""

    return {table.name for table in FORKED_TABLES} | {
        table.name for table in HISTORY_TABLES
    } == {table.name for table in CATALOGUE_TABLES}


__all__: Sequence[str] = [
    "FORKED_TABLES",
    "copy_statement",
    "fork_statements",
    "forked_table_names",
    "source_relation",
    "uncopied_table_names",
]
