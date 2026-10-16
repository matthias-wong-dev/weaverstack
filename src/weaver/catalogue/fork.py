"""Copy one catalogue's installed state into another.

History stays where it happened, so ``_.Log`` and ``_.LoadStatistic`` remain in
the source. The destination keeps the ``Warehouse/_weaver`` rows from its own
build. ``_.Mirror`` moves only when present because no document declares it.

The copy is server-side: Fabric spells another Warehouse in the same workspace
three-part, so each table moves in one statement.

The source may be an older catalogue. Its columns are copied by name: a table
it predates starts empty, a nullable column it predates is left null, and an
Installation without Fabric identities takes the ones preflight verified.

See https://docs.weaverstack.dev/core-concepts/catalogue/.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Sequence

from ..declaration.model import WAREHOUSE
from ..errors import CatalogueStateError
from .builtin import BUILTIN_ITEM
from .tables import (
    AUDIT_COLUMN_NAMES,
    CATALOGUE_SCHEMA,
    CATALOGUE_TABLES,
    CURRENT_STATE_TABLES,
    HISTORY_TABLES,
    INSTALLATION,
    MIRROR,
    PROJECTED_TABLES,
    SCOPE_ITEM_NAME,
    SCOPE_ITEM_TYPE,
)
from .tsql import identifier, literal

#: Current installed state, excluding history owned by the source installation.
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


def copy_statement(
    table, *, source_catalogue: str, columns: Mapping[str, str] | None = None
) -> str:
    """Copy one table's rows from ``source_catalogue`` into this catalogue.

    Named columns make the copy independent of declaration order. All values,
    including ``build_datetime``, retain their source values. ``columns`` maps
    each destination column to the source column holding it, and a destination
    column it omits is left null; by default the source has this version's
    columns.
    """

    pairs = _pairs(table, columns)
    return (
        f"insert into {local_relation(table)} "
        f"({', '.join(identifier(public) for public, _ in pairs)})\n"
        f"select {', '.join(identifier(source) for _, source in pairs)}\n"
        f"  from {source_relation(source_catalogue, table)}\n"
        f" where {_excluding_builtin(table, columns)};"
    )


def _pairs(table, columns) -> list[tuple[str, str]]:
    if columns is None:
        return [(public, public) for public in table.public_columns]
    return [
        (public, columns[public])
        for public in table.public_columns
        if public in columns
    ]


def _excluding_builtin(table, columns=None, *, alias: str = "") -> str:
    # Both scope columns are non-null, so the negated conjunction is exact.
    source = dict(_pairs(table, columns))
    item_type = alias + identifier(source[table.public_name_of(SCOPE_ITEM_TYPE)])
    item_name = alias + identifier(source[table.public_name_of(SCOPE_ITEM_NAME)])
    return (
        f"not ({item_type} = {literal(WAREHOUSE)}"
        f" and {item_name} = {literal(BUILTIN_ITEM.item_name)})"
    )


def copy_statements(
    *, source_catalogue: str, borrowed: bool = False, source: ForkSource | None = None
) -> tuple[str, ...]:
    """Copy every table the source holds, ``_.Mirror`` last when borrowed.

    ``source`` maps an older catalogue onto this version's tables.
    """

    if source is None:
        return tuple(
            copy_statement(table, source_catalogue=source_catalogue)
            for table in copied_tables(borrowed=borrowed)
        )
    statements = []
    for table in copied_tables(borrowed=source.borrowed):
        columns = source.columns.get(table.name)
        if columns is None:
            continue
        if table is INSTALLATION and source.legacy_installation:
            copied = installation_copy_statement(
                source_catalogue=source_catalogue,
                columns=columns,
                identities=source.identities,
            )
            statements.extend([copied] if copied else [])
        else:
            statements.append(
                copy_statement(
                    table, source_catalogue=source_catalogue, columns=columns
                )
            )
    return tuple(statements)


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


# --- an older source ----------------------------------------------------------

#: Installation columns an older source lacks. Preflight resolves them from Fabric.
IDENTITY_COLUMNS = ("workspace_name", "workspace_id", "target_name", "item_id")


@dataclass(frozen=True)
class ForkSource:
    """The source catalogue's tables mapped onto this version's.

    ``columns`` maps each copied table the source holds, by name, from
    destination column to the source column holding it. A table it omits was
    introduced after the source and starts empty.
    """

    columns: Mapping[str, Mapping[str, str]]
    borrowed: bool = False
    #: The source Installation's columns, when it records no Fabric identities.
    legacy_installation: tuple[str, ...] = ()
    #: Every legacy Installation row with its verified identity, set by preflight.
    identities: tuple[Mapping, ...] | None = None


def project_source(shape, *, source: str) -> ForkSource:
    """Map ``shape``, casefolded table to casefolded column to its spelling.

    Columns the destination does not hold are left behind. A missing table that
    every catalogue has, or a missing required column, refuses the mirror.
    """

    from .state import INTRODUCED_TABLES

    borrowed = MIRROR.name.casefold() in shape
    missing: list[str] = []
    mapped: dict[str, dict[str, str]] = {}
    legacy: tuple[str, ...] = ()
    for table in copied_tables(borrowed=borrowed):
        present = shape.get(table.name.casefold())
        if present is None:
            if table.name not in INTRODUCED_TABLES:
                missing.append(table.name)
            continue
        identity = table is INSTALLATION and any(
            INSTALLATION.public_name_of(name).casefold() not in present
            for name in IDENTITY_COLUMNS
        )
        if identity:
            legacy = tuple(present.values())
        columns: dict[str, str] = {}
        for name in table.physical_columns:
            if identity and name in IDENTITY_COLUMNS:
                continue
            public = table.public_name_of(name)
            if public.casefold() in present:
                columns[public] = present[public.casefold()]
            elif name in AUDIT_COLUMN_NAMES or table.column(name).not_null:
                missing.append(f"{table.name}.{public}")
        mapped[table.name] = columns
    if missing:
        raise CatalogueStateError(
            f"The catalogue in {source} cannot be mirrored: it has no "
            + ", ".join(missing)
            + "."
        )
    return ForkSource(columns=mapped, borrowed=borrowed, legacy_installation=legacy)


def installation_copy_statement(
    *, source_catalogue: str, columns: Mapping[str, str], identities
) -> str:
    """Copy a legacy Installation with the identities preflight verified."""

    if not identities:
        return ""
    keys = [
        INSTALLATION.public_name_of(name) for name in (SCOPE_ITEM_TYPE, SCOPE_ITEM_NAME)
    ]
    resolved = [INSTALLATION.public_name_of(name) for name in IDENTITY_COLUMNS]
    projection = []
    for name in INSTALLATION.physical_columns:
        public = INSTALLATION.public_name_of(name)
        if name in IDENTITY_COLUMNS:
            projection.append((public, f"r.{identifier(public)}"))
        elif public in columns:
            projection.append((public, f"s.{identifier(columns[public])}"))
    values = ",\n       ".join(
        "("
        + ", ".join(
            literal(row[name])
            for name in (SCOPE_ITEM_TYPE, SCOPE_ITEM_NAME, *IDENTITY_COLUMNS)
        )
        + ")"
        for row in identities
    )
    joined = " and ".join(
        f"r.{identifier(key)} = s.{identifier(columns[key])}" for key in keys
    )
    return (
        f"insert into {local_relation(INSTALLATION)} "
        f"({', '.join(identifier(public) for public, _ in projection)})\n"
        f"select {', '.join(expression for _, expression in projection)}\n"
        f"  from {source_relation(source_catalogue, INSTALLATION)} as s\n"
        f"  join (values {values})\n"
        f"    as r ({', '.join(identifier(name) for name in keys + resolved)})\n"
        f"    on {joined}\n"
        f" where {_excluding_builtin(INSTALLATION, columns, alias='s.')};"
    )


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
    "ForkSource",
    "copied_tables",
    "copy_statement",
    "copy_statements",
    "create_statement",
    "fork_statements",
    "forked_table_names",
    "installation_copy_statement",
    "local_relation",
    "project_source",
    "source_relation",
    "uncopied_table_names",
]
