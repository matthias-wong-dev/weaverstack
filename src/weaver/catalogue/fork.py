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
    LINEAGE_TABLES,
    MIRROR,
    PROJECTED_TABLES,
    SCOPE_ITEM_NAME,
    SCOPE_ITEM_TYPE,
)
from .tsql import identifier, literal

#: Current installed state, excluding history owned by the source installation.
FORKED_TABLES = PROJECTED_TABLES + CURRENT_STATE_TABLES

#: Forked only when the source holds them. A catalogue built by an older Weaver
#: does not, and its items gain the rows on their next build.
OPTIONAL_TABLES = LINEAGE_TABLES

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
    *,
    source_catalogue: str,
    borrowed: bool = False,
    graphed: bool = True,
    source: ForkSource | None = None,
) -> tuple[str, ...]:
    """Copy every table the source holds, ``_.Mirror`` last when borrowed.

    ``source`` maps an older catalogue onto this version's tables.
    """

    if source is None:
        return tuple(
            copy_statement(table, source_catalogue=source_catalogue)
            for table in copied_tables(borrowed=borrowed, graphed=graphed)
        )
    statements = []
    for table in copied_tables(borrowed=source.borrowed, graphed=graphed):
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
    *, source_catalogue: str, borrowed: bool = False, graphed: bool = True
) -> tuple[str, ...]:
    """Return the state-copy statements, including ``_.Mirror`` when present."""

    statements = [
        copy_statement(table, source_catalogue=source_catalogue)
        for table in copied_tables(graphed=graphed)
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


def copied_tables(*, borrowed: bool = False, graphed: bool = True) -> tuple:
    """Return copied tables in order, with an existing ``_.Mirror`` last.

    ``graphed`` says whether the source holds :data:`OPTIONAL_TABLES`.
    """

    forked = tuple(
        table for table in FORKED_TABLES if graphed or table not in OPTIONAL_TABLES
    )
    return forked + (MIRROR,) if borrowed else forked


def forked_table_names(
    *, borrowed: bool = False, graphed: bool = True
) -> tuple[str, ...]:
    return tuple(
        table.name for table in copied_tables(borrowed=borrowed, graphed=graphed)
    )


def uncopied_table_names() -> tuple[str, ...]:
    return tuple(table.name for table in HISTORY_TABLES)


def _every_declared_table_is_accounted_for() -> bool:
    return {table.name for table in FORKED_TABLES} | {
        table.name for table in HISTORY_TABLES
    } == {table.name for table in CATALOGUE_TABLES}


__all__: Sequence[str] = [
    "FORKED_TABLES",
    "OPTIONAL_TABLES",
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


# --- lineage keys in the destination --------------------------------------------


def lineage_key_statements() -> tuple[str, ...]:
    """Key forked lineage by the destination's workspaces.

    A key hashes ``<workspace GUID>|<node ID>`` (see
    :func:`weaver.catalogue.lineage_rows.node_key`), and forked rows carry the
    source's workspaces. Each endpoint takes the workspace of the item that owns
    it, as ``_.Installation`` now records; an endpoint in no recorded item, and
    an External read, are keyed by the ID alone.
    """

    from .lineage_rows import NODE_KEY_DIGITS
    from .tables import EDGE_EXTERNAL, INSTALLATION, LINEAGE_EDGE, LINEAGE_NODE

    def column(table, name: str) -> str:
        return identifier(table.public_name_of(name))

    item_type, item_name = (
        column(INSTALLATION, "item_type"),
        column(INSTALLATION, "item_name"),
    )
    guid = f"nullif(lower(cast([i].{column(INSTALLATION, 'workspace_id')} as varchar(36))), '')"

    def key(node_id: str) -> str:
        text = (
            f"case when {guid} is null then cast({node_id} as varchar(8000)) else "
            f"cast(concat({guid}, '|', {node_id}) as varchar(8000)) end"
        )
        return (
            f"lower(left(convert(varchar(64), hashbytes('SHA2_256', {text}), 2), "
            f"{NODE_KEY_DIGITS}))"
        )

    def owned_by(node_id: str) -> str:
        item = f"concat([i].{item_type}, '/', [i].{item_name})"
        return (
            f"({node_id} = {item} or left({node_id}, len({item}) + 1) = "
            f"concat({item}, '/'))"
        )

    node, edge = local_relation(LINEAGE_NODE), local_relation(LINEAGE_EDGE)
    same_item = (
        f"[i].{item_type} = [{{alias}}].{item_type} and "
        f"[i].{item_name} = [{{alias}}].{item_name}"
    )
    installation = local_relation(INSTALLATION)
    node_id = f"[n].{column(LINEAGE_NODE, 'node_id')}"
    external = literal(LINEAGE_EDGE.column("edge_kind").to_public(EDGE_EXTERNAL))
    statements = [
        f"update [n] set {column(LINEAGE_NODE, 'node_key')} = {key(node_id)}\n"
        f"from {node} as [n]\n"
        f"left join {installation} as [i] on {same_item.format(alias='n')};",
    ]
    for end, where in (
        ("downstream", ""),
        ("upstream", f"\nwhere [e].{column(LINEAGE_EDGE, 'edge_kind')} <> {external}"),
        (
            "through",
            f"\nwhere [e].{column(LINEAGE_EDGE, 'through_node_id')} is not null",
        ),
    ):
        ends = f"[e].{column(LINEAGE_EDGE, f'{end}_node_id')}"
        statements.append(
            f"update [e] set {column(LINEAGE_EDGE, f'{end}_node_key')} = {key(ends)}\n"
            f"from {edge} as [e]\n"
            f"left join {installation} as [i] on {owned_by(ends)}{where};"
        )
    return tuple(statements)
