"""Copy one catalogue's installed state into another.

History stays where it happened, so ``_.Log`` and ``_.LoadStatistic`` remain in
the source. The destination keeps the ``Warehouse/_weaver`` rows from its own
build. ``_.Mirror`` moves only when present because no document declares it.

The copy is server-side: Fabric spells another Warehouse in the same workspace
three-part, so each table moves in one statement.

The source may be an older catalogue. Its physical shape, read once before
anything is emptied, maps each source column onto this version's by name. A
table the source predates starts empty, a nullable column it predates is left
null, a retired column is left behind, and an Installation without recorded
Fabric identities takes the identities preflight verified. Anything else the
destination cannot hold refuses the mirror before the first wipe.

See https://docs.weaverstack.dev/core-concepts/catalogue/.
"""

from __future__ import annotations

from dataclasses import dataclass, field
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
    RETIRED_COLUMNS,
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
            statements.append(
                installation_copy_statement(
                    source_catalogue=source_catalogue,
                    columns=columns,
                    identities=source.identities,
                )
            )
        else:
            statements.append(
                copy_statement(
                    table, source_catalogue=source_catalogue, columns=columns
                )
            )
    return tuple(statements)


def fork_statements(
    *, source_catalogue: str, borrowed: bool = False, source: ForkSource | None = None
) -> tuple[str, ...]:
    """Return the state-copy statements, including ``_.Mirror`` when present."""

    borrowed = source.borrowed if source is not None else borrowed
    statements = list(
        copy_statements(
            source_catalogue=source_catalogue, borrowed=borrowed, source=source
        )
    )
    if borrowed:
        statements.insert(len(statements) - 1, create_statement(MIRROR))
    return tuple(statements)


# --- an older source ----------------------------------------------------------

#: The source catalogue's tables and columns, read before anything is emptied.
SOURCE_SHAPE_QUERY = (
    "select c.TABLE_NAME, c.COLUMN_NAME, c.IS_NULLABLE, c.DATA_TYPE, "
    "c.CHARACTER_MAXIMUM_LENGTH "
    "from INFORMATION_SCHEMA.COLUMNS as c "
    "join INFORMATION_SCHEMA.TABLES as t "
    "on t.TABLE_SCHEMA = c.TABLE_SCHEMA and t.TABLE_NAME = c.TABLE_NAME "
    f"where c.TABLE_SCHEMA = {literal(CATALOGUE_SCHEMA)} "
    "and t.TABLE_TYPE = N'BASE TABLE'"
)

#: Installation columns a legacy source lacks or holds unverified. Preflight
#: resolves them from Fabric.
IDENTITY_COLUMNS = ("workspace_name", "workspace_id", "target_name", "item_id")

_LENGTHS = {"varchar", "nvarchar", "char", "nchar", "varbinary", "binary"}


@dataclass(frozen=True)
class SourceColumn:
    name: str
    nullable: bool
    data_type: str
    #: Character or byte length; ``-1`` is ``max``.
    length: int | None = None

    @property
    def spelled(self) -> str:
        if self.data_type.casefold() not in _LENGTHS or self.length is None:
            return self.data_type
        return f"{self.data_type}({'max' if self.length < 0 else self.length})"


def source_shape(rows) -> dict[str, dict[str, SourceColumn]]:
    """``SOURCE_SHAPE_QUERY`` rows under casefolded table and column names."""

    shape: dict[str, dict[str, SourceColumn]] = {}
    for row in rows:
        values = dict(row)
        length = values.get("CHARACTER_MAXIMUM_LENGTH")
        column = SourceColumn(
            name=str(values["COLUMN_NAME"]),
            nullable=str(values["IS_NULLABLE"]).upper() == "YES",
            data_type=str(values["DATA_TYPE"]),
            length=None if length is None else int(length),
        )
        table = str(values["TABLE_NAME"]).casefold()
        shape.setdefault(table, {})[column.name.casefold()] = column
    return shape


@dataclass(frozen=True)
class ForkSource:
    """The source catalogue's tables mapped onto this version's.

    ``columns`` maps each copied table the source holds, by name, from
    destination column to the source column holding it. A table it omits was
    introduced after the source and starts empty.
    """

    columns: Mapping[str, Mapping[str, str]]
    borrowed: bool = False
    #: Installation records no verified Fabric identities.
    legacy_installation: bool = False
    #: The legacy Installation's physical column names.
    installation_columns: tuple[str, ...] = ()
    #: Every legacy Installation row with its verified identity, set by preflight.
    identities: tuple[Mapping, ...] | None = None
    #: ``(table, source column)`` pairs the source allows null and this version
    #: requires.
    nullable: tuple[tuple[str, str], ...] = field(default=())
    #: The source has exactly this version's tables and columns.
    is_current: bool = False

    @property
    def introduced(self) -> tuple[str, ...]:
        """Copied tables the source predates, which start empty."""

        return tuple(
            table.name
            for table in copied_tables(borrowed=self.borrowed)
            if table.name not in self.columns
        )


def project_source(shape, *, source: str) -> ForkSource:
    """Map ``shape`` onto this version's tables, or refuse what cannot move.

    Every problem is reported at once.
    """

    from .state import INTRODUCED_TABLES

    borrowed = MIRROR.name.casefold() in shape
    problems: list[str] = []
    mapped: dict[str, dict[str, str]] = {}
    nullable: list[tuple[str, str]] = []
    legacy = False
    installation_columns: tuple[str, ...] = ()
    current = True
    for table in copied_tables(borrowed=borrowed):
        present = shape.get(table.name.casefold())
        if present is None:
            current = False
            if table.name not in INTRODUCED_TABLES:
                problems.append(f"it has no {table.name} table")
            continue
        current = current and len(present) == len(table.public_columns)
        known = {public.casefold() for public in table.public_columns} | {
            retired.casefold() for retired in RETIRED_COLUMNS.get(table.name, ())
        }
        problems.extend(
            f"{table.name}.{column.name} is not a column this Weaver version knows"
            for key, column in present.items()
            if key not in known
        )
        identity = table is INSTALLATION and _legacy_installation(present)
        if identity:
            legacy = True
            installation_columns = tuple(column.name for column in present.values())
            if "item id" in present and "target item guid" in present:
                problems.append("Installation holds both Item ID and Target item GUID")
        columns: dict[str, str] = {}
        for name in table.physical_columns:
            if identity and name in IDENTITY_COLUMNS:
                continue
            public = table.public_name_of(name)
            column = present.get(public.casefold())
            audit = name in AUDIT_COLUMN_NAMES
            required = audit or table.column(name).not_null
            if column is None:
                if required:
                    problems.append(f"{table.name}.{public} is missing")
                continue
            declared = _AUDIT_TYPE if audit else table.column(name).warehouse_type
            if not _fits(column, declared):
                problems.append(
                    f"{table.name}.{column.name} is {column.spelled}, which does "
                    f"not fit {declared}"
                )
            if column.nullable and required:
                nullable.append((table.name, column.name))
            current = current and column.name == public
            columns[public] = column.name
        mapped[table.name] = columns
    if problems:
        raise CatalogueStateError(
            f"The catalogue in {source} cannot be mirrored by this Weaver "
            "version: " + "; ".join(problems) + "."
        )
    return ForkSource(
        columns=mapped,
        borrowed=borrowed,
        legacy_installation=legacy,
        installation_columns=installation_columns,
        nullable=tuple(nullable),
        is_current=current and not legacy and not nullable,
    )


def _legacy_installation(present) -> bool:
    return any(
        INSTALLATION.public_name_of(name).casefold() not in present
        for name in IDENTITY_COLUMNS
    ) or bool({"workspace id", "item id"} & set(present))


def _fits(column: SourceColumn, declared: str) -> bool:
    base, _, size = declared.partition("(")
    if column.data_type.casefold() != base.casefold():
        return False
    if base.casefold() not in _LENGTHS or not size:
        return True
    limit = size.rstrip(")")
    if limit.casefold() == "max":
        return True
    return column.length is not None and 0 <= column.length <= int(limit)


def null_count_statement(nullable) -> str:
    """Count copied source rows holding null where this version needs a value.

    Runs on the source catalogue's own connection.
    """

    from .tables import table as catalogue_table

    return "\nunion all\n".join(
        f"select {literal(table)} as [Table], {literal(column)} as [Column], "
        f"count_big(*) as [Rows] "
        f"from {identifier(CATALOGUE_SCHEMA)}.{identifier(table)} "
        f"where {identifier(column)} is null "
        f"and {_excluding_builtin(catalogue_table(table))}"
        for table, column in nullable
    )


def installation_copy_statement(
    *, source_catalogue: str, columns: Mapping[str, str], identities
) -> str:
    """Copy a legacy Installation with the identities preflight verified.

    Only resolved rows join, so if the source's installations changed after
    preflight the copy fails instead of landing a row without an identity.
    """

    if identities is None:
        raise CatalogueStateError(
            "a legacy Installation is copied only with verified identities"
        )
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
    source = source_relation(source_catalogue, INSTALLATION)
    kept = _excluding_builtin(INSTALLATION, columns, alias="s.")
    count = len(identities)
    statements = []
    if identities:
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
        statements.append(
            f"insert into {local_relation(INSTALLATION)} "
            f"({', '.join(identifier(public) for public, _ in projection)})\n"
            f"select {', '.join(expression for _, expression in projection)}\n"
            f"  from {source} as s\n"
            f"  join (values {values})\n"
            f"    as r ({', '.join(identifier(name) for name in keys + resolved)})\n"
            f"    on {joined}\n"
            f" where {kept};"
        )
    changed = literal(
        "The source catalogue's installations changed during the mirror. Run "
        "mirror again."
    )
    statements.append(
        f"if (select count_big(*) from {source} as s where {kept}) <> {count}\n"
        f"   or (select count_big(*) from {local_relation(INSTALLATION)} as s "
        f"where {_excluding_builtin(INSTALLATION, alias='s.')}) <> {count}\n"
        f"    throw 50000, {changed}, 1;"
    )
    return "\n".join(statements)


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
    "SOURCE_SHAPE_QUERY",
    "SourceColumn",
    "copied_tables",
    "copy_statement",
    "copy_statements",
    "create_statement",
    "fork_statements",
    "forked_table_names",
    "installation_copy_statement",
    "local_relation",
    "null_count_statement",
    "project_source",
    "source_relation",
    "source_shape",
    "uncopied_table_names",
]
