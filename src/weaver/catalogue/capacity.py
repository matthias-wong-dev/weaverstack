"""What the catalogue can store, and what a Warehouse table can hold.

Both are physical limits with no give in them. A value wider than its catalogue
column is narrowed by the cast that writes it, so the estate would go on
describing the project with a description that is not the one the author wrote.
A Warehouse table past the platform's column ceiling cannot be created at all.

Neither needs a workspace to detect, so both are refused while the project is
still a project.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterator

from ..errors import DiscoveryError
from .tables import PROJECTED_TABLES, CatalogueColumn, CatalogueTable

#: Microsoft's documented maximum columns per Fabric Warehouse table. See
#: https://learn.microsoft.com/en-us/fabric/data-warehouse/tables, checked on
#: 21 September 2026.
WAREHOUSE_MAX_COLUMNS = 1024

_VARCHAR = re.compile(r"varchar\((\d+)\)", re.IGNORECASE)


def capacity_of(column: CatalogueColumn) -> int | None:
    """A column's byte capacity, or ``None`` where its type bounds nothing.

    ``varchar(n)`` is a byte count in the UTF-8 collations a Fabric Warehouse
    supports, not a character count. See
    https://learn.microsoft.com/en-us/sql/t-sql/data-types/char-and-varchar-transact-sql.
    """

    match = _VARCHAR.fullmatch(column.warehouse_type)
    return int(match.group(1)) if match else None


def stored_size(value: object) -> int:
    """What storing ``value`` costs, in the bytes the column counts.

    The stored value, not its SQL spelling: doubling an apostrophe is how a
    literal is written, and the column never sees it.
    """

    return len(str(value).encode("utf-8"))


@dataclass(frozen=True)
class Overflow:
    """One value too large for the column that would hold it."""

    table: str
    column: CatalogueColumn
    capacity: int
    size: int
    #: The reference the value came from, where it is not written in place.
    reference: str | None = None

    def describe(self) -> str:
        said = (
            f"{self.column.public_name} is {self.size} bytes and "
            f"{self.table} stores {self.capacity}"
        )
        return said if self.reference is None else f"{said}, from {self.reference}"


def overflows(table: CatalogueTable, row) -> Iterator[Overflow]:
    """Every value in a projected row too large for its catalogue column."""

    for name in table.column_names:
        column = table.column(name)
        capacity = capacity_of(column)
        if capacity is None:
            continue
        value = column.to_public(row.get(name))
        if value is None or not isinstance(value, str):
            continue
        size = stored_size(value)
        if size > capacity:
            yield Overflow(
                table=table.name,
                column=column,
                capacity=capacity,
                size=size,
                reference=_reference_for(row, name),
            )


def _reference_for(row, name: str) -> str | None:
    """Where a resolved value came from, when the row records a reference."""

    reference = row.get(f"{name}_reference")
    return str(reference) if reference else None


# --- the offline check --------------------------------------------------------


def validate_repository_capacity(repository) -> None:
    """Refuse a project the catalogue could not describe, or Fabric could not build.

    Called once composition has settled, so every reference is resolved and the
    values checked are the ones an installation would publish.
    """

    _refuse_wide_warehouse_tables(repository)
    _refuse_oversized_catalogue_values(repository)


def _refuse_wide_warehouse_tables(repository) -> None:
    from ..declaration.metadata import TABLE
    from ..declaration.model import WAREHOUSE

    for identity, source in sorted(repository.source_documents.items(), key=str_key):
        if identity.item.item_type != WAREHOUSE or source.document.kind != TABLE:
            continue
        if not source.document.has_declared_schema:
            # Its shape comes from the query, which only the engine can resolve.
            # The generated script counts it there.
            continue
        authored = len(source.document.schema)
        total = len(source.document.effective_schema)
        if total <= WAREHOUSE_MAX_COLUMNS:
            continue
        raise DiscoveryError(
            f"{_named(source, identity)}:\n"
            f"  The table would have {total} columns and a Fabric Warehouse "
            f"table holds {WAREHOUSE_MAX_COLUMNS}.\n"
            f"  {authored} declared, plus {total - authored} Weaver adds "
            "(identity, audit and signature columns)."
        )


def _refuse_oversized_catalogue_values(repository) -> None:
    from .projection import project_item_catalogue

    by_name = {table.name: table for table in PROJECTED_TABLES}
    for item in sorted(repository.items, key=lambda one: str(one.identity)):
        identity = item.identity
        owned = tuple(item.documents) + tuple(item.validations)
        projection = project_item_catalogue(repository, item=identity, retained=owned)
        authored = _authored_by_row_identity(repository, owned)
        for name, rows in sorted(projection.rows.items()):
            table = by_name.get(name)
            if table is None:
                continue
            for row in rows:
                for overflow in overflows(table, row):
                    subject = authored.get(_row_identity(row)) or str(identity)
                    raise DiscoveryError(
                        f"{subject}:\n  {overflow.describe()}. Shorten it."
                    )


def str_key(pair):
    return str(pair[0])


def _named(source, identity) -> str:
    return source.relative_path or str(identity)


def _row_identity(row) -> tuple[str, str]:
    """A projected row's object, however that row spells it.

    A foreign key names both ends, and the row belongs to the referring one.
    """

    for prefix in ("", "foreign_"):
        schema = row.get(f"{prefix}schema_name")
        object_name = row.get(f"{prefix}object_name")
        if schema and object_name:
            return (str(schema), str(object_name))
    return ("", "")


def _authored_by_row_identity(repository, owned) -> dict[tuple[str, str], str]:
    """The authored file behind each identity a projected row can carry.

    A Lakehouse row stores its area with its schema, so the key is built the way
    projection builds it rather than from the document identity alone.
    """

    from .claims import catalogue_schema

    named: dict[tuple[str, str], str] = {}
    for identity in owned:
        document = repository.source_documents.get(identity)
        if document is None:
            continue
        key = (catalogue_schema(identity), identity.object_id.object)
        named[key] = _named(document, identity)
    return named


__all__ = [
    "WAREHOUSE_MAX_COLUMNS",
    "Overflow",
    "capacity_of",
    "overflows",
    "stored_size",
    "validate_repository_capacity",
]
