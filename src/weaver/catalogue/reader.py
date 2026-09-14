"""Read catalogue tables through their expected schema over TDS.

Missing tables are bootstrap state and missing columns are compatible older
state. Both are determined from the cached ``_`` schema, never from query
failures, so transport and permission errors remain failures.
"""

from __future__ import annotations

from typing import Any, Sequence

from .render import InstallationScope, InstallationScopes, Row
from .tables import CatalogueTable
from .tsql import identifier, qualified_name


def read_table(
    catalogue: Any,
    table: CatalogueTable,
    *,
    scope: InstallationScope | InstallationScopes | None = None,
    predicate: str | None = None,
    order: Sequence[str] = (),
    top: int | None = None,
) -> tuple[Row, ...]:
    """Read rows projected through the expected catalogue schema.

    Scope predicates stay in SQL. ``order`` names internal columns and is
    required with ``top``. Returned mappings use internal keys and vocabulary.
    """

    if catalogue is None:
        raise ValueError(
            f"reading {table.qualified} needs a connection to the Warehouse the "
            "Weaver catalogue lives in"
        )
    if top is not None and not order:
        raise ValueError(
            f"reading the first {top} rows of {table.qualified} needs an order"
        )

    present = catalogue.columns_of(table)
    if present is None:
        # The build that first writes the catalogue also creates its tables.
        return ()

    projected = ", ".join(
        _projected_column(table, column, present) for column in table.columns
    )
    conditions = [
        condition
        for condition in (None if scope is None else scope.predicate, predicate)
        if condition
    ]
    where = f" WHERE {' AND '.join(conditions)}" if conditions else ""
    limit = "" if top is None else f"TOP {int(top)} "
    ordering = (
        ""
        if not order
        else " ORDER BY "
        + ", ".join(identifier(table.public_name_of(name)) for name in order)
    )
    rows = catalogue.rows(
        f"SELECT {limit}{projected} FROM {qualified_name(table)}{where}{ordering}"
    )
    return tuple(_internal(table, row) for row in rows)


def _projected_column(table: CatalogueTable, column, present: dict[str, str]) -> str:
    actual = present.get(column.public_name.casefold())
    alias = identifier(column.name)
    if actual is None:
        # Older catalogues expose newly expected columns as typed nulls.
        return f"CAST(NULL AS {column.warehouse_type}) AS {alias}"
    return f"CAST({identifier(actual)} AS {column.warehouse_type}) AS {alias}"


def _internal(table: CatalogueTable, row) -> Row:
    values = dict(row)
    return {
        column.name: column.from_public(_python(values.get(column.name), column))
        for column in table.columns
    }


def _python(value, column):
    from .tables import BOOLEAN

    if value is None:
        return None
    if column.type == BOOLEAN:
        return bool(value)
    return value


def read_installation(
    catalogue: Any, *, scope: InstallationScope, tables=None
) -> dict[str, tuple[Row, ...]]:
    """Read selected catalogue tables for one installation."""

    from .tables import PROJECTED_TABLES

    return {
        table.name: read_table(catalogue, table, scope=scope)
        for table in (tables if tables is not None else PROJECTED_TABLES)
    }


def read_installations(
    catalogue: Any, *, scopes: InstallationScopes, tables=None
) -> dict[str, tuple[Row, ...]]:
    """Read each selected table once across all requested installation scopes."""

    from .tables import PROJECTED_TABLES

    wanted = tables if tables is not None else PROJECTED_TABLES
    if not scopes:
        # An empty scope must not become an unscoped whole-catalogue read.
        return {table.name: () for table in wanted}
    return {table.name: read_table(catalogue, table, scope=scopes) for table in wanted}
