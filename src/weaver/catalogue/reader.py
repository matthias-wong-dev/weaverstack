"""Read catalogue tables through their expected schema, over TDS.

Two absences are ordinary and read as data: a missing table is bootstrap, since
the build that writes the catalogue is the build that creates it, and a missing
column is upgrade, where a newer Weaver compares against a table an older one
created.

Neither is recognised from a failure. The connection asks the ``_`` schema what
it holds, once, and a table or column absent from that answer is absent —
so a permission error or a broken connection stays a failure rather than
reading as "no rows", which would tell the next build that nothing is
catalogued. That is a licence to remove an estate.
"""

from __future__ import annotations

from typing import Any

from .render import InstallationScope, InstallationScopes, Row
from .tables import CatalogueTable
from .tsql import identifier, qualified_name


def read_table(
    catalogue: Any,
    table: CatalogueTable,
    *,
    scope: InstallationScope | InstallationScopes | None = None,
) -> tuple[Row, ...]:
    """Every row of one catalogue table, projected through its expected schema.

    ``catalogue`` is a :class:`CatalogueConnection` bound to the Warehouse the
    catalogue lives in.

    ``scope`` narrows the read to one installation, which is what a build wants:
    it compares and writes within one item and has no business seeing another's
    rows.

    Returns plain dictionaries under the internal snake-case keys, with stored
    vocabularies mapped back — the same shape the projection produces, so the
    two can be compared directly.
    """

    if catalogue is None:
        raise ValueError(
            f"reading {table.qualified} needs a connection to the Warehouse the "
            "Weaver catalogue lives in"
        )

    present = catalogue.columns_of(table)
    if present is None:
        # Bootstrap: the build that writes the catalogue is the build that
        # creates it, so an absent table is state rather than a failure.
        return ()

    projected = ", ".join(
        _projected_column(table, column, present) for column in table.columns
    )
    where = "" if scope is None else f" WHERE {scope.predicate}"
    rows = catalogue.rows(f"SELECT {projected} FROM {qualified_name(table)}{where}")
    return tuple(_internal(table, row) for row in rows)


def _projected_column(table: CatalogueTable, column, present: dict[str, str]) -> str:
    """One column of the expected schema, as a select expression.

    Aliased back to the internal name, so nothing above the reader sees the
    public spelling.
    """

    actual = present.get(column.public_name.casefold())
    alias = identifier(column.name)
    if actual is None:
        # Older shape: give this Weaver the column it expects, as a typed null.
        return f"CAST(NULL AS {column.warehouse_type}) AS {alias}"
    return f"CAST({identifier(actual)} AS {column.warehouse_type}) AS {alias}"


def _internal(table: CatalogueTable, row) -> Row:
    """One stored row under internal keys, with vocabularies mapped back."""

    values = dict(row)
    return {
        column.name: column.from_public(_python(values.get(column.name), column))
        for column in table.columns
    }


def _python(value, column):
    """One stored value as the projection would have produced it.

    A ``bit`` comes back as 0 or 1 and a projection holds a bool, so a
    comparison between them would differ for a row that has not changed.
    """

    from .tables import BOOLEAN

    if value is None:
        return None
    if column.type == BOOLEAN:
        return bool(value)
    return value


def read_installation(
    catalogue: Any, *, scope: InstallationScope, tables=None
) -> dict[str, tuple[Row, ...]]:
    """Every catalogue table, read for one installation.

    Keyed by table name, so a caller compares table by table against the
    projection without repeating the scope — which it could otherwise forget.
    """

    from .tables import CATALOGUE_TABLES

    return {
        table.name: read_table(catalogue, table, scope=scope)
        for table in (tables if tables is not None else CATALOGUE_TABLES)
    }


def read_installations(
    catalogue: Any, *, scopes: InstallationScopes, tables=None
) -> dict[str, tuple[Row, ...]]:
    """Every catalogue table, read **once** for every installation at issue.

    Bound items share the same physical tables, so reading per item would cost
    ``catalogue tables × bound items`` round trips to answer what one predicate
    per table already answers. Rows come back for every scope together and the
    grouping is done in Python — see
    :func:`weaver.catalogue.state.read_catalogue_state`.

    Still scoped: nothing outside ``scopes`` is returned.
    """

    from .tables import CATALOGUE_TABLES

    wanted = tables if tables is not None else CATALOGUE_TABLES
    if not scopes:
        # Nothing was asked for. Reading with no predicate would return the whole
        # catalogue, so the answer is stated rather than queried.
        return {table.name: () for table in wanted}
    return {table.name: read_table(catalogue, table, scope=scopes) for table in wanted}
