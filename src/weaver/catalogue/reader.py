"""Read catalogue tables through their expected schema.

Missing tables and compatible missing columns are handled as bootstrap or upgrade
state; other read failures propagate.
"""

from __future__ import annotations

from typing import Any

from ..spark.catalogue import is_absent
from .render import (
    InstallationScope,
    InstallationScopes,
    Row,
    identifier,
    qualified_name,
)
from .tables import CatalogueTable

def read_table(
    catalogue: Any,
    table: CatalogueTable,
    *,
    scope: InstallationScope | InstallationScopes | None = None,
) -> tuple[Row, ...]:
    """Every row of one catalogue table, projected through its expected schema.

    ``catalogue`` is a :class:`~weaver.spark.catalogue.SparkCatalogue` bound to
    the Weaver Lakehouse — the read has to say where the catalogue is, because
    the session is not necessarily pointed at it.

    ``scope`` narrows the read to one installation, which is what a build wants:
    it compares and writes within one ``(repository, target_type)`` and has no
    business seeing another's rows.

    Returns plain dictionaries of ``str``/``bool``/``None`` — the same shape the
    projection produces, so the two can be compared directly.
    """

    if catalogue is None:
        raise ValueError(
            f"reading {table.qualified} needs a Spark catalogue bound to the Weaver "
            "Lakehouse — the catalogue lives there, and a session alone does not "
            "say which Lakehouse that is"
        )

    name = catalogue.expand(qualified_name(table))
    try:
        existing = catalogue.columns_of(name)
    except Exception as exception:
        if is_absent(exception):
            return ()
        raise

    # Case-folded, because the local metastore lowercases column names where
    # Fabric preserves them, and a column's presence must not depend on that.
    present = {column.lower(): column for column in existing}

    projected = ", ".join(
        _projected_column(column, present.get(column.name.lower()))
        for column in table.columns
    )
    where = ""
    if scope is not None:
        where = f" WHERE {scope.predicate}"

    return tuple(catalogue.rows(f"SELECT {projected} FROM {name}{where}"))


def _projected_column(column, actual: str | None) -> str:
    """One column of the expected schema, as a select expression.

    Rendered as SQL rather than built with ``pyspark.sql.functions`` so this module
    names no Spark API — the core stays importable without PySpark, and a session
    is only ever duck-typed. It also keeps the projection inspectable as text.
    """

    if actual is None:
        # Older shape: give this Weaver the column it expects, as a typed null.
        return f"CAST(NULL AS {column.type.upper()}) AS {identifier(column.name)}"
    # Cast even when present: an older catalogue may have stored a boolean as a
    # string, and a comparison against a projected boolean would then differ for a
    # row that has not actually changed.
    return (
        f"CAST({identifier(actual)} AS {column.type.upper()}) "
        f"AS {identifier(column.name)}"
    )


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

    The read a build wants. A build is pointed at several items and they share
    the same physical catalogue tables, so reading per item would cost

    .. code-block:: text

        catalogue tables × bound items

    round trips to answer what one predicate per table already answers. More
    items make the predicate longer and the result larger; they do not make it
    another read. That is the whole difference, and it is why this returns rows
    for all scopes together and leaves the grouping to Python — see
    :func:`weaver.catalogue.state.read_catalogue_state`.

    Still scoped, and returns nothing outside ``scopes``: a build has no more
    business seeing an unrelated installation's rows than it had before.
    """

    from .tables import CATALOGUE_TABLES

    wanted = tables if tables is not None else CATALOGUE_TABLES
    if not scopes:
        # Nothing was asked for. Reading with no predicate would return the whole
        # catalogue, so the answer is stated rather than queried.
        return {table.name: () for table in wanted}
    return {table.name: read_table(catalogue, table, scope=scopes) for table in wanted}
