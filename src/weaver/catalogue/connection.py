"""Reading the Weaver catalogue over TDS.

One object, bound to the Warehouse the catalogue lives in, through which every
catalogue read goes. It exists to hold two things a bare query callable cannot:
the shape of the ``_`` schema, read once, and the judgement about what an absent
table means.

The shape matters for cost. A build reads eleven tables, and asking the engine
what columns each one has would double the round trips to answer a question one
query over ``INFORMATION_SCHEMA`` answers for the whole schema.
"""

from __future__ import annotations

from typing import Any, Callable, Mapping

from ..errors import CommandError
from .tables import CATALOGUE_SCHEMA
from .tsql import literal


class CatalogueConnection:
    """Catalogue reads against one Warehouse, over TDS.

    ``query`` runs one T-SQL question and returns its rows as mappings. Nothing
    here holds a connection: the Session owns transport lifetime, and this owns
    only what the catalogue means.
    """

    def __init__(
        self,
        query: Callable[[str], Any],
        execute: Callable[[str], Any] | None = None,
    ) -> None:
        self._query = query
        self._execute = execute
        self._shape: dict[str, dict[str, str]] | None = None

    # --- the shape of `_` ----------------------------------------------------

    def shape(self) -> Mapping[str, Mapping[str, str]]:
        """Every ``_`` table this Warehouse holds, and the columns it has.

        Keyed by casefolded table name, then by casefolded column name, so a
        lookup never depends on the engine's collation. Read once per
        connection: the catalogue does not change shape underneath a build.
        """

        if self._shape is None:
            found: dict[str, dict[str, str]] = {}
            rows = self._query(
                "SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                f"WHERE TABLE_SCHEMA = {literal(CATALOGUE_SCHEMA)}"
            )
            for row in rows:
                values = dict(row)
                table = str(values["TABLE_NAME"])
                column = str(values["COLUMN_NAME"])
                found.setdefault(table.casefold(), {})[column.casefold()] = column
            self._shape = found
        return self._shape

    def columns_of(self, table) -> dict[str, str] | None:
        """This table's columns, or None when the Warehouse does not hold it.

        None is the bootstrap answer and is data: the build that writes the
        catalogue is the build that creates it. It is distinguished by asking
        the schema rather than by reading a failure, so a permission error
        cannot be mistaken for an empty catalogue.
        """

        return self.shape().get(table.name.casefold())

    def forget_shape(self) -> None:
        """Read the schema again — after a build created or altered a table."""

        self._shape = None

    # --- reading -------------------------------------------------------------

    def rows(self, statement: str):
        return self._query(statement)

    def execute(self, statement: str) -> None:
        """Run one catalogue statement that returns nothing.

        Reading is always available; writing is not. A connection made only to
        read says so here rather than letting a caller discover it from the
        transport.
        """

        if self._execute is None:
            raise CommandError(
                "this catalogue connection can read but not write; it was made "
                "without a way to execute statements"
            )
        self._execute(statement)


def catalogue_connection(session, workspace=None) -> CatalogueConnection:
    """The catalogue connection for a Session's configured catalogue Warehouse.

    The one construction, both positions: inside Fabric the query runs on the
    session's own identity, from a desktop it crosses over TDS. Nothing above
    can tell, and neither needs Spark.
    """

    from ..targets import WarehouseTarget

    resolved = session.workspace_or_default(workspace)
    target = WarehouseTarget(warehouse=resolved.catalogue_item)
    return CatalogueConnection(
        lambda statement: session.query_tsql(
            statement, target=target, workspace=resolved
        ),
        lambda statement: session.execute_tsql(
            statement, target=target, workspace=resolved
        ),
    )


__all__ = ["CatalogueConnection", "catalogue_connection"]
