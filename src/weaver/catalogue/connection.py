"""Read the Weaver catalogue over TDS with one cached schema inventory."""

from __future__ import annotations

from typing import Any, Callable, Mapping

from ..errors import CommandError
from .tables import CATALOGUE_SCHEMA
from .tsql import literal


class CatalogueConnection:
    """Catalogue access for one Warehouse; the Session owns transport lifetime."""

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
        """Return the cached ``_`` schema under casefolded table and column keys."""

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
        """Return columns, or ``None`` when schema inventory shows no table."""

        return self.shape().get(table.name.casefold())

    def forget_shape(self) -> None:
        """Read the schema again, after a build created or altered a table."""

        self._shape = None

    # --- reading -------------------------------------------------------------

    def rows(self, statement: str):
        return self._query(statement)

    def execute(self, statement: str) -> None:
        if self._execute is None:
            raise CommandError(
                "this catalogue connection is read-only; no statement executor "
                "was provided"
            )
        self._execute(statement)


def catalogue_connection(session, workspace=None) -> CatalogueConnection:
    """Connect to a Session's configured catalogue Warehouse without Spark."""

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
