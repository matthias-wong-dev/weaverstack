"""Desktop-only SQL setup and inspection for Warehouse integration tests.

This module is test infrastructure.  It deliberately does not participate in
Fabric-native connection construction or Weaver's wipe implementation.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from weaver.sql import SqlExecutor


@dataclass(frozen=True, order=True)
class CatalogObject:
    schema: str
    name: str
    kind: str


def populate_warehouse(executor: SqlExecutor, fixture: Path) -> None:
    executor.execute_script(fixture.read_text(encoding="utf-8"))


def user_objects(executor: SqlExecutor) -> set[CatalogObject]:
    """Inspect user-created objects independently through catalogue views."""

    rows = executor.query(
        """
        select
            schema_name(objects.schema_id) as schema_name
          , objects.name                  as object_name
          , objects.type                  as object_type
        from sys.objects as objects
        where objects.is_ms_shipped = 0
          and objects.type in (N'U', N'V', N'P', N'FN', N'IF', N'TF')
          and lower(schema_name(objects.schema_id)) not in
              (N'dbo', N'guest', N'information_schema', N'sys', N'queryinsights', N'_rsc')
        order by schema_name(objects.schema_id), objects.name
        """
    )
    return {
        CatalogObject(
            schema=str(row["schema_name"]),
            name=str(row["object_name"]),
            kind=str(row["object_type"]).strip(),
        )
        for row in rows
    }


def system_schemas(executor: SqlExecutor) -> set[str]:
    rows = executor.query(
        """
        select name
        from sys.schemas
        where lower(name) in
            (N'dbo', N'guest', N'information_schema', N'sys', N'queryinsights')
        """
    )
    return {str(row["name"]).lower() for row in rows}
