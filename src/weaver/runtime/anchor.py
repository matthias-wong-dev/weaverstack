"""Anchor a standalone authored object to its installed catalogue identity."""

from __future__ import annotations

from typing import Any

from ..errors import ConfigError

#: TestDictionary supplies validation identity because validations have no own
#: Registry row for a materialised object.
ANCHOR_TABLES = ("Installation", "Registry", "Bookmark", "TestDictionary")


def anchored(object: Any, catalogue: str) -> tuple[Any, Any]:
    """Resolve the catalogue and object identity together at construction."""

    from ..catalogue.state import catalogue_in
    from ..catalogue.tables import CATALOGUE_TABLES

    wanted = tuple(table for table in CATALOGUE_TABLES if table.name in ANCHOR_TABLES)
    read = catalogue_in(_workspace_named(catalogue), tables=wanted)
    try:
        return read, resolved_identity(object, read)
    except BaseException:
        read.close()
        raise


def resolved_identity(object: Any, catalogue: Any):
    """Resolve an object's installed identity from the catalogue."""

    from ..targets import LAKEHOUSE_KIND

    schema, name = object.identity
    if getattr(object, "_validation_kind", ""):
        return catalogue.installed_validation(
            target_kind=LAKEHOUSE_KIND,
            target_name=object.lakehouse.name,
            schema=schema,
            object=name,
        )
    return catalogue.installed_object(
        target_kind=LAKEHOUSE_KIND,
        target_name=object.lakehouse.name,
        schema=schema,
        object=name,
        is_files=object._is_files,
    )


def _workspace_named(catalogue: str):
    """Use the current Fabric workspace without falling back to a default."""

    from ..sessions.host import current_workspace_name
    from ..workspaces import Workspace

    name = current_workspace_name()
    if not name:
        raise ConfigError(
            f"Cannot anchor to catalogue {catalogue!r} outside a Fabric session. "
            "Run this load with `weaver load`."
        )
    return Workspace(workspace=name, catalogue=catalogue)


__all__ = ["ANCHOR_TABLES", "anchored", "resolved_identity"]
