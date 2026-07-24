"""Prune execution — reconcile the target to the managed set, before creating.

A build is a reconciliation: anything in the target that the bundle does not
manage is removed first, so the target ends up as exactly what the repository
declares. The keep-set is the bundle's managed inventory (derived from its own
create actions), never the source repository — the installer still never reads it.

Reconciliation is scoped to the one Lakehouse the batch is bound to, and driven
by that Lakehouse's own storage so it is safe even where a Spark session shares a
catalog across work: a Delta table is a directory under ``Tables/``, a folder a
directory under ``Files/``, and the catalog is cleaned up alongside. Four kinds,
run in dependency order (views, then tables, then folders, then schemas):

- ``prune_views``   drop catalog views in managed schemas that are not managed;
- ``prune_delta``   delete unmanaged table directories and their registrations;
- ``prune_folders`` delete unmanaged folder directories (never the reserved areas);
- ``prune_schemas`` drop unmanaged schema databases and their directories.
"""

from __future__ import annotations

from typing import Any

from ...errors import InstallError
from ...hosts import BUILD_BUNDLES_AREA, REPOS_AREA
from ...store import StoreError
from ..models import (
    PRUNE_DELTA,
    PRUNE_FOLDERS,
    PRUNE_SCHEMAS,
    PRUNE_VIEWS,
    BuildAction,
)
from .base import InstallationContext

#: Files areas that are never folder resources, so a prune never touches them.
_RESERVED_FILES_AREAS = frozenset({REPOS_AREA, BUILD_BUNDLES_AREA})


class PruneExecutor:
    name = "prune"

    def execute(
        self,
        action: BuildAction,
        payload: bytes | None,
        context: InstallationContext,
    ) -> dict[str, Any] | None:
        handler = {
            PRUNE_VIEWS: _prune_views,
            PRUNE_DELTA: _prune_delta,
            PRUNE_FOLDERS: _prune_folders,
            PRUNE_SCHEMAS: _prune_schemas,
        }.get(action.kind)
        if handler is None:
            raise InstallError(f"prune action {action.id!r} has unknown kind {action.kind!r}")
        dropped = handler(context)
        return {"dropped": dropped}


# --- helpers -----------------------------------------------------------------


def _tables_root(context: InstallationContext):
    return context.resolver.tables_root(context.target.lakehouse)


def _files_root(context: InstallationContext):
    return context.resolver.files_root(context.target.lakehouse)


def _child_dirs(context: InstallationContext, root) -> list:
    store = context.store
    if not store.exists(root) or not store.is_directory(root):
        return []
    try:
        return [entry for entry in store.list(root) if entry.is_directory]
    except StoreError:  # pragma: no cover - root vanished between checks
        return []


def _database_names(context: InstallationContext) -> set[str]:
    if context.spark is None:
        return set()
    rows = context.spark.sql("SHOW DATABASES").collect()
    return {row[0].lower() for row in rows}


def _ident(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


# --- the four reconciliations ------------------------------------------------


def _prune_views(context: InstallationContext) -> list[str]:
    if context.spark is None:
        return []
    databases = _database_names(context)
    dropped: list[str] = []
    for schema in sorted(context.managed.schemas):
        if schema.lower() not in databases:
            continue
        rows = context.spark.sql(f"SHOW VIEWS IN {_ident(schema)}").collect()
        for row in rows:
            data = row.asDict()
            if data.get("isTemporary"):
                continue
            name = data.get("viewName") or data.get("name")
            if name is None:
                continue
            qualified = f"{schema}.{name}"
            if not context.managed.has_view(qualified):
                context.spark.sql(f"DROP VIEW IF EXISTS {_ident(schema)}.{_ident(name)}")
                dropped.append(qualified)
    return dropped


def _prune_delta(context: InstallationContext) -> list[str]:
    databases = _database_names(context)
    dropped: list[str] = []
    for schema_entry in _child_dirs(context, _tables_root(context)):
        schema = schema_entry.name
        for object_entry in _child_dirs(context, schema_entry.location):
            qualified = f"{schema}.{object_entry.name}"
            if context.managed.has_table(qualified):
                continue
            if context.spark is not None and schema.lower() in databases:
                context.spark.sql(
                    f"DROP TABLE IF EXISTS {_ident(schema)}.{_ident(object_entry.name)}"
                )
            context.store.delete(object_entry.location, recursive=True)
            dropped.append(qualified)
    return dropped


def _prune_folders(context: InstallationContext) -> list[str]:
    dropped: list[str] = []
    for schema_entry in _child_dirs(context, _files_root(context)):
        schema = schema_entry.name
        if schema in _RESERVED_FILES_AREAS:
            continue
        if not context.managed.has_folder_schema(schema):
            context.store.delete(schema_entry.location, recursive=True)
            dropped.append(f"{schema}.*")
            continue
        for object_entry in _child_dirs(context, schema_entry.location):
            qualified = f"{schema}.{object_entry.name}"
            if not context.managed.has_folder(qualified):
                context.store.delete(object_entry.location, recursive=True)
                dropped.append(qualified)
    return dropped


def _prune_schemas(context: InstallationContext) -> list[str]:
    databases = _database_names(context)
    dropped: list[str] = []
    for schema_entry in _child_dirs(context, _tables_root(context)):
        schema = schema_entry.name
        if context.managed.has_schema(schema):
            continue
        if context.spark is not None and schema.lower() in databases:
            context.spark.sql(f"DROP DATABASE IF EXISTS {_ident(schema)} CASCADE")
        context.store.delete(schema_entry.location, recursive=True)
        dropped.append(schema)
    return dropped
