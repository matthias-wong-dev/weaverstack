"""Reconciling a bound physical target against what an item declares.

Building says what must exist. Pruning says what must stop existing, and it is
the half that can destroy data, so it is deliberately narrow: only objects the
target already holds, only in schemas the bound item declares, and only after
the inventory has been frozen at plan time rather than re-read at install time.

The Delta side reads the Spark catalogue for the bound Lakehouse and the Files
area for its Folders. The Warehouse side reads the target's own SQL catalogue,
in the environment the build is running in.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from ..catalogue.tables import CATALOGUE_SCHEMA
from ..workspaces import BUILD_BUNDLES_AREA, WEAVER_ITEMS_AREA
from ..spark import SparkCatalogue, object_token, schema_token
from ..declaration.metadata import DELTA_TARGET, FOLDER_TARGET, TABLE, VIEW
from ..declaration.source import SourceDocument
from ..store import Store
from ..targets import ItemRef
from .models import (
    PRUNE_FOLDER,
    PRUNE_SCHEMA,
    PRUNE_TABLE,
    PRUNE_VIEW,
    BuildAction,
    BuildBatch,
    BuildSequence,
)
from .payloads import PRUNE_SEQUENCE, payload_path, sha256_hex
from .targets import BoundTarget

#: Files areas a prune never touches: they are Weaver's own, not an item's
#: materialised output.
_RESERVED_FILES_AREAS = frozenset({WEAVER_ITEMS_AREA, BUILD_BUNDLES_AREA})

#: Schemas a prune never touches. A schema-enabled Fabric Lakehouse has a default
#: ``dbo`` schema that cannot be dropped and that Weaver does not manage; ``_``
#: holds Weaver's own catalogue, which no item owns. A build normally cannot see
#: `_` at all — it lives in the Weaver Lakehouse and prune is scoped to the bound
#: destination's own storage — but an item built *into* the Weaver Lakehouse
#: would, and a prune that dropped the catalogue would take the record of every
#: installation with it.
_RESERVED_SCHEMAS = frozenset({"dbo", CATALOGUE_SCHEMA})

#: Warehouse schemas that belong to the engine rather than to any item.
_RESERVED_SQL_SCHEMAS = frozenset(
    {"dbo", "guest", "information_schema", "sys", "queryinsights", "_rsc"}
)
@dataclass(frozen=True)
class _Managed:
    """The keep-set the build diffs the target against, folded for comparison."""

    schemas: frozenset[str]
    folder_schemas: frozenset[str]
    folders: frozenset[str]
    tables: frozenset[str]
    views: frozenset[str]


@dataclass(frozen=True)
class TargetInventory:
    """Transport-neutral physical state prepared before bundle generation."""

    target_id: str
    kind: str
    target_name: str
    schemas: tuple[str, ...] = ()
    folder_schemas: tuple[str, ...] = ()
    folders: tuple[str, ...] = ()
    tables: tuple[str, ...] = ()
    views: tuple[str, ...] = ()

    def has_object(self, schema: str, name: str, object_type: str) -> bool:
        physical_schema = schema
        values = self.views if object_type == "view" else self.tables
        if object_type == "folder":
            prefix = "Files/"
            if schema.casefold().startswith(prefix.casefold()):
                physical_schema = schema[len(prefix) :]
            values = self.folders
        qualified = f"{physical_schema}.{name}".casefold()
        return qualified in {value.casefold() for value in values}


def read_lakehouse_inventory(
    target: BoundTarget, *, resolver, store: Store, spark=None
) -> TargetInventory:
    """Read every Weaver-manageable object in one Lakehouse."""

    lakehouse = ItemRef(target.item_id)
    tables_root = resolver.tables_root(lakehouse)
    files_root = resolver.files_root(lakehouse)
    reserved_schemas = set(_RESERVED_SCHEMAS)
    if target.logical_item_name == "_weaver":
        reserved_schemas.discard(CATALOGUE_SCHEMA)
    schemas = tuple(
        entry.name
        for entry in _child_dirs(store, tables_root)
        if entry.name.casefold() not in reserved_schemas
    )
    catalogue = _catalogue_for(resolver, lakehouse, spark)
    if (
        target.logical_item_name == "_weaver"
        and catalogue is not None
        and catalogue.schema_exists(CATALOGUE_SCHEMA)
        and CATALOGUE_SCHEMA.casefold()
        not in {schema.casefold() for schema in schemas}
    ):
        # The empty catalogue schema is catalogue state, not storage state: until
        # its first table exists there is no Tables/_ directory for the store to
        # discover.  The package-owned control item is the one safe exception to
        # storage-only schema discovery because it is the attached Lakehouse.
        schemas += (CATALOGUE_SCHEMA,)
    tables = tuple(
        f"{schema}.{entry.name}"
        for schema in schemas
        for entry in _child_dirs(store, tables_root / schema)
    )
    folder_schema_entries = tuple(
        entry
        for entry in _child_dirs(store, files_root)
        if entry.name not in _RESERVED_FILES_AREAS
    )
    folders = tuple(
        f"{entry.name}.{child.name}"
        for entry in folder_schema_entries
        for child in _child_dirs(store, entry.location)
    )
    views: tuple[str, ...] = ()
    if catalogue is not None:
        views = tuple(
            f"{schema}.{view}"
            for schema in schemas
            for view in catalogue.views(schema)
        )
    return TargetInventory(
        target_id=target.id,
        kind=target.kind,
        target_name=target.name,
        schemas=tuple(sorted(schemas, key=str.casefold)),
        folder_schemas=tuple(
            sorted((entry.name for entry in folder_schema_entries), key=str.casefold)
        ),
        folders=tuple(sorted(folders, key=str.casefold)),
        tables=tuple(sorted(tables, key=str.casefold)),
        views=tuple(sorted(views, key=str.casefold)),
    )


def read_warehouse_inventory(target: BoundTarget, *, sql) -> TargetInventory:
    """Read every Weaver-manageable schema, table and view in one Warehouse."""

    rows = sql.query(
        """
        select schema_name(objects.schema_id) as schema_name,
               objects.name as object_name,
               objects.type as object_type
        from sys.objects as objects
        where objects.is_ms_shipped = 0
          and objects.type in (N'U', N'V')
        order by schema_name(objects.schema_id), objects.name
        """
    )
    objects = [
        (
            str(row["schema_name"]),
            str(row["object_name"]),
            str(row["object_type"]).strip(),
        )
        for row in rows
        if str(row["schema_name"]).casefold() not in _RESERVED_SQL_SCHEMAS
    ]
    schema_rows = sql.query(
        """
        select schemas.name as name
        from sys.schemas as schemas
        left join sys.database_principals as owners
          on owners.principal_id = schemas.principal_id
        where owners.is_fixed_role is null or owners.is_fixed_role = 0
        """
    )
    schemas = tuple(
        sorted(
            {
                str(row["name"])
                for row in schema_rows
                if str(row["name"]).casefold() not in _RESERVED_SQL_SCHEMAS
            },
            key=str.casefold,
        )
    )
    return TargetInventory(
        target_id=target.id,
        kind=target.kind,
        target_name=target.name,
        schemas=schemas,
        tables=tuple(
            sorted(
                (f"{schema}.{name}" for schema, name, kind in objects if kind == "U"),
                key=str.casefold,
            )
        ),
        views=tuple(
            sorted(
                (f"{schema}.{name}" for schema, name, kind in objects if kind == "V"),
                key=str.casefold,
            )
        ),
    )


def render_inventory_prune(
    target: BoundTarget,
    inventory: TargetInventory,
    managed: _Managed,
    payloads: dict[str, bytes],
) -> BuildSequence | None:
    """Purely render prune actions from one already-read inventory."""

    actions: list[BuildAction] = []
    if target.kind == "warehouse":
        for qualified in inventory.views:
            if qualified.casefold() not in managed.views:
                schema, name = qualified.split(".", 1)
                actions.append(
                    _drop_action(
                        target,
                        PRUNE_VIEW,
                        "view",
                        qualified,
                        f"drop view if exists {_tsql_ident(schema)}.{_tsql_ident(name)};",
                        payloads,
                        executor="tsql",
                        extension=".sql",
                    )
                )
        for qualified in inventory.tables:
            if qualified.casefold() not in managed.tables:
                schema, name = qualified.split(".", 1)
                actions.append(
                    _drop_action(
                        target,
                        PRUNE_TABLE,
                        "table",
                        qualified,
                        f"drop table if exists {_tsql_ident(schema)}.{_tsql_ident(name)};",
                        payloads,
                        executor="tsql",
                        extension=".sql",
                    )
                )
        for schema in inventory.schemas:
            if schema.casefold() not in managed.schemas:
                actions.append(
                    _drop_action(
                        target,
                        PRUNE_SCHEMA,
                        "schema",
                        schema,
                        f"drop schema if exists {_tsql_ident(schema)};",
                        payloads,
                        executor="tsql",
                        extension=".sql",
                    )
                )
    else:
        orphan_schemas = {
            schema.casefold()
            for schema in inventory.schemas
            if schema.casefold() not in managed.schemas
        }
        for qualified in inventory.views:
            schema, name = qualified.split(".", 1)
            if (
                schema.casefold() not in orphan_schemas
                and qualified.casefold() not in managed.views
            ):
                actions.append(
                    _drop_action(
                        target,
                        PRUNE_VIEW,
                        "view",
                        qualified,
                        f"DROP VIEW IF EXISTS {object_token(schema, name)}",
                        payloads,
                    )
                )
        for qualified in inventory.tables:
            schema, name = qualified.split(".", 1)
            if (
                schema.casefold() not in orphan_schemas
                and qualified.casefold() not in managed.tables
            ):
                actions.append(
                    _drop_action(
                        target,
                        PRUNE_TABLE,
                        "table",
                        qualified,
                        f"DROP TABLE IF EXISTS {object_token(schema, name)}",
                        payloads,
                    )
                )
        for schema in inventory.folder_schemas:
            if schema.casefold() not in managed.folder_schemas:
                actions.append(_prune_folder_action(target, f"folder:{schema}"))
        for qualified in inventory.folders:
            schema, _name = qualified.split(".", 1)
            if (
                schema.casefold() in managed.folder_schemas
                and qualified.casefold() not in managed.folders
            ):
                actions.append(_prune_folder_action(target, f"folder:{qualified}"))
        for schema in inventory.schemas:
            if schema.casefold() in orphan_schemas:
                actions.append(
                    _drop_action(
                        target,
                        PRUNE_SCHEMA,
                        "schema",
                        schema,
                        f"DROP SCHEMA IF EXISTS {schema_token(schema)} CASCADE",
                        payloads,
                    )
                )
    if not actions:
        return None
    return BuildSequence(
        number=PRUNE_SEQUENCE,
        description="prune unmanaged objects",
        batches=(
            BuildBatch(
                id=f"{PRUNE_SEQUENCE:03d}-{target.id}",
                target_id=target.id,
                actions=tuple(actions),
            ),
        ),
    )


def _managed_sets(
    documents: Mapping[str, SourceDocument], object_target_kind: str = DELTA_TARGET
) -> _Managed:
    """The keep-set for one physical side: Delta objects, or Warehouse ones."""

    tables = {d.qualified for d in documents.values() if d.target_kind == object_target_kind and d.kind == TABLE}
    views = {d.qualified for d in documents.values() if d.target_kind == object_target_kind and d.kind == VIEW}
    folders = {d.qualified for d in documents.values() if d.target_kind == FOLDER_TARGET}
    return _Managed(
        schemas=frozenset(name.split(".", 1)[0].lower() for name in tables | views),
        folder_schemas=frozenset(name.split(".", 1)[0].lower() for name in folders),
        folders=frozenset(name.lower() for name in folders),
        tables=frozenset(name.lower() for name in tables),
        views=frozenset(name.lower() for name in views),
    )


def _drop_action(
    target,
    kind,
    slug,
    name,
    statement,
    payloads,
    *,
    executor: str = "spark_sql",
    extension: str = ".spark.sql",
) -> BuildAction:
    content = (statement + "\n").encode("utf-8")
    path = payload_path(PRUNE_SEQUENCE, "prune", f"{slug}-{name}{extension}")
    payloads[path] = content
    return BuildAction(
        id=f"prune-{slug}-{name}",
        kind=kind,
        resource_node_id=None,
        executor=executor,
        payload=path,
        payload_sha256=sha256_hex(content),
    )


def _prune_folder_action(target, resource: str) -> BuildAction:
    return BuildAction(
        id=f"prune-{resource}",
        kind=PRUNE_FOLDER,
        resource_node_id=resource,
        executor="folder",
        payload=None,
        payload_sha256=None,
    )


def _child_dirs(store: Store, root) -> list:
    if not store.exists(root) or not store.is_directory(root):
        return []
    return sorted(
        (entry for entry in store.list(root) if entry.is_directory), key=lambda e: e.name
    )


def _catalogue_for(resolver, lakehouse: ItemRef, spark) -> "SparkCatalogue | None":
    """Catalogue operations against the Lakehouse being reconciled.

    None without a session — prune still reconciles tables, folders and schemas
    from storage, and simply cannot see views, which is the documented cost of
    generating without one.
    """

    if spark is None:
        return None
    resolve = getattr(resolver, "spark_destination", None)
    if resolve is None:  # pragma: no cover - both shipped resolvers provide it
        return None
    return SparkCatalogue(spark, resolve(lakehouse))


def _tsql_ident(name: str) -> str:
    """A bracket-quoted T-SQL identifier."""

    return "[" + name.replace("]", "]]") + "]"
