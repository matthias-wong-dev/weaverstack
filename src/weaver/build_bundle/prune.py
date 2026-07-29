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
from typing import Any, Mapping

from ..catalogue.tables import CATALOGUE_SCHEMA
from ..workspaces import BUILD_BUNDLES_AREA, WEAVER_ITEMS_AREA, Workspace
from ..spark import SparkCatalogue, object_token, schema_token
from ..declaration.metadata import DELTA_TARGET, FOLDER_TARGET, SQL_TARGET, TABLE, VIEW
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
        qualified = f"{schema}.{name}".casefold()
        values = self.views if object_type == "view" else self.tables
        if object_type == "folder":
            values = self.folders
        return qualified in {value.casefold() for value in values}


def read_lakehouse_inventory(
    target: BoundTarget, *, resolver, store: Store, spark=None
) -> TargetInventory:
    """Read every Weaver-manageable object in one Lakehouse."""

    lakehouse = ItemRef(target.item_id)
    tables_root = resolver.tables_root(lakehouse)
    files_root = resolver.files_root(lakehouse)
    schemas = tuple(
        entry.name
        for entry in _child_dirs(store, tables_root)
        if entry.name.casefold() not in _RESERVED_SCHEMAS
    )
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
    catalogue = _catalogue_for(resolver, lakehouse, spark)
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


def _prune_sequence(
    target: BoundTarget,
    resolver,
    store: Store,
    spark,
    managed: _Managed,
    payloads: dict[str, bytes],
) -> BuildSequence | None:
    """Inspect the target now and freeze a concrete DROP for each unmanaged object.

    The build reads the target's own storage (and, with a session, its catalogue)
    and emits visible drops — ``DROP TABLE``/``VIEW``/``SCHEMA`` as Spark SQL
    payloads, an unmanaged folder as a directory-removing action. The installer
    runs exactly these; it never enumerates. Reconciliation is scoped to the one
    bound Lakehouse's ``Tables``/``Files`` storage, so a shared catalogue cannot
    make a build reach into another Lakehouse.

    Both halves of the inspection now name the Lakehouse being reconciled, which
    is what makes reconciling a Lakehouse other than the attached one correct
    rather than lucky.
    """

    # Store addressing, not Spark addressing: inspection *lists* the target, and
    # on Fabric that is the DFS location, while a LakehouseSparkLocation carries
    # the `abfss://` roots Spark writes through. Same Lakehouse, two transports —
    # conflating them would have prune listing a URL Spark cannot read a directory
    # from.
    #
    # Schemas come from storage on both workspaces, and have to: Fabric refuses
    # `SHOW SCHEMAS IN `workspace`.`lakehouse`` — a bare `SHOW SCHEMAS` answers
    # only for the *attached* Lakehouse — so asking the catalogue would have
    # reconciled the destination against the control plane's inventory.
    lakehouse = ItemRef(target.item_id)
    tables_root = resolver.tables_root(lakehouse)
    files_root = resolver.files_root(lakehouse)
    catalogue = _catalogue_for(resolver, lakehouse, spark)

    existing_schemas = [
        entry.name
        for entry in _child_dirs(store, tables_root)
        if entry.name.lower() not in _RESERVED_SCHEMAS
    ]
    orphan_schemas = {s.lower() for s in existing_schemas if s.lower() not in managed.schemas}

    actions: list[BuildAction] = []

    # Views (catalogue only, since a view is not a directory): drop those not
    # managed, per schema that survives — an orphan schema is dropped whole below
    # and takes its views with it. Asked of the *destination's* catalogue, so a
    # build reconciling a Lakehouse the session is not attached to sees that
    # Lakehouse's views rather than the control plane's.
    if catalogue is not None:
        for schema in existing_schemas:
            if schema.lower() in orphan_schemas:
                continue
            for view in catalogue.views(schema):
                if f"{schema}.{view}".lower() in managed.views:
                    continue
                actions.append(
                    _drop_action(target, "prune_view", "view", f"{schema}.{view}",
                                 f"DROP VIEW IF EXISTS {object_token(schema, view)}", payloads)
                )

    # Tables: unmanaged ones in a schema that survives (an orphan schema is
    # dropped whole below).
    for schema_entry in _child_dirs(store, tables_root):
        schema = schema_entry.name
        if schema.lower() in orphan_schemas or schema.lower() in _RESERVED_SCHEMAS:
            continue
        for object_entry in _child_dirs(store, schema_entry.location):
            qualified = f"{schema}.{object_entry.name}"
            if qualified.lower() not in managed.tables:
                actions.append(
                    _drop_action(target, "prune_table", "table", qualified,
                                 f"DROP TABLE IF EXISTS {object_token(schema, object_entry.name)}",
                                 payloads)
                )

    # Folders: an unmanaged folder object, or a whole unmanaged folder schema.
    for schema_entry in _child_dirs(store, files_root):
        schema = schema_entry.name
        if schema in _RESERVED_FILES_AREAS:
            continue
        if schema.lower() not in managed.folder_schemas:
            actions.append(_prune_folder_action(target, f"folder:{schema}"))
            continue
        for object_entry in _child_dirs(store, schema_entry.location):
            qualified = f"{schema}.{object_entry.name}"
            if qualified.lower() not in managed.folders:
                actions.append(_prune_folder_action(target, f"folder:{qualified}"))

    # Schemas: drop the whole orphan schema, which cascades to its tables/views.
    # SCHEMA (not DATABASE) works in Fabric and its local emulator — Fabric's
    # Trident Spark refuses CREATE/DROP DATABASE on a Lakehouse, but accepts SCHEMA.
    for schema in sorted({s for s in existing_schemas if s.lower() in orphan_schemas}):
        actions.append(
            _drop_action(target, "prune_schema", "schema", schema,
                         f"DROP SCHEMA IF EXISTS {schema_token(schema)} CASCADE", payloads)
        )

    if not actions:
        return None
    batch = BuildBatch(id=f"{PRUNE_SEQUENCE:03d}-{target.id}", target_id=target.id, actions=tuple(actions))
    return BuildSequence(
        number=PRUNE_SEQUENCE, description="prune unmanaged objects", batches=(batch,)
    )


def _warehouse_prune_sequence(
    target: BoundTarget,
    sql,
    workspace: Workspace,
    managed: _Managed,
    payloads: dict[str, bytes],
) -> BuildSequence | None:
    """Inspect the Warehouse catalogue now and freeze a concrete DROP per orphan.

    The Warehouse counterpart of :func:`_prune_sequence`: reconciliation reads
    ``sys.objects``/``sys.schemas`` at *plan* time (target inspection is a
    planning concern — build-philosophy §6) and compiles each unmanaged table,
    view and schema into an explicit T-SQL drop. The installer runs exactly these
    and enumerates nothing.

    Order is dependency-safe and matters more than on the Lakehouse: T-SQL has no
    ``DROP SCHEMA … CASCADE``, so views are dropped before the tables they read,
    and a schema only after everything in it has gone.

    Reading the target is **Fabric-native by default**, like
    :func:`weaver.wipe.wipe_sql_target`: Weaver runs in Fabric, so it inspects the
    Warehouse through its own session identity. A desktop caller crossing into
    Fabric — a developer, or the CLI — injects ``desktop_sql_executor``
    explicitly. Either way the inventory is read where the build is planned, and
    the drops are frozen into the bundle from there.
    """

    owns_sql = sql is None
    if sql is None:
        from ..fabric.sql import fabric_sql_executor
        from ..targets import WarehouseTarget

        sql = fabric_sql_executor(
            WarehouseTarget(warehouse=ItemRef(target.item_id)), workspace
        )
    try:
        return _warehouse_prune_actions(target, sql, managed, payloads)
    finally:
        if owns_sql and hasattr(sql, "close"):
            sql.close()


def _warehouse_prune_actions(
    target: BoundTarget,
    sql,
    managed: _Managed,
    payloads: dict[str, bytes],
) -> BuildSequence | None:
    """Compile the frozen drops from one catalogue reading."""

    rows = sql.query(
        """
        select
            schema_name(objects.schema_id) as schema_name
          , objects.name                  as object_name
          , objects.type                  as object_type
        from sys.objects as objects
        where objects.is_ms_shipped = 0
          and objects.type in (N'U', N'V')
        order by schema_name(objects.schema_id), objects.name
        """
    )
    existing = [
        (str(row["schema_name"]), str(row["object_name"]), str(row["object_type"]).strip())
        for row in rows
        if str(row["schema_name"]).lower() not in _RESERVED_SQL_SCHEMAS
    ]

    # A fixed database role owns a schema of its own — `db_owner`, `db_datareader`
    # and seven more — and those are not Weaver's to drop, or anyone's: `DROP
    # SCHEMA` on one fails. They are excluded by *ownership* rather than by adding
    # nine more names to the reserved list, because the reserved list is a
    # statement about Weaver's conventions and this is a statement about SQL's.
    schema_rows = sql.query(
        """
        select schemas.name as name
        from sys.schemas as schemas
        left join sys.database_principals as owners
          on owners.principal_id = schemas.principal_id
        where owners.is_fixed_role is null
           or owners.is_fixed_role = 0
        """
    )
    existing_schemas = [
        str(row["name"])
        for row in schema_rows
        if str(row["name"]).lower() not in _RESERVED_SQL_SCHEMAS
    ]

    def unmanaged(schema: str, name: str, keep: frozenset[str]) -> bool:
        return f"{schema}.{name}".lower() not in keep

    actions: list[BuildAction] = []

    # Views first — a view may read a table this same prune drops.
    for schema, name, kind in existing:
        if kind == "V" and unmanaged(schema, name, managed.views):
            actions.append(
                _drop_action(
                    target, PRUNE_VIEW, "view", f"{schema}.{name}",
                    f"drop view if exists {_tsql_ident(schema)}.{_tsql_ident(name)};",
                    payloads, executor="tsql", extension=".sql",
                )
            )

    for schema, name, kind in existing:
        if kind == "U" and unmanaged(schema, name, managed.tables):
            actions.append(
                _drop_action(
                    target, PRUNE_TABLE, "table", f"{schema}.{name}",
                    f"drop table if exists {_tsql_ident(schema)}.{_tsql_ident(name)};",
                    payloads, executor="tsql", extension=".sql",
                )
            )

    # Schemas last, and only those the bundle does not manage: by now everything
    # inside an orphan schema has been dropped above, so the schema is empty.
    for schema in sorted({s for s in existing_schemas if s.lower() not in managed.schemas}):
        actions.append(
            _drop_action(
                target, PRUNE_SCHEMA, "schema", schema,
                f"drop schema if exists {_tsql_ident(schema)};",
                payloads, executor="tsql", extension=".sql",
            )
        )

    if not actions:
        return None
    batch = BuildBatch(
        id=f"{PRUNE_SEQUENCE:03d}-{target.id}", target_id=target.id, actions=tuple(actions)
    )
    return BuildSequence(
        number=PRUNE_SEQUENCE, description="prune unmanaged objects", batches=(batch,)
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
