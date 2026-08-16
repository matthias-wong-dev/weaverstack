"""Plan removals from a bound target against an item's declared state.

Prune uses the inventory frozen during planning and only considers objects in
schemas and file areas managed by the bound item.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping

from ..catalogue.tables import CATALOGUE_SCHEMA
from ..declaration.metadata import DELTA_TARGET, FOLDER_TARGET, SQL_TARGET, TABLE, VIEW
from ..declaration.model import PROCEDURE_SHAPE, WeaverDocumentId
from ..declaration.source import SourceDocument
from ..errors import BuildError
from ..etl import LOAD_ROOT
from ..store import Store
from ..targets import ItemRef
from ..workspaces import CLI_AREA
from .changes import (
    FOLDER as FOLDER_KIND,
)
from .changes import (
    FOLDER_SCHEMA as FOLDER_SCHEMA_KIND,
)
from .changes import (
    SCHEMA as SCHEMA_KIND,
)
from .changes import (
    TABLE as TABLE_KIND,
)
from .changes import (
    VIEW as VIEW_KIND,
)
from .changes import (
    TargetChange,
    removed,
)
from .models import (
    PRUNE_FOLDER,
    PRUNE_SCHEMA,
    PRUNE_TABLE,
    PRUNE_VIEW,
    InstallAction,
)
from .payloads import sha256_hex
from .targets import BoundTarget

#: Weaver-owned Files areas excluded from prune.
_RESERVED_FILES_AREAS = frozenset({CLI_AREA})

#: Delta schemas excluded from prune because Weaver does not manage them.
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
    """Transport-neutral physical state prepared before bundle generation.

    ``files`` and ``procedures`` are what a load layer installs, read like
    everything else here. A type the inventory could not see would be disproved
    by every reconciliation and rebuilt on every build.
    """

    target_id: str
    kind: str
    target_name: str
    schemas: tuple[str, ...] = ()
    folder_schemas: tuple[str, ...] = ()
    folders: tuple[str, ...] = ()
    tables: tuple[str, ...] = ()
    views: tuple[str, ...] = ()
    #: Deployed load files, as ``<path beneath Files>/<filename>``.
    files: tuple[str, ...] = ()
    #: Generated load procedures, as ``<schema>.<name>``.
    procedures: tuple[str, ...] = ()

    def to_mapping(self) -> dict[str, object]:
        """A versioned JSON-safe representation for remote state handover."""

        return {
            "format_version": 1,
            "target_id": self.target_id,
            "kind": self.kind,
            "target_name": self.target_name,
            "schemas": list(self.schemas),
            "folder_schemas": list(self.folder_schemas),
            "folders": list(self.folders),
            "tables": list(self.tables),
            "views": list(self.views),
            "files": list(self.files),
            "procedures": list(self.procedures),
        }

    @classmethod
    def from_mapping(cls, mapping) -> "TargetInventory":
        """Reconstruct an inventory returned by an in-Fabric state read."""

        version = mapping.get("format_version")
        if version != 1:
            raise BuildError(
                f"unsupported target inventory format_version {version!r}; expected 1"
            )
        return cls(
            target_id=mapping["target_id"],
            kind=mapping["kind"],
            target_name=mapping["target_name"],
            schemas=tuple(mapping.get("schemas", ())),
            folder_schemas=tuple(mapping.get("folder_schemas", ())),
            folders=tuple(mapping.get("folders", ())),
            tables=tuple(mapping.get("tables", ())),
            views=tuple(mapping.get("views", ())),
            files=tuple(mapping.get("files", ())),
            procedures=tuple(mapping.get("procedures", ())),
        )

    def update_using(self, plan) -> "TargetInventory":
        """This target as the plan intends to leave it.

        The build's declared effect on this target, applied. What it gives is a
        *prediction*, and the value of a prediction is that it can be wrong: an
        estate built from a repository and read back should equal the same
        repository's declared inventory, and if applying a build's own summary to
        the state it was planned against does not reach that, the build does not
        converge.

        Reads the summary rather than inferring one from the actions, because an
        inference would be a model of what executors do living where no executor
        could correct it. The summary is held to the actions separately, by
        bijection over action ids.
        """

        from .changes import apply_to

        return apply_to(self, plan.target_changes.get(self.target_id, ()))

    def has_object(self, schema: str, name: str, object_type: str) -> bool:
        """Whether the target holds this object, asked of the right collection.

        Branching on the type is not a convenience: falling through to ``tables``
        for a type this did not know about would answer *no* for something that
        is plainly there, and reconciliation reads a *no* as proof the claim is
        stale.
        """

        if object_type == "file":
            # A file is addressed by path, and its schema already *is* the path
            # beneath Files — so the two halves join with a separator rather than
            # the dot a two-part object name uses.
            return _holds(self.files, f"{schema}/{name}")
        if object_type == "stored_procedure":
            return _holds(self.procedures, f"{schema}.{name}")
        physical_schema = schema
        values = self.views if object_type == "view" else self.tables
        if object_type == "folder":
            prefix = "Files/"
            if schema.casefold().startswith(prefix.casefold()):
                physical_schema = schema[len(prefix) :]
            values = self.folders
        return _holds(values, f"{physical_schema}.{name}")


def _holds(values: Iterable[str], qualified: str) -> bool:
    return qualified.casefold() in {value.casefold() for value in values}


def read_lakehouse_inventory(
    target: BoundTarget, *, resolver, store: Store, catalogue=None
) -> TargetInventory:
    """Read every Weaver-manageable object in one Lakehouse.

    Storage answers everything but the views, which exist only in the
    catalogue — so ``catalogue`` is optional and its absence means the views
    cannot be listed, not that there are none.
    """

    lakehouse = ItemRef(target.item_id)
    tables_root = resolver.tables_root(lakehouse)
    files_root = resolver.files_root(lakehouse)
    control_item = target.logical_item_name == "_weaver"
    reserved_schemas = set(_RESERVED_SCHEMAS)
    if control_item:
        reserved_schemas.discard(CATALOGUE_SCHEMA)
    schemas = tuple(
        entry.name
        for entry in _child_dirs(store, tables_root)
        if (
            entry.name.casefold() == CATALOGUE_SCHEMA.casefold()
            if control_item
            else entry.name.casefold() not in reserved_schemas
        )
    )
    if (
        control_item
        and catalogue is not None
        and catalogue.schema_exists(CATALOGUE_SCHEMA)
        and CATALOGUE_SCHEMA.casefold() not in {schema.casefold() for schema in schemas}
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
    # The same narrowing the Delta side uses, and for the same reason. The
    # control item's Files area holds Weaver's own working directories — the
    # declaration, retained bundles, CLI handover — none of which is a Folder
    # object; what it *does* declare is the task log, under the reserved schema.
    # Excluding the whole area instead left that folder unobservable, so every
    # build concluded it was absent and tried to create it again.
    folder_schema_entries = tuple(
        entry
        for entry in _child_dirs(store, files_root)
        if (
            entry.name.casefold() == CATALOGUE_SCHEMA.casefold()
            if control_item
            else entry.name not in _RESERVED_FILES_AREAS
        )
    )
    folders = tuple(
        f"{entry.name}.{child.name}"
        for entry in folder_schema_entries
        for child in _child_dirs(store, entry.location)
    )
    views: tuple[str, ...] = ()
    if catalogue is not None:
        views = tuple(
            f"{schema}.{view}" for schema in schemas for view in catalogue.views(schema)
        )
    files = () if control_item else _load_files(store, files_root)
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
        files=files,
    )


def _load_files(store: Store, files_root) -> tuple[str, ...]:
    """Every deployed load file, as the path beneath ``Files`` that names it.

    Scoped to the runtime tree rather than the whole Files area: a Folder
    object's contents are data an item loaded, so walking all of Files would
    inventory rows as artefacts. The load tree is where a build puts individual
    files it claims one by one.
    """

    root = files_root / LOAD_ROOT.split("/")[0]
    if not store.exists(root) or not store.is_directory(root):
        return ()
    prefix = files_root.value.rstrip("/") + "/"
    return tuple(
        sorted(
            (
                entry.location.value[len(prefix) :]
                for entry in store.list(root, recursive=True)
                if not entry.is_directory
            ),
            key=str.casefold,
        )
    )


def read_warehouse_inventory(target: BoundTarget, *, sql) -> TargetInventory:
    """Read every Weaver-manageable schema, table, view and procedure.

    For the built-in catalogue item the answer is the ``_`` schema and nothing
    else, exactly as it is for a Lakehouse. That restriction is the whole of the
    shared-host guarantee: the catalogue may live in a Warehouse that already
    holds a user's schemas, and an inventory that could see them would offer
    them to prune as orphans of an item that never declared them.
    """

    catalogue_item = target.logical_item_name == "_weaver"

    def managed(schema: str) -> bool:
        if catalogue_item:
            return schema.casefold() == CATALOGUE_SCHEMA.casefold()
        return schema.casefold() not in _RESERVED_SQL_SCHEMAS

    rows = sql.query(
        """
        select schema_name(objects.schema_id) as schema_name,
               objects.name as object_name,
               objects.type as object_type
        from sys.objects as objects
        where objects.is_ms_shipped = 0
          and objects.type in (N'U', N'V', N'P')
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
        if managed(str(row["schema_name"]))
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
            {str(row["name"]) for row in schema_rows if managed(str(row["name"]))},
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
        procedures=tuple(
            sorted(
                (f"{schema}.{name}" for schema, name, kind in objects if kind == "P"),
                key=str.casefold,
            )
        ),
    )


def render_inventory_prune(
    target: BoundTarget,
    inventory: TargetInventory,
    managed: _Managed,
    payloads: dict[str, bytes],
) -> tuple[tuple[InstallAction, ...], tuple[TargetChange, ...]]:
    """Purely render prune actions from one already-read inventory.

    ``payloads`` is filled with the frozen drops, keyed by bare filename: the
    caller owns which sequence these actions land in and therefore which payload
    directory they live under.

    The changes come back alongside, and here they are load-bearing: a prune
    action carries no ``resource_node_id``, because the object it removes has no
    node in the repository, so what it destroys is otherwise recorded nowhere
    reachable without parsing SQL.
    """

    actions: list[InstallAction] = []
    changes: list[TargetChange] = []
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
                changes.append(removed(VIEW_KIND, qualified, actions[-1].id))
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
                changes.append(removed(TABLE_KIND, qualified, actions[-1].id))
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
                changes.append(removed(SCHEMA_KIND, schema, actions[-1].id))
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
                        f"DROP VIEW IF EXISTS {target.spark_target.qualify(schema, name)}",
                        payloads,
                    )
                )
                changes.append(removed(VIEW_KIND, qualified, actions[-1].id))
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
                        f"DROP TABLE IF EXISTS {target.spark_target.qualify(schema, name)}",
                        payloads,
                    )
                )
                changes.append(removed(TABLE_KIND, qualified, actions[-1].id))
        for schema in inventory.folder_schemas:
            if schema.casefold() not in managed.folder_schemas:
                actions.append(_prune_folder_action(target, f"folder:{schema}"))
                changes.append(removed(FOLDER_SCHEMA_KIND, schema, actions[-1].id))
        for qualified in inventory.folders:
            schema, _name = qualified.split(".", 1)
            if (
                schema.casefold() in managed.folder_schemas
                and qualified.casefold() not in managed.folders
            ):
                actions.append(_prune_folder_action(target, f"folder:{qualified}"))
                changes.append(removed(FOLDER_KIND, qualified, actions[-1].id))
        for schema in inventory.schemas:
            if schema.casefold() in orphan_schemas:
                actions.append(
                    _drop_action(
                        target,
                        PRUNE_SCHEMA,
                        "schema",
                        schema,
                        f"DROP SCHEMA IF EXISTS "
                        f"{target.spark_target.qualified_schema(schema)} CASCADE",
                        payloads,
                    )
                )
                changes.append(removed(SCHEMA_KIND, schema, actions[-1].id))
    return tuple(actions), tuple(changes)


def managed_sets(
    documents: Mapping[str, SourceDocument],
    object_target_kind: str = DELTA_TARGET,
    *,
    alias_destinations: Iterable[WeaverDocumentId] = (),
    load_identities: Iterable[WeaverDocumentId] = (),
) -> _Managed:
    """The keep-set for one physical side: Delta objects, or Warehouse ones.

    ``alias_destinations`` belong in the keep-set: they are desired state in
    this item as a declared document is, produced elsewhere, and a build
    that pruned the shortcut it was about to create would be destructive and
    pointless. Which set one joins follows its physical form — a folder under
    Files, a view in a Warehouse, a table directory in a Lakehouse.

    ``load_identities`` contribute the ``_`` schema a Warehouse's generated load
    procedures live in, which nothing declares: without it every build would
    drop the schema it had just created. Derived from the artefacts rather than
    added unconditionally, so the schema goes when the last procedure does. The
    Lakehouse runtime tree needs nothing here, being a declared folder.
    """

    tables = {
        d.qualified
        for d in documents.values()
        if d.target_kind == object_target_kind and d.kind == TABLE
    }
    views = {
        d.qualified
        for d in documents.values()
        if d.target_kind == object_target_kind and d.kind == VIEW
    }
    folders = {
        d.qualified for d in documents.values() if d.target_kind == FOLDER_TARGET
    }
    for destination in alias_destinations:
        qualified = destination.object_id.qualified
        if destination.is_files:
            folders.add(qualified)
        elif object_target_kind == SQL_TARGET:
            views.add(qualified)
        else:
            tables.add(qualified)
    schemas = {name.split(".", 1)[0].lower() for name in tables | views}
    schemas.update(
        identity.object_id.schema.lower()
        for identity in load_identities
        if identity.shape == PROCEDURE_SHAPE
    )
    return _Managed(
        schemas=frozenset(schemas),
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
) -> InstallAction:
    content = (statement + "\n").encode("utf-8")
    filename = f"{slug}-{name}{extension}"
    payloads[filename] = content
    return InstallAction(
        id=f"prune-{slug}-{name}",
        kind=kind,
        resource_node_id=None,
        executor=executor,
        payload=filename,
        payload_sha256=sha256_hex(content),
    )


def _prune_folder_action(target, resource: str) -> InstallAction:
    return InstallAction(
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
        (entry for entry in store.list(root) if entry.is_directory),
        key=lambda e: e.name,
    )


def _tsql_ident(name: str) -> str:
    """A bracket-quoted T-SQL identifier."""

    return "[" + name.replace("]", "]]") + "]"
