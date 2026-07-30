"""Render one item's frozen physical prune, drop, schema and build stages.

Every function here plans for exactly one logical item against exactly one bound
target. That is the shape multi-item build needs: the item graph orders the items,
and inside an item the document graph orders the work, so nothing here reaches
across items or chooses a sequence number.
"""

from __future__ import annotations

import json
from dataclasses import replace

from ..declaration.metadata import DELTA_TARGET, FOLDER, SQL_TARGET, TABLE, VIEW
from ..declaration.model import WeaverItemId
from ..errors import BuildError
from ..spark.tokens import object_token
from .models import (
    BUILD_FOLDER,
    BUILD_TABLE,
    BUILD_VIEW,
    CREATE_SCHEMA,
    DROP_FOLDER,
    DROP_TABLE,
    DROP_VIEW,
    BuildAction,
    BuildBatch,
)
from .payloads import sha256_hex
from .prune import managed_sets, render_inventory_prune
from .stages import BUILD, DROP, PRUNE, SCHEMA, PlannedStage
from .targets import WAREHOUSE_TARGET

_OBJECT_KIND = {TABLE: BUILD_TABLE, VIEW: BUILD_VIEW}
_DROP_KIND = {FOLDER: DROP_FOLDER, TABLE: DROP_TABLE, VIEW: DROP_VIEW}
_DECLARATION_KIND = {"folder": FOLDER, "table": TABLE, "view": VIEW}

#: Kinds that change Delta storage, and therefore leave a Lakehouse's SQL
#: analytics endpoint describing something that is no longer there.
DELTA_MUTATING_KINDS = frozenset(
    {
        BUILD_TABLE,
        BUILD_VIEW,
        DROP_TABLE,
        DROP_VIEW,
        "prune_table",
        "prune_view",
        "prune_schema",
    }
)


def _slug(value) -> str:
    return str(value).replace("/", "--").replace(" ", "-")


def item_prune_stage(
    repository,
    selected_ids,
    *,
    item: WeaverItemId,
    target,
    inventory,
) -> PlannedStage | None:
    """Freeze one item's authoritative repository/inventory diff."""

    documents = {
        str(identity): repository.source_documents[identity]
        for identity in selected_ids
        if identity.item == item
    }
    target_kind = SQL_TARGET if target.kind == WAREHOUSE_TARGET else DELTA_TARGET
    managed = managed_sets(
        documents,
        target_kind,
        alias_destinations=[
            alias.destination
            for alias in repository.aliases
            if alias.destination.item == item
        ],
    )

    payloads: dict[str, bytes] = {}
    actions = render_inventory_prune(target, inventory, managed, payloads)
    if not actions:
        return None

    # Two items pruning a same-named object share the merged stage's payload
    # directory, so each item's frozen drops carry its own prefix.
    item_slug = _slug(item)
    return PlannedStage(
        phase=PRUNE,
        slug="item-prune",
        description="prune unmanaged objects by logical item",
        payloads={f"{item_slug}-{name}": data for name, data in payloads.items()},
        batches=(
            BuildBatch(
                id=f"item-prune-{item_slug}",
                target_id=target.id,
                actions=tuple(
                    _prefixed(action, item_slug) for action in actions
                ),
            ),
        ),
    )


def _prefixed(action: BuildAction, item_slug: str) -> BuildAction:
    return replace(
        action,
        id=f"{item_slug}-{action.id}",
        payload=None if action.payload is None else f"{item_slug}-{action.payload}",
    )


def item_drop_stages(
    repository,
    selected_for_drop,
    *,
    item: WeaverItemId,
    target,
    registered,
) -> tuple[PlannedStage, ...]:
    """One item's managed drops, dependants before dependencies."""

    selected = {identity for identity in selected_for_drop if identity.item == item}
    if not selected:
        return ()
    graph = repository.dependency_graph.subgraph({str(value) for value in selected})
    identities = {str(identity): identity for identity in selected}
    stages = []
    for index, layer in enumerate(reversed(graph.layers())):
        payloads: dict[str, bytes] = {}
        actions = tuple(
            _drop_action(identities[node], registered[identities[node]].object_type, target, payloads)
            for node in sorted(layer)
        )
        stages.append(
            PlannedStage(
                phase=DROP,
                index=index,
                slug="managed-drop",
                description="drop selected rebuild dependency layer",
                payloads=payloads,
                batches=(
                    BuildBatch(
                        id=f"managed-drop-{_slug(item)}",
                        target_id=target.id,
                        actions=actions,
                    ),
                ),
            )
        )
    return tuple(stages)


def _drop_action(identity, installed_type, target, payloads) -> BuildAction:
    try:
        installed_kind = _DECLARATION_KIND[installed_type]
    except KeyError as exc:
        raise BuildError(
            f"registered document {identity} has unsupported type {installed_type!r}"
        ) from exc
    kind = _DROP_KIND[installed_kind]
    action_slug = _slug(identity)
    if installed_kind == FOLDER:
        return BuildAction(
            id=f"managed-drop-{action_slug}",
            kind=kind,
            resource_node_id=str(identity),
            executor="folder",
            payload=None,
            payload_sha256=None,
        )

    schema = identity.object_id.schema
    name = identity.object_id.object
    if target.kind == WAREHOUSE_TARGET:
        keyword = "view" if installed_kind == VIEW else "table"
        statement = f"drop {keyword} {_tsql_ident(schema)}.{_tsql_ident(name)};\n"
        executor, extension = "tsql", ".sql"
    else:
        keyword = "VIEW" if installed_kind == VIEW else "TABLE"
        statement = f"DROP {keyword} {object_token(schema, name)}\n"
        executor, extension = "spark_sql", ".spark.sql"
    content = statement.encode("utf-8")
    filename = f"drop-{action_slug}{extension}"
    payloads[filename] = content
    return BuildAction(
        id=f"managed-drop-{action_slug}",
        kind=kind,
        resource_node_id=str(identity),
        executor=executor,
        payload=filename,
        payload_sha256=sha256_hex(content),
    )


def _tsql_ident(name: str) -> str:
    return "[" + name.replace("]", "]]") + "]"


def item_schema_stage(
    selected_ids,
    *,
    item: WeaverItemId,
    target,
    inventory,
    extra_schemas=(),
) -> PlannedStage | None:
    """The schemas this item needs and its target does not already hold.

    ``extra_schemas`` carries the schemas the item's planned aliases land in.
    They are the item's own declared schemas — an alias destination has to be —
    but no *document* of the item need live in them, so a namespace an alias
    depends on would otherwise never be created.
    """

    present = {schema.casefold() for schema in inventory.schemas}
    wanted = {
        identity.object_id.schema
        for identity in selected_ids
        if identity.item == item and not identity.is_files
    } | set(extra_schemas)
    schemas = sorted(
        schema for schema in wanted if schema.casefold() not in present
    )
    if not schemas:
        return None

    item_slug = _slug(item)
    payloads: dict[str, bytes] = {}
    actions = []
    for schema in schemas:
        if target.kind == WAREHOUSE_TARGET:
            content = f"create schema [{schema.replace(']', ']]')}];\n".encode("utf-8")
            executor, extension = "tsql", ".sql"
        else:
            content = (json.dumps({"schema": schema}, sort_keys=True) + "\n").encode()
            executor, extension = "spark_schema", ".schema.json"
        filename = f"create-{item_slug}-{schema}{extension}"
        payloads[filename] = content
        actions.append(
            BuildAction(
                id=f"schema-{item_slug}-{schema}",
                kind=CREATE_SCHEMA,
                resource_node_id=None,
                executor=executor,
                payload=filename,
                payload_sha256=sha256_hex(content),
            )
        )
    return PlannedStage(
        phase=SCHEMA,
        slug="create-schemas",
        description="create item-owned schemas",
        payloads=payloads,
        batches=(
            BuildBatch(id=f"{item_slug}", target_id=target.id, actions=tuple(actions)),
        ),
    )


def item_build_stages(
    repository,
    selected_for_build,
    *,
    item: WeaverItemId,
    target,
) -> tuple[PlannedStage, ...]:
    """One item's declared documents, in forward dependency layers."""

    selected = {identity for identity in selected_for_build if identity.item == item}
    if not selected:
        return ()
    graph = repository.dependency_graph.subgraph({str(value) for value in selected})
    identities = {str(identity): identity for identity in selected}
    stages = []
    for index, layer in enumerate(graph.layers()):
        payloads: dict[str, bytes] = {}
        actions = tuple(
            _build_action(
                identities[node],
                repository.source_documents[identities[node]],
                payloads,
            )
            for node in sorted(layer)
        )
        stages.append(
            PlannedStage(
                phase=BUILD,
                index=index,
                slug="build-objects",
                description="build dependency layer",
                payloads=payloads,
                batches=(
                    BuildBatch(
                        id=f"{_slug(item)}", target_id=target.id, actions=actions
                    ),
                ),
            )
        )
    return tuple(stages)


def _build_action(identity, source, payloads):
    action_slug = _slug(identity)
    if source.kind == FOLDER:
        return BuildAction(
            id=f"folder-{action_slug}",
            kind=BUILD_FOLDER,
            resource_node_id=str(identity),
            executor="folder",
            payload=None,
            payload_sha256=None,
        )
    ddl = source.create_ddl()
    filename = f"{action_slug}{ddl.extension}"
    content = ddl.content.encode("utf-8")
    payloads[filename] = content
    return BuildAction(
        id=f"object-{action_slug}",
        kind=_OBJECT_KIND[source.kind],
        resource_node_id=str(identity),
        executor=ddl.executor,
        payload=filename,
        payload_sha256=sha256_hex(content),
    )
