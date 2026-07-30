"""Render frozen physical prune, drop, schema and build actions."""

from __future__ import annotations

import json
from dataclasses import replace

from ..declaration.metadata import DELTA_TARGET, FOLDER, SQL_TARGET, TABLE, VIEW
from ..declaration.model import WeaverDocumentId, WeaverItemId
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
    BuildSequence,
)
from .payloads import (
    OBJECT_SEQUENCE_STEP,
    PRUNE_SEQUENCE,
    check_sequence_headroom,
    payload_path,
    sha256_hex,
)
from .targets import WAREHOUSE_TARGET

_OBJECT_KIND = {TABLE: BUILD_TABLE, VIEW: BUILD_VIEW}
_DROP_KIND = {FOLDER: DROP_FOLDER, TABLE: DROP_TABLE, VIEW: DROP_VIEW}
_DECLARATION_KIND = {"folder": FOLDER, "table": TABLE, "view": VIEW}


def _slug(value) -> str:
    return str(value).replace("/", "--").replace(" ", "-")


def render_inventory_prune_sequence(
    repository,
    selected_ids,
    target_by_item,
    *,
    target_inventories,
    payloads,
):
    """Freeze each bound item's authoritative repository/inventory diff."""

    from .prune import _managed_sets, render_inventory_prune

    batches = []
    for item in sorted(target_by_item, key=str):
        target = target_by_item[item]
        documents = {
            str(identity): repository.source_documents[identity]
            for identity in selected_ids
            if identity.item == item
        }
        target_kind = SQL_TARGET if target.kind == WAREHOUSE_TARGET else DELTA_TARGET
        managed = _managed_sets(documents, target_kind)
        inventory = target_inventories[item]
        temporary_payloads = {}
        sequence = render_inventory_prune(
            target, inventory, managed, temporary_payloads
        )
        if sequence is None:
            continue

        item_slug = _slug(item)
        rewritten_actions = []
        for action in sequence.batches[0].actions:
            new_payload = None
            if action.payload is not None:
                content = temporary_payloads[action.payload]
                filename = action.payload.rsplit("/", 1)[-1]
                new_payload = payload_path(
                    PRUNE_SEQUENCE, "item-prune", f"{item_slug}-{filename}"
                )
                payloads[new_payload] = content
            rewritten_actions.append(
                replace(action, id=f"{item_slug}-{action.id}", payload=new_payload)
            )
        batches.append(replace(sequence.batches[0], actions=tuple(rewritten_actions)))

    if not batches:
        return None
    return BuildSequence(
        number=PRUNE_SEQUENCE,
        description="prune unmanaged objects by logical item",
        batches=tuple(batches),
    )


def render_selected_drops(
    repository,
    selected: set[WeaverDocumentId],
    targets,
    *,
    registered,
    start: int,
    payloads,
) -> tuple[BuildSequence, ...]:
    if not selected:
        return ()
    graph = repository.dependency_graph.subgraph({str(value) for value in selected})
    identities = {str(identity): identity for identity in selected}
    sequences = []
    for layer_index, layer in enumerate(reversed(graph.layers())):
        number = start + layer_index * OBJECT_SEQUENCE_STEP
        check_sequence_headroom(number)
        by_item: dict[WeaverItemId, list[WeaverDocumentId]] = {}
        for node in layer:
            identity = identities[node]
            by_item.setdefault(identity.item, []).append(identity)
        batches = []
        for item in sorted(by_item, key=str):
            actions = tuple(
                _drop_action(
                    number,
                    identity,
                    registered[identity].object_type,
                    targets[item],
                    payloads,
                )
                for identity in sorted(by_item[item], key=str)
            )
            batches.append(
                BuildBatch(
                    id=f"{number:03d}-managed-drop-{_slug(item)}",
                    target_id=targets[item].id,
                    actions=actions,
                )
            )
        sequences.append(
            BuildSequence(
                number=number,
                description="drop selected rebuild dependency layer",
                batches=tuple(batches),
            )
        )
    return tuple(sequences)


def _drop_action(number, identity, installed_type, target, payloads) -> BuildAction:
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
    path = payload_path(number, "managed-drop", f"drop-{action_slug}{extension}")
    payloads[path] = content
    return BuildAction(
        id=f"managed-drop-{action_slug}",
        kind=kind,
        resource_node_id=str(identity),
        executor=executor,
        payload=path,
        payload_sha256=sha256_hex(content),
    )


def _tsql_ident(name: str) -> str:
    return "[" + name.replace("]", "]]") + "]"


def render_selected_builds(
    repository,
    selected: set[WeaverDocumentId],
    targets,
    inventories,
    *,
    start: int,
    payloads,
) -> tuple[BuildSequence, ...]:
    sequences = []
    schema_sequence = _schema_sequence(
        start, selected, targets, inventories, payloads
    )
    if schema_sequence is not None:
        sequences.append(schema_sequence)
    start += OBJECT_SEQUENCE_STEP
    if selected:
        graph = repository.dependency_graph.subgraph({str(value) for value in selected})
        for layer_index, layer in enumerate(graph.layers()):
            number = start + layer_index * OBJECT_SEQUENCE_STEP
            check_sequence_headroom(number)
            sequences.append(
                _item_layer_sequence(
                    number,
                    layer,
                    repository.source_documents,
                    targets,
                    payloads,
                )
            )
    return tuple(sequences)


def _schema_sequence(number, selected, targets, inventories, payloads):
    batches = []
    for item in sorted(targets, key=str):
        target = targets[item]
        present = {schema.casefold() for schema in inventories[item].schemas}
        schemas = sorted(
            schema
            for schema in {
                identity.object_id.schema
                for identity in selected
                if identity.item == item and not identity.is_files
            }
            if schema.casefold() not in present
        )
        actions = []
        for schema in schemas:
            if target.kind == WAREHOUSE_TARGET:
                statement = f"create schema [{schema.replace(']', ']]')}];\n"
                content = statement.encode("utf-8")
                executor, extension = "tsql", ".sql"
            else:
                content = (json.dumps({"schema": schema}, sort_keys=True) + "\n").encode()
                executor, extension = "spark_schema", ".schema.json"
            item_slug = _slug(item)
            path = payload_path(
                number,
                "create-schemas",
                f"create-{item_slug}-{schema}{extension}",
            )
            payloads[path] = content
            actions.append(
                BuildAction(
                    id=f"schema-{item_slug}-{schema}",
                    kind=CREATE_SCHEMA,
                    resource_node_id=None,
                    executor=executor,
                    payload=path,
                    payload_sha256=sha256_hex(content),
                )
            )
        if actions:
            batches.append(
                BuildBatch(
                    id=f"{number:03d}-{_slug(item)}",
                    target_id=target.id,
                    actions=tuple(actions),
                )
            )
    if not batches:
        return None
    check_sequence_headroom(number)
    return BuildSequence(
        number=number,
        description="create item-owned schemas",
        batches=tuple(batches),
    )


def _item_layer_sequence(number, nodes, documents, targets, payloads):
    by_item: dict[WeaverItemId, list[WeaverDocumentId]] = {}
    identities = {str(identity): identity for identity in documents}
    for node in nodes:
        identity = identities[node]
        by_item.setdefault(identity.item, []).append(identity)
    batches = []
    for item in sorted(by_item, key=str):
        actions = tuple(
            _build_action(number, identity, documents[identity], payloads)
            for identity in sorted(by_item[item], key=str)
        )
        batches.append(
            BuildBatch(
                id=f"{number:03d}-{_slug(item)}",
                target_id=targets[item].id,
                actions=actions,
            )
        )
    return BuildSequence(
        number=number,
        description="build dependency layer",
        batches=tuple(batches),
    )


def _build_action(number, identity, source, payloads):
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
    path = payload_path(number, "build-objects", f"{action_slug}{ddl.extension}")
    content = ddl.content.encode("utf-8")
    payloads[path] = content
    return BuildAction(
        id=f"object-{action_slug}",
        kind=_OBJECT_KIND[source.kind],
        resource_node_id=str(identity),
        executor=ddl.executor,
        payload=path,
        payload_sha256=sha256_hex(content),
    )
