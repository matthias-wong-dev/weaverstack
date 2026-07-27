"""Plan an item-oriented repository as one coordinated build bundle."""

from __future__ import annotations

import json
from dataclasses import replace

from ..errors import BuildError
from ..locations import Location
from ..ses.metadata import FOLDER, TABLE, VIEW
from ..ses.model import LAKEHOUSE, WeaverDocumentId, WeaverItemId, WeaverRepository
from ..ses.source import SourceDocument
from ..store import Store
from .bundle import SUPPORTED_FORMAT_VERSION, BuildBundle, compute_bundle_id, write_bundle
from .models import (
    BUILD_FOLDER,
    BUILD_TABLE,
    BUILD_VIEW,
    CREATE_SCHEMA,
    OMIT_TARGET_UNBOUND,
    BuildAction,
    BuildBatch,
    BuildPlan,
    BuildSequence,
    OmittedNode,
)
from .payloads import SCHEMA_SEQUENCE, payload_path, sha256_hex
from .targets import ItemBindings, WAREHOUSE_TARGET

_OBJECT_KIND = {TABLE: BUILD_TABLE, VIEW: BUILD_VIEW}


def generate_item_build_bundle(
    repository: WeaverRepository,
    *,
    bindings: ItemBindings,
    output: Location,
    store: Store,
    prune: bool = False,
) -> BuildBundle:
    """Freeze all bound items into one manifest and certified snapshot.

    Item-scoped prune lands at R7. Until then callers must opt out explicitly;
    this planner refuses to imply that target-kind prune is safe for the new
    ownership model.
    """

    if prune:
        raise BuildError("item-scoped prune is not implemented until R7; pass prune=False")
    by_item = bindings.by_item
    if not by_item:
        raise BuildError("at least one Weaver item must be bound")
    known = {item.identity for item in repository.items}
    unknown = set(by_item) - known
    if unknown:
        raise BuildError(
            "binding names item(s) absent from the repository: "
            + ", ".join(sorted(map(str, unknown)))
        )

    selected_ids = {
        identity for identity in repository.source_documents if identity.item in by_item
    }
    alias_use = [
        edge
        for edge in repository.dependency_edges
        if edge.consumer in selected_ids and edge.uses_alias
    ]
    if alias_use:
        raise NotImplementedError("Alias usage is not yet supported")

    targets = tuple(
        by_item[item].to_bound_target() for item in sorted(by_item, key=str)
    )
    target_by_item = {
        item: by_item[item].to_bound_target() for item in sorted(by_item, key=str)
    }
    payloads: dict[str, bytes] = {}
    sequences: list[BuildSequence] = []

    schema_sequence = _schema_sequence(repository, selected_ids, target_by_item, payloads)
    if schema_sequence is not None:
        sequences.append(schema_sequence)

    if selected_ids:
        selected_nodes = {str(identity) for identity in selected_ids}
        graph = repository.dependency_graph.subgraph(selected_nodes)
        for layer_index, layer in enumerate(graph.layers()):
            number = 30 + layer_index * 10
            sequences.append(
                _item_layer_sequence(
                    number,
                    layer,
                    repository.source_documents,
                    target_by_item,
                    payloads,
                )
            )

    omitted = tuple(
        OmittedNode(
            node_id=str(identity),
            reason=OMIT_TARGET_UNBOUND,
            detail=f"item {identity.item} is not bound",
        )
        for identity in sorted(repository.source_documents, key=str)
        if identity not in selected_ids
    )
    plan = BuildPlan(
        format_version=SUPPORTED_FORMAT_VERSION,
        bundle_id="",
        repository_name=repository.name,
        repository_signature=repository.signature,
        targets=targets,
        sequences=tuple(sequences),
        omitted_nodes=omitted,
    )
    plan = replace(plan, bundle_id=compute_bundle_id(plan))
    return write_bundle(
        output,
        plan=plan,
        payloads=payloads,
        snapshot=_snapshot(repository, store),
        store=store,
    )


def _schema_sequence(
    repository: WeaverRepository,
    selected: set[WeaverDocumentId],
    targets,
    payloads: dict[str, bytes],
) -> BuildSequence | None:
    batches: list[BuildBatch] = []
    for item in sorted(targets, key=str):
        target = targets[item]
        schemas = sorted(
            {
                identity.object_id.schema
                for identity in selected
                if identity.item == item and not identity.is_files
            }
        )
        actions: list[BuildAction] = []
        for schema in schemas:
            if target.kind == WAREHOUSE_TARGET:
                statement = (
                    "if not exists (select 1 from sys.schemas where name = "
                    f"'{schema.replace(chr(39), chr(39) * 2)}')\n"
                    f"    exec('create schema [{schema.replace(']', ']]')}]');\n"
                )
                content = statement.encode("utf-8")
                executor, extension = "tsql", ".sql"
            else:
                content = (json.dumps({"schema": schema}, sort_keys=True) + "\n").encode()
                executor, extension = "spark_schema", ".schema.json"
            item_slug = _slug(item)
            path = payload_path(
                SCHEMA_SEQUENCE,
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
                    id=f"{SCHEMA_SEQUENCE:03d}-{_slug(item)}",
                    target_id=target.id,
                    actions=tuple(actions),
                )
            )
    if not batches:
        return None
    return BuildSequence(
        number=SCHEMA_SEQUENCE,
        description="create item-owned schemas",
        batches=tuple(batches),
    )


def _item_layer_sequence(
    number: int,
    nodes: tuple[str, ...],
    documents,
    targets,
    payloads: dict[str, bytes],
) -> BuildSequence:
    by_item: dict[WeaverItemId, list[WeaverDocumentId]] = {}
    identities = {str(identity): identity for identity in documents}
    for node in nodes:
        identity = identities[node]
        by_item.setdefault(identity.item, []).append(identity)
    batches: list[BuildBatch] = []
    for item in sorted(by_item, key=str):
        actions = tuple(
            _action(number, identity, documents[identity], payloads)
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


def _action(
    number: int,
    identity: WeaverDocumentId,
    source: SourceDocument,
    payloads: dict[str, bytes],
) -> BuildAction:
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


def _snapshot(repository: WeaverRepository, store: Store) -> dict[str, bytes]:
    if repository.root is None:
        raise BuildError("a discovered repository root is required to certify a snapshot")
    paths = {source.relative_path for source in repository.source_documents.values()}
    paths.update(schema.relative_path for schema in repository.schema_documents.values())
    paths.update(repository.support_files)
    return {
        relative: store.read(repository.root.join(*relative.split("/")))
        for relative in sorted(paths)
    }


def _slug(value) -> str:
    return str(value).replace("/", "--").replace(" ", "-")
