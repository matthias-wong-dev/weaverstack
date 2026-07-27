"""Plan an item-oriented repository as one coordinated build bundle."""

from __future__ import annotations

import json
from dataclasses import replace

from ..errors import BuildError
from ..locations import Location
from ..ses.metadata import DELTA_TARGET, FOLDER, SQL_TARGET, TABLE, VIEW
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
    PUBLISH_REGISTRY,
    RECONCILE_CATALOGUE,
    RECORD_INSTALLATION,
    BuildAction,
    BuildBatch,
    BuildPlan,
    BuildSequence,
    OmittedNode,
)
from .payloads import SCHEMA_SEQUENCE, payload_path, sha256_hex
from .payloads import (
    CATALOGUE_SEQUENCE,
    INSTALLATION_SEQUENCE,
    PRUNE_SEQUENCE,
    REGISTRY_SEQUENCE,
)
from .targets import LAKEHOUSE_TARGET, ItemBindings, LakehouseBinding, WAREHOUSE_TARGET

_OBJECT_KIND = {TABLE: BUILD_TABLE, VIEW: BUILD_VIEW}


def generate_item_build_bundle(
    repository: WeaverRepository,
    *,
    bindings: ItemBindings,
    output: Location,
    store: Store,
    prune: bool = True,
    catalogue: bool = False,
    control_lakehouse: LakehouseBinding | None = None,
    resolver=None,
    spark=None,
    host=None,
    sql_by_item=None,
) -> BuildBundle:
    """Freeze all bound items into one manifest and certified snapshot.

    Prune reconciles each bound physical item against only the documents owned by
    its logical item. Inventory is frozen into the bundle; install never lists a
    target. Lakehouse planning needs a resolver, supplied directly or obtained
    from ``host``. Warehouse planning opens Fabric-native SQL from ``host`` unless
    the caller supplies an executor in ``sql_by_item``.
    """

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

    if prune:
        if resolver is None and host is not None:
            from ..resolution import resolver_for

            resolver = resolver_for(host)
        prune_sequence = _item_prune_sequence(
            repository,
            selected_ids,
            target_by_item,
            resolver=resolver,
            store=store,
            spark=spark,
            host=host,
            sql_by_item=sql_by_item,
            payloads=payloads,
        )
        if prune_sequence is not None:
            sequences.append(prune_sequence)

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
    if catalogue:
        if control_lakehouse is None:
            raise BuildError("catalogue publication requires the control-plane Lakehouse binding")
        control_target = _control_target(control_lakehouse, targets)
        sequences.extend(
            _catalogue_sequences(
                repository,
                selected_ids,
                target_by_item,
                control_target,
                payloads,
            )
        )
        if all(target.id != control_target.id for target in targets):
            targets = targets + (control_target,)
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
    snapshot = {
        relative: store.read(repository.root.join(*relative.split("/")))
        for relative in sorted(paths)
        if relative not in repository.generated_files
    }
    snapshot.update(repository.generated_files)
    return dict(sorted(snapshot.items()))


def _slug(value) -> str:
    return str(value).replace("/", "--").replace(" ", "-")


def _control_target(binding: LakehouseBinding, targets):
    physical = binding.to_bound_target()
    for target in targets:
        if target.kind == physical.kind and target.item_id == physical.item_id:
            return target
    return replace(physical, id=f"control-{physical.id}")


def _catalogue_sequences(
    repository,
    selected_ids,
    target_by_item,
    control_target,
    payloads,
):
    from .. import __version__
    from ..catalogue.item_projection import project_item_installation
    from ..catalogue.item_reconcile import reconcile_item

    groups = {
        CATALOGUE_SEQUENCE: ("reconcile item catalogue dictionaries", RECONCILE_CATALOGUE, []),
        INSTALLATION_SEQUENCE: ("record item installations", RECORD_INSTALLATION, []),
        REGISTRY_SEQUENCE: ("publish item registry", PUBLISH_REGISTRY, []),
    }
    for item in sorted(target_by_item, key=str):
        retained = [identity for identity in selected_ids if identity.item == item]
        projection = project_item_installation(
            repository,
            item=item,
            retained=retained,
            target_name=target_by_item[item].name,
            weaver_version=__version__,
        )
        reconciliation = reconcile_item(projection)
        for number, (_description, kind, batches) in groups.items():
            reconciliation_group = {
                CATALOGUE_SEQUENCE: reconciliation.dictionaries,
                INSTALLATION_SEQUENCE: (reconciliation.installation,),
                REGISTRY_SEQUENCE: (reconciliation.registry,),
            }[number]
            actions = []
            for table_plan in reconciliation_group:
                for verb, statement in (("delete", table_plan.delete), ("merge", table_plan.merge)):
                    if statement is None:
                        continue
                    content = statement.encode("utf-8")
                    name = f"{_slug(item)}-{table_plan.table.name}-{verb}.spark.sql"
                    path = payload_path(number, "item-catalogue", name)
                    payloads[path] = content
                    actions.append(
                        BuildAction(
                            id=f"catalogue-{_slug(item)}-{table_plan.table.name}-{verb}",
                            kind=kind,
                            resource_node_id=None,
                            executor="spark_sql",
                            payload=path,
                            payload_sha256=sha256_hex(content),
                        )
                    )
            if actions:
                batches.append(
                    BuildBatch(
                        id=f"{number:03d}-catalogue-{_slug(item)}",
                        target_id=control_target.id,
                        actions=tuple(actions),
                    )
                )
    return tuple(
        BuildSequence(number=number, description=description, batches=tuple(batches))
        for number, (description, _kind, batches) in groups.items()
        if batches
    )


def _item_prune_sequence(
    repository,
    selected_ids,
    target_by_item,
    *,
    resolver,
    store,
    spark,
    host,
    sql_by_item,
    payloads,
):
    """Freeze one item-owned inventory diff per bound physical item.

    The proven target inspectors remain shared with the flat planner. Their
    output is namespaced here because two logical items may contain the same
    schema/object names and therefore need distinct manifest action ids and
    payload paths even when their physical targets differ.
    """

    from .planner import _managed_sets, _prune_sequence, _warehouse_prune_sequence

    batches = []
    supplied_sql = sql_by_item or {}
    for item in sorted(target_by_item, key=str):
        target = target_by_item[item]
        documents = {
            str(identity): repository.source_documents[identity]
            for identity in selected_ids
            if identity.item == item
        }
        target_kind = SQL_TARGET if target.kind == WAREHOUSE_TARGET else DELTA_TARGET
        managed = _managed_sets(documents, target_kind)
        temporary_payloads = {}
        if target.kind == WAREHOUSE_TARGET:
            sql = supplied_sql.get(item)
            if sql is None and host is None:
                raise BuildError(
                    f"pruning Warehouse item {item} requires host or sql_by_item[{item}]"
                )
            sequence = _warehouse_prune_sequence(
                target, sql, host, managed, temporary_payloads
            )
        else:
            if target.kind != LAKEHOUSE_TARGET:
                raise BuildError(f"unsupported physical target kind for {item}: {target.kind}")
            if resolver is None:
                raise BuildError(
                    f"pruning Lakehouse item {item} requires resolver or host"
                )
            sequence = _prune_sequence(
                target, resolver, store, spark, managed, temporary_payloads
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
                replace(
                    action,
                    id=f"{item_slug}-{action.id}",
                    payload=new_payload,
                )
            )
        batches.append(
            replace(sequence.batches[0], actions=tuple(rewritten_actions))
        )

    if not batches:
        return None
    return BuildSequence(
        number=PRUNE_SEQUENCE,
        description="prune unmanaged objects by logical item",
        batches=tuple(batches),
    )
