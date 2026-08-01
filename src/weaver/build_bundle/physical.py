"""Render one item's frozen physical prune, drop, schema and build stages.

Every function here plans for exactly one logical item against exactly one bound
target. That is the shape multi-item build needs: the item graph orders the items,
and inside an item the document graph orders the work, so nothing here reaches
across items or chooses a sequence number.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, replace
from typing import Mapping

from ..declaration.metadata import DELTA_TARGET, FOLDER, SQL_TARGET, TABLE, VIEW
from ..declaration.model import WeaverItemId
from ..errors import BuildError
from ..spark.tokens import object_token
from ..etl import FILE_TYPE, PROCEDURE_TYPE, item_load_artefacts
from .models import (
    BUILD_FOLDER,
    BUILD_PROCEDURE,
    DELETE_FILE,
    DROP_PROCEDURE,
    WRITE_FILE,
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
from .stages import BUILD, DROP, LOAD, PRUNE, SCHEMA, PlannedStage
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
    """An identity as something safe to name a payload file and an action after.

    Separators, spaces and the shape marker's colon all go: a payload path must
    stay relative and inside the bundle, and a colon reads as a drive letter on
    one of the platforms a bundle is unpacked on.
    """

    return str(value).replace("/", "--").replace(" ", "-").replace(":", "-")


def item_prune_stage(
    repository,
    selected_ids,
    *,
    item: WeaverItemId,
    target,
    inventory,
) -> PlannedStage | None:
    """Freeze one item's authoritative repository/inventory diff.

    The keep-set is derived here rather than handed in, and the load artefacts
    are why. They contribute the ``_`` schema a Warehouse's generated procedures
    live in, which no document declares — so a caller that did not think to pass
    them would produce a prune that drops the schema the same build just created.
    A destructive default is not something to leave reachable, and the repository
    is already here, so nothing has to be remembered.
    """

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
        load_identities=[
            artefact.identity for artefact in item_load_artefacts(repository, item=item)
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


@dataclass(frozen=True)
class RenderedAction:
    """One authored document turned into one action and its frozen payload.

    The smallest unit the build has: a declaration in, an executable out. Kept as
    a value rather than written straight into a stage's payload dict so that the
    mapping from *declaration* to *bundle action* — which executor runs it, what
    the payload is called, what its hash is — can be examined on its own, without
    planning an item or generating a bundle to see it.

    ``payloads`` is empty for a folder, which is created rather than executed.
    """

    action: BuildAction
    payloads: Mapping[str, bytes]


def render_document_build_action(identity, source) -> RenderedAction:
    """The build action and payload one declared document renders to.

    This is where ``source.create_ddl()`` becomes something a bundle can carry:
    the DDL says *what statement*, and this says what action runs it, under what
    id, with which executor, and against which frozen bytes. The two are separate
    claims and are worth failing separately.
    """

    action_slug = _slug(identity)
    if source.kind == FOLDER:
        # A folder has no statement to run: it is a directory the installer
        # creates, so there is nothing to freeze and nothing to hash.
        return RenderedAction(
            action=BuildAction(
                id=f"folder-{action_slug}",
                kind=BUILD_FOLDER,
                resource_node_id=str(identity),
                executor="folder",
                payload=None,
                payload_sha256=None,
            ),
            payloads={},
        )
    ddl = source.create_ddl()
    filename = f"{action_slug}{ddl.extension}"
    content = ddl.content.encode("utf-8")
    return RenderedAction(
        action=BuildAction(
            id=f"object-{action_slug}",
            kind=_OBJECT_KIND[source.kind],
            resource_node_id=str(identity),
            executor=ddl.executor,
            payload=filename,
            payload_sha256=sha256_hex(content),
        ),
        payloads={filename: content},
    )


def render_load_build_action(artefact) -> RenderedAction:
    """The action and frozen payload one load artefact installs as.

    The load half of :func:`render_document_build_action`, and the same claim:
    what runs it, under what id, against which bytes. A file is written into the
    runtime tree by the ``load_file`` executor; a procedure is a create-or-alter
    run by ``tsql``, which needs no knowledge that it happens to be a procedure.
    """

    action_slug = _slug(artefact.identity)
    if artefact.is_file:
        filename = f"{action_slug}.payload"
        executor, kind = "load_file", WRITE_FILE
    else:
        filename = f"{action_slug}.sql"
        executor, kind = "tsql", BUILD_PROCEDURE
    return RenderedAction(
        action=BuildAction(
            id=f"load-{action_slug}",
            kind=kind,
            resource_node_id=str(artefact.identity),
            executor=executor,
            payload=filename,
            payload_sha256=sha256_hex(artefact.payload),
        ),
        payloads={filename: artefact.payload},
    )


def item_load_stages(
    artefacts,
    selected_for_build,
    *,
    item: WeaverItemId,
    target,
) -> tuple[PlannedStage, ...]:
    """One item's load layer: the last thing it does, and one barrier wide.

    A single stage rather than dependency layers, because there are no
    dependencies to express — nothing here runs anything, so a deployed module
    and a generated procedure have no ordering between them. What they *do*
    depend on is the item's structural work, and that is expressed by the layer
    being last rather than by an edge.

    Empty when the item has no selected load work, which is a phase with nothing
    to do rather than an empty barrier: an unpopulated stage takes no sequence
    number.
    """

    selected = [
        artefact
        for artefact in artefacts
        if artefact.identity.item == item and artefact.identity in selected_for_build
    ]
    if not selected:
        return ()
    payloads: dict[str, bytes] = {}
    actions = []
    for artefact in sorted(selected, key=lambda value: str(value.identity)):
        rendered = render_load_build_action(artefact)
        payloads.update(rendered.payloads)
        actions.append(rendered.action)
    return (
        PlannedStage(
            phase=LOAD,
            slug="load",
            description="install load artefacts",
            payloads=payloads,
            batches=(
                BuildBatch(
                    id=f"{_slug(item)}", target_id=target.id, actions=tuple(actions)
                ),
            ),
        ),
    )


def item_load_removals(
    removed,
    *,
    item: WeaverItemId,
    target,
    registered,
) -> tuple[PlannedStage, ...]:
    """Frozen removals for load artefacts the source has stopped claiming.

    Driven by the previous Registry rows rather than by a diff against the
    target, and that is what makes a rename ordinary: the old identity is no
    longer claimed, so its row names exactly what to remove and where, while the
    new identity is simply new. Nothing has to notice that the two are related.

    The removals ride in the item's load layer alongside its writes. They cannot
    collide — an identity is either still claimed or not — and keeping them
    together means everything the load layer does to a target is in one barrier.
    """

    # Scoped by what the Registry says each removed object *is*, not by what its
    # identity looks like. A removed table is removed by the inventory prune,
    # which can see it; only the two the prune cannot see are handled here.
    selected = sorted(
        (
            identity
            for identity in removed
            if identity.item == item
            and registered[identity].object_type in (FILE_TYPE, PROCEDURE_TYPE)
        ),
        key=str,
    )
    payloads: dict[str, bytes] = {}
    actions = []
    for identity in selected:
        object_type = registered[identity].object_type
        action_slug = _slug(identity)
        if object_type == FILE_TYPE:
            actions.append(
                BuildAction(
                    id=f"load-remove-{action_slug}",
                    kind=DELETE_FILE,
                    resource_node_id=str(identity),
                    executor="load_file",
                    payload=None,
                    payload_sha256=None,
                )
            )
            continue
        statement = (
            "drop procedure if exists "
            f"{_tsql_ident(identity.object_id.schema)}."
            f"{_tsql_ident(identity.object_id.object)};\n"
        )
        content = statement.encode("utf-8")
        filename = f"drop-{action_slug}.sql"
        payloads[filename] = content
        actions.append(
            BuildAction(
                id=f"load-remove-{action_slug}",
                kind=DROP_PROCEDURE,
                resource_node_id=str(identity),
                executor="tsql",
                payload=filename,
                payload_sha256=sha256_hex(content),
            )
        )
    if not actions:
        return ()
    return (
        PlannedStage(
            phase=LOAD,
            slug="load",
            description="install load artefacts",
            payloads=payloads,
            batches=(
                BuildBatch(
                    id=f"remove-{_slug(item)}",
                    target_id=target.id,
                    actions=tuple(actions),
                ),
            ),
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
        actions = []
        for node in sorted(layer):
            identity = identities[node]
            rendered = render_document_build_action(
                identity, repository.source_documents[identity]
            )
            payloads.update(rendered.payloads)
            actions.append(rendered.action)
        actions = tuple(actions)
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


