"""Plan an item-oriented repository as a build bundle.

Item dependency layers and stages order the presentation. Execution order is the
physical DAG compiled from each action's dependency keys, so independent items
and branches overlap.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Mapping

from ..catalogue.claims import without_claims
from ..catalogue.state import Catalogue
from ..catalogue.tables import ROLE_SHORTCUT
from ..declaration.model import (
    LAKEHOUSE,
    SEMANTIC_MODEL,
    WAREHOUSE,
    WeaverDocumentId,
    WeaverItemId,
    WeaverRepository,
)
from ..errors import BuildError
from ..etl import item_runtime_artefacts, load_schemas, runtime_artefacts
from ..locations import Location
from ..store import Store
from .bundle import (
    BuildBundle,
    compute_bundle_id,
    write_bundle,
)
from .catalogue_actions import (
    collect_claims,
    render_catalogue_after_build,
    render_catalogue_before_build,
    render_mirror_deregistration,
)
from .dependencies import (
    DECERTIFIED,
    PHYSICAL_COMPLETE,
    PREPARED,
    UPGRADED,
    catalogue_step_key,
)
from .documents import lakehouse_build_stages, warehouse_build_stages
from .drops import lakehouse_drop_stages, warehouse_drop_stages
from .endpoints import lakehouse_endpoint_refresh_stage, narrow_endpoint_refreshes
from .execution import BundleExecution, ExecutionIdentity, select_spark_home
from .executors.sql_endpoint_refresh import (
    AWAIT_EXECUTOR,
    START_EXECUTOR,
    START_TABLES_EXECUTOR,
)
from .executors.sql_endpoint_refresh import CONTRACTS as ENDPOINT_REFRESH_CONTRACTS
from .incremental import (
    BuildSelection,
    installed_as_pointer,
    select_build,
    stale_through_shortcuts,
)
from .models import OMIT_TARGET_UNBOUND, OmittedNode
from .prune import TargetInventory, lakehouse_prune_stage, warehouse_prune_stage
from .runtime import item_runtime_removals, item_runtime_stages
from .runtime_tables import (
    VIEW_STATE_SLUG,
    changed_semantic_validations,
    render_runtime_state_reconciliation,
    runtime_state_establishment,
    runtime_state_invalidation,
    view_state_establishment,
)
from .schemas import lakehouse_schema_stage, warehouse_schema_stage
from .shortcuts import plan_lakehouse_shortcuts, plan_warehouse_shortcuts
from .stages import PlannedStage, enumerate_stages, merge_layer_stages
from .targets import BoundTarget, ItemBindings, WarehouseBinding


def generate_item_build_bundle(
    repository: WeaverRepository,
    *,
    bindings: ItemBindings,
    output: Location,
    store: Store,
    target_inventories: Mapping[WeaverItemId, TargetInventory] | None = None,
    catalogue: Catalogue,
    stale_claims: tuple = (),
    catalogue_binding: WarehouseBinding | None,
    execution: ExecutionIdentity | None = None,
    shortcut_sources: Mapping[str, object] | None = None,
) -> BuildBundle:
    if catalogue_binding is None:
        from .workflow import require_catalogue_for

        require_catalogue_for(bindings)
    if execution is None:
        # The bindings already name the workspace. An orchestrated build resolves
        # ids and the Environment and passes them in; planning alone knows
        # neither, and says so rather than inventing them.
        execution = ExecutionIdentity(
            workspace_name=next(
                (
                    b.workspace_name
                    for b in (catalogue_binding, *(e.target for e in bindings.entries))
                    if getattr(b, "workspace_name", None)
                ),
                "",
            )
        )
    by_item = bindings.by_item
    if not by_item:
        raise BuildError("Select at least one Weaver item to build")
    known = {item.identity for item in repository.items}
    unknown = set(by_item) - known
    if unknown:
        raise BuildError(
            "Item(s) not found in the project: " + ", ".join(sorted(map(str, unknown)))
        )

    # Documents drive prune, schema and physical build planning. Shortcut
    # destinations and runtime artefacts are selected separately for their own
    # executors. Validations are selected only for catalogue publication; their
    # compiled runtime artefacts have separate identities.
    (
        selected_documents,
        selected_shortcuts,
        selected_loads,
        selected_validations,
    ) = _selectable(repository, by_item)
    selected_ids = selected_documents | selected_shortcuts | selected_loads
    certifiable_ids = selected_ids | selected_validations

    from .semantic import bind_semantic_target

    inventories = dict(target_inventories or {})
    target_by_item = {
        item: bind_semantic_target(
            by_item[item].to_bound_target(), inventories.get(item)
        )
        for item in sorted(by_item, key=str)
    }
    targets = tuple(target_by_item.values())
    for item, target in target_by_item.items():
        inventory = inventories.get(item)
        if inventory is None:
            raise BuildError(f"planning {item} requires a prepared target inventory")
        if inventory.target_id != target.id:
            raise BuildError(
                f"inventory for {item} describes {inventory.target_id}, not {target.id}"
            )

    from .reports import bind_reports

    repository = bind_reports(repository, target_by_item, catalogue)
    registered = _registered_in(catalogue, by_item)
    selection = select_items(
        repository, catalogue, by_item=by_item, inventories=inventories
    )
    selected_for_drop = set(selection.selected_for_drop)
    selected_for_build = set(selection.selected_for_build)
    removed = set(registered) - selected_ids

    if catalogue_binding is None:
        return _semantic_bundle_without_catalogue(
            repository,
            selection=selection,
            selected_for_build=selected_for_build,
            certifiable_ids=certifiable_ids,
            target_by_item=target_by_item,
            execution=execution,
            output=output,
            store=store,
        )
    catalogue_target = _catalogue_target(catalogue_binding, targets)
    if all(target.id != catalogue_target.id for target in targets):
        targets = targets + (catalogue_target,)
    from ..catalogue.builtin import BUILTIN_ITEM

    # For a shortcut producer outside this build, ``_.Installation`` supplies the
    # target. Build bindings take precedence over installed targets.
    installed_sources = installed_shortcut_sources(
        repository,
        catalogue,
        build_bindings=by_item,
        workspace_of=catalogue_target,
    )
    shortcut_target_by_item = {**installed_sources, **target_by_item}
    shortcut_target_by_item.setdefault(BUILTIN_ITEM, catalogue_target)
    # Shortcut actions refer to source targets by plan target id.
    declared_ids = {target.id for target in targets}
    targets = targets + tuple(
        source for source in installed_sources.values() if source.id not in declared_ids
    )

    from .catalogue_actions import render_catalogue_upgrade

    upgrade = render_catalogue_upgrade(catalogue, catalogue_target=catalogue_target)
    stages: list[PlannedStage] = (
        [upgrade.declaring(provides=(UPGRADED, PREPARED))]
        if upgrade is not None
        else []
    )
    omitted: list[OmittedNode] = []

    # Decertify everything rebuilt, including pointers refreshed in place.
    # Deleting the Registry row also lets publication record a new build_datetime.
    decertified = removed | selected_for_build
    # Publication must compare against the post-deletion catalogue. Otherwise an
    # unchanged projection could compare equal and leave a rebuilt row deleted.
    deleted_claims = collect_claims(catalogue, decertified, stale_claims=stale_claims)
    catalogue_after_deletions = without_claims(catalogue, deleted_claims)

    catalogue_before = render_catalogue_before_build(
        catalogue,
        decertified,
        catalogue_target=catalogue_target,
        stale_claims=stale_claims,
    )
    if catalogue_before is not None:
        stages.append(
            catalogue_before.declaring(
                requires=(UPGRADED,), provides=(DECERTIFIED, PREPARED)
            )
        )

    # Runtime state is reset here, between decertification and the first
    # physical action, and never after it. See
    # :mod:`weaver.build_bundle.runtime_tables`.
    runtime_state = runtime_state_invalidation(
        repository,
        items=tuple(target_by_item),
        selected_for_build=selected_for_build,
        catalogue=catalogue,
    )
    established_state = runtime_state_establishment(
        repository,
        items=tuple(target_by_item),
        selected_for_build=selected_for_build
        | changed_semantic_validations(repository, catalogue, items=target_by_item),
        holds_table=_catalogue_holds(inventories),
    )
    reconciliation = render_runtime_state_reconciliation(
        runtime_state,
        catalogue_target=catalogue_target,
        establishment=established_state,
    )
    if reconciliation is not None:
        stages.append(
            reconciliation.declaring(
                requires=(UPGRADED, DECERTIFIED), provides=(PREPARED,)
            )
        )

    view_state = view_state_establishment(
        repository,
        items=tuple(target_by_item),
        selected_for_build=selected_for_build,
    )

    for layer in _item_layers(repository, target_by_item):
        layer_stages: list[PlannedStage] = []
        for item in layer:
            planned = plan_item_build(
                repository,
                item=item,
                target=target_by_item[item],
                inventory=inventories[item],
                target_by_item=shortcut_target_by_item,
                selected_documents=selected_documents,
                selected_shortcuts=selected_shortcuts,
                shortcut_sources=shortcut_sources,
                selected_for_drop=selected_for_drop
                - selected_loads
                - selected_validations,
                selected_for_build=selected_for_build
                - selected_loads
                - selected_validations,
                selected_loads=selected_for_build & selected_loads,
                removed=removed,
                registered=registered,
                catalogue_target=catalogue_target,
                mirrored=catalogue.mirrors,
            )
            layer_stages.extend(planned.stages)
            omitted.extend(planned.omitted)
        stages.extend(merge_layer_stages(layer_stages))

    _refuse_selected_omissions(omitted)

    published = [
        render_runtime_state_reconciliation(
            (),
            catalogue_target=catalogue_target,
            establishment=view_state,
            slug=VIEW_STATE_SLUG,
            description="record the Views this build created",
            index=1,
        ),
        # Deregister a mirror only after physical work gives the object its own
        # rows.
        render_mirror_deregistration(
            catalogue,
            selected_for_build,
            catalogue_target=catalogue_target,
        ),
        *render_catalogue_after_build(
            repository,
            certifiable_ids,
            target_by_item,
            catalogue_target=catalogue_target,
            # Compare publication against the catalogue after claim deletion.
            current=catalogue_after_deletions,
            selected_models={
                identity
                for identity in selected_for_build
                if identity.item in repository.semantic_models
            },
        ),
    ]
    # Publication certifies only physical work that succeeded, and the Registry
    # is published last.
    previous = PHYSICAL_COMPLETE
    for index, stage in enumerate(stage for stage in published if stage is not None):
        stages.append(
            stage.declaring(requires=(previous,), provides=(catalogue_step_key(index),))
        )
        previous = catalogue_step_key(index)

    sequences, payloads, target_changes, required = enumerate_stages(
        narrow_endpoint_refreshes(stages),
        targets=targets,
        completion_target_id=catalogue_target.id,
    )

    omitted.extend(
        OmittedNode(
            node_id=str(identity),
            reason=OMIT_TARGET_UNBOUND,
            detail=f"item {identity.item} is not bound",
        )
        for identity in sorted(repository.source_documents, key=str)
        if identity not in certifiable_ids
    )
    from ..mutation.models import MutationPlan, PhysicalScope

    plan = MutationPlan(
        targets=targets,
        sequences=sequences,
        execution=BundleExecution.of(
            execution,
            catalogue_target_id=catalogue_target.id,
            spark_home_target_id=select_spark_home(
                target_by_item.values(), needed=_needs_spark(sequences)
            ),
        ),
        build_envelope={
            "repository_name": repository.name,
            "repository_signature": repository.signature,
            "selection": selection.to_mapping(),
            "omitted_nodes": [
                node.to_mapping()
                for node in sorted(omitted, key=lambda n: (n.node_id, n.reason))
            ],
            "target_changes": {
                key: [change.to_mapping() for change in value]
                for key, value in sorted(target_changes.items())
            },
            "runtime_state": [one.to_mapping() for one in runtime_state],
            "runtime_state_established": [
                one.to_mapping() for one in (*established_state, *view_state)
            ],
        },
        protected_scopes=tuple(
            PhysicalScope(source.id, "")
            for source in installed_sources.values()
            if source.id not in {target.id for target in target_by_item.values()}
        ),
        required_completion=required,
        driver_contracts=(
            ENDPOINT_REFRESH_CONTRACTS
            if any(
                a.executor in REFRESH_EXECUTORS
                for sequence in sequences
                for batch in sequence.batches
                for a in batch.actions
            )
            else ()
        ),
    )
    plan = replace(plan, bundle_id=compute_bundle_id(plan))
    return write_bundle(
        output,
        plan=plan,
        payloads=payloads,
        store=store,
    )


REFRESH_EXECUTORS = frozenset({START_EXECUTOR, START_TABLES_EXECUTOR, AWAIT_EXECUTOR})


def _needs_spark(sequences) -> bool:
    """Whether any planned action has to run through a Spark session."""

    from .execution import needs_spark

    return any(
        needs_spark(action)
        for sequence in sequences
        for batch in sequence.batches
        for action in batch.actions
    )


def _catalogue_holds(inventories):
    """Return whether the catalogue target already holds a runtime table.

    Reconciliation precedes physical work, so a build creating ``_`` records its
    objects on the next build or first load.
    """

    from ..catalogue.builtin import BUILTIN_ITEM
    from ..catalogue.tables import CATALOGUE_SCHEMA

    inventory = inventories.get(BUILTIN_ITEM)
    if inventory is None:
        return lambda table: False
    return lambda table: inventory.has_object(CATALOGUE_SCHEMA, table.name, "table")


def _refuse_selected_omissions(omitted: list[OmittedNode]) -> None:
    if not omitted:
        return
    details = "; ".join(
        f"{node.node_id}: {node.detail or node.reason}"
        for node in sorted(omitted, key=lambda node: (node.node_id, node.reason))
    )
    raise BuildError(f"selected object(s) could not be materialised: {details}")


def select_items(
    repository: WeaverRepository, catalogue, *, by_item: Mapping, inventories
) -> BuildSelection:
    """Select what a Build of ``by_item`` creates, drops and rebuilds."""

    documents, shortcuts, loads, _validations = _selectable(repository, by_item)
    # Freshness may depend on an item outside this build, so read it before
    # narrowing ``registered`` to bound items.
    stale_consumers = stale_through_shortcuts(
        repository, catalogue.registered, bound_items=by_item
    )
    return select_build(
        repository,
        _registered_in(catalogue, by_item),
        selected=documents | shortcuts | loads,
        stale_consumers=stale_consumers,
        inventories=inventories,
        mirrored=catalogue.mirrors,
    )


def _registered_in(catalogue, by_item: Mapping) -> dict:
    return {
        identity: document
        for identity, document in catalogue.registered.items()
        if identity.item in by_item and document.object_role != "source"
    }


def _selectable(
    repository: WeaverRepository, by_item: Mapping
) -> tuple[set, set, set, set]:
    return (
        {
            identity
            for identity, source in repository.source_documents.items()
            if identity.item in by_item and not source.is_validation
        }
        | {
            WeaverDocumentId.parse(str(item))
            for item in repository.semantic_models
            if item in by_item
        }
        | {
            WeaverDocumentId.report_root(item)
            for item in repository.reports
            if item in by_item
        },
        {
            declaration.destination
            for declaration in repository.shortcuts
            if declaration.destination.item in by_item
        }
        | {
            shortcut.destination
            for shortcut in repository.logical_shortcuts
            if shortcut.destination.item in by_item
        },
        {
            artefact.identity
            for artefact in runtime_artefacts(repository)
            if artefact.identity.item in by_item
        },
        {
            identity
            for identity, source in repository.source_documents.items()
            if identity.item in by_item and source.is_validation
        },
    )


def certifiable_identities(repository: WeaverRepository, by_item: Mapping) -> set:
    """Return every object the bound items could certify, including unchanged ones."""

    documents, shortcuts, loads, validations = _selectable(repository, by_item)
    return documents | shortcuts | loads | validations


def installable_identities(repository: WeaverRepository, by_item: Mapping) -> set:
    """Return physical identities considered by incremental installation."""

    documents, shortcuts, artefacts, _validations = _selectable(repository, by_item)
    return documents | shortcuts | artefacts


def _item_layers(
    repository: WeaverRepository,
    target_by_item: Mapping[WeaverItemId, object],
) -> tuple[tuple[WeaverItemId, ...], ...]:
    layers = repository.item_layers
    if not layers:
        raise BuildError(
            f"repository {repository.name!r} has no item dependency layers; "
            "build order is unknown"
        )
    placed = {item for layer in layers for item in layer}
    missing = set(target_by_item) - placed
    if missing:
        raise BuildError(
            "Cannot determine build order for item(s): "
            + ", ".join(sorted(map(str, missing)))
        )
    return tuple(
        selected
        for selected in (
            tuple(item for item in layer if item in target_by_item) for layer in layers
        )
        if selected
    )


@dataclass(frozen=True)
class PlannedItem:
    """One item's ordered stages and unplannable selected nodes."""

    stages: tuple[PlannedStage, ...]
    omitted: tuple[OmittedNode, ...]
    #: Shortcut destinations represented by ``omitted``.
    uncertified: frozenset


def plan_item_build(
    repository: WeaverRepository,
    *,
    item: WeaverItemId,
    target,
    inventory: TargetInventory,
    target_by_item,
    selected_documents,
    selected_shortcuts,
    selected_for_drop,
    selected_for_build,
    registered,
    catalogue_target,
    selected_loads=(),
    removed=(),
    shortcut_sources=None,
    mirrored=(),
) -> PlannedItem:
    """Plan one item's ordered physical stages from a selection and inventory."""

    arguments = dict(
        repository=repository,
        item=item,
        target=target,
        inventory=inventory,
        target_by_item=target_by_item,
        selected_documents=selected_documents,
        selected_shortcuts=selected_shortcuts,
        selected_for_drop=selected_for_drop,
        selected_for_build=selected_for_build,
        registered=registered,
        catalogue_target=catalogue_target,
        selected_loads=selected_loads,
        removed=removed,
        shortcut_sources=shortcut_sources,
        mirrored=mirrored,
    )
    if item.item_type == SEMANTIC_MODEL:
        from .semantic import semantic_readback_stage, semantic_stage

        stages = ()
        if WeaverDocumentId.parse(str(item)) in selected_for_build:
            stages = (semantic_stage(repository, item, target),)
            if any(
                contribution.model == item
                and WeaverDocumentId.report_root(report) in selected_for_build
                for report, contribution in repository.reports.items()
            ):
                stages += (semantic_readback_stage(repository, item, target),)
        return PlannedItem(stages, (), frozenset())
    if item.item_type == "Report":
        from .reports import report_stages

        stages = (
            report_stages(repository, item, target)
            if WeaverDocumentId.report_root(item) in selected_for_build
            else ()
        )
        return PlannedItem(stages, (), frozenset())
    if item.item_type == LAKEHOUSE:
        return _plan_lakehouse_item(**arguments)
    if item.item_type == WAREHOUSE:
        return _plan_warehouse_item(**arguments)
    raise BuildError(f"unsupported Weaver item type {item.item_type!r}")


def _plan_lakehouse_item(**arguments) -> PlannedItem:
    target = arguments["target"]
    return _plan_item(
        **arguments,
        shortcut_planner=plan_lakehouse_shortcuts,
        prune_planner=lakehouse_prune_stage,
        drop_planner=lakehouse_drop_stages,
        schema_planner=lakehouse_schema_stage,
        build_planner=lakehouse_build_stages,
        runtime_destination=target.spark_target,
        endpoint_planner=lakehouse_endpoint_refresh_stage,
    )


def _plan_warehouse_item(**arguments) -> PlannedItem:
    return _plan_item(
        **arguments,
        shortcut_planner=plan_warehouse_shortcuts,
        prune_planner=warehouse_prune_stage,
        drop_planner=warehouse_drop_stages,
        schema_planner=warehouse_schema_stage,
        build_planner=warehouse_build_stages,
        runtime_destination=None,
        endpoint_planner=None,
    )


def _plan_item(
    repository,
    *,
    item,
    target,
    inventory,
    target_by_item,
    selected_documents,
    selected_shortcuts,
    selected_for_drop,
    selected_for_build,
    registered,
    catalogue_target,
    selected_loads,
    removed,
    shortcut_sources,
    mirrored,
    shortcut_planner,
    prune_planner,
    drop_planner,
    schema_planner,
    build_planner,
    runtime_destination,
    endpoint_planner,
) -> PlannedItem:
    shortcuts = shortcut_planner(
        repository,
        item=item,
        target=target,
        target_by_item=target_by_item,
        selected=selected_for_build & selected_shortcuts,
        sources=shortcut_sources,
        catalogue_target=catalogue_target,
    )
    artefacts = item_runtime_artefacts(
        repository,
        item=item,
        destination=runtime_destination,
    )
    stages: list[PlannedStage] = []

    # Prune sees every declared shortcut destination, not only selected ones; an
    # unchanged shortcut remains desired state. The stage derives load artefacts.
    prune = prune_planner(
        repository,
        selected_documents,
        item=item,
        target=target,
        inventory=inventory,
    )
    if prune is not None:
        stages.append(prune)
    stages.extend(
        drop_planner(
            repository,
            selected_for_drop - _retained_pointers(selected_shortcuts, registered),
            item=item,
            target=target,
            inventory=inventory,
            registered=registered,
            reused_names=pointers_whose_name_is_reused(
                selected_for_drop,
                selected_for_build,
                selected_shortcuts,
                registered,
                mirrored,
            ),
            mirrored=mirrored,
        )
    )
    schemas = schema_planner(
        selected_documents,
        item=item,
        target=target,
        inventory=inventory,
        # Generated Warehouse load procedures require ``_``, which no document
        # declares. Deriving it from artefacts avoids creating it without procedures.
        extra_schemas=tuple(shortcuts.schemas) + load_schemas(artefacts),
    )
    if schemas is not None:
        stages.append(schemas)
    if shortcuts.stage is not None:
        stages.append(shortcuts.stage)
    stages.extend(
        build_planner(
            repository,
            selected_for_build - selected_shortcuts,
            item=item,
            target=target,
        )
    )

    if endpoint_planner is not None:
        refresh = endpoint_planner(stages, item=item, target=target)
        if refresh is not None:
            stages.append(refresh)

    # Runtime installation follows structure and endpoint refresh.
    # Removals come from prior Registry rows, not the target diff.
    stages.extend(
        item_runtime_stages(
            artefacts,
            selected_loads,
            item=item,
            target=target,
            repository=repository,
        )
    )
    stages.extend(
        item_runtime_removals(removed, item=item, target=target, registered=registered)
    )
    return PlannedItem(
        stages=tuple(stages),
        omitted=shortcuts.omitted,
        uncertified=frozenset(shortcuts.omitted_destinations)
        & frozenset(selected_for_build),
    )


def pointers_whose_name_is_reused(
    selected_for_drop,
    selected_for_build,
    selected_shortcuts,
    registered: Mapping,
    mirrored=(),
):
    """Return dropped pointers whose names this plan reuses for owned objects.

    OneLake may reserve a shortcut's name after Fabric stops listing it. Pointer
    replacements stay in place and do not need this wait.
    """

    mirrored = set(mirrored)
    return {
        identity
        for identity in selected_for_drop
        if identity in selected_for_build
        and identity not in selected_shortcuts
        and installed_as_pointer(registered, mirrored, identity)
    }


def _retained_pointers(selected_shortcuts, registered: Mapping) -> set:
    """Return shortcut destinations that managed drop must leave in place.

    Existing pointers are overwritten in place. A destination certified under a
    different role is dropped for the kind change. Without a Registry row, the
    existing object is not Weaver's to remove.
    """

    retained = set()
    for identity in selected_shortcuts:
        document = registered.get(identity)
        if document is None or document.object_role == ROLE_SHORTCUT:
            retained.add(identity)
    return retained


def installed_shortcut_sources(
    repository: WeaverRepository,
    catalogue: Catalogue,
    *,
    build_bindings: Mapping[WeaverItemId, object],
    workspace_of: BoundTarget,
) -> dict[WeaverItemId, BoundTarget]:
    """Where a logical shortcut's source already lives, for items outside this build.

    A build binding wins; otherwise ``_.Installation`` says where the item is.
    An installed-only target is referenceable and is not a writable build
    target. Only the items a selected logical shortcut names are resolved.
    """

    from ..installed import installed_targets

    wanted = {
        shortcut.source.item
        for shortcut in repository.logical_shortcuts
        if shortcut.destination.item in build_bindings
        and shortcut.source.item not in build_bindings
    }
    if not wanted:
        return {}
    found = {}
    for item, reference in installed_targets(catalogue).items():
        if item not in wanted:
            continue
        found[item] = BoundTarget(
            id=f"{reference.kind}-{reference.name}",
            kind=reference.kind,
            item_id=reference.name,
            item_name=reference.name,
            workspace_id=workspace_of.workspace_id,
            workspace_name=workspace_of.workspace_name,
            logical_item_type=item.item_type,
            logical_item_name=item.item_name,
        )
    return found


def _semantic_bundle_without_catalogue(
    repository,
    *,
    selection,
    selected_for_build,
    certifiable_ids,
    target_by_item,
    execution,
    output,
    store,
):
    """Deploy and read back semantic models in a workspace with no catalogue.

    Nothing is installed or certified, so each selected model deploys and its
    readback verifies the definition on the model itself.
    """

    from ..mutation.models import MutationPlan
    from .semantic import semantic_readback_stage, semantic_stage

    stages: list[PlannedStage] = []
    for item, target in target_by_item.items():
        if item.item_type == "Report":
            from .reports import report_stages

            if WeaverDocumentId.report_root(item) in selected_for_build:
                stages.extend(report_stages(repository, item, target))
        elif WeaverDocumentId.parse(str(item)) in selected_for_build:
            stages.append(semantic_stage(repository, item, target))
            stages.append(semantic_readback_stage(repository, item, target))
    targets = tuple(target_by_item.values())
    sequences, payloads, target_changes, required = enumerate_stages(
        stages, targets=targets, completion_target_id=targets[0].id
    )
    omitted = [
        OmittedNode(
            node_id=str(identity),
            reason=OMIT_TARGET_UNBOUND,
            detail=f"item {identity.item} is not bound",
        )
        for identity in sorted(repository.source_documents, key=str)
        if identity not in certifiable_ids
    ]
    plan = MutationPlan(
        targets=targets,
        sequences=sequences,
        execution=BundleExecution.of(
            execution, catalogue_target_id=None, spark_home_target_id=None
        ),
        build_envelope={
            "repository_name": repository.name,
            "repository_signature": repository.signature,
            "selection": selection.to_mapping(),
            "omitted_nodes": [
                node.to_mapping()
                for node in sorted(omitted, key=lambda n: (n.node_id, n.reason))
            ],
            "target_changes": {
                key: [change.to_mapping() for change in value]
                for key, value in sorted(target_changes.items())
            },
            "runtime_state": [],
            "runtime_state_established": [],
        },
        required_completion=required,
    )
    plan = replace(plan, bundle_id=compute_bundle_id(plan))
    return write_bundle(output, plan=plan, payloads=payloads, store=store)


def _catalogue_target(binding: WarehouseBinding, targets):
    physical = binding.to_bound_target()
    matching = tuple(
        target
        for target in targets
        if target.kind == physical.kind and target.item_id == physical.item_id
    )
    for target in matching:
        if target.logical_item_name == "_weaver":
            return target
    if matching:
        return matching[0]
    return replace(physical, id=f"control-{physical.id}")
