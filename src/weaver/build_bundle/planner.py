"""Plan an item-oriented repository as an ordered build bundle.

Item dependency layers are the outer barriers; document dependencies order work
within each item. Items in one layer share stage barriers, while a consumer waits
for every producer item, including endpoint refreshes, to finish.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Mapping

from ..catalogue.claims import without_claims
from ..catalogue.state import Catalogue
from ..catalogue.tables import ROLE_SHORTCUT
from ..declaration.model import LAKEHOUSE, WAREHOUSE, WeaverItemId, WeaverRepository
from ..errors import BuildError
from ..etl import item_runtime_artefacts, load_schemas, runtime_artefacts
from ..locations import Location
from ..store import Store
from .bundle import (
    SUPPORTED_FORMAT_VERSION,
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
from .documents import lakehouse_build_stages, warehouse_build_stages
from .drops import lakehouse_drop_stages, warehouse_drop_stages
from .endpoints import lakehouse_endpoint_refresh_stage
from .incremental import installed_as_pointer, select_build, stale_through_shortcuts
from .models import OMIT_TARGET_UNBOUND, BuildPlan, OmittedNode
from .prune import TargetInventory, lakehouse_prune_stage, warehouse_prune_stage
from .runtime import item_runtime_removals, item_runtime_stages
from .runtime_tables import (
    VIEW_STATE_SLUG,
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
    catalogue_binding: WarehouseBinding,
    shortcut_sources: Mapping[str, object] | None = None,
) -> BuildBundle:
    if catalogue_binding is None:
        raise BuildError("a catalogue Warehouse binding is required")
    by_item = bindings.by_item
    if not by_item:
        raise BuildError("at least one Weaver item must be bound")
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
    selected_ids = (
        selected_documents | selected_shortcuts | selected_loads | selected_validations
    )

    targets = tuple(
        by_item[item].to_bound_target() for item in sorted(by_item, key=str)
    )
    target_by_item = {
        item: by_item[item].to_bound_target() for item in sorted(by_item, key=str)
    }
    inventories = dict(target_inventories or {})
    for item, target in target_by_item.items():
        inventory = inventories.get(item)
        if inventory is None:
            raise BuildError(f"planning {item} requires a prepared target inventory")
        if inventory.target_id != target.id:
            raise BuildError(
                f"inventory for {item} describes {inventory.target_id}, not {target.id}"
            )

    # Freshness may depend on an item outside this build, so read it before
    # narrowing ``registered`` to bound items.
    stale_consumers = stale_through_shortcuts(
        repository, catalogue.registered, bound_items=by_item
    )
    registered = {
        identity: document
        for identity, document in catalogue.registered.items()
        if identity.item in by_item
    }
    selection = select_build(
        repository,
        registered,
        selected=selected_ids,
        stale_consumers=stale_consumers,
        inventories=inventories,
        mirrored=catalogue.mirrors,
    )
    selected_for_drop = set(selection.selected_for_drop)
    selected_for_build = set(selection.selected_for_build)
    removed = set(registered) - selected_ids

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

    stages: list[PlannedStage] = []
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
        stages.append(catalogue_before)

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
        selected_for_build=selected_for_build,
        holds_table=_catalogue_holds(inventories),
    )
    reconciliation = render_runtime_state_reconciliation(
        runtime_state,
        catalogue_target=catalogue_target,
        establishment=established_state,
    )
    if reconciliation is not None:
        stages.append(reconciliation)

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

    recorded_views = render_runtime_state_reconciliation(
        (),
        catalogue_target=catalogue_target,
        establishment=view_state,
        slug=VIEW_STATE_SLUG,
        description="record the Views this build created",
        index=1,
    )
    if recorded_views is not None:
        stages.append(recorded_views)

    # Deregister a mirror only after physical work gives the object its own rows.
    deregistered = render_mirror_deregistration(
        catalogue,
        selected_for_build,
        catalogue_target=catalogue_target,
    )
    if deregistered is not None:
        stages.append(deregistered)

    stages.extend(
        render_catalogue_after_build(
            repository,
            selected_ids,
            target_by_item,
            catalogue_target=catalogue_target,
            # Compare publication against the catalogue after claim deletion.
            current=catalogue_after_deletions,
        )
    )

    sequences, payloads, target_changes = enumerate_stages(stages)

    omitted.extend(
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
        sequences=sequences,
        selection=selection,
        omitted_nodes=tuple(
            sorted(omitted, key=lambda node: (node.node_id, node.reason))
        ),
        target_changes=target_changes,
        runtime_state=runtime_state,
        runtime_state_established=(*established_state, *view_state),
    )
    plan = replace(plan, bundle_id=compute_bundle_id(plan))
    return write_bundle(
        output,
        plan=plan,
        payloads=payloads,
        store=store,
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


def _selectable(
    repository: WeaverRepository, by_item: Mapping
) -> tuple[set, set, set, set]:
    return (
        {
            identity
            for identity, source in repository.source_documents.items()
            if identity.item in by_item and not source.is_validation
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
        item_runtime_stages(artefacts, selected_loads, item=item, target=target)
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
