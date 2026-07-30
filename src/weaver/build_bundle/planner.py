"""Orchestrate one item-oriented repository into a coordinated build bundle."""

from __future__ import annotations

from dataclasses import replace
from typing import Mapping

from ..catalogue.state import ReconciledCatalogue
from ..declaration.model import WeaverItemId, WeaverRepository
from ..errors import BuildError
from ..locations import Location
from ..store import Store
from .bundle import SUPPORTED_FORMAT_VERSION, BuildBundle, compute_bundle_id, write_bundle
from .catalogue_actions import (
    render_catalogue_after_build,
    render_catalogue_before_build,
)
from .incremental import select_build
from .models import OMIT_TARGET_UNBOUND, BuildPlan, BuildSequence, OmittedNode
from .endpoint_actions import (
    render_application_endpoint_refresh,
    render_control_endpoint_refresh,
)
from .payloads import MANAGED_DROP_SEQUENCE_START, OBJECT_SEQUENCE_STEP
from .physical import (
    render_inventory_prune_sequence,
    render_selected_builds,
    render_selected_drops,
)
from .prune import TargetInventory
from .targets import ItemBindings, LakehouseBinding


def generate_item_build_bundle(
    repository: WeaverRepository,
    *,
    bindings: ItemBindings,
    output: Location,
    store: Store,
    target_inventories: Mapping[WeaverItemId, TargetInventory] | None = None,
    reconciled_catalogue: ReconciledCatalogue,
    control_lakehouse: LakehouseBinding,
) -> BuildBundle:
    """Freeze the one incremental build model into an installable bundle."""

    if control_lakehouse is None:
        raise BuildError("every build needs an explicit control-plane Lakehouse")
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
    if any(
        edge.consumer in selected_ids and edge.uses_alias
        for edge in repository.dependency_edges
    ):
        raise NotImplementedError("Alias usage is not yet supported")

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

    registered = {
        identity: document
        for identity, document in reconciled_catalogue.registered.items()
        if identity.item in by_item
    }
    selection = select_build(repository, registered, selected=selected_ids)
    selected_for_drop = set(selection.selected_for_drop)
    selected_for_build = set(selection.selected_for_build)
    removed = set(registered) - selected_ids

    payloads: dict[str, bytes] = {}
    sequences: list[BuildSequence] = []
    control_target = _control_target(control_lakehouse, targets)
    if all(target.id != control_target.id for target in targets):
        targets = targets + (control_target,)

    catalogue_before = render_catalogue_before_build(
        reconciled_catalogue,
        removed | selected_for_drop,
        control_target=control_target,
        payloads=payloads,
    )
    if catalogue_before is not None:
        sequences.append(catalogue_before)

    physical_sequences: list[BuildSequence] = []
    prune = render_inventory_prune_sequence(
        repository,
        selected_ids,
        target_by_item,
        target_inventories=inventories,
        payloads=payloads,
    )
    if prune is not None:
        physical_sequences.append(prune)

    drops = render_selected_drops(
        repository,
        selected_for_drop,
        target_by_item,
        registered=registered,
        start=MANAGED_DROP_SEQUENCE_START,
        payloads=payloads,
    )
    physical_sequences.extend(drops)
    build_start = MANAGED_DROP_SEQUENCE_START + len(drops) * OBJECT_SEQUENCE_STEP
    physical_sequences.extend(
        render_selected_builds(
            repository,
            selected_for_build,
            target_by_item,
            inventories,
            start=build_start,
            payloads=payloads,
        )
    )
    sequences.extend(physical_sequences)
    application_refresh = render_application_endpoint_refresh(
        physical_sequences,
        targets=targets,
        control_target=control_target,
    )
    if application_refresh is not None:
        sequences.append(application_refresh)
    sequences.extend(
        render_catalogue_after_build(
            repository,
            selected_ids,
            target_by_item,
            control_target=control_target,
            payloads=payloads,
        )
    )
    sequences.append(render_control_endpoint_refresh(control_target))

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
        selection=selection,
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


def _control_target(binding: LakehouseBinding, targets):
    physical = binding.to_bound_target()
    for target in targets:
        if target.kind == physical.kind and target.item_id == physical.item_id:
            return target
    return replace(physical, id=f"control-{physical.id}")
