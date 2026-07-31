"""Orchestrate one item-oriented repository into a coordinated build bundle.

A build is planned as an ordered series of **item** builds. The repository owns
an acyclic item dependency graph and its topological layers; this walks those
layers and plans each item as one coherent group of stages:

.. code-block:: text

    catalogue claim removal, when required

    item layer 0
        producer item A          prune, drops, schemas, aliases, documents, refresh
        independent producer B   prune, drops, schemas, aliases, documents, refresh
    item layer 1
        consumer item C          prune, drops, schemas, aliases, documents, refresh

    final batched catalogue publication
    Weaver Lakehouse SQL endpoint refresh

Items in the same layer share their barriers — one batch each — because nothing
orders them against each other. Items in different layers never do, which is the
one invariant multi-item build rests on: a consumer's aliases and documents
cannot begin until every item it reaches into has finished, endpoint included.

Inside an item the document dependency graph still decides everything. The item
graph is the outer boundary, not a replacement for it.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Mapping

from ..catalogue.state import ReconciledCatalogue
from ..declaration.model import WeaverItemId, WeaverRepository
from ..errors import BuildError
from ..locations import Location
from ..store import Store
from .aliases import plan_item_aliases
from .bundle import SUPPORTED_FORMAT_VERSION, BuildBundle, compute_bundle_id, write_bundle
from .catalogue_actions import (
    render_catalogue_after_build,
    render_catalogue_before_build,
)
from .endpoints import item_refresh_stage
from .incremental import select_build, stale_alias_destinations
from .models import OMIT_TARGET_UNBOUND, BuildPlan, OmittedNode
from .physical import (
    item_build_stages,
    item_drop_stages,
    item_prune_stage,
    item_schema_stage,
)
from .prune import TargetInventory
from .stages import PlannedStage, enumerate_stages, merge_layer_stages
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

    # Two kinds of node are selectable, and most of what follows needs exactly
    # one of them. Documents are what prune, schemas and the physical build
    # pipelines are about; alias destinations are registered objects too — so
    # they take part in selection and certification — but they are materialised
    # by the alias executor rather than by any document stage.
    selected_documents = {
        identity for identity in repository.source_documents if identity.item in by_item
    }
    selected_aliases = {
        alias.destination
        for alias in repository.aliases
        if alias.destination.item in by_item
    }
    selected_ids = selected_documents | selected_aliases

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

    # Freshness is read before ``registered`` is narrowed, because the whole
    # point is to compare against an item this build does *not* include.
    stale_aliases = stale_alias_destinations(
        repository, reconciled_catalogue.registered, bound_items=by_item
    )
    registered = {
        identity: document
        for identity, document in reconciled_catalogue.registered.items()
        if identity.item in by_item
    }
    selection = select_build(
        repository, registered, selected=selected_ids, stale_aliases=stale_aliases
    )
    selected_for_drop = set(selection.selected_for_drop)
    selected_for_build = set(selection.selected_for_build)
    removed = set(registered) - selected_ids

    control_target = _control_target(control_lakehouse, targets)
    if all(target.id != control_target.id for target in targets):
        targets = targets + (control_target,)

    stages: list[PlannedStage] = []
    omitted: list[OmittedNode] = []

    catalogue_before = render_catalogue_before_build(
        reconciled_catalogue,
        removed | selected_for_drop,
        control_target=control_target,
    )
    if catalogue_before is not None:
        stages.append(catalogue_before)

    # Alias destinations this build wanted but could not materialise. They must
    # not reach the Registry: a row there means the object's work succeeded, and
    # for these no work was even planned.
    uncertified: set = set()

    for layer in _item_layers(repository, target_by_item):
        layer_stages: list[PlannedStage] = []
        for item in layer:
            planned = plan_item_build(
                repository,
                item=item,
                target=target_by_item[item],
                inventory=inventories[item],
                target_by_item=target_by_item,
                selected_documents=selected_documents,
                selected_aliases=selected_aliases,
                selected_for_drop=selected_for_drop,
                selected_for_build=selected_for_build,
                registered=registered,
            )
            layer_stages.extend(planned.stages)
            omitted.extend(planned.omitted)
            uncertified |= planned.uncertified
        stages.extend(merge_layer_stages(layer_stages))

    stages.extend(
        render_catalogue_after_build(
            repository,
            selected_ids - uncertified,
            target_by_item,
            control_target=control_target,
        )
    )

    sequences, payloads = enumerate_stages(stages)

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
        omitted_nodes=tuple(sorted(omitted, key=lambda node: (node.node_id, node.reason))),
    )
    plan = replace(plan, bundle_id=compute_bundle_id(plan))
    return write_bundle(
        output,
        plan=plan,
        payloads=payloads,
        snapshot=_snapshot(repository, store),
        store=store,
    )


def _item_layers(
    repository: WeaverRepository,
    target_by_item: Mapping[WeaverItemId, object],
) -> tuple[tuple[WeaverItemId, ...], ...]:
    """The bound items, grouped by their repository topological layer.

    Selection is document-based and the bindings are sparse, so an unbound
    producer simply drops out — but the items that remain keep the repository's
    order rather than being re-derived here. That is the point of the repository
    owning the graph: one authoritative ordering, consumed rather than rebuilt.
    """

    layers = repository.item_layers
    if not layers:
        raise BuildError(
            f"repository {repository.name!r} carries no item dependency layers, so "
            "the order its items must be built in is unknown"
        )
    placed = {item for layer in layers for item in layer}
    missing = set(target_by_item) - placed
    if missing:
        raise BuildError(
            "bound item(s) absent from the repository item graph: "
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
    """One item's physical plan: what to do, what was left out, what is uncertified."""

    #: The item's contiguous stages, in the order they must run.
    stages: tuple[PlannedStage, ...]
    #: Nodes this item could not plan, each carrying why.
    omitted: tuple[OmittedNode, ...]
    #: Alias destinations this item could not materialise *and* was asked to
    #: build. Withheld from certification: an alias whose source item is unbound
    #: has no physical form under these bindings, and a Registry row for it would
    #: claim an installation that never happened. One already installed from an
    #: earlier build is left certified — it is still there — so only the
    #: intersection with the build selection is withheld.
    uncertified: frozenset


def plan_item_build(
    repository: WeaverRepository,
    *,
    item: WeaverItemId,
    target,
    inventory: TargetInventory,
    target_by_item,
    selected_documents,
    selected_aliases,
    selected_for_drop,
    selected_for_build,
    registered,
) -> PlannedItem:
    """One item's physical plan, from prepared inputs.

    The seam between deciding *what* to build and arranging a whole bundle. It
    takes a selection that has already been made and an inventory that has
    already been read, and answers only: for this one item against this one
    target, which stages run and in what order.

    Everything above it stays out — item layers, catalogue publication, the
    control-plane target, bundle identity, writing. So a claim about one item's
    prune-then-drop-then-schema-then-build ordering can be made without
    generating a bundle to see it, which is what kept such claims in the
    integration suite.
    """

    aliases = plan_item_aliases(
        repository,
        item=item,
        target=target,
        target_by_item=target_by_item,
        selected=selected_for_build & selected_aliases,
    )
    stages: list[PlannedStage] = []

    # Prune is given every *declared* alias destination, never only the selected
    # ones: an alias this build decided not to touch is still desired state, and
    # a prune that could not see it would delete the very thing incremental
    # selection just chose to keep.
    prune = item_prune_stage(
        repository, selected_documents, item=item, target=target, inventory=inventory
    )
    if prune is not None:
        stages.append(prune)
    stages.extend(
        item_drop_stages(
            repository,
            selected_for_drop - selected_aliases,
            item=item,
            target=target,
            registered=registered,
        )
    )
    schemas = item_schema_stage(
        selected_documents,
        item=item,
        target=target,
        inventory=inventory,
        extra_schemas=aliases.schemas,
    )
    if schemas is not None:
        stages.append(schemas)
    if aliases.stage is not None:
        stages.append(aliases.stage)
    stages.extend(
        item_build_stages(
            repository,
            selected_for_build - selected_aliases,
            item=item,
            target=target,
        )
    )

    refresh = item_refresh_stage(stages, item=item, target=target)
    if refresh is not None:
        stages.append(refresh)
    return PlannedItem(
        stages=tuple(stages),
        omitted=aliases.omitted,
        uncertified=frozenset(aliases.omitted_destinations) & frozenset(selected_for_build),
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
