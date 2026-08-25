"""Orchestrate one item-oriented repository into a coordinated build bundle.

A build is planned as an ordered series of **item** builds. The repository owns
an acyclic item dependency graph and its topological layers; this walks those
layers and plans each item as one coherent group of stages:

.. code-block:: text

    catalogue claim removal, when required

    item layer 0
        producer item A          prune, drops, schemas, shortcuts, documents, refresh
        independent producer B   prune, drops, schemas, shortcuts, documents, refresh
    item layer 1
        consumer item C          prune, drops, schemas, shortcuts, documents, refresh

    final batched catalogue publication

Items in the same layer share their barriers, one batch each, because nothing
orders them against each other. Items in different layers never do: a consumer's
shortcuts and documents cannot begin until every item it reaches into has
finished, endpoint included.

Inside an item the document dependency graph decides everything; the item graph
is the outer boundary rather than a replacement.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Mapping

from ..catalogue.claims import without_claims
from ..catalogue.state import Catalogue
from ..declaration.model import WeaverItemId, WeaverRepository
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
)
from .endpoints import item_refresh_stage
from .incremental import select_build, stale_shortcut_destinations
from .models import OMIT_TARGET_UNBOUND, BuildPlan, OmittedNode
from .physical import (
    item_build_stages,
    item_drop_stages,
    item_load_removals,
    item_load_stages,
    item_prune_stage,
    item_schema_stage,
)
from .prune import TargetInventory
from .runtime_tables import (
    render_runtime_state_reconciliation,
    runtime_state_invalidation,
)
from .shortcuts import plan_item_shortcuts
from .stages import PlannedStage, enumerate_stages, merge_layer_stages
from .targets import WAREHOUSE_TARGET, ItemBindings, WarehouseBinding


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
    """Freeze the one incremental build model into an installable bundle."""

    if catalogue_binding is None:
        raise BuildError("every build needs an explicit catalogue Warehouse")
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

    # Four kinds of node are selectable, and most of what follows needs one of
    # them. Documents drive prune, schemas and the physical build pipelines.
    # Shortcut destinations are registered objects, so they take part in selection
    # and certification, but the shortcut executor materialises them. Load
    # artefacts are signed from their own content and installed by the item's
    # final layer, so they stay out of anything assuming a parsed declaration.
    #
    # Validations are entirely logical: selected so their catalogue rows
    # publish, and never reaching a physical stage, because nothing is
    # materialised under a Test ID. What one compiles to is a runtime artefact
    # with an identity of its own.
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

    # Freshness is read before ``registered`` is narrowed, because the whole
    # point is to compare against an item this build does *not* include.
    stale_shortcuts = stale_shortcut_destinations(
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
        stale_shortcuts=stale_shortcuts,
        inventories=inventories,
    )
    selected_for_drop = set(selection.selected_for_drop)
    selected_for_build = set(selection.selected_for_build)
    removed = set(registered) - selected_ids

    catalogue_target = _catalogue_target(catalogue_binding, targets)
    if all(target.id != catalogue_target.id for target in targets):
        targets = targets + (catalogue_target,)
    from ..catalogue.builtin import BUILTIN_ITEM

    shortcut_target_by_item = dict(target_by_item)
    shortcut_target_by_item.setdefault(BUILTIN_ITEM, catalogue_target)

    stages: list[PlannedStage] = []
    omitted: list[OmittedNode] = []

    # Collected once and used twice. These rows are deleted before any physical
    # work, so publication compares against the catalogue without them — an
    # object dropped and rebuilt whose projection did not change would otherwise
    # compare equal, produce no merge, and stay deleted.
    deleted_claims = collect_claims(
        catalogue, removed | selected_for_drop, stale_claims=stale_claims
    )
    catalogue_after_deletions = without_claims(catalogue, deleted_claims)

    catalogue_before = render_catalogue_before_build(
        catalogue,
        removed | selected_for_drop,
        catalogue_target=catalogue_target,
        stale_claims=stale_claims,
    )
    if catalogue_before is not None:
        stages.append(catalogue_before)

    # Current state is invalidated here, between decertification and the first
    # physical action, and never after it — see
    # :mod:`weaver.build_bundle.runtime_tables`. Against the catalogue this build
    # read: which rows are obsolete is arithmetic over rows it holds, and a build
    # creating the tables read none.
    runtime_state = runtime_state_invalidation(
        repository,
        items=tuple(target_by_item),
        selected_for_build=selected_for_build,
        catalogue=catalogue,
    )
    reconciliation = render_runtime_state_reconciliation(
        runtime_state, catalogue_target=catalogue_target
    )
    if reconciliation is not None:
        stages.append(reconciliation)

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
            )
            layer_stages.extend(planned.stages)
            omitted.extend(planned.omitted)
        stages.extend(merge_layer_stages(layer_stages))

    _refuse_selected_omissions(omitted)

    stages.extend(
        render_catalogue_after_build(
            repository,
            selected_ids,
            target_by_item,
            catalogue_target=catalogue_target,
            # The catalogue as the claim deletions above will leave it, not as
            # it was read — see `without_claims`.
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
    )
    plan = replace(plan, bundle_id=compute_bundle_id(plan))
    return write_bundle(
        output,
        plan=plan,
        payloads=payloads,
        store=store,
    )


def _refuse_selected_omissions(omitted: list[OmittedNode]) -> None:
    """Fail when a bound item's selected object has no materialisation plan."""

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
    """The four selectable kinds, separately — see the comment at the call site."""

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
    """Every object a build of these items could certify.

    Everything a bound item owns, whatever this build decides to do about it:
    an object left alone because nothing changed is still certified. What a
    build did is narrowed later, by the planner's uncertified set.

    Shares ``_selectable`` with the planner rather than re-deriving the same
    three sets, so the two cannot disagree about what a build certifies.
    """

    documents, shortcuts, loads, validations = _selectable(repository, by_item)
    return documents | shortcuts | loads | validations


def _item_layers(
    repository: WeaverRepository,
    target_by_item: Mapping[WeaverItemId, object],
) -> tuple[tuple[WeaverItemId, ...], ...]:
    """Group bound items by their repository topological layer."""

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
    """One item's physical plan and any selected nodes it could not plan."""

    #: The item's contiguous stages, in the order they must run.
    stages: tuple[PlannedStage, ...]
    #: Selected nodes this bound item could not plan, each carrying why. The
    #: bundle planner refuses any such result before writing a bundle.
    omitted: tuple[OmittedNode, ...]
    #: Shortcut destinations represented by ``omitted``. This preserves the item
    #: planning seam's complete result even though whole-bundle generation fails.
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
) -> PlannedItem:
    """One item's physical plan, from prepared inputs.

    The seam between deciding what to build and arranging a bundle: given a
    selection already made and an inventory already read, which stages run for
    this item against this target, and in what order.

    Item layers, catalogue publication, the control-plane target, bundle
    identity and writing all stay above it, so an ordering claim can be made
    without generating a bundle.
    """

    shortcuts = plan_item_shortcuts(
        repository,
        item=item,
        target=target,
        target_by_item=target_by_item,
        selected=selected_for_build & selected_shortcuts,
        sources=shortcut_sources,
    )
    artefacts = item_runtime_artefacts(
        repository,
        item=item,
        # The load layer installs these, so their bodies are rendered here
        # against the target this item is bound to. A Warehouse names its
        # objects over TDS and has no Spark destination.
        destination=None if target.kind == WAREHOUSE_TARGET else target.spark_target,
    )
    stages: list[PlannedStage] = []

    # Prune is given every *declared* shortcut destination, never only the selected
    # ones: a shortcut this build decided not to touch is still desired state, and
    # a prune that could not see it would delete the very thing incremental
    # selection just chose to keep. Load artefacts are treated the same way, and
    # the stage derives them itself.
    prune = item_prune_stage(
        repository,
        selected_documents,
        item=item,
        target=target,
        inventory=inventory,
    )
    if prune is not None:
        stages.append(prune)
    stages.extend(
        item_drop_stages(
            repository,
            selected_for_drop - selected_shortcuts,
            item=item,
            target=target,
            inventory=inventory,
        )
    )
    schemas = item_schema_stage(
        selected_documents,
        item=item,
        target=target,
        inventory=inventory,
        # `_` is where a Warehouse's generated load procedures live, and no
        # document declares an object in it — so like a shortcut's namespace it
        # would never be created if only documents were consulted. It is derived
        # from the artefacts, so an item with no procedures asks for no schema.
        extra_schemas=tuple(shortcuts.schemas) + load_schemas(artefacts),
    )
    if schemas is not None:
        stages.append(schemas)
    if shortcuts.stage is not None:
        stages.append(shortcuts.stage)
    stages.extend(
        item_build_stages(
            repository,
            selected_for_build - selected_shortcuts,
            item=item,
            target=target,
        )
    )

    refresh = item_refresh_stage(stages, item=item, target=target)
    if refresh is not None:
        stages.append(refresh)

    # The load layer closes the item, after its structure is built and its
    # endpoint has caught up. Removals ride in it too: they come from the
    # previous Registry rows rather than from any diff against the target, so
    # they need no earlier barrier to be safe.
    stages.extend(item_load_stages(artefacts, selected_loads, item=item, target=target))
    stages.extend(
        item_load_removals(removed, item=item, target=target, registered=registered)
    )
    return PlannedItem(
        stages=tuple(stages),
        omitted=shortcuts.omitted,
        uncertified=frozenset(shortcuts.omitted_destinations)
        & frozenset(selected_for_build),
    )


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
