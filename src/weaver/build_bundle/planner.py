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

Items in the same layer share their barriers, one batch each, because nothing
orders them against each other. Items in different layers never do: a consumer's
aliases and documents cannot begin until every item it reaches into has
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
from .aliases import plan_item_aliases
from .bundle import SUPPORTED_FORMAT_VERSION, BuildBundle, compute_bundle_id, write_bundle
from .catalogue_actions import (
    collect_claims,
    render_catalogue_after_build,
    render_catalogue_before_build,
)
from .endpoints import item_refresh_stage
from .incremental import select_build, stale_alias_destinations
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
from .stages import PlannedStage, enumerate_stages, merge_layer_stages
from .targets import WAREHOUSE_TARGET, ItemBindings, LakehouseBinding


def generate_item_build_bundle(
    repository: WeaverRepository,
    *,
    bindings: ItemBindings,
    output: Location,
    store: Store,
    target_inventories: Mapping[WeaverItemId, TargetInventory] | None = None,
    catalogue: Catalogue,
    stale_claims: tuple = (),
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

    # Four kinds of node are selectable, and most of what follows needs one of
    # them. Documents drive prune, schemas and the physical build pipelines.
    # Alias destinations are registered objects, so they take part in selection
    # and certification, but the alias executor materialises them. Load
    # artefacts are signed from their own content and installed by the item's
    # final layer, so they stay out of anything assuming a parsed declaration.
    #
    # Validations are entirely logical: selected so their catalogue rows
    # publish, and never reaching a physical stage, because nothing is
    # materialised under a Test ID. What one compiles to is a runtime artefact
    # with an identity of its own.
    (
        selected_documents,
        selected_aliases,
        selected_loads,
        selected_validations,
    ) = _selectable(repository, by_item)
    selected_ids = (
        selected_documents | selected_aliases | selected_loads | selected_validations
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
    stale_aliases = stale_alias_destinations(
        repository, catalogue.registered, bound_items=by_item
    )
    registered = {
        identity: document
        for identity, document in catalogue.registered.items()
        if identity.item in by_item
    }
    selection = select_build(
        repository, registered, selected=selected_ids, stale_aliases=stale_aliases
    )
    selected_for_drop = set(selection.selected_for_drop)
    selected_for_build = set(selection.selected_for_build)
    removed = set(registered) - selected_ids

    control_target = _control_target(control_lakehouse, targets)
    control_destination = control_target.spark_target
    if all(target.id != control_target.id for target in targets):
        targets = targets + (control_target,)

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
        control_target=control_target,
        control_destination=control_destination,
        stale_claims=stale_claims,
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
                selected_for_drop=selected_for_drop - selected_loads - selected_validations,
                selected_for_build=selected_for_build - selected_loads - selected_validations,
                selected_loads=selected_for_build & selected_loads,
                removed=removed,
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
            control_destination=control_destination,
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
        omitted_nodes=tuple(sorted(omitted, key=lambda node: (node.node_id, node.reason))),
        target_changes=target_changes,
    )
    plan = replace(plan, bundle_id=compute_bundle_id(plan))
    return write_bundle(
        output,
        plan=plan,
        payloads=payloads,
        store=store,
    )


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
            alias.destination
            for alias in repository.aliases
            if alias.destination.item in by_item
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

    documents, aliases, loads, validations = _selectable(repository, by_item)
    return documents | aliases | loads | validations


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
    selected_loads=(),
    removed=(),
) -> PlannedItem:
    """One item's physical plan, from prepared inputs.

    The seam between deciding what to build and arranging a bundle: given a
    selection already made and an inventory already read, which stages run for
    this item against this target, and in what order.

    Item layers, catalogue publication, the control-plane target, bundle
    identity and writing all stay above it, so an ordering claim can be made
    without generating a bundle.
    """

    aliases = plan_item_aliases(
        repository,
        item=item,
        target=target,
        target_by_item=target_by_item,
        selected=selected_for_build & selected_aliases,
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

    # Prune is given every *declared* alias destination, never only the selected
    # ones: an alias this build decided not to touch is still desired state, and
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
        # `_` is where a Warehouse's generated load procedures live, and no
        # document declares an object in it — so like an alias's namespace it
        # would never be created if only documents were consulted. It is derived
        # from the artefacts, so an item with no procedures asks for no schema.
        extra_schemas=tuple(aliases.schemas) + load_schemas(artefacts),
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

    # The load layer closes the item, after its structure is built and its
    # endpoint has caught up. Removals ride in it too: they come from the
    # previous Registry rows rather than from any diff against the target, so
    # they need no earlier barrier to be safe.
    stages.extend(
        item_load_stages(artefacts, selected_loads, item=item, target=target)
    )
    stages.extend(
        item_load_removals(removed, item=item, target=target, registered=registered)
    )
    return PlannedItem(
        stages=tuple(stages),
        omitted=aliases.omitted,
        uncertified=frozenset(aliases.omitted_destinations) & frozenset(selected_for_build),
    )


def _control_target(binding: LakehouseBinding, targets):
    physical = binding.to_bound_target()
    for target in targets:
        if target.kind == physical.kind and target.item_id == physical.item_id:
            return target
    return replace(physical, id=f"control-{physical.id}")
