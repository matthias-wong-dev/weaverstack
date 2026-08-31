"""Repairing a physically damaged consumer from an intact catalogue.

The estate built once and settled. Then one item's physical target was emptied
and the catalogue was left alone, which is what a wipe of a single Warehouse
does. Building that item on its own has to restore it, including the logical
shortcut views that reach a producer the build does not include.

Two bindings answer two different questions, and this is where they part:

.. code-block:: text

    build bindings      the items this build may modify
    _.Installation      where items already are

The producer is absent from the first and present in the second. Requiring it in
the build bindings would mean no downstream item was ever independently
rebuildable.

Pure Python throughout. The post-wipe state is constructed rather than reached by
calling `wipe()`: what is under test is the planner's arithmetic over a complete
catalogue and an empty inventory, and both are values.
"""

from __future__ import annotations

import pathlib

import pytest
from factories import (
    LOAD_CONSUMER,
    LOAD_CONSUMER_TARGET,
    LOAD_PRODUCER,
    LOAD_PRODUCER_TARGET,
    installed_catalogue,
    item_bindings,
    item_id,
    load_estate,
    load_estate_bindings,
    target_inventory,
)
from support.weaver_test import weaver_test
from support.workspaces import WORKSPACE

from weaver.build_bundle import WarehouseBinding, generate_item_build_bundle
from weaver.build_bundle.models import OMIT_TARGET_UNBOUND
from weaver.build_bundle.shortcuts import view_statement
from weaver.catalogue.state import Catalogue
from weaver.declaration.model import WeaverDocumentId
from weaver.errors import BuildError
from weaver.locations import Location
from weaver.store import FilesystemStore
from weaver.targets import ItemRef

#: The consumer's own name for the producer's table, and what a build has to put
#: back. A Warehouse shortcut is a view, so this is `create or alter view`.
SHORTCUT = f"{LOAD_CONSUMER}/Sales.Order"


@pytest.fixture
def settled(tmp_path):
    """The repository and the catalogue a successful whole-estate build left."""

    repository = load_estate(tmp_path / "repo")
    catalogue = installed_catalogue(repository, load_estate_bindings())
    return repository, catalogue


def _consumer_only_bundle(tmp_path, repository, catalogue, *, name="repair"):
    """A build of the consumer alone, against an emptied consumer target."""

    bindings = item_bindings((LOAD_CONSUMER, LOAD_CONSUMER_TARGET))
    return generate_item_build_bundle(
        repository,
        bindings=bindings,
        output=Location(str(tmp_path / name)),
        store=FilesystemStore(),
        target_inventories={
            item_id(LOAD_CONSUMER): target_inventory(
                target_id=bindings.entries[0].to_bound_target().id,
                kind="warehouse",
                target_name=LOAD_CONSUMER_TARGET,
            )
        },
        catalogue=catalogue,
        catalogue_binding=WarehouseBinding(
            ItemRef("Weaver_Control"), workspace_name=WORKSPACE
        ),
    )


def _shortcut_statements(bundle) -> list[str]:
    """Every payload a planned shortcut action names, read from the bundle."""

    root = pathlib.Path(bundle.location.value)
    return [
        (root / action.payload).read_bytes().decode()
        for _sequence, _batch, action in bundle.plan.actions()
        if action.kind == "create_shortcut"
    ]


# --- the state the wipe left ---------------------------------------------------


@weaver_test()
def test_the_catalogue_still_certifies_what_the_target_no_longer_holds(settled):
    """The premise: an intact catalogue over an empty target.

    The Registry row certifies the object as built. The inventory places it
    outside the target. A repair starts from both.
    """

    _repository, catalogue = settled
    shortcut = WeaverDocumentId.parse(SHORTCUT)

    assert shortcut in catalogue.registered
    assert catalogue.registered[shortcut].object_role == "shortcut"
    # And the producer's own binding is still recorded, which is what the repair
    # resolves the source through.
    from weaver.installed import installed_targets

    installed = installed_targets(catalogue)
    assert installed[item_id(LOAD_PRODUCER)].name == LOAD_PRODUCER_TARGET


@weaver_test()
def test_a_missing_shortcut_is_new_and_selected_for_build(tmp_path, settled):
    """Signature agrees, inventory says absent, so the object is new.

    The existing reconciliation, asserted here because the repair rests on it.
    """

    repository, catalogue = settled
    bundle = _consumer_only_bundle(tmp_path, repository, catalogue)
    shortcut = WeaverDocumentId.parse(SHORTCUT)
    selection = bundle.plan.selection

    assert shortcut in selection.impact.new
    assert shortcut in selection.selected_for_build


# --- resolving the producer the build does not include -------------------------


@weaver_test()
def test_the_shortcut_resolves_its_producer_through_installation(tmp_path, settled):
    """
    Intent: A consumer-only build materialises a logical shortcut whose producer
    is already installed and not part of this build.

    Proof: the generated view names the producer's installed physical target.
    Resolved through the build's bindings alone the producer looked unbound, and
    the build refused the estate it was asked to repair.
    """

    repository, catalogue = settled
    bundle = _consumer_only_bundle(tmp_path, repository, catalogue)

    statements = _shortcut_statements(bundle)
    assert statements, "the repair has to plan the shortcut it is missing"
    body = "\n".join(statements)

    assert "create or alter view" in body.casefold()
    # The producer's installed target, which this build never bound.
    assert f"[{LOAD_PRODUCER_TARGET}]" in body
    assert "[Sales].[Order]" in body or "[Sales].[Order]" in body


@weaver_test()
def test_the_producer_is_not_a_binding_of_this_build(tmp_path, settled):
    """The distinction the repair rests on, stated directly."""

    repository, catalogue = settled
    bundle = _consumer_only_bundle(tmp_path, repository, catalogue)

    # Declared, so the installer can resolve the frozen source by target id.
    declared = {target.item_name for target in bundle.plan.targets}
    assert LOAD_PRODUCER_TARGET in declared

    # And no batch writes to it: this build modifies the consumer alone.
    written_to = {
        batch.target_id for _sequence, batch, _action in bundle.plan.actions()
    }
    producer_id = next(
        target.id
        for target in bundle.plan.targets
        if target.item_name == LOAD_PRODUCER_TARGET
    )
    assert producer_id not in written_to


@weaver_test()
def test_no_node_is_omitted_as_unbound(tmp_path, settled):
    """The failure this repairs, named by its reason."""

    repository, catalogue = settled
    bundle = _consumer_only_bundle(tmp_path, repository, catalogue)

    # The producer's own documents are omitted as unbound, which is what a
    # scoped build does with an item it was not pointed at. What must not be
    # omitted is the consumer's shortcut, whose source is installed.
    omitted = {node.node_id for node in bundle.plan.omitted_nodes}
    assert f"shortcut:{SHORTCUT}" not in omitted
    assert not [
        node
        for node in bundle.plan.omitted_nodes
        if node.reason == OMIT_TARGET_UNBOUND and LOAD_CONSUMER in node.node_id
    ]
    # What remains belongs to items this build was not pointed at, which is the
    # ordinary scoping and not a failure to resolve anything.
    assert all(
        not node.node_id.startswith(LOAD_CONSUMER)
        for node in bundle.plan.omitted_nodes
        if node.reason == OMIT_TARGET_UNBOUND
    )


# --- an unbound source ---------------------------------------------------------


@weaver_test()
def test_a_producer_neither_selected_nor_installed_is_still_unbound(tmp_path, settled):
    """The protection this must not weaken.

    With no build binding and no installation row, nothing says where the source
    is. The build raises, and no address is supplied for it.
    """

    repository, catalogue = settled
    without_producer = Catalogue(
        rows={
            item: tables
            for item, tables in catalogue.rows.items()
            if item != item_id(LOAD_PRODUCER)
        }
    )

    with pytest.raises(BuildError, match="is not bound"):
        _consumer_only_bundle(tmp_path, repository, without_producer, name="unbound")


@weaver_test()
def test_the_statement_is_the_ordinary_view_shortcut(settled):
    """One implementation of what a view shortcut is, asserted at its source."""

    repository, catalogue = settled
    from weaver.build_bundle.planner import installed_shortcut_sources

    declaration = next(
        one for one in repository.shortcuts if one.owner == item_id(LOAD_CONSUMER)
    )
    sources = installed_shortcut_sources(
        repository,
        catalogue,
        build_bindings={item_id(LOAD_CONSUMER): None},
        workspace_of=load_estate_bindings().entries[0].to_bound_target(),
    )
    source_target = sources[item_id(LOAD_PRODUCER)]
    logical = {pair.destination: pair.source for pair in repository.logical_shortcuts}

    statement = view_statement(declaration, source_target, logical)

    assert statement.startswith("create or alter view")
    assert f"[{LOAD_PRODUCER_TARGET}]" in statement
