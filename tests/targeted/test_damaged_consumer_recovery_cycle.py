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


# --- the repair ----------------------------------------------------------------


@weaver_test()
def test_a_consumer_only_build_restores_a_shortcut_over_an_installed_producer(
    tmp_path, settled
):
    """The whole claim, from the damaged premise to the statement it generates.

    The Registry row certifies the shortcut as built and the inventory places it
    outside the target, so it reads as new. Its producer is bound by nothing in
    this build, and the view names the producer's installed physical target.
    """

    repository, catalogue = settled
    shortcut = WeaverDocumentId.parse(SHORTCUT)
    assert catalogue.registered[shortcut].object_role == "shortcut"

    bundle = _consumer_only_bundle(tmp_path, repository, catalogue)
    selection = bundle.plan.selection

    assert shortcut in selection.impact.new
    assert shortcut in selection.selected_for_build

    body = "\n".join(_shortcut_statements(bundle))
    assert "create or alter view" in body.casefold()
    assert f"[{LOAD_PRODUCER_TARGET}]" in body
    assert "[Sales].[Order]" in body


@weaver_test()
def test_the_installed_producer_is_declared_and_never_written_to(tmp_path, settled):
    """Referenceable, not writable. The distinction the repair rests on.

    The producer is declared among the plan's targets, because the installer
    resolves a frozen source by target id. No batch names it, because this build
    modifies the consumer alone.
    """

    repository, catalogue = settled
    bundle = _consumer_only_bundle(tmp_path, repository, catalogue)

    producer = next(
        target
        for target in bundle.plan.targets
        if target.item_name == LOAD_PRODUCER_TARGET
    )
    written_to = {
        batch.target_id for _sequence, batch, _action in bundle.plan.actions()
    }

    assert producer.id not in written_to
    # And the consumer's shortcut is not set aside for want of a binding.
    assert not [
        node
        for node in bundle.plan.omitted_nodes
        if node.reason == OMIT_TARGET_UNBOUND and LOAD_CONSUMER in node.node_id
    ]


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
