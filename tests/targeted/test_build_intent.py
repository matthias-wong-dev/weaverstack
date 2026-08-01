"""The build's declared effect, held to the actions that bring it about.

A plan says what will run; `target_changes` says what it will mean. Written side
by side because inferring the second from the first would put a model of executor
semantics somewhere no executor could correct it — but a summary the planner
writes about its own plan proves nothing on its own.

This is what makes it prove something. Every physical action must be named by a
change and every change must name a real action, so the two cannot drift apart
without failing here. Adding an artefact type means emitting an action *and* a
change; forget either and the correspondence breaks.

Per item type, because the two physical sides emit different actions and a
Lakehouse-only check would not see a procedure or a Warehouse schema at all.
"""

from __future__ import annotations

import pytest
from factories import (
    ITEM,
    WAREHOUSE_ITEM,
    FixtureInventory,
    estate_bindings,
    estate_inventories,
    full_estate,
    item_id,
    target_inventory,
)

from weaver import ItemRef, LocalStore, Location
from weaver.build_bundle import LakehouseBinding, generate_item_build_bundle
from weaver.build_bundle.changes import ADD, OBJECT_KINDS, REMOVE, TargetChange
from weaver.catalogue.state import Catalogue

#: Actions that change a target. Anything here must be accounted for by a
#: change; anything not here is either catalogue work, which does not touch the
#: estate, or a refresh, which publishes rather than alters.
PHYSICAL_KINDS = frozenset(
    {
        "create_schema",
        "create_alias",
        "build_folder",
        "build_table",
        "build_view",
        "drop_folder",
        "drop_table",
        "drop_view",
        "prune_table",
        "prune_view",
        "prune_schema",
        "prune_folder",
        "write_file",
        "build_procedure",
        "delete_file",
        "drop_procedure",
    }
)


@pytest.fixture
def repository(tmp_path):
    return full_estate(tmp_path / "repo")


def build(repository, tmp_path, *, inventories=None, catalogue=None):
    """A build against whatever prior state a test wants to describe."""

    return generate_item_build_bundle(
        repository,
        bindings=estate_bindings(),
        output=Location(str(tmp_path / "bundle")),
        store=LocalStore(),
        target_inventories=inventories
        if inventories is not None
        else estate_inventories(repository, empty=True),
        catalogue=catalogue if catalogue is not None else Catalogue({}),
        control_lakehouse=LakehouseBinding(ItemRef("Weaver_Control")),
    )


def target_id_of(item: str) -> str:
    bound = {b.item: b.to_bound_target() for b in estate_bindings().entries}
    return bound[item_id(item)].id


def actions_on(plan, target_id: str):
    return [
        action
        for _sequence, batch, action in plan.actions()
        if batch.target_id == target_id and action.kind in PHYSICAL_KINDS
    ]


# --- the correspondence -------------------------------------------------------


@pytest.mark.parametrize("item", [ITEM, WAREHOUSE_ITEM])
def test_every_action_that_touches_a_target_is_declared(repository, tmp_path, item):
    """The guard that makes the summary worth writing.

    Not a strict pairing: one alias action stands for every alias an item
    consumes, so it declares several changes. What must hold is that no action
    changes a target without saying so.
    """

    plan = build(repository, tmp_path).plan
    target_id = target_id_of(item)

    declared = {change.action_id for change in plan.target_changes.get(target_id, ())}
    performed = {action.id for action in actions_on(plan, target_id)}

    assert performed, f"{item} planned no physical work to check"
    assert performed - declared == set(), sorted(performed - declared)


@pytest.mark.parametrize("item", [ITEM, WAREHOUSE_ITEM])
def test_every_declared_change_names_an_action_that_runs(repository, tmp_path, item):
    """The other direction, and the one that catches a summary of good intentions.

    A change naming no action is a claim about state that nothing will bring
    about — which is precisely how a self-certifying summary would look.
    """

    plan = build(repository, tmp_path).plan
    target_id = target_id_of(item)

    declared = {change.action_id for change in plan.target_changes.get(target_id, ())}
    performed = {action.id for action in actions_on(plan, target_id)}

    assert declared - performed == set(), sorted(declared - performed)


def test_no_change_is_attributed_to_the_wrong_target(repository, tmp_path):
    """A change lands in the section of the target its action runs against.

    Worth stating because the sections are keyed by target id and merged across
    stages and item layers: a fold that lost the key would produce a plausible
    summary describing the wrong estate.
    """

    plan = build(repository, tmp_path).plan
    by_action = {
        action.id: batch.target_id for _s, batch, action in plan.actions()
    }

    for target_id, changes in plan.target_changes.items():
        for change in changes:
            assert by_action[change.action_id] == target_id, change


def test_both_item_types_declare_something(repository, tmp_path):
    """Guard the parametrisation: an empty section would satisfy both directions.

    If a target planned no work at all, the correspondence above would hold
    vacuously and the test would be about nothing.
    """

    plan = build(repository, tmp_path).plan

    for item in (ITEM, WAREHOUSE_ITEM):
        assert plan.target_changes.get(target_id_of(item)), item


def test_a_warehouse_declares_the_kinds_only_it_has(repository, tmp_path):
    """The reason this is parametrised rather than run once.

    A procedure and the `_` schema exist on one side only. A Lakehouse-only check
    would pass while nothing at all was declared about them.
    """

    plan = build(repository, tmp_path).plan
    changes = plan.target_changes[target_id_of(WAREHOUSE_ITEM)]

    assert any(
        change.object_kind == "stored_procedure" and change.effect == ADD
        for change in changes
    )
    assert any(
        change.object_kind == "schema" and change.name == "_" for change in changes
    )


def test_a_lakehouse_declares_the_kinds_only_it_has(repository, tmp_path):
    plan = build(repository, tmp_path).plan
    changes = plan.target_changes[target_id_of(ITEM)]

    assert any(change.object_kind == "file" for change in changes)
    assert any(change.object_kind == "folder" for change in changes)


# --- the summary as a record --------------------------------------------------


def test_the_summary_travels_in_the_manifest(repository, tmp_path):
    """Inside the hashed plan, not beside it.

    A sibling file outside the bundle identity could be edited after
    certification, which is the thing frozen payloads exist to prevent — so the
    summary a reviewer reads is the summary the installation was certified with,
    and it survives the round trip that proves it.
    """

    from weaver.build_bundle import BuildPlan

    plan = build(repository, tmp_path).plan
    restored = BuildPlan.from_mapping(plan.to_mapping())

    assert restored.target_changes == plan.target_changes
    assert "target_changes" in plan.to_mapping()


def test_the_summary_is_part_of_bundle_identity(repository, tmp_path):
    """Changing what a build claims it will do changes what the build *is*."""

    from dataclasses import replace

    from weaver.build_bundle import compute_bundle_id

    plan = build(repository, tmp_path).plan
    target_id = target_id_of(ITEM)
    tampered = replace(
        plan,
        target_changes={
            **plan.target_changes,
            target_id: plan.target_changes[target_id][:-1],
        },
    )

    assert compute_bundle_id(tampered) != compute_bundle_id(plan)


def test_a_declared_change_is_a_shape_the_inventory_can_hold(repository, tmp_path):
    """Every kind names a collection a real inventory reports.

    A change of a kind nothing could apply would be a summary that reads well and
    predicts nothing, which is the failure mode hardest to see by eye.
    """

    plan = build(repository, tmp_path).plan
    empty = target_inventory()

    for changes in plan.target_changes.values():
        for change in changes:
            assert change.object_kind in OBJECT_KINDS
            assert change.effect in (ADD, REMOVE)
            assert hasattr(empty, _COLLECTION_FOR[change.object_kind])


_COLLECTION_FOR = {
    "schema": "schemas",
    "table": "tables",
    "view": "views",
    "folder": "folders",
    "folder_schema": "folder_schemas",
    "file": "files",
    "stored_procedure": "procedures",
}
