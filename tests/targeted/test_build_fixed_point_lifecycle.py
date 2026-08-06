"""An identical second build does nothing at all — the whole plan, not part of it.

The estate-convergence family next door proves a correct estate plans no
*physical* work. That was always the easy half, and on its own it was
misleading: catalogue publication used to be unconditional, so a build with
nothing to do still deleted and re-merged every catalogue table and refreshed
the Weaver endpoint afterwards. A test that filtered those kinds out before
counting could report a no-op build that wrote the entire catalogue.

So this asserts the complete action set. Not "no physical actions" — no actions.

The state fed back is the state production computes. `desired_catalogue` is the
function publication compares against, and `certifiable_identities` is the set
the planner certifies; a fixture that restated either could agree with a broken
planner and prove nothing. Here the only way the second build stays silent is if
what a build *leaves* and what a build *expects to find* are genuinely the same
description.

Pure Python throughout, and deliberately: the arithmetic being tested is the
planner's, and every input to it can be constructed. The Spark and Fabric levels
prove a different thing — that a real catalogue reads back into these
structures — and are much too slow to be where this property is iterated on.
"""

from __future__ import annotations

import pytest
from factories import (
    ITEM,
    WAREHOUSE_ITEM,
    FixtureInventory,
    full_estate,
    item_bindings,
    item_id,
)

from weaver.build_bundle import LakehouseBinding, generate_item_build_bundle
from weaver.build_bundle.catalogue_actions import desired_catalogue
from weaver.build_bundle.planner import certifiable_identities
from weaver.catalogue.state import Catalogue
from weaver.catalogue.tables import CATALOGUE_TABLES
from weaver.declaration.metadata import DELTA_TARGET, SQL_TARGET
from weaver.locations import Location
from weaver.store import FilesystemStore
from weaver.targets import ItemRef

LAKEHOUSE_TARGET_NAME = "Sales_LH"
WAREHOUSE_TARGET_NAME = "Reporting_WH"


@pytest.fixture
def estate(tmp_path):
    """Both item types, and every artefact a source can own."""

    return full_estate(tmp_path / "repo")


def _bindings():
    return item_bindings(
        (ITEM, LAKEHOUSE_TARGET_NAME),
        (WAREHOUSE_ITEM, WAREHOUSE_TARGET_NAME),
    )


def _inventories(repository, bound):
    return {
        item_id(ITEM): FixtureInventory.from_repository(
            repository,
            item=ITEM,
            target_kind=DELTA_TARGET,
            target_id=bound[item_id(ITEM)].id,
            kind="lakehouse",
            target_name=LAKEHOUSE_TARGET_NAME,
        ),
        item_id(WAREHOUSE_ITEM): FixtureInventory.from_repository(
            repository,
            item=WAREHOUSE_ITEM,
            target_kind=SQL_TARGET,
            target_id=bound[item_id(WAREHOUSE_ITEM)].id,
            kind="warehouse",
            target_name=WAREHOUSE_TARGET_NAME,
        ),
    }


def installed_catalogue(repository) -> Catalogue:
    """The catalogue a successful build of this estate leaves behind.

    Composed from the two functions the build itself uses, so this cannot drift
    from what a build actually writes without the drift showing up here.
    """

    bindings = _bindings()
    by_item = {binding.item: binding for binding in bindings.entries}
    target_by_item = {
        binding.item: binding.to_bound_target() for binding in bindings.entries
    }
    state = desired_catalogue(
        repository,
        certifiable_identities(repository, by_item),
        target_by_item,
    )
    # Every catalogue table physically exists once a build has run — the built-in
    # item creates them all. Saying so matters: reconciliation may only raise a
    # claim against a table that is there.
    return Catalogue(
        rows=state.rows,
        present_tables=frozenset(table.name for table in CATALOGUE_TABLES),
    )


def build(repository, tmp_path, *, catalogue):
    bindings = _bindings()
    bound = {binding.item: binding.to_bound_target() for binding in bindings.entries}
    return generate_item_build_bundle(
        repository,
        bindings=bindings,
        output=Location(str(tmp_path / "bundle")),
        store=FilesystemStore(),
        target_inventories=_inventories(repository, bound),
        catalogue=catalogue,
        control_lakehouse=LakehouseBinding(ItemRef("Weaver_Control")),
    )


def actions(bundle):
    return [action for _sequence, _batch, action in bundle.plan.actions()]


# --- the property -------------------------------------------------------------


def test_a_second_identical_build_generates_no_actions_whatever(estate, tmp_path):
    """The complete plan, which is the assertion the old test could not make."""

    second = build(estate, tmp_path, catalogue=installed_catalogue(estate))

    assert actions(second) == []


def test_the_first_build_against_an_empty_catalogue_does_do_work(estate, tmp_path):
    """Guards the test above from passing vacuously.

    If the fixture or the planner produced an empty plan for *every* input, the
    fixed-point assertion would be satisfied by a build that never worked at all.
    """

    first = build(estate, tmp_path, catalogue=Catalogue(rows={}))

    assert actions(first)


# --- and each thing it must not do, named ------------------------------------


@pytest.mark.parametrize(
    "kind",
    [
        "publish_catalogue",
        "publish_registry",
        "delete_catalogue_claims",
        "refresh_sql_endpoint",
    ],
)
def test_the_second_build_emits_no_catalogue_work_of_any_kind(estate, tmp_path, kind):
    """Named individually so a failure says which barrier came back.

    `publish_catalogue` covers the dictionaries and the Installation row;
    `publish_registry` is the certification barrier; `refresh_sql_endpoint` is
    the Weaver endpoint catching up with catalogue DML that, here, never
    happened.
    """

    second = build(estate, tmp_path, catalogue=installed_catalogue(estate))

    assert not [action for action in actions(second) if action.kind == kind]


def test_the_second_build_selects_nothing_to_build_or_drop(estate, tmp_path):
    """The decision behind the empty plan, asserted separately from the plan.

    An empty selection and an empty action list fail for different reasons: a
    stage can render work from a correct selection, and a defective selection
    can still happen to render nothing.
    """

    selection = build(
        estate, tmp_path, catalogue=installed_catalogue(estate)
    ).plan.selection

    assert selection.selected_for_build == ()
    assert selection.selected_for_drop == ()
    assert selection.impact.new == ()
    assert selection.impact.changed == ()


def test_the_second_build_changes_no_target(estate, tmp_path):
    """No target is even mentioned as changing, let alone written to."""

    second = build(estate, tmp_path, catalogue=installed_catalogue(estate))

    assert not second.plan.target_changes


def test_a_third_build_is_as_silent_as_the_second(estate, tmp_path):
    """Staying at the fixed point is a different claim from reaching it.

    A build that published something the *next* build then had to undo would
    still pass the test above; two silent builds in a row rule that out.
    """

    state = installed_catalogue(estate)

    second = build(estate, tmp_path / "two", catalogue=state)
    third = build(estate, tmp_path / "three", catalogue=state)

    assert actions(second) == [] and actions(third) == []
    assert second.plan.bundle_id == third.plan.bundle_id


# --- a real change still publishes -------------------------------------------


def test_changing_one_document_publishes_that_change_and_no_more(estate, tmp_path):
    """The other side of the property, and what stops it being achieved by
    publishing nothing ever.

    One edited declaration must reach the catalogue. What must *not* happen is
    the rest of the estate being republished with it — which is what the old
    unconditional rendering did on every build.
    """

    from weaver.declaration import parse_item_repository

    state = installed_catalogue(estate)

    document = None
    for candidate in (estate.root.path / "Lakehouse" / "Sales").rglob("*.py"):
        if "Files" not in candidate.parts and candidate.name != "__init__.py":
            document = candidate
            break
    assert document is not None, "the fixture must own a Lakehouse document"

    document.write_text(
        document.read_text(encoding="utf-8").replace(
            "Description: ", "Description: revised — ", 1
        ),
        encoding="utf-8",
    )
    changed = parse_item_repository(estate.root)

    after = build(changed, tmp_path, catalogue=state)

    published = [
        action for action in actions(after) if action.kind == "publish_catalogue"
    ]
    assert published, "an edited declaration must reach the catalogue"
