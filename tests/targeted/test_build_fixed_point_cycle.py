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

from dataclasses import replace

import pytest
from factories import (
    ITEM,
    WAREHOUSE_ITEM,
    FixtureInventory,
    full_estate,
    item_bindings,
    item_id,
)
from support.weaver_test import weaver_test
from support.workspaces import WORKSPACE

from weaver.build_bundle import (
    WarehouseBinding,
    generate_item_build_bundle,
)
from weaver.build_bundle.catalogue_actions import desired_catalogue
from weaver.build_bundle.planner import certifiable_identities
from weaver.build_bundle.shortcuts import ResolvedShortcutSource
from weaver.catalogue.state import Catalogue
from weaver.catalogue.tables import PRESENTED_RUNTIME_TABLES, PROJECTED_TABLES
from weaver.declaration.metadata import DELTA_TARGET, SQL_TARGET
from weaver.locations import Location
from weaver.spark import FabricSparkTarget
from weaver.store import FilesystemStore
from weaver.targets import ItemRef

WEAVER = FabricSparkTarget(workspace="Demo", lakehouse="Weaver")

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
        materialised=frozenset(table.name for table in PROJECTED_TABLES),
    )


#: Where each of the catalogue's runtime tables sits, as a build resolves them
#: before planning. Supplied here because the references a built target is given
#: are stages like any other, and a fixed point that never planned them would be
#: a fixed point over a smaller build than the one that runs.
RUNTIME_SOURCES = {
    table.name: ResolvedShortcutSource(
        workspace_id="ws-1",
        item_id="item-1",
        item_name="Weaver_Control",
        path=f"Tables/_/{table.name}",
    )
    for table in PRESENTED_RUNTIME_TABLES
}

#: Every runtime table's name, as a Lakehouse inventory reports the references
#: it already holds.
PRESENTED = tuple(table.name for table in PRESENTED_RUNTIME_TABLES)


def build(repository, tmp_path, *, catalogue, runtime_references: bool = True):
    """One bundle for this estate.

    ``runtime_references`` says whether the built targets already present the
    catalogue's runtime tables. An estate a build has finished with does; that is
    what makes the second build's reference stage empty rather than absent.
    """

    bindings = _bindings()
    bound = {binding.item: binding.to_bound_target() for binding in bindings.entries}
    inventories = {
        item: replace(
            inventory,
            runtime_references=PRESENTED if runtime_references else (),
        )
        if inventory.kind == "lakehouse"
        else inventory
        for item, inventory in _inventories(repository, bound).items()
    }
    return generate_item_build_bundle(
        repository,
        bindings=bindings,
        output=Location(str(tmp_path / "bundle")),
        store=FilesystemStore(),
        target_inventories=inventories,
        catalogue=catalogue,
        catalogue_binding=WarehouseBinding(
            ItemRef("Weaver_Control"), workspace_name=WORKSPACE
        ),
    )


def actions(bundle):
    return [action for _sequence, _batch, action in bundle.plan.actions()]


# --- the property -------------------------------------------------------------


@weaver_test()
def test_a_second_identical_build_generates_no_actions_whatever(estate, tmp_path):
    """The complete plan, which is the assertion the old test could not make."""

    second = build(estate, tmp_path, catalogue=installed_catalogue(estate))

    assert actions(second) == []


@weaver_test()
def test_the_first_build_against_an_empty_catalogue_does_do_work(estate, tmp_path):
    """Guards the test above from passing vacuously.

    If the fixture or the planner produced an empty plan for *every* input, the
    fixed-point assertion would be satisfied by a build that never worked at all.
    """

    first = build(estate, tmp_path, catalogue=Catalogue(rows={}))

    assert actions(first)


@weaver_test()
def test_the_first_build_installs_the_runtime_references_and_the_second_does_not():
    """The stage that is easiest to leave out of a fixed point, named here.

    A reference the first build did not install is one the second has to, and
    then the second build is not a no-op. Both halves are asserted, because a
    plan that never contains the stage satisfies the second half for the wrong
    reason — which is how this went unnoticed.
    """

    import tempfile
    from pathlib import Path as _Path

    from factories import full_estate

    with tempfile.TemporaryDirectory() as directory:
        root = _Path(directory)
        estate = full_estate(root / "repo")
        first = build(
            estate,
            root / "first",
            catalogue=Catalogue(rows={}),
            runtime_references=False,
        )
        second = build(estate, root / "second", catalogue=installed_catalogue(estate))

    assert [
        action.id for action in actions(first) if action.id.startswith("shortcuts-")
    ]
    assert not [
        action.id for action in actions(second) if action.id.startswith("shortcuts-")
    ]


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
@weaver_test()
def test_the_second_build_emits_no_catalogue_work_of_any_kind(estate, tmp_path, kind):
    """Named individually so a failure says which barrier came back.

    `publish_catalogue` covers the dictionaries and the Installation row;
    `publish_registry` is the certification barrier; `refresh_sql_endpoint` is
    the Weaver endpoint catching up with catalogue DML that, here, never
    happened.
    """

    second = build(estate, tmp_path, catalogue=installed_catalogue(estate))

    assert not [action for action in actions(second) if action.kind == kind]


@weaver_test()
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


@weaver_test()
def test_the_second_build_changes_no_target(estate, tmp_path):
    """No target is even mentioned as changing, let alone written to."""

    second = build(estate, tmp_path, catalogue=installed_catalogue(estate))

    assert not second.plan.target_changes


@weaver_test()
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


# --- a rebuilt object is re-certified, not silently dropped -------------------


@weaver_test()
def test_an_object_dropped_and_rebuilt_is_published_again(estate, tmp_path):
    """The bug the Fabric journey found, at the level it should have been caught.

    A build removes the catalogue claims of everything it is about to drop
    *before* any physical work, so nothing stays certified while the object
    behind it is replaced. The catalogue the planner read still holds those
    rows — it was read before any of that was decided.

    So a diff against the catalogue *as read* would compare an unchanged
    projection equal, emit no merge, and leave the row deleted by the
    before-stage with nothing to put it back. The object would come out of the
    build physically present and permanently uncertified, and the next load —
    which reads the catalogue, not the estate — would not find it.

    Here the whole estate is dropped and rebuilt while its declarations are
    unchanged, which is exactly that shape.
    """

    from weaver.build_bundle.catalogue_actions import collect_claims
    from weaver.build_bundle.planner import certifiable_identities
    from weaver.catalogue.tables import REGISTRY

    state = installed_catalogue(estate)
    bindings = _bindings()
    by_item = {binding.item: binding for binding in bindings.entries}
    identities = certifiable_identities(estate, by_item)

    # Everything certified is about to be dropped: the before-stage will delete
    # every claim, so every row must be published again.
    claims = collect_claims(state, identities)
    assert claims, "the fixture must hold claims for this to be about anything"

    from weaver.catalogue.claims import without_claims

    remaining = without_claims(state, claims)

    for item, tables in remaining.rows.items():
        assert tables.get(REGISTRY.name, ()) == (), (
            f"{item} kept Registry rows the claim deletion removes"
        )

    # And publishing against that state re-merges them.
    from weaver.catalogue.reconcile import publish

    result = publish(remaining, state)

    assert result.registry.merge, "a dropped object must be certified again"


@weaver_test()
def test_an_unchanged_descendant_is_re_certified_by_the_build_that_rebuilds_it(
    estate, tmp_path
):
    """The bug end-to-end, through the planner, where it actually bit.

    `DWG.ActiveCustomer` is a view over `DWG.Customer`. Editing Customer makes
    ActiveCustomer an *impacted descendant*: it is dropped and rebuilt, but its
    own declaration did not change, so its projected catalogue rows are
    identical to what is persisted.

    That is the precise shape the diff got wrong. Its claims are deleted before
    the physical work, and comparing against the catalogue as *read* found
    nothing to do — so the view came out of the build present and permanently
    uncertified. The next load reads the catalogue rather than the estate, and
    could not see it.
    """

    from weaver.declaration import parse_item_repository

    state = installed_catalogue(estate)

    customer = estate.root.path / "Lakehouse" / "Sales" / "DWG__Customer.py"
    customer.write_text(
        customer.read_text(encoding="utf-8").replace(
            "Description: ", "Description: revised — ", 1
        ),
        encoding="utf-8",
    )
    changed = parse_item_repository(estate.root)

    bundle = build(changed, tmp_path, catalogue=state)

    registry = [
        action
        for _sequence, _batch, action in bundle.plan.actions()
        if action.kind == "publish_registry"
    ]
    assert registry, "the rebuild must publish a Registry barrier"

    payload = bundle.store.read(
        bundle.location.join(*registry[0].payload.split("/"))
    ).decode()

    assert "ActiveCustomer" in payload, (
        "the rebuilt descendant's certification was deleted and never restored"
    )


@weaver_test()
def test_the_claim_view_only_ever_removes_rows(estate):
    """A narrowing, so the worst a mistake can do is publish something twice."""

    from weaver.build_bundle.catalogue_actions import collect_claims
    from weaver.build_bundle.planner import certifiable_identities
    from weaver.catalogue.claims import without_claims

    state = installed_catalogue(estate)
    by_item = {binding.item: binding for binding in _bindings().entries}
    claims = collect_claims(state, certifiable_identities(estate, by_item))

    remaining = without_claims(state, claims)

    for item, tables in state.rows.items():
        for name, rows in tables.items():
            assert len(remaining.rows[item].get(name, ())) <= len(rows)
    assert remaining.materialised == state.materialised


@weaver_test()
def test_no_claims_leaves_the_catalogue_untouched(estate):
    from weaver.catalogue.claims import without_claims

    state = installed_catalogue(estate)

    assert without_claims(state, ()) is state


# --- a real change still publishes -------------------------------------------


@weaver_test()
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
