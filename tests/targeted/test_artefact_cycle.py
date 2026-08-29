"""An artefact lifecycle converges on what the source declares, wherever it starts.

```text
pytest --collect-only -q -k converges
```

The family, and the four starting states it covers:

```text
a correct estate    plans nothing, and selects nothing
nothing at all      reaches the declared estate
a damaged estate    repairs it, a deleted object, a stray, a leftover schema
after a deletion    loses that object and nothing else
```

Plus a second pass over the first, because reaching the fixed point and staying
there are different claims.

Two tests here are not convergence claims and keep descriptive names, because
they guard the family against passing vacuously: a bundle with no actions at all
would satisfy every assertion above, so one asserts the catalogue tail is still
published, and one asserts the plan is deterministic.

The general statement is that a build is a convergence operator onto
`from_repository`. The special case, an estate that is already correct, is
where it started, and reads first below.

## A correct estate plans nothing. The whole build, as one property.

Every other test here asks whether one decision is right. This asks whether they
compose: give the planner a catalogue derived from the source, an inventory
derived from the source, and that same source, and it must find nothing to do.

```text
Catalogue.from_repository(...)      what the source says should be installed
for_targets(...)                    binding-specific shortcut certification
FixtureInventory.from_repository()  what the source says should be there
generate_item_build_bundle(...)     must produce no physical action at all
```

The three states agree by construction, so any physical action is a false
one, something claimed as absent that is present, or as changed that is not.
That is a different class of defect from the ones a narrow test finds, and it is
the class that costs a real estate something: an object dropped and rebuilt for
no reason, or a schema removed the same build created.

Both item types. The two physical sides are not symmetric,
a Lakehouse's generated `_` is a folder document while a Warehouse's is a
schema nothing declares, and a Lakehouse-only fixture stops being
representative exactly where that asymmetry begins.

Catalogue publication is not physical work and is expected: its statements are
idempotent and are emitted whether or not anything changed, which is what makes
them correct against a prior state the planner never saw.
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
from support.weaver_test import weaver_test
from support.workspaces import WORKSPACE

from weaver.build_bundle import (
    WarehouseBinding,
    generate_item_build_bundle,
)
from weaver.build_bundle.planner import certifiable_identities
from weaver.catalogue.state import Catalogue, for_targets
from weaver.declaration import parse_item_repository
from weaver.locations import Location
from weaver.store import FilesystemStore
from weaver.targets import ItemRef

LAKEHOUSE_TARGET_NAME = "Sales_LH"
WAREHOUSE_TARGET_NAME = "Reporting_WH"

#: Everything a build does to a target. Deliberately exhaustive rather than a
#: sample: this test's value is that a new physical kind cannot be added without
#: someone deciding whether a no-op build may emit it, and a list of four kinds
#: is how a spurious `prune_schema` went unnoticed.
PHYSICAL_KINDS = frozenset(
    {
        "create_schema",
        "create_shortcut",
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
        "refresh_sql_endpoint",
    }
)


def _write(root, relative: str, text: str) -> None:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


@pytest.fixture
def estate(tmp_path):
    """Both item types, and every artefact a source can own."""

    return full_estate(tmp_path / "repo")


def build(repository, tmp_path):
    """Plan the whole estate against the state the source itself describes."""

    bindings = item_bindings(
        (ITEM, LAKEHOUSE_TARGET_NAME),
        (WAREHOUSE_ITEM, WAREHOUSE_TARGET_NAME),
    )
    # Target ids come from the binding rather than being spelled here: the
    # planner refuses an inventory that describes a different target, which is
    # the check that stops a fixture answering for the wrong one.
    bound = {binding.item: binding.to_bound_target() for binding in bindings.entries}
    # Repository projection carries the logical runtime-reference rows. Binding
    # adds the Registry certification that makes this a physically correct
    # estate; Installation remains absent so the catalogue tail still has one
    # genuine change to publish.
    catalogue = for_targets(
        Catalogue.from_repository(repository),
        repository,
        certifiable_identities(repository, bindings.by_item),
        {item: target.kind for item, target in bound.items()},
    )
    bundle = generate_item_build_bundle(
        repository,
        bindings=bindings,
        output=Location(str(tmp_path / "bundle")),
        store=FilesystemStore(),
        target_inventories={
            item_id(ITEM): FixtureInventory.from_repository(
                repository,
                item=ITEM,
                target_id=bound[item_id(ITEM)].id,
                kind="lakehouse",
                target_name=LAKEHOUSE_TARGET_NAME,
            ),
            item_id(WAREHOUSE_ITEM): FixtureInventory.from_repository(
                repository,
                item=WAREHOUSE_ITEM,
                target_id=bound[item_id(WAREHOUSE_ITEM)].id,
                kind="warehouse",
                target_name=WAREHOUSE_TARGET_NAME,
            ),
        },
        # Production, not a fixture: the desired catalogue constructors the
        # build itself uses, stopped just before Installation is composed.
        catalogue=catalogue,
        catalogue_binding=WarehouseBinding(
            ItemRef("Weaver_Control"), workspace_name=WORKSPACE
        ),
    )
    return bundle, {target.id for target in bound.values()}


def physical(bundle, estate_targets) -> list[str]:
    """Physical actions against the estate, which is what "no work" is about.

    The catalogue's own target is excluded, and only it: a build always writes
    its catalogue, so counting that would make a correct no-op build look like
    work.
    """

    return [
        action.id
        for _sequence, batch, action in bundle.plan.actions()
        if action.kind in PHYSICAL_KINDS and batch.target_id in estate_targets
    ]


@weaver_test()
def test_converges_from_a_correct_estate_by_planning_nothing(estate, tmp_path):
    """The property, stated once.

    Reported by action id rather than as a count, because the useful failure
    names which object a build wanted to touch and the count does not.
    """

    assert physical(*build(estate, tmp_path)) == []


@weaver_test()
def test_converges_from_a_correct_estate_by_selecting_nothing(estate, tmp_path):
    """The decision behind the actions, asserted separately.

    An empty selection and an empty action list fail for different reasons,
    selection could be right while a stage rendered work anyway, which is exactly
    what a keep-set defect looks like.
    """

    selection = build(estate, tmp_path)[0].plan.selection

    assert selection.selected_for_build == ()
    assert selection.selected_for_drop == ()
    assert selection.impact.new == ()
    assert selection.impact.changed == ()


@weaver_test()
def test_whatever_the_tail_publishes_is_only_ever_catalogue_work(estate, tmp_path):
    """ "No work" must not be able to pass by planning nothing at all.

    The estate here is already correct, so no physical action is expected, but
    a bundle with no actions whatever would satisfy the two tests above for
    entirely the wrong reason. This pins what is left: everything the build still
    does is catalogue work, and nothing else has crept
    in under the cover of a quiet plan.

    Publication is a difference now, so what appears here depends on what the
        persisted catalogue already holds. This fixture's state is the repository's
        logical projection plus binding-specific shortcut certification, but carries
        no Installation row. So the Installation row is new and the tail
        publishes it. A build against a catalogue that has everything,
        Installation included, publishes nothing at all; that is the fixed point,
        and it is proven in `test_build_fixed_point_cycle`.
    """

    kinds = {
        action.kind
        for _sequence, _batch, action in build(estate, tmp_path)[0].plan.actions()
    }

    assert "publish_catalogue" in kinds, "the new Installation row is recorded"
    # And nothing physical: an item whose objects are all unchanged has only
    # its catalogue tail to write.
    assert kinds <= {
        "delete_catalogue_claims",
        "publish_catalogue",
        "publish_registry",
    }


@weaver_test()
def test_the_bundle_is_identical_the_second_time(estate, tmp_path):
    """Same inputs, same identity, the determinism claim, on the no-op path.

    Cheap here and worth having: a plan that varied between two runs of an
    unchanged estate would mean something non-deterministic reached it, and the
    no-op case is where that is easiest to see.
    """

    first, _ = build(estate, tmp_path / "one")
    second, _ = build(estate, tmp_path / "two")

    assert first.bundle_id == second.bundle_id


# --- convergence from anywhere ------------------------------------------------
#
# The fixed point above is one point. These say a build reaches it, from a
# fresh target, from a damaged one, and from a correct one after a source is
# deleted. Each applies the build's own declared effect to the state it was
# planned against, and compares the result with what the source declares, so
# what is being asserted is that the plan closes the gap it was given.


def converged(repository, tmp_path, *, inventories, catalogue):
    """Where each target ends up, and where the source says it should be.

    Reconciliation first, because convergence is a property of the workflow
    rather than of the planner alone. A catalogue claiming an object the target
    no longer holds is exactly the damage a build is supposed to repair, and the
    step that turns "claimed but absent" into "rebuild this" is the reconciler.
    Planning against an unreconciled catalogue would leave the object missing and
    the claim intact, correct for the inputs given, and not what a build does.
    """

    from factories import estate_inventories

    from weaver.catalogue.state import reconcile_catalogue_state

    reconciled = reconcile_catalogue_state(catalogue, inventories=inventories)
    bundle = generate_item_build_bundle(
        repository,
        bindings=item_bindings(
            (ITEM, LAKEHOUSE_TARGET_NAME), (WAREHOUSE_ITEM, WAREHOUSE_TARGET_NAME)
        ),
        output=Location(str(tmp_path / "bundle")),
        store=FilesystemStore(),
        target_inventories=inventories,
        catalogue=reconciled.catalogue,
        stale_claims=reconciled.stale_claims,
        catalogue_binding=WarehouseBinding(
            ItemRef("Weaver_Control"), workspace_name=WORKSPACE
        ),
    )
    declared = estate_inventories(repository)
    reached = {
        item: inventory.update_using(bundle.plan)
        for item, inventory in inventories.items()
    }
    return reached, declared


def holdings(inventory) -> dict[str, tuple[str, ...]]:
    """What a target holds, folded, the shape two inventories compare on."""

    return {
        field: tuple(sorted((v.casefold() for v in getattr(inventory, field))))
        for field in (
            "schemas",
            "tables",
            "views",
            "folders",
            "folder_schemas",
            "files",
            "procedures",
        )
    }


def assert_reaches_the_declared_estate(reached, declared):
    for item, inventory in reached.items():
        assert holdings(inventory) == holdings(declared[item]), item


@weaver_test()
def test_converges_from_nothing_to_the_declared_estate(estate, tmp_path):
    """Empty target, empty catalogue: everything is new and nothing is stale."""

    from factories import estate_inventories

    reached, declared = converged(
        estate,
        tmp_path,
        inventories=estate_inventories(estate, empty=True),
        catalogue=Catalogue({}),
    )

    assert_reaches_the_declared_estate(reached, declared)


@weaver_test()
def test_converges_from_a_damaged_estate_by_repairing_it(estate, tmp_path):
    """Objects deleted behind Weaver's back, and strays that nothing declares.

    Two kinds of damage the design promises to close, and they close by
    different routes: the missing object is a claim the inventory disproves, so
    reconciliation withdraws it and the object is rebuilt; the stray is simply
    undeclared, so prune removes it. Both end at the same declared estate.
    """

    from dataclasses import replace

    from factories import estate_inventories, item_id

    damaged = estate_inventories(estate)
    lakehouse = item_id(ITEM)
    intact = damaged[lakehouse]
    damaged[lakehouse] = replace(
        intact,
        # gone from the target, still claimed by the catalogue
        tables=tuple(t for t in intact.tables if not t.endswith(".Customer")),
        # never declared by anything
        views=intact.views + ("DWG.LeftBehind",),
        schemas=intact.schemas + ("Abandoned",),
    )
    catalogue = Catalogue.from_repository(estate)

    reached, declared = converged(
        estate, tmp_path, inventories=damaged, catalogue=catalogue
    )

    assert_reaches_the_declared_estate(reached, declared)


@weaver_test()
def test_converges_after_a_deletion_by_losing_only_that_object(estate, tmp_path):
    """The one that matters most, because over-broad pruning destroys estates.

    A correct estate, one source removed, and the build must take exactly that
    object and its deployed copy, the copy because a Python document owns two
    targets, and losing the module would be as wrong as keeping the table.
    """

    from factories import estate_inventories, item_id

    before = estate_inventories(estate)
    catalogue = Catalogue.from_repository(estate)

    # The estate as it is once the author deletes one document.
    (tmp_path / "repo" / ITEM / "DWG.Summary.sql").unlink()
    after = parse_item_repository(Location(str(tmp_path / "repo")))

    reached, declared = converged(
        after, tmp_path, inventories=before, catalogue=catalogue
    )

    assert_reaches_the_declared_estate(reached, declared)
    lost = set(holdings(before[item_id(ITEM)])["tables"]) - set(
        holdings(reached[item_id(ITEM)])["tables"]
    )
    assert lost == {"dwg.summary"}
    lost_files = set(holdings(before[item_id(ITEM)])["files"]) - set(
        holdings(reached[item_id(ITEM)])["files"]
    )
    assert lost_files == {"_/load/dwg__summary.py"}


@weaver_test()
def test_converges_and_stays_converged_on_a_second_pass(estate, tmp_path):
    """Reaching the fixed point is one claim; staying there is another.

    A build that converged but left the estate subtly different from what the
    source declares would pass the tests above and plan work forever after.
    """

    from factories import estate_inventories

    reached, _declared = converged(
        estate,
        tmp_path,
        inventories=estate_inventories(estate, empty=True),
        catalogue=Catalogue({}),
    )
    settled, declared = converged(
        estate,
        tmp_path / "second",
        inventories=reached,
        catalogue=Catalogue.from_repository(estate),
    )

    assert_reaches_the_declared_estate(settled, declared)
    for item, inventory in settled.items():
        assert holdings(inventory) == holdings(reached[item]), item
