"""One build, two physical sides, and the barrier between them.

``cross-item-journey`` is the journey estate plus the Warehouse that reports on
it: a Delta table published into the Warehouse through a shortcut, materialised
there, viewed, and reconciled against its source by a Test. It is the one shape
no single-target estate can express.

So the *order* a build gives it is asserted here, in pure Python, where a plan is
a value and no engine is needed. What that leaves for Fabric is whether the
statements this order produces are accepted, which is a different claim in a
different place.

The order is the subject. A Warehouse reads a Lakehouse table across a SQL
analytics endpoint, and an endpoint that has not caught up reports the table as
missing or stale — so the Lakehouse's objects, the refresh, and the Warehouse's
objects have to fall in that sequence. Being present is not enough.
"""

from __future__ import annotations

import pytest
from factories import FixtureCatalogue, item_bindings, target_inventory
from support.build_envs import CROSS_ITEM_JOURNEY_FIXTURE
from support.weaver_test import weaver_test
from support.workspaces import WORKSPACE

from weaver.build_bundle import (
    WarehouseBinding,
    effective_item_bindings,
    generate_item_build_bundle,
)
from weaver.declaration import parse_item_repository
from weaver.locations import Location
from weaver.store import FilesystemStore
from weaver.targets import ItemRef

LAKEHOUSE = "Sales_LH"
WAREHOUSE = "Reporting_WH"


@pytest.fixture(scope="module")
def repository():
    return parse_item_repository(Location(str(CROSS_ITEM_JOURNEY_FIXTURE.path)))


@pytest.fixture(scope="module")
def plan(repository, tmp_path_factory):
    """The bundle a first build of the whole estate emits, into empty targets."""

    bindings = effective_item_bindings(
        item_bindings(
            ("Lakehouse/Sales", LAKEHOUSE), ("Warehouse/Reporting", WAREHOUSE)
        ),
        control_item=ItemRef("Weaver"),
        workspace_name=WORKSPACE,
    )
    inventories = {}
    for binding in bindings.entries:
        bound = binding.to_bound_target()
        inventories[binding.item] = target_inventory(
            target_id=bound.id, kind=bound.kind, target_name=bound.name
        )
    bundle = generate_item_build_bundle(
        repository,
        bindings=bindings,
        output=Location(str(tmp_path_factory.mktemp("cross-item-bundle"))),
        store=FilesystemStore(),
        target_inventories=inventories,
        catalogue=FixtureCatalogue.from_registry_rows(),
        catalogue_binding=WarehouseBinding(ItemRef("Weaver"), workspace_name=WORKSPACE),
    )
    return bundle.plan


def _at(plan) -> dict:
    """Each action's sequence number, by the action id the manifest gave it."""

    return {action.id: sequence.number for sequence, _batch, action in plan.actions()}


def _when(plan, ending: str) -> int:
    at = _at(plan)
    matches = [number for action_id, number in at.items() if action_id.endswith(ending)]
    assert matches, f"no action ends with {ending!r}; the plan has {sorted(at)}"
    assert len(matches) == 1, f"{ending!r} matches several actions: {matches}"
    return matches[0]


@weaver_test()
def test_the_estate_declares_both_physical_sides(repository):
    """Neither half is inferable from the other, so both are declared."""

    items = {str(model.identity) for model in repository.items}

    assert {"Lakehouse/Sales", "Warehouse/Reporting"} <= items
    assert [
        (str(shortcut.destination), shortcut.target)
        for shortcut in repository.shortcuts
    ] == [("Warehouse/Reporting/Rpt.PortableCustomer", "Lakehouse/Sales/DWG.Customer")]


@weaver_test()
def test_one_bundle_carries_both_targets(plan):
    """Installed together, so the ordering the build gives them is a real one."""

    assert {target.logical_item_name for target in plan.targets} == {
        "Sales",
        "Reporting",
        "_weaver",
    }
    assert plan.omitted_nodes == ()


@weaver_test()
def test_the_warehouse_waits_for_the_lakehouse_it_reads(plan):
    """The composition claim: source, then barrier, then consumer.

    A Warehouse object reading an shortcut Delta table reaches it over the SQL
    analytics endpoint, and the endpoint is eventually consistent with the
    Lakehouse. Building the Warehouse side first would read a table the endpoint
    has not seen yet — and the estate would be self-consistent on each side
    while the crossing between them was stale.
    """

    produced = _when(plan, "Lakehouse--Sales--DWG.Customer")
    refreshed = _when(plan, "refresh-sql-endpoint-Lakehouse--Sales")
    shortcut = _when(plan, "shortcuts-Warehouse--Reporting")
    reported = _when(plan, "Warehouse--Reporting--Rpt.CustomerReport")
    viewed = _when(plan, "Warehouse--Reporting--Rpt.ActiveCustomerReport")

    assert produced < refreshed < shortcut < reported < viewed


@weaver_test()
def test_the_warehouse_side_is_reached_over_tds(plan):
    """A Warehouse shortcut is a T-SQL view over the endpoint, not a shortcut.

    Which is why this estate is Fabric-only in a way the Lakehouse journey is
    not.
    """

    by_target = {
        batch.target_id.split("--")[0]: action.executor
        for _sequence, batch, action in plan.actions()
        if action.id.endswith("shortcuts-Warehouse--Reporting")
    }

    assert by_target == {"Warehouse-Reporting": "tsql_batch"}


@weaver_test()
def test_the_warehouse_report_carries_a_load_procedure_and_a_test(plan):
    """What gives the run graph something to dispatch on the far side.

    A view would need none, so the report is a table: the crossing is only a
    real ordering constraint if both sides have work to order.
    """

    procedures = {
        action.id
        for _sequence, _batch, action in plan.actions()
        if action.id.startswith("load-Warehouse--Reporting--procedure")
    }

    assert procedures == {
        "load-Warehouse--Reporting--procedure-_--Load-Rpt.CustomerReport",
        "load-Warehouse--Reporting--procedure-_--Test-Rpt.ReportReconciles",
        # And the two generic entry points, which are what a person calls to run
        # one of those by hand and have the outcome recorded.
        "load-Warehouse--Reporting--procedure-_--Load",
        "load-Warehouse--Reporting--procedure-_--Test",
    }


@weaver_test()
def test_the_catalogue_is_published_after_both_sides_are_built(plan):
    """One estate, one certification, and it comes last."""

    assert _when(plan, "publish-registry") > _when(
        plan, "Warehouse--Reporting--Rpt.ActiveCustomerReport"
    )
