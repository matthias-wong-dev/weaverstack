"""Nothing remote starts until every required Fabric item has been found.

The cost being defended is asymmetric. Listing a workspace's items is one REST
call; creating a Livy session is tens of seconds and a slice of a capacity. A
build that starts the session and *then* discovers a missing Warehouse has paid
the expensive thing to learn what the cheap thing already knew — and it reports
the discovery as a Spark failure about a catalogue rather than as a sentence
about an item nobody created.

The fake client here holds a workspace inventory and counts what is asked of it,
so both halves are checkable with no tenant: that the checks are made from one
listing, and that a failure stops before the session factory is ever reached.
"""

from __future__ import annotations

import pytest

from weaver.build_bundle.targets import (
    ItemBinding,
    ItemBindings,
    LakehouseBinding,
    WarehouseBinding,
)
from weaver.declaration.model import WeaverItemId
from weaver.errors import CommandError
from weaver.fabric.preflight import (
    PreflightError,
    preflight_fabric_targets,
    required_items,
)
from weaver.targets import ItemRef
from support.workspaces import WORKSPACE

WORKSPACE = "Analytics"


class FakeClient:
    """A workspace whose inventory is stated, and whose calls are counted."""

    def __init__(self, items):
        self.items = list(items)
        self.paths: list[str] = []

    def paged(self, path):
        self.paths.append(path)
        if path == "workspaces":
            return [{"id": "ws-id", "displayName": WORKSPACE}]
        if path.startswith("workspaces/ws-id/items"):
            return [
                {"id": f"{name}-id", "displayName": name, "type": item_type}
                for name, item_type in self.items
            ]
        return []

    @property
    def item_listings(self) -> int:
        return sum(1 for path in self.paths if "items" in path)


def _bindings(*targets):
    entries = []
    for item_name, physical, kind in targets:
        binding = (
            LakehouseBinding(ItemRef(physical), workspace_name=WORKSPACE)
            if kind == "Lakehouse"
            else WarehouseBinding(ItemRef(physical), workspace_name=WORKSPACE)
        )
        entries.append(
            ItemBinding(WeaverItemId.parse(f"{kind}/{item_name}"), binding)
        )
    return ItemBindings(tuple(entries))


def _complete_estate():
    return FakeClient(
        [
            ("Weaver", "Lakehouse"),
            ("Weaver", "SQLEndpoint"),
            ("Sales_LH", "Lakehouse"),
            ("Reporting", "Warehouse"),
            ("WeaverEnv", "Environment"),
        ]
    )


def _preflight(client, bindings, *, environment="WeaverEnv"):
    return preflight_fabric_targets(
        bindings,
        workspace=WORKSPACE,
        control_item=ItemRef("Weaver"),
        environment=environment,
        client=client,
    )


# --- what a build requires ----------------------------------------------------


def test_every_bound_target_and_the_control_lakehouse_are_required():
    wanted = required_items(
        _bindings(
            ("Sales", "Sales_LH", "Lakehouse"),
            ("Reporting", "Reporting", "Warehouse"),
        ),
        control_item=ItemRef("Weaver"),
        environment="WeaverEnv",
    )

    assert {(item.name, item.item_type) for item in wanted} == {
        ("Weaver", "Lakehouse"),
        ("WeaverEnv", "Environment"),
        ("Sales_LH", "Lakehouse"),
        ("Reporting", "Warehouse"),
    }


def test_the_catalogue_is_required_once_though_it_arrives_twice():
    """`effective_item_bindings` binds `_weaver` to the control Lakehouse.

    So the Weaver Lakehouse reaches preflight both as itself and as that
    binding's target. Checking it twice would be harmless but would make the
    failure report name it twice, which reads as two problems.
    """

    wanted = required_items(
        _bindings(
            ("Sales", "Sales_LH", "Lakehouse"),
            ("_weaver", "Weaver", "Lakehouse"),
        ),
        control_item=ItemRef("Weaver"),
    )

    assert sum(1 for item in wanted if item.name == "Weaver") == 1


# --- one listing, however many targets ---------------------------------------


def test_the_workspace_inventory_is_read_once_for_every_target():
    client = _complete_estate()

    _preflight(
        client,
        _bindings(
            ("Sales", "Sales_LH", "Lakehouse"),
            ("Reporting", "Reporting", "Warehouse"),
        ),
    )

    assert client.item_listings == 1


def test_adding_targets_does_not_add_listings():
    """The scaling rule, stated as the comparison that would catch a regression."""

    few = _complete_estate()
    _preflight(few, _bindings(("Sales", "Sales_LH", "Lakehouse")))

    many = FakeClient(
        [("Weaver", "Lakehouse"), ("WeaverEnv", "Environment")]
        + [(f"LH_{index}", "Lakehouse") for index in range(20)]
    )
    _preflight(
        many,
        _bindings(*[(f"Item{index}", f"LH_{index}", "Lakehouse") for index in range(20)]),
    )

    assert many.item_listings == few.item_listings == 1


def test_a_successful_preflight_resolves_the_items_it_checked():
    client = _complete_estate()

    result = _preflight(client, _bindings(("Sales", "Sales_LH", "Lakehouse")))

    assert result.item("Sales_LH", "Lakehouse").id == "Sales_LH-id"
    assert result.workspace.name == WORKSPACE


def test_a_successful_preflight_writes_nothing():
    client = _complete_estate()

    _preflight(client, _bindings(("Sales", "Sales_LH", "Lakehouse")))

    assert all("items" in path or path == "workspaces" for path in client.paths)
    assert not hasattr(client, "requests"), "preflight must never issue a write"


# --- and what it refuses ------------------------------------------------------


def test_a_missing_workspace_fails_before_anything_is_listed():
    client = FakeClient([])
    client.paged = lambda path: []  # noqa: E731 - the workspace does not exist

    with pytest.raises(CommandError, match="Workspace 'Analytics' was not found"):
        _preflight(client, _bindings(("Sales", "Sales_LH", "Lakehouse")))


def test_a_missing_catalogue_fails():
    client = FakeClient([("Sales_LH", "Lakehouse"), ("WeaverEnv", "Environment")])

    with pytest.raises(PreflightError, match="Weaver Lakehouse 'Weaver' was not found"):
        _preflight(client, _bindings(("Sales", "Sales_LH", "Lakehouse")))


def test_a_missing_environment_fails():
    client = FakeClient([("Weaver", "Lakehouse"), ("Sales_LH", "Lakehouse")])

    with pytest.raises(PreflightError, match="Environment 'WeaverEnv' was not found"):
        _preflight(client, _bindings(("Sales", "Sales_LH", "Lakehouse")))


def test_a_missing_bound_lakehouse_fails():
    client = _complete_estate()

    with pytest.raises(PreflightError, match="Lakehouse target 'Absent_LH'"):
        _preflight(client, _bindings(("Sales", "Absent_LH", "Lakehouse")))


def test_a_missing_bound_warehouse_fails():
    client = _complete_estate()

    with pytest.raises(PreflightError, match="Warehouse target 'Absent_WH'"):
        _preflight(client, _bindings(("Reporting", "Absent_WH", "Warehouse")))


def test_every_missing_item_is_reported_together():
    """One round trip to learn the estate is not ready, not one per item."""

    client = FakeClient([("WeaverEnv", "Environment")])

    with pytest.raises(PreflightError) as raised:
        _preflight(
            client,
            _bindings(
                ("Sales", "Sales_LH", "Lakehouse"),
                ("Reporting", "Reporting", "Warehouse"),
            ),
        )

    message = str(raised.value)
    assert "Weaver Lakehouse 'Weaver' was not found" in message
    assert "Lakehouse target 'Sales_LH' was not found" in message
    assert "Warehouse target 'Reporting' was not found" in message


def test_a_name_that_exists_as_the_wrong_type_says_so():
    """The common mistake, and the one a bare absence sends you hunting for."""

    client = FakeClient(
        [
            ("Weaver", "Lakehouse"),
            ("WeaverEnv", "Environment"),
            ("Reporting", "Lakehouse"),
        ]
    )

    with pytest.raises(PreflightError, match="holds a Lakehouse of that name"):
        _preflight(client, _bindings(("Reporting", "Reporting", "Warehouse")))


def test_a_lakehouses_sql_endpoint_sibling_is_not_reported_as_a_type_confusion():
    """Every Lakehouse grows one; it is a facet, not a competing item."""

    client = _complete_estate()

    with pytest.raises(PreflightError) as raised:
        _preflight(client, _bindings(("Weaver", "Weaver", "Warehouse")))

    assert "SQLEndpoint" not in str(raised.value)


def test_an_ambiguous_name_fails_rather_than_picking_one():
    client = FakeClient(
        [
            ("Weaver", "Lakehouse"),
            ("WeaverEnv", "Environment"),
            ("Sales_LH", "Lakehouse"),
            ("Sales_LH", "Lakehouse"),
        ]
    )

    with pytest.raises(PreflightError, match="ambiguous"):
        _preflight(client, _bindings(("Sales", "Sales_LH", "Lakehouse")))
