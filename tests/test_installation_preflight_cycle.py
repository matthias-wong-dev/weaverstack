"""Each operation verifies the installations it depends on, and no others."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from support.weaver_test import weaver_test
from support.workspaces import (
    CreatingInventoryClient,
    InventoryClient,
    _identifier,
    given_resolver,
    given_workspace,
)

from weaver.catalogue.installation import (
    check_installations,
    resolve_installations,
    verify_installation,
    verify_installations,
)
from weaver.catalogue.state import Catalogue
from weaver.errors import BuildError, InstallationIdentityError
from weaver.fabric.resolution import FabricResolver
from weaver.targets import ItemRef

OTHER = "11111111-1111-1111-1111-111111111111"


def row(resolver, kind, item, target, **changes):
    """An installation row recording the identity Fabric would give ``target``."""

    values = {
        "item_type": kind,
        "item_name": item,
        "target_name": target,
        "workspace_name": resolver.configuration.workspace,
        "workspace_id": _identifier("workspace", resolver.configuration.workspace),
        "item_id": _identifier(kind, target),
        "weaver_version": "0.0.0+test",
        "signature": "installation",
    }
    values.update(changes)
    return values


def listings(resolver):
    return [
        path
        for path in resolver.client.requested
        if path.partition("?")[0].endswith("/items")
    ]


# --- the helper ---------------------------------------------------------------


@weaver_test()
def test_a_matching_installation_resolves_to_its_physical_item():
    resolver = given_resolver(warehouses=["Sales"])
    resolved = verify_installation(
        row(resolver, "Warehouse", "Orders", "Sales"), resolver=resolver
    )
    assert resolved.id == _identifier("Warehouse", "Sales")
    assert resolved is resolver.resolve(ItemRef("Sales"), item_type="Warehouse")


@weaver_test()
def test_a_replaced_item_of_the_same_name_is_refused():
    resolver = given_resolver(warehouses=["Sales"])
    with pytest.raises(InstallationIdentityError, match="Target item GUID") as raised:
        verify_installation(
            row(resolver, "Warehouse", "Orders", "Sales", item_id=OTHER),
            resolver=resolver,
        )
    assert "unbind Warehouse/Orders" in str(raised.value)
    assert isinstance(raised.value, BuildError)


@weaver_test()
def test_a_missing_item_names_the_next_action():
    resolver = given_resolver(warehouses=["Sales"])
    with pytest.raises(
        InstallationIdentityError, match="Restore it, or unbind"
    ) as raised:
        verify_installation(
            row(resolver, "Warehouse", "Orders", "Gone"), resolver=resolver
        )
    assert "Gone" in str(raised.value)


@weaver_test()
@pytest.mark.parametrize("field", ["workspace_id", "item_id"])
@pytest.mark.parametrize("invalid", ["", "Sales", " " + OTHER])
def test_an_invalid_identifier_is_refused(field, invalid):
    resolver = given_resolver(warehouses=["Sales"])
    with pytest.raises(InstallationIdentityError, match="invalid .*GUID"):
        verify_installation(
            row(resolver, "Warehouse", "Orders", "Sales", **{field: invalid}),
            resolver=resolver,
        )


@weaver_test()
def test_another_workspace_is_refused():
    resolver = given_resolver(warehouses=["Sales"])
    with pytest.raises(InstallationIdentityError, match="Workspace GUID"):
        verify_installation(
            row(resolver, "Warehouse", "Orders", "Sales", workspace_id=OTHER),
            resolver=resolver,
        )


@weaver_test()
def test_a_renamed_workspace_with_the_same_guid_is_not_a_mismatch():
    resolver = given_resolver(warehouses=["Sales"])
    resolved = verify_installation(
        row(
            resolver,
            "Warehouse",
            "Orders",
            "Sales",
            workspace_name="Name before the rename",
        ),
        resolver=resolver,
    )
    assert resolved.name == "Sales"


@weaver_test()
def test_a_workspace_missing_from_configuration_fails_loudly():
    workspace = given_workspace(workspace="Name in configuration")
    client = InventoryClient("Name after the rename", [("Warehouse", "Sales")])
    resolver = FabricResolver(workspace, client=client)
    recorded = row(resolver, "Warehouse", "Orders", "Sales")
    with pytest.raises(
        InstallationIdentityError,
        match="update the workspace name in workspace configuration",
    ):
        verify_installation(recorded, resolver=resolver)


@weaver_test()
def test_a_batch_reports_each_inconsistency_and_resolves_the_rest():
    resolver = given_resolver(warehouses=["Sales", "Stock"])
    resolved, faults = check_installations(
        [
            row(resolver, "Warehouse", "Orders", "Sales"),
            row(resolver, "Warehouse", "Items", "Stock", item_id=OTHER),
        ],
        resolver=resolver,
    )
    assert set(resolved) == {("Warehouse", "Orders")}
    assert set(faults) == {("Warehouse", "Items")}
    with pytest.raises(InstallationIdentityError, match="Warehouse/Items"):
        verify_installations(
            [row(resolver, "Warehouse", "Items", "Stock", item_id=OTHER)],
            resolver=resolver,
        )


# --- scope --------------------------------------------------------------------


class Spy:
    """A resolver that records which items an operation asked about."""

    def __init__(self, resolver):
        self._resolver = resolver
        self.asked = []

    def __getattr__(self, name):
        return getattr(self._resolver, name)

    def resolve(self, item, *, item_type):
        self.asked.append((item_type, item.name))
        return self._resolver.resolve(item, item_type=item_type)


def estate_rows(resolver):
    return [
        row(resolver, "Warehouse", "Orders", "Sales"),
        row(resolver, "Warehouse", "Stock", "Stock"),
        row(resolver, "Warehouse", "Stale", "Stale", item_id=OTHER),
        row(resolver, "Warehouse", "Elsewhere", "Sales", workspace_id=OTHER),
        row(resolver, "Warehouse", "Vanished", "Vanished"),
    ]


@weaver_test()
def test_an_unrelated_stale_installation_does_not_block_a_selected_build():
    from weaver.declaration.model import WeaverItemId

    resolver = Spy(given_resolver(warehouses=["Sales", "Stock", "Stale"]))
    orders, stock = (
        WeaverItemId.parse(f"Warehouse/{name}") for name in ("Orders", "Stock")
    )
    rows = estate_rows(resolver)
    resolved = resolve_installations(
        Catalogue({}), rows, resolver=resolver, items=(orders, stock)
    )
    assert [r["item_name"] for r in resolved] == [r["item_name"] for r in rows]
    assert sorted(resolver.asked) == [("Warehouse", "Sales"), ("Warehouse", "Stock")]


@weaver_test()
@pytest.mark.parametrize(
    "name,message",
    [
        ("Stale", "Target item GUID"),
        ("Elsewhere", "Workspace GUID"),
        ("Vanished", "Restore it"),
    ],
)
def test_selecting_an_inconsistent_installation_fails_the_build(name, message):
    from weaver.declaration.model import WeaverItemId

    resolver = given_resolver(warehouses=["Sales", "Stock", "Stale"])
    with pytest.raises(InstallationIdentityError, match=message):
        resolve_installations(
            Catalogue({}),
            estate_rows(resolver),
            resolver=resolver,
            items=(WeaverItemId.parse(f"Warehouse/{name}"),),
        )


@weaver_test()
def test_only_a_schema_upgrade_resolves_the_whole_estate():
    from test_installation_identity_cycle import legacy_connection

    from weaver.catalogue.tables import INSTALLATION

    resolver = given_resolver(warehouses=["Sales", "Stock", "Stale"])
    rows = [
        {**r, "workspace_id": None, "item_id": None} for r in estate_rows(resolver)[:3]
    ]
    connection, _ = legacy_connection()
    upgrading = Catalogue(
        {}, installation_upgrade={"columns": connection.nullability_of(INSTALLATION)}
    )
    assert len(resolve_installations(upgrading, rows, resolver=resolver)) == 3
    assert {r["item_name"] for r in upgrading.installation_upgrade["resolved"]} == {
        "Orders",
        "Stock",
        "Stale",
    }
    routine = Spy(given_resolver(warehouses=["Sales", "Stock", "Stale"]))
    resolve_installations(Catalogue({}), rows, resolver=routine)
    assert routine.asked == []


# --- Build --------------------------------------------------------------------


def build_state(monkeypatch, resolver, rows, *, selected):
    """``read_build_state`` over a catalogue that holds these installations."""

    from weaver.build_bundle.targets import ItemBinding, ItemBindings, WarehouseBinding
    from weaver.build_bundle.workflow import read_build_state
    from weaver.catalogue.tables import INSTALLATION
    from weaver.declaration.model import WeaverItemId
    from weaver.sessions import TestSession

    items = {WeaverItemId("Warehouse", name): target for name, target in selected}
    monkeypatch.setattr(
        "weaver.catalogue.connection.catalogue_connection", lambda *_a, **_k: object()
    )
    monkeypatch.setattr(
        "weaver.catalogue.installation.installation_schema_upgrade", lambda _c: None
    )
    monkeypatch.setattr("weaver.catalogue.reader.read_table", lambda *_a, **_k: rows)
    monkeypatch.setattr(
        "weaver.build_bundle.workflow._read_catalogue",
        lambda **kw: Catalogue(
            {
                WeaverItemId(r["item_type"], r["item_name"]): {
                    INSTALLATION.name: (dict(r),)
                }
                for r in rows
                if WeaverItemId(r["item_type"], r["item_name"]) in kw["required"]
            }
        ),
    )
    inventory = SimpleNamespace(query=lambda _statement: [])
    bindings = ItemBindings(
        tuple(
            ItemBinding(item, WarehouseBinding(ItemRef(target)))
            for item, target in items.items()
        )
    )
    session = TestSession(workspace=resolver.configuration, resolver=resolver)
    return read_build_state(
        bindings,
        required_catalogue_items=tuple(items),
        session=session,
        sql_by_item={item: inventory for item in items},
    )


@weaver_test()
def test_an_unrelated_stale_installation_does_not_block_a_valid_build_state(
    monkeypatch,
):
    resolver = given_resolver(warehouses=["Sales", "Stale"])
    rows = [
        row(resolver, "Warehouse", "Orders", "Sales"),
        row(resolver, "Warehouse", "Stale", "Stale", item_id=OTHER),
    ]
    state = build_state(monkeypatch, resolver, rows, selected=[("Orders", "Sales")])
    inventory = next(iter(state.target_inventories.values()))
    assert inventory.item_id == _identifier("Warehouse", "Sales")
    # The unrelated installation is never resolved, so a one-item build lists once.
    assert len(listings(resolver)) == 1


@weaver_test()
def test_a_replaced_selected_item_fails_before_its_inventory_is_read(monkeypatch):
    resolver = given_resolver(warehouses=["Sales"])
    rows = [row(resolver, "Warehouse", "Orders", "Sales", item_id=OTHER)]
    with pytest.raises(InstallationIdentityError, match="Target item GUID"):
        build_state(monkeypatch, resolver, rows, selected=[("Orders", "Sales")])


@weaver_test()
def test_a_producer_the_build_requires_is_verified_with_the_same_helper(monkeypatch):
    resolver = given_resolver(warehouses=["Sales", "Producer"])
    rows = [
        row(resolver, "Warehouse", "Orders", "Sales"),
        row(resolver, "Warehouse", "Feed", "Producer", item_id=OTHER),
    ]
    from weaver.declaration.model import WeaverItemId

    with pytest.raises(InstallationIdentityError, match="Warehouse/Feed"):
        resolve_installations(
            Catalogue({}),
            rows,
            resolver=resolver,
            items=(
                WeaverItemId.parse("Warehouse/Orders"),
                WeaverItemId.parse("Warehouse/Feed"),
            ),
        )
    assert len(listings(resolver)) == 1


# --- Mirror -------------------------------------------------------------------


def check_mirror_of(monkeypatch, resolver, rows):
    """``check_mirror`` selecting ``Warehouse/Orders`` over a source with ``rows``."""

    from support.catalogue_shapes import current, shape

    import weaver.operations.mirror as module
    from weaver.catalogue.fork import project_source
    from weaver.catalogue.tables import INSTALLATION
    from weaver.declaration.model import WeaverItemId
    from weaver.sessions import TestSession
    from weaver.workspaces import CatalogueRef

    workspace = resolver.configuration
    plan = module.MirrorPlan(
        workspace=workspace,
        source=CatalogueRef(workspace=workspace.workspace, name="Weaver"),
        destination=CatalogueRef(workspace=workspace.workspace, name="Weaver_Dev"),
        items=("Warehouse/Orders=Warehouse/Orders_Dev",),
    )
    monkeypatch.setattr(
        module,
        "_prove_source",
        lambda *_a, **_k: project_source(shape(current()), source="Weaver"),
    )
    monkeypatch.setattr(
        "weaver.catalogue.state.catalogue_for",
        lambda *_a, **_k: Catalogue(
            {
                WeaverItemId(r["item_type"], r["item_name"]): {
                    INSTALLATION.name: (dict(r),)
                }
                for r in rows
            }
        ),
    )
    session = TestSession(workspace=workspace, resolver=resolver)
    return plan, session, module


@weaver_test()
def test_mirror_proceeds_when_the_selected_source_matches(monkeypatch):
    resolver = given_resolver(warehouses=["Sales", "Orders_Dev", "Stale"])
    rows = [
        row(resolver, "Warehouse", "Orders", "Sales"),
        row(resolver, "Warehouse", "Stale", "Stale", item_id=OTHER),
    ]
    plan, session, module = check_mirror_of(monkeypatch, resolver, rows)
    resolved = module.check_mirror(plan, session=session)
    assert [str(each.item) for each in resolved.items] == ["Warehouse/Orders"]


@weaver_test()
def test_mirror_refuses_a_replaced_source_before_scheduling_destination_writes(
    monkeypatch,
):
    import weaver.mirror_plan as compiled

    resolver = given_resolver(warehouses=["Sales", "Orders_Dev"])
    rows = [row(resolver, "Warehouse", "Orders", "Sales", item_id=OTHER)]
    plan, session, module = check_mirror_of(monkeypatch, resolver, rows)
    monkeypatch.setattr(
        compiled,
        "mirror_mutation_plan",
        lambda *_a, **_k: pytest.fail("the destination was planned for emptying"),
    )
    with pytest.raises(InstallationIdentityError, match="Target item GUID"):
        module.mirror(plan=plan, session=session)
    assert not session.calls


@weaver_test()
def test_mirror_refuses_a_missing_source_item(monkeypatch):
    resolver = given_resolver(warehouses=["Orders_Dev"])
    rows = [row(resolver, "Warehouse", "Orders", "Sales")]
    plan, session, module = check_mirror_of(monkeypatch, resolver, rows)
    with pytest.raises(InstallationIdentityError, match="Sales"):
        module.check_mirror(plan, session=session)


# --- Wipe and Unbind ----------------------------------------------------------


@weaver_test()
def test_unbind_allows_build_to_register_a_recreated_items_identity(
    tmp_path, monkeypatch
):
    from factories import single_document_repository, warehouse_table
    from test_unbind_cycle import _Catalogue

    import weaver
    from weaver.build_bundle.catalogue_actions import desired_catalogue
    from weaver.build_bundle.targets import (
        ItemBinding,
        WarehouseBinding,
        bind_inventory_target,
    )
    from weaver.catalogue.tables import INSTALLATION
    from weaver.declaration.model import WeaverItemId
    from weaver.mutation.executor import MutationReport, MutationResult
    from weaver.sessions import TestSession

    resolver = given_resolver(warehouses=["Weaver", "Sales"])
    stale = row(resolver, "Warehouse", "Orders", "Sales", item_id=OTHER)
    catalogue = _Catalogue((stale,))
    monkeypatch.setattr(
        "weaver.catalogue.connection.catalogue_connection",
        lambda *_a, **_k: catalogue,
    )
    session = TestSession(workspace=resolver.configuration, resolver=resolver)
    plan = weaver.plan_unbind("Warehouse/Sales", session=session)
    actions = []

    def execute(mutation, _payloads):
        selected = [action for _s, _b, action in mutation.actions()]
        actions.extend(action.kind for action in selected)
        return MutationReport(
            mutation.bundle_id,
            tuple(MutationResult(action.id, "succeeded") for action in selected),
        )

    monkeypatch.setattr(session, "execute_mutation", execute)
    result = weaver.unbind(plan=plan, session=session)

    assert result.logical_items == ("Warehouse/Orders",)
    assert actions == ["unbind_catalogue_claims"]

    state = build_state(monkeypatch, resolver, (), selected=[("Orders", "Sales")])
    item = WeaverItemId.parse("Warehouse/Orders")
    inventory = state.target_inventories[item]
    binding = ItemBinding(item, WarehouseBinding(ItemRef("Sales")))
    bound = bind_inventory_target(binding.to_bound_target(), inventory)
    repository = single_document_repository(
        tmp_path,
        item=str(item),
        documents={"DWG.Customer.sql": warehouse_table("DWG.Customer")},
    )
    desired = desired_catalogue(repository, repository.source_documents, {item: bound})
    (registered,) = desired.rows[item][INSTALLATION.name]
    assert registered["item_id"] == _identifier("Warehouse", "Sales")
    assert registered["item_id"] != stale["item_id"]


# --- Load and Test ------------------------------------------------------------


def lakehouse_run(resolver, rows, nodes, *, edges=(), fault_tolerant=True, **policy):
    """A Runner over these nodes and the catalogue holding ``rows``, and what ran."""

    from test_run_cycle import controlled

    from weaver.catalogue.tables import INSTALLATION
    from weaver.declaration.model import WeaverItemId
    from weaver.run import Runner, RunRequest, RunState
    from weaver.run.graph import RunGraph
    from weaver.sessions import TestSession

    catalogue = Catalogue(
        {
            WeaverItemId(r["item_type"], r["item_name"]): {
                INSTALLATION.name: (dict(r),)
            }
            for r in rows
        }
    )
    request = RunRequest.load(
        [WeaverItemId("Lakehouse", "Raw")], fault_tolerant=fault_tolerant, **policy
    )
    runner = Runner(
        RunState(catalogue=catalogue), request, workspace=resolver.configuration
    )
    runner._graph = RunGraph(
        nodes=tuple(nodes), edges=tuple(edges), items=request.items
    )
    dispatch = controlled({})
    session = TestSession(workspace=resolver.configuration, resolver=resolver)
    return runner.run(session=session, dispatch=dispatch), dispatch


def load_node(item, target):
    from test_run_cycle import node

    from weaver.declaration.model import WeaverDocumentId
    from weaver.targets import PhysicalTargetRef

    return node(
        f"{item}-load",
        target=PhysicalTargetRef("lakehouse", target),
        logical_id=WeaverDocumentId.parse(f"Lakehouse/{item}/Tables/Sales.Order"),
    )


@weaver_test()
def test_a_load_whose_installations_match_runs_normally():
    from weaver.run.result import SUCCEEDED

    resolver = given_resolver(lakehouses=["Raw_LH", "Mart_LH"])
    rows = [
        row(resolver, "Lakehouse", "Raw", "Raw_LH"),
        row(resolver, "Lakehouse", "Mart", "Mart_LH"),
    ]
    result, dispatch = lakehouse_run(
        resolver, rows, [load_node("Raw", "Raw_LH"), load_node("Mart", "Mart_LH")]
    )
    assert sorted(dispatch.seen) == ["Mart-load", "Raw-load"]
    assert {one.status for one in result.nodes} == {SUCCEEDED}
    assert len(listings(resolver)) == 1


@weaver_test()
def test_a_replaced_item_fails_its_node_and_its_dependants_not_the_rest():
    from weaver.run.result import (
        BLOCKED,
        CATALOGUE_BINDING_INVALID,
        INVALID,
        SUCCEEDED,
    )

    resolver = given_resolver(lakehouses=["Raw_LH", "Stale_LH", "Mart_LH"])
    rows = [
        row(resolver, "Lakehouse", "Raw", "Raw_LH"),
        row(resolver, "Lakehouse", "Stale", "Stale_LH", item_id=OTHER),
        row(resolver, "Lakehouse", "Mart", "Mart_LH"),
    ]
    nodes = [
        load_node("Raw", "Raw_LH"),
        load_node("Stale", "Stale_LH"),
        load_node("Mart", "Mart_LH"),
    ]
    result, dispatch = lakehouse_run(
        resolver, rows, nodes, edges=[("Stale-load", "Mart-load")]
    )
    assert result.by_node["Stale-load"].status == INVALID
    (message,) = result.by_node["Stale-load"].messages
    assert message.code == CATALOGUE_BINDING_INVALID
    assert "Target item GUID" in message.message
    assert result.by_node["Mart-load"].status == BLOCKED
    assert result.by_node["Raw-load"].status == SUCCEEDED
    assert dispatch.seen == ["Raw-load"]


@weaver_test()
def test_an_installation_no_node_uses_cannot_fail_the_run():
    from weaver.run.result import SUCCEEDED

    resolver = given_resolver(lakehouses=["Raw_LH"])
    rows = [
        row(resolver, "Lakehouse", "Raw", "Raw_LH"),
        row(resolver, "Lakehouse", "Stale", "Gone_LH", item_id=OTHER),
    ]
    result, _ = lakehouse_run(resolver, rows, [load_node("Raw", "Raw_LH")])
    assert [one.status for one in result.nodes] == [SUCCEEDED]


@weaver_test()
def test_a_missing_item_fails_its_node_with_the_next_action():
    from weaver.run.result import INVALID

    resolver = given_resolver(lakehouses=["Raw_LH"])
    rows = [row(resolver, "Lakehouse", "Raw", "Gone_LH")]
    result, dispatch = lakehouse_run(resolver, rows, [load_node("Raw", "Gone_LH")])
    assert result.by_node["Raw-load"].status == INVALID
    assert "Restore it, or unbind" in result.by_node["Raw-load"].messages[0].message
    assert not dispatch.seen


@weaver_test()
def test_a_test_checks_its_expected_data_source():
    from test_run_cycle import node

    from weaver.declaration.model import WeaverDocumentId
    from weaver.run.result import INVALID, SUCCEEDED
    from weaver.targets import PhysicalTargetRef

    resolver = given_resolver(lakehouses=["Raw_LH", "Stale_LH"])
    rows = [
        row(resolver, "Lakehouse", "Raw", "Raw_LH"),
        row(resolver, "Lakehouse", "Stale", "Stale_LH", item_id=OTHER),
    ]

    def validation(name, expected):
        return node(
            name,
            target=PhysicalTargetRef("lakehouse", "Raw_LH"),
            logical_id=WeaverDocumentId.parse("Lakehouse/Raw/Tables/Sales.Order"),
            installed=SimpleNamespace(
                expected_target=PhysicalTargetRef("lakehouse", expected)
            ),
        )

    result, dispatch = lakehouse_run(
        resolver,
        rows,
        [validation("matching", "Raw_LH"), validation("stale", "Stale_LH")],
    )
    assert result.by_node["stale"].status == INVALID
    assert result.by_node["matching"].status == SUCCEEDED
    assert dispatch.seen == ["matching"]


@weaver_test()
def test_a_dry_run_reaches_no_workspace():
    resolver = given_resolver(lakehouses=["Raw_LH"])
    rows = [row(resolver, "Lakehouse", "Raw", "Raw_LH", item_id=OTHER)]
    lakehouse_run(resolver, rows, [load_node("Raw", "Raw_LH")], dry_run=True)
    assert resolver.client.requested == []


# --- Health -------------------------------------------------------------------


@weaver_test()
def test_health_reports_an_inconsistent_item_and_still_checks_the_others(monkeypatch):
    from datetime import datetime, timezone
    from unittest.mock import patch

    from weaver.catalogue.tables import INSTALLATION
    from weaver.declaration.model import WeaverItemId
    from weaver.health import INSTALLATION_INCONSISTENT
    from weaver.operations.health import run_health
    from weaver.sessions import TestSession
    from weaver.targets import PhysicalTargetRef

    resolver = given_resolver(lakehouses=["Raw_LH", "Stale_LH", "Mart_LH"])
    rows = [
        row(resolver, "Lakehouse", "Raw", "Raw_LH"),
        row(resolver, "Lakehouse", "Stale", "Stale_LH", item_id=OTHER),
        row(resolver, "Lakehouse", "Mart", "Mart_LH"),
    ]
    catalogue = Catalogue(
        {
            WeaverItemId(r["item_type"], r["item_name"]): {
                INSTALLATION.name: (dict(r),)
            }
            for r in rows
        }
    )
    read = []

    def inventories(session, *, workspace, targets, dag):
        read.extend(str(target) for target in targets)
        return {}

    now = datetime(2026, 4, 23, 12, 0, tzinfo=timezone.utc)
    workspace = resolver.configuration
    monkeypatch.setattr("weaver.operations.health._inventories", inventories)
    with patch(
        "weaver.catalogue.state.read_installed_catalogue", return_value=catalogue
    ):
        report = run_health(
            TestSession(workspace=workspace, resolver=resolver),
            workspace=workspace,
            as_of=now,
            generated_at=now,
        )
    (finding,) = [
        f for f in report.build.findings if f.code == INSTALLATION_INCONSISTENT
    ]
    assert finding.target == str(PhysicalTargetRef("lakehouse", "Stale_LH"))
    assert "Lakehouse/Stale" in finding.message
    assert sorted(read) == [
        str(PhysicalTargetRef("lakehouse", name)) for name in ("Mart_LH", "Raw_LH")
    ]
    assert len(listings(resolver)) == 1


# --- Fabric requests ----------------------------------------------------------


@weaver_test()
def test_many_installations_in_one_workspace_cost_one_item_listing():
    names = [f"Warehouse{number}" for number in range(8)]
    resolver = given_resolver(warehouses=names)
    resolved, faults = check_installations(
        [
            row(resolver, "Warehouse", f"Item{number}", name)
            for number, name in enumerate(names)
        ],
        resolver=resolver,
    )
    assert len(resolved) == 8 and not faults
    assert resolver.client.requested == [
        "workspaces",
        f"workspaces/{resolver.workspace.id}/items",
    ]


@weaver_test()
def test_a_single_installation_keeps_its_typed_listing():
    resolver = given_resolver(warehouses=["Sales"])
    verify_installation(
        row(resolver, "Warehouse", "Orders", "Sales"), resolver=resolver
    )
    assert resolver.client.requested == [
        "workspaces",
        f"workspaces/{resolver.workspace.id}/items?type=Warehouse",
    ]


@weaver_test()
def test_target_inventories_resolve_every_binding_from_one_listing():
    from weaver.build_bundle.targets import ItemBinding, ItemBindings, WarehouseBinding
    from weaver.build_bundle.workflow import read_target_inventories
    from weaver.declaration.model import WeaverItemId
    from weaver.sessions import TestSession

    names = [f"Warehouse{number}" for number in range(6)]
    resolver = given_resolver(warehouses=names)
    bindings = ItemBindings(
        tuple(
            ItemBinding(
                WeaverItemId("Warehouse", f"Item{number}"),
                WarehouseBinding(ItemRef(name)),
            )
            for number, name in enumerate(names)
        )
    )
    query = SimpleNamespace(query=lambda _statement: [])
    inventories = read_target_inventories(
        bindings,
        session=TestSession(workspace=resolver.configuration, resolver=resolver),
        sql_by_item={binding.item: query for binding in bindings.entries},
    )
    assert len(inventories) == 6
    assert len(listings(resolver)) == 1


@weaver_test()
def test_an_item_created_after_the_listing_is_still_found():
    workspace = given_workspace(workspace="Demo")
    client = CreatingInventoryClient("Demo", [("Warehouse", "Sales")])
    resolver = FabricResolver(workspace, client=client)
    resolver.discover_once()
    client.items.append(("SemanticModel", "Reporting"))
    found = resolver.resolve(ItemRef("Reporting"), item_type="SemanticModel")
    assert found.id == _identifier("SemanticModel", "Reporting")
    resolver.discover_once()
    assert len([p for p in client.requested if p.endswith("/items")]) == 1


@weaver_test()
def test_the_in_fabric_resolver_lists_the_workspace_once_for_a_batch():
    from weaver.fabric import FabricSessionResolver
    from weaver.workspaces import Workspace

    runtime = SimpleNamespace(
        context={
            "currentWorkspaceName": "Analytics",
            "currentWorkspaceId": _identifier("workspace", "Analytics"),
        }
    )
    client = InventoryClient(
        "Analytics",
        [("Warehouse", "Sales"), ("Warehouse", "Stock"), ("Lakehouse", "Raw")],
    )
    resolver = FabricSessionResolver(
        Workspace(workspace="Analytics"),
        runtime=runtime,
        lakehouse=SimpleNamespace(),
        client=client,
    )
    resolved, faults = check_installations(
        [
            row(resolver, "Warehouse", "Orders", "Sales"),
            row(resolver, "Warehouse", "Items", "Stock"),
            row(resolver, "Lakehouse", "Landing", "Raw"),
        ],
        resolver=resolver,
    )
    assert len(resolved) == 3 and not faults
    assert client.requested == [f"workspaces/{resolver.workspace.id}/items"]
