"""Pure-Python manifest tests for coordinated logical-item builds."""

from __future__ import annotations

import json
import shutil

import pytest

from weaver import ItemRef, LocalStore, Location
from weaver.build_bundle import (
    InstallationEnvironment,
    ItemBinding,
    ItemBindings,
    LakehouseBinding,
    WarehouseBinding,
    generate_item_build_bundle as _generate_item_build_bundle,
    install_bundle,
    load_bundle,
)
from weaver.errors import BuildError
from weaver.declaration import parse_item_repository
from weaver.declaration.model import WeaverItemId
from weaver.build_bundle.prune import (
    TargetInventory,
    read_lakehouse_inventory,
    read_warehouse_inventory,
)
from weaver.catalogue.state import Catalogue

from test_item_dependencies import _dependency_estate
from test_item_repository import _estate, _folder, _schema, _write


class _AliasInventory:
    """A Warehouse already holding an alias view and one genuine orphan."""

    def query(self, statement):
        if "from sys.objects" in statement:
            return [
                {
                    "schema_name": "Sales",
                    "object_name": "PortableCustomer",
                    "object_type": "V",
                },
                {"schema_name": "Sales", "object_name": "Ghost", "object_type": "U"},
            ]
        return [{"name": "Sales"}]


def _binding(logical: str, physical: str):
    item = WeaverItemId.parse(logical)
    if item.item_type == "Lakehouse":
        target = LakehouseBinding(ItemRef(physical))
    else:
        target = WarehouseBinding(ItemRef(physical))
    return ItemBinding(item, target)


def _repository(root):
    return parse_item_repository(Location(str(root)))


def _stage(bundle, description):
    """One barrier, by what it says it is.

    Sequence numbers are a consequence of the assembled plan now, so a test that
    named one would be asserting arithmetic rather than order.
    """

    return next(
        sequence
        for sequence in bundle.plan.sequences
        if sequence.description == description
    )


def _stages(bundle, description):
    return tuple(
        sequence
        for sequence in bundle.plan.sequences
        if sequence.description == description
    )


def generate_item_build_bundle(repository, **kwargs):
    """Test adapter that prepares state before exercising the pure planner."""

    bindings = kwargs["bindings"]
    resolver = kwargs.pop("resolver", None)
    spark = kwargs.pop("spark", None)
    sql_by_item = kwargs.pop("sql_by_item", {})
    kwargs.pop("workspace", None)
    inventories = dict(kwargs.pop("target_inventories", {}))
    for binding in bindings.entries:
        target = binding.to_bound_target()
        if binding.item in inventories:
            continue
        if binding.item in sql_by_item:
            inventories[binding.item] = read_warehouse_inventory(
                target, sql=sql_by_item[binding.item]
            )
        elif resolver is not None:
            inventories[binding.item] = read_lakehouse_inventory(
                target,
                resolver=resolver,
                store=kwargs["store"],
                spark=spark,
            )
        else:
            inventories[binding.item] = TargetInventory(
                target_id=target.id,
                kind=target.kind,
                target_name=target.name,
            )
    kwargs.setdefault("target_inventories", inventories)
    kwargs.setdefault("catalogue", Catalogue({}))
    kwargs.setdefault(
        "control_lakehouse", LakehouseBinding(ItemRef("Weaver_Control"))
    )
    return _generate_item_build_bundle(repository, **kwargs)


def test_one_bundle_coordinates_multiple_typed_items(tmp_path):
    repository = _repository(_estate(tmp_path))
    bundle = generate_item_build_bundle(
        repository,
        bindings=ItemBindings(
            (
                _binding("Lakehouse/Raw", "Raw_Dev"),
                _binding("Warehouse/Audit", "Audit_Dev"),
            )
        ),
        output=Location(str(tmp_path / "bundle")),
        store=LocalStore(),
    )

    assert {
        (target.logical_item_type, target.logical_item_name)
        for target in bundle.plan.targets
        if target.logical_item_type is not None
    } == {
        ("Lakehouse", "Raw"),
        ("Warehouse", "Audit"),
    }
    assert any(len(sequence.batches) == 2 for sequence in bundle.plan.sequences)
    assert all(batch.target_id in bundle.plan.target_ids for sequence in bundle.plan.sequences for batch in sequence.batches)


def test_same_physical_item_cannot_be_bound_twice(tmp_path):
    with pytest.raises(BuildError, match="physical Lakehouse target is bound more than once"):
        ItemBindings(
            (
                _binding("Lakehouse/Raw", "Shared"),
                _binding("Lakehouse/Curated", "Shared"),
            )
        )


def test_at_least_one_binding_is_required(tmp_path):
    repository = _repository(_estate(tmp_path))
    with pytest.raises(BuildError, match="at least one Weaver item"):
        generate_item_build_bundle(
            repository,
            bindings=ItemBindings(()),
            output=Location(str(tmp_path / "bundle")),
            store=LocalStore(),
        )


def test_alias_to_an_unbound_source_item_is_omitted_with_its_reason(tmp_path):
    """An alias needs a bound source: there is otherwise nothing to point at.

    Bindings are deliberately sparse, so this is an omission rather than an
    error — and a stated one, because the planner is the only thing allowed to
    decide an alias has no physical form.
    """

    repository = _repository(_dependency_estate(tmp_path))
    bundle = generate_item_build_bundle(
        repository,
        bindings=ItemBindings((_binding("Warehouse/Reporting", "Reporting_Dev"),)),
        output=Location(str(tmp_path / "bundle")),
        store=LocalStore(),
    )

    omitted = {
        node.node_id: node
        for node in bundle.plan.omitted_nodes
        if node.reason == "alias_unsupported"
    }
    assert set(omitted) == {"alias:Warehouse/Reporting/Sales.PortableCustomer"}
    assert "Lakehouse/Curated is not bound" in omitted[
        "alias:Warehouse/Reporting/Sales.PortableCustomer"
    ].detail
    assert not any(
        action.kind == "create_alias" for _s, _b, action in bundle.plan.actions()
    )

    # And it is not certified either. A Registry row means the object's work
    # succeeded; here no work was even planned, so a row would claim an
    # installation that never happened.
    registry = next(
        action
        for _s, _b, action in bundle.plan.actions()
        if action.kind == "publish_registry"
    )
    payload = LocalStore().read(
        bundle.location.join(*registry.payload.split("/"))
    ).decode()
    assert "PortableCustomer" not in payload


def test_warehouse_alias_is_a_view_over_the_bound_source(tmp_path):
    repository = _repository(_dependency_estate(tmp_path))
    store = LocalStore()
    bundle = generate_item_build_bundle(
        repository,
        bindings=ItemBindings(
            (
                _binding("Lakehouse/Curated", "Curated_Dev"),
                _binding("Warehouse/Reporting", "Reporting_Dev"),
            )
        ),
        output=Location(str(tmp_path / "bundle")),
        store=store,
    )

    alias = next(
        action
        for _s, _b, action in bundle.plan.actions()
        if action.kind == "create_alias"
    )
    # One action for the item's aliases, and each statement its own batch —
    # T-SQL will not accept a CREATE VIEW that is not first in its batch.
    assert alias.executor == "tsql_batch"
    assert alias.id == "aliases-Warehouse--Reporting"
    statements = json.loads(
        store.read(bundle.location.join(*alias.payload.split("/"))).decode()
    )
    assert statements == [
        "create or alter view [Sales].[PortableCustomer] as select * from "
        "[Curated_Dev].[Sales].[Customer];"
    ]
    assert not bundle.plan.omitted_nodes or all(
        node.reason != "alias_unsupported" for node in bundle.plan.omitted_nodes
    )


def test_lakehouse_alias_freezes_both_addresses_by_target_id(tmp_path):
    root = _dependency_estate(tmp_path)
    _write(
        root,
        "Lakehouse/Curated/alias.yml",
        "aliases:\n  Sales.Landed: Lakehouse/Raw/Sales.Customer\n",
    )
    repository = _repository(root)
    store = LocalStore()
    bundle = generate_item_build_bundle(
        repository,
        bindings=ItemBindings(
            (
                _binding("Lakehouse/Raw", "Raw_Dev"),
                _binding("Lakehouse/Curated", "Curated_Dev"),
            )
        ),
        output=Location(str(tmp_path / "bundle")),
        store=store,
    )

    alias = next(
        action
        for _s, _b, action in bundle.plan.actions()
        if action.kind == "create_alias"
    )
    assert alias.executor == "alias"
    frozen = json.loads(
        store.read(bundle.location.join(*alias.payload.split("/"))).decode()
    )
    assert len(frozen["aliases"]) == 1
    assert frozen["aliases"][0] == {
        "alias": "Lakehouse/Curated/Sales.Landed",
        "area": "Tables",
        "object": "Landed",
        "schema": "Sales",
        "source": "Lakehouse/Raw/Sales.Customer",
        "source_area": "Tables",
        "source_object": "Customer",
        "source_schema": "Sales",
        "source_target_id": "Lakehouse-Raw--lakehouse-Raw_Dev",
    }


def test_an_alias_is_materialised_before_the_documents_that_use_it(tmp_path):
    repository = _repository(_dependency_estate(tmp_path))
    bundle = generate_item_build_bundle(
        repository,
        bindings=ItemBindings(
            (
                _binding("Lakehouse/Curated", "Curated_Dev"),
                _binding("Warehouse/Reporting", "Reporting_Dev"),
            )
        ),
        output=Location(str(tmp_path / "bundle")),
        store=LocalStore(),
    )

    at = {
        action.id: sequence.number for sequence, _batch, action in bundle.plan.actions()
    }
    # The source item produces the table, its endpoint catches up, and only then
    # does the consuming item's alias — and the document reading it — exist.
    assert (
        at["object-Lakehouse--Curated--Sales.Customer"]
        < at["refresh-sql-endpoint-Lakehouse--Curated"]
        < at["aliases-Warehouse--Reporting"]
        < at["object-Warehouse--Reporting--Sales.Customer"]
    )


def test_an_items_schemas_are_created_before_its_aliases(tmp_path):
    repository = _repository(_dependency_estate(tmp_path))
    bundle = generate_item_build_bundle(
        repository,
        bindings=ItemBindings(
            (
                _binding("Lakehouse/Curated", "Curated_Dev"),
                _binding("Warehouse/Reporting", "Reporting_Dev"),
            )
        ),
        output=Location(str(tmp_path / "bundle")),
        store=LocalStore(),
    )

    at = {
        action.id: sequence.number for sequence, _batch, action in bundle.plan.actions()
    }
    assert (
        at["schema-Warehouse--Reporting-Sales"]
        < at["aliases-Warehouse--Reporting"]
    )


def test_an_alias_destination_is_not_pruned_as_an_orphan(tmp_path):
    repository = _repository(_dependency_estate(tmp_path))
    item = WeaverItemId.parse("Warehouse/Reporting")
    bundle = generate_item_build_bundle(
        repository,
        bindings=ItemBindings(
            (
                _binding("Lakehouse/Curated", "Curated_Dev"),
                _binding(str(item), "Reporting_Dev"),
            )
        ),
        output=Location(str(tmp_path / "bundle")),
        store=LocalStore(),
        sql_by_item={item: _AliasInventory()},
    )

    pruned = {
        action.id
        for _s, _b, action in bundle.plan.actions()
        if action.kind.startswith("prune")
    }
    assert "Warehouse--Reporting-prune-view-Sales.PortableCustomer" not in pruned
    assert "Warehouse--Reporting-prune-table-Sales.Ghost" in pruned


def test_authored_three_part_name_is_preserved_in_payload(tmp_path):
    repository = _repository(_dependency_estate(tmp_path))
    bundle = generate_item_build_bundle(
        repository,
        bindings=ItemBindings((_binding("Warehouse/Audit", "Audit_Dev"),)),
        output=Location(str(tmp_path / "bundle")),
        store=LocalStore(),
    )
    payloads = [
        LocalStore().read(bundle.location.join(*action.payload.split("/"))).decode()
        for _, _, action in bundle.plan.actions()
        if action.payload and action.kind == "build_table"
    ]
    assert any("Raw_LH.Sales.Customer" in payload for payload in payloads)


def test_bundle_identity_is_deterministic_for_same_repository_and_bindings(tmp_path):
    repository = _repository(_estate(tmp_path))
    bindings = ItemBindings((_binding("Lakehouse/Raw", "Raw_Dev"),))
    first = generate_item_build_bundle(
        repository,
        bindings=bindings,
        output=Location(str(tmp_path / "first")),
        store=LocalStore(),
    )
    second = generate_item_build_bundle(
        repository,
        bindings=bindings,
        output=Location(str(tmp_path / "second")),
        store=LocalStore(),
    )
    assert first.bundle_id == second.bundle_id


class _NoopExecutor:
    def execute(self, action, payload, context):
        return {"ran": action.id}


class _Resolver:
    def lakehouse_spark_location(self, item):
        return None

    def spark_destination(self, item):
        return None


def test_installer_never_reopens_or_interprets_source_repository(tmp_path):
    root = _estate(tmp_path)
    repository = _repository(root)
    store = LocalStore()
    bundle = generate_item_build_bundle(
        repository,
        bindings=ItemBindings((_binding("Lakehouse/Raw", "Raw_Dev"),)),
        output=Location(str(tmp_path / "bundle")),
        store=store,
    )
    shutil.rmtree(root)
    reloaded = load_bundle(bundle.location, store=store)
    noop = _NoopExecutor()
    environment = InstallationEnvironment(
        store=store,
        resolver=_Resolver(),
        executors={
            "spark_schema": noop,
            "spark_sql": noop,
            "spark_sql_batch": noop,
            "spark_table": noop,
            "folder": noop,
            "alias": noop,
            "tsql_batch": noop,
            "sql_endpoint_refresh": noop,
        },
    )
    report = install_bundle(reloaded, environment=environment)
    assert report.status == "succeeded"


def test_item_prune_reconciles_tables_and_files_owned_by_one_lakehouse_item(
    tmp_path, lakehouses
):
    repository = _repository(_estate(tmp_path))
    tables = lakehouses.resolver.tables_root(lakehouses.target)
    files = lakehouses.resolver.files_root(lakehouses.target)
    for relative in (
        tables / "Sales" / "Customer",
        tables / "Sales" / "Ghost",
        files / "Sales" / "Customer",
        files / "Sales" / "OldFolder",
    ):
        lakehouses.store.make_directory(relative)

    bundle = generate_item_build_bundle(
        repository,
        bindings=ItemBindings((_binding("Lakehouse/Raw", lakehouses.target.name),)),
        output=Location(str(tmp_path / "bundle")),
        store=lakehouses.store,
        resolver=lakehouses.resolver,
    )

    prune = _stage(bundle, "prune unmanaged objects by logical item")
    assert {action.kind for batch in prune.batches for action in batch.actions} == {
        "prune_table",
        "prune_folder",
    }
    assert {
        action.resource_node_id
        for batch in prune.batches
        for action in batch.actions
        if action.kind == "prune_folder"
    } == {"folder:Sales.OldFolder"}
    assert all("Customer" not in action.id for batch in prune.batches for action in batch.actions)


def test_item_prune_is_the_default_and_false_is_the_explicit_escape_hatch(
    tmp_path, lakehouses
):
    repository = _repository(_estate(tmp_path))
    lakehouses.store.make_directory(
        lakehouses.resolver.tables_root(lakehouses.target) / "Sales" / "Ghost"
    )
    binding = ItemBindings((_binding("Lakehouse/Raw", lakehouses.target.name),))

    reconciled = generate_item_build_bundle(
        repository,
        bindings=binding,
        output=Location(str(tmp_path / "reconciled")),
        store=lakehouses.store,
        resolver=lakehouses.resolver,
    )
    jammed = generate_item_build_bundle(
        repository,
        bindings=binding,
        output=Location(str(tmp_path / "jammed")),
        store=lakehouses.store,
    )

    assert any(action.kind.startswith("prune") for _s, _b, action in reconciled.plan.actions())
    assert not any(action.kind.startswith("prune") for _s, _b, action in jammed.plan.actions())


def test_two_same_type_items_have_independent_prune_batches(tmp_path, lakehouses):
    repository = _repository(_estate(tmp_path))
    second = ItemRef("Curated_Dev")
    for target, orphan in ((lakehouses.target, "RawGhost"), (second, "CuratedGhost")):
        lakehouses.store.make_directory(lakehouses.resolver.files_root(target))
        lakehouses.store.make_directory(lakehouses.resolver.tables_root(target) / "Sales" / orphan)

    bundle = generate_item_build_bundle(
        repository,
        bindings=ItemBindings(
            (
                _binding("Lakehouse/Raw", lakehouses.target.name),
                _binding("Lakehouse/Curated", second.name),
            )
        ),
        output=Location(str(tmp_path / "bundle")),
        store=lakehouses.store,
        resolver=lakehouses.resolver,
    )

    prune = _stage(bundle, "prune unmanaged objects by logical item")
    assert len(prune.batches) == 2
    by_target = {
        batch.target_id: {action.id for action in batch.actions}
        for batch in prune.batches
    }
    assert any("Lakehouse--Raw-prune-table-Sales.RawGhost" in ids for ids in by_target.values())
    assert any(
        "Lakehouse--Curated-prune-table-Sales.CuratedGhost" in ids
        for ids in by_target.values()
    )


def test_rebinding_prune_has_no_opinion_about_the_old_physical_item(tmp_path, lakehouses):
    repository = _repository(_estate(tmp_path))
    old = ItemRef("Raw_Old")
    new = ItemRef("Raw_New")
    for target in (old, new):
        lakehouses.store.make_directory(lakehouses.resolver.files_root(target))
        lakehouses.store.make_directory(
            lakehouses.resolver.tables_root(target) / "Sales" / "Ghost"
        )

    bundle = generate_item_build_bundle(
        repository,
        bindings=ItemBindings((_binding("Lakehouse/Raw", new.name),)),
        output=Location(str(tmp_path / "bundle")),
        store=lakehouses.store,
        resolver=lakehouses.resolver,
    )

    prune_targets = {
        batch.target_id
        for sequence in _stages(bundle, "prune unmanaged objects by logical item")
        for batch in sequence.batches
    }
    assert prune_targets == {"Lakehouse-Raw--lakehouse-Raw_New"}
    assert lakehouses.store.exists(
        lakehouses.resolver.tables_root(old) / "Sales" / "Ghost"
    )


# --- item order is the outer build structure ---------------------------------


def test_sequence_numbers_describe_the_assembled_order_and_nothing_else(tmp_path):
    repository = _repository(_estate(tmp_path))
    bundle = generate_item_build_bundle(
        repository,
        bindings=ItemBindings(
            (
                _binding("Lakehouse/Raw", "Raw_Dev"),
                _binding("Warehouse/Audit", "Audit_Dev"),
            )
        ),
        output=Location(str(tmp_path / "bundle")),
        store=LocalStore(),
    )

    numbers = [sequence.number for sequence in bundle.plan.sequences]
    assert numbers == list(range(1, len(numbers) + 1))


def test_a_consumer_items_whole_group_follows_its_producers(tmp_path):
    """The invariant multi-item build rests on, stated as barriers.

    ``Warehouse/Reporting`` reaches into ``Lakehouse/Curated`` through an alias,
    so nothing of Reporting's may share a barrier with — let alone precede — any
    of Curated's.
    """

    repository = _repository(_dependency_estate(tmp_path))
    bundle = generate_item_build_bundle(
        repository,
        bindings=ItemBindings(
            (
                _binding("Lakehouse/Curated", "Curated_Dev"),
                _binding("Warehouse/Reporting", "Reporting_Dev"),
            )
        ),
        output=Location(str(tmp_path / "bundle")),
        store=LocalStore(),
    )

    numbers = {"Curated": set(), "Reporting": set()}
    for sequence, batch, _action in bundle.plan.actions():
        for item in numbers:
            if item in batch.target_id:
                numbers[item].add(sequence.number)

    assert numbers["Curated"] and numbers["Reporting"]
    assert max(numbers["Curated"]) < min(numbers["Reporting"])


def test_independent_items_share_their_barriers(tmp_path):
    """Nothing orders two items in one layer, so they are not serialised."""

    repository = _repository(_estate(tmp_path))
    bundle = generate_item_build_bundle(
        repository,
        bindings=ItemBindings(
            (
                _binding("Lakehouse/Raw", "Raw_Dev"),
                _binding("Lakehouse/Curated", "Curated_Dev"),
            )
        ),
        output=Location(str(tmp_path / "bundle")),
        store=LocalStore(),
    )

    shared = _stage(bundle, "build dependency layer")
    assert {batch.target_id for batch in shared.batches} == {
        "Lakehouse-Raw--lakehouse-Raw_Dev",
        "Lakehouse-Curated--lakehouse-Curated_Dev",
    }


# --- the endpoint refresh at the item boundary --------------------------------


def _refreshed(bundle):
    return {
        batch.target_id
        for sequence, batch, action in bundle.plan.actions()
        if action.kind == "refresh_sql_endpoint"
    }


def test_a_lakehouse_item_that_mutated_delta_is_closed_by_a_refresh(tmp_path):
    repository = _repository(_estate(tmp_path))
    bundle = generate_item_build_bundle(
        repository,
        bindings=ItemBindings((_binding("Lakehouse/Raw", "Raw_Dev"),)),
        output=Location(str(tmp_path / "bundle")),
        store=LocalStore(),
    )

    refresh = _stage(bundle, "refresh mutated Lakehouse SQL endpoints")
    build = _stage(bundle, "build dependency layer")
    assert refresh.number > build.number
    assert _refreshed(bundle) >= {"Lakehouse-Raw--lakehouse-Raw_Dev"}


def test_a_warehouse_item_has_no_endpoint_of_its_own_to_refresh(tmp_path):
    repository = _repository(_estate(tmp_path))
    bundle = generate_item_build_bundle(
        repository,
        bindings=ItemBindings((_binding("Warehouse/Audit", "Audit_Dev"),)),
        output=Location(str(tmp_path / "bundle")),
        store=LocalStore(),
    )

    # Only the control Lakehouse's own refresh, after catalogue publication.
    assert _refreshed(bundle) == {"control-lakehouse-Weaver_Control"}


def test_an_item_whose_only_work_is_folders_needs_no_refresh(tmp_path):
    """A Folder is a directory in Files. The SQL endpoint describes tables."""

    root = tmp_path / "Estate"
    _write(root, "Lakehouse/Raw/schemas/Sales.yml", _schema("Sales"))
    _write(root, "Lakehouse/Raw/Files/Sales__Landing.py", _folder("Sales.Landing"))
    repository = _repository(root)
    bundle = generate_item_build_bundle(
        repository,
        bindings=ItemBindings((_binding("Lakehouse/Raw", "Raw_Dev"),)),
        output=Location(str(tmp_path / "bundle")),
        store=LocalStore(),
    )

    assert _refreshed(bundle) == {"control-lakehouse-Weaver_Control"}
    assert any(action.kind == "build_folder" for _s, _b, action in bundle.plan.actions())


class _WarehouseInventory:
    def query(self, statement):
        if "from sys.objects" in statement:
            return [
                {"schema_name": "Sales", "object_name": "Change", "object_type": "U"},
                {"schema_name": "Sales", "object_name": "Ghost", "object_type": "U"},
            ]
        return [{"name": "Sales"}]


def test_warehouse_item_prune_uses_its_item_owned_keep_set(tmp_path):
    repository = _repository(_estate(tmp_path))
    item = WeaverItemId.parse("Warehouse/Audit")
    bundle = generate_item_build_bundle(
        repository,
        bindings=ItemBindings((_binding(str(item), "Audit_Dev"),)),
        output=Location(str(tmp_path / "bundle")),
        store=LocalStore(),
        sql_by_item={item: _WarehouseInventory()},
    )

    actions = [
        action
        for sequence, _batch, action in bundle.plan.actions()
        if sequence.description == "prune unmanaged objects by logical item"
    ]
    assert [action.kind for action in actions] == ["prune_table"]
    assert actions[0].id == "Warehouse--Audit-prune-table-Sales.Ghost"


def test_catalogue_tail_is_item_scoped_and_registry_is_last(tmp_path):
    repository = _repository(_estate(tmp_path))
    bundle = generate_item_build_bundle(
        repository,
        bindings=ItemBindings(
            (
                _binding("Lakehouse/Raw", "Raw_Dev"),
                _binding("Warehouse/Audit", "Audit_Dev"),
            )
        ),
        output=Location(str(tmp_path / "bundle")),
        store=LocalStore(),
        control_lakehouse=LakehouseBinding(ItemRef("Weaver_Control")),
    )

    assert [sequence.description for sequence in bundle.plan.sequences[-3:]] == [
        "publish catalogue dictionaries and installations",
        "publish item registry last",
        "refresh the Weaver Lakehouse SQL endpoint after catalogue DML",
    ]
    registry = bundle.plan.sequences[-2]
    assert all(
        action.kind == "publish_registry"
        for batch in registry.batches
        for action in batch.actions
    )
    assert len(registry.batches) == 1
    registry_payloads = [
        LocalStore().read(bundle.location.join(*action.payload.split("/"))).decode()
        for batch in registry.batches
        for action in batch.actions
    ]
    assert "`item_name` = 'Raw'" in registry_payloads[0]
    assert "`item_name` = 'Audit'" in registry_payloads[0]
    control_refresh = bundle.plan.sequences[-1]
    assert [
        (batch.target_id, action.kind)
        for batch in control_refresh.batches
        for action in batch.actions
    ] == [("control-lakehouse-Weaver_Control", "refresh_sql_endpoint")]


def test_each_affected_lakehouse_refreshes_inside_its_own_item_group(tmp_path):
    """The refresh moved from a global tail into each item's group.

    A single barrier after all physical work is correct for one item and wrong the
    moment a second reads the first: the consumer would be built against endpoint
    metadata that had not caught up. So each mutated Lakehouse is closed by its own
    refresh, before anything in a later item layer starts.
    """

    repository = _repository(_estate(tmp_path))
    bundle = generate_item_build_bundle(
        repository,
        bindings=ItemBindings(
            (
                _binding("Lakehouse/Raw", "Raw_Dev"),
                _binding("Lakehouse/Curated", "Curated_Dev"),
                _binding("Warehouse/Audit", "Audit_Dev"),
            )
        ),
        output=Location(str(tmp_path / "bundle")),
        store=LocalStore(),
    )

    refreshed = {
        batch.target_id
        for _sequence, batch, action in bundle.plan.actions()
        if action.kind == "refresh_sql_endpoint"
    }
    # Both Lakehouses, and the control plane after catalogue DML. The Warehouse
    # gets none: it *is* reached over SQL and has no endpoint of its own to sync.
    assert refreshed == {
        "Lakehouse-Raw--lakehouse-Raw_Dev",
        "Lakehouse-Curated--lakehouse-Curated_Dev",
        "control-lakehouse-Weaver_Control",
    }

    at = {
        action.id: sequence.number for sequence, _batch, action in bundle.plan.actions()
    }
    # Each item's refresh closes that item, before the catalogue tail.
    assert (
        at["object-Lakehouse--Raw--Sales.Customer"]
        < at["refresh-sql-endpoint-Lakehouse--Raw"]
        < at["refresh-sql-endpoint-control"]
    )


def test_a_lakehouse_without_delta_mutations_gets_no_refresh(tmp_path):
    root = _estate(tmp_path)
    (root / "Lakehouse/Curated/Sales__Customer.py").unlink()
    repository = _repository(root)
    bundle = generate_item_build_bundle(
        repository,
        bindings=ItemBindings(
            (
                _binding("Lakehouse/Raw", "Raw_Dev"),
                _binding("Lakehouse/Curated", "Curated_Dev"),
            )
        ),
        output=Location(str(tmp_path / "bundle")),
        store=LocalStore(),
    )

    refreshed = {
        batch.target_id
        for _sequence, batch, action in bundle.plan.actions()
        if action.kind == "refresh_sql_endpoint"
    }
    assert "Lakehouse-Curated--lakehouse-Curated_Dev" not in refreshed
    assert "Lakehouse-Raw--lakehouse-Raw_Dev" in refreshed


def test_catalogue_requires_an_explicit_control_plane_target(tmp_path):
    repository = _repository(_estate(tmp_path))
    with pytest.raises(BuildError, match="control-plane Lakehouse"):
        generate_item_build_bundle(
            repository,
            bindings=ItemBindings((_binding("Lakehouse/Raw", "Raw_Dev"),)),
            output=Location(str(tmp_path / "bundle")),
            store=LocalStore(),
            control_lakehouse=None,
        )


def test_builtin_weaver_item_builds_through_the_same_planner(tmp_path):
    repository = _repository(_estate(tmp_path))
    control = LakehouseBinding(ItemRef("Weaver_Control"))
    bundle = generate_item_build_bundle(
        repository,
        bindings=ItemBindings(
            (ItemBinding(WeaverItemId.parse("Lakehouse/_weaver"), control),)
        ),
        output=Location(str(tmp_path / "bundle")),
        store=LocalStore(),
        control_lakehouse=control,
    )

    physical = [
        action
        for _sequence, _batch, action in bundle.plan.actions()
        if action.kind == "build_table"
    ]
    assert len(physical) == 10
    assert (
        bundle.plan.sequences[-1].description
        == "refresh the Weaver Lakehouse SQL endpoint after catalogue DML"
    )
    assert bundle.plan.sequences[-2].description == "publish item registry last"
    assert bundle.plan.targets[0].logical_item_name == "_weaver"
