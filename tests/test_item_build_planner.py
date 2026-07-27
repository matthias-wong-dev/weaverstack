"""Pure-Python manifest tests for coordinated logical-item builds."""

from __future__ import annotations

import shutil

import pytest

from weaver import ItemRef, LocalStore, Location
from weaver.build_bundle import (
    InstallationEnvironment,
    ItemBinding,
    ItemBindings,
    LakehouseBinding,
    WarehouseBinding,
    generate_item_build_bundle,
    install_bundle,
    load_bundle,
)
from weaver.errors import BuildError
from weaver.ses import read_weaver_repository
from weaver.ses.model import WeaverItemId

from test_item_dependencies import _dependency_estate
from test_item_repository import _estate


def _binding(logical: str, physical: str):
    item = WeaverItemId.parse(logical)
    if item.item_type == "Lakehouse":
        target = LakehouseBinding(ItemRef(physical))
    else:
        target = WarehouseBinding(ItemRef(physical))
    return ItemBinding(item, target)


def _repository(root):
    return read_weaver_repository(Location(str(root)))


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
        prune=False,
    )

    assert {(target.logical_item_type, target.logical_item_name) for target in bundle.plan.targets} == {
        ("Lakehouse", "Raw"),
        ("Warehouse", "Audit"),
    }
    assert any(len(sequence.batches) == 2 for sequence in bundle.plan.sequences)
    assert all(batch.target_id in bundle.plan.target_ids for sequence in bundle.plan.sequences for batch in sequence.batches)


def test_same_physical_item_may_be_bound_twice_with_distinct_manifest_targets(tmp_path):
    repository = _repository(_estate(tmp_path))
    bundle = generate_item_build_bundle(
        repository,
        bindings=ItemBindings(
            (
                _binding("Lakehouse/Raw", "Shared"),
                _binding("Lakehouse/Curated", "Shared"),
            )
        ),
        output=Location(str(tmp_path / "bundle")),
        store=LocalStore(),
        prune=False,
    )

    assert [target.item_id for target in bundle.plan.targets] == ["Shared", "Shared"]
    assert len(bundle.plan.target_ids) == 2


def test_at_least_one_binding_is_required(tmp_path):
    repository = _repository(_estate(tmp_path))
    with pytest.raises(BuildError, match="at least one Weaver item"):
        generate_item_build_bundle(
            repository,
            bindings=ItemBindings(()),
            output=Location(str(tmp_path / "bundle")),
            store=LocalStore(),
            prune=False,
        )


def test_retained_alias_use_fails_before_writing_any_bundle(tmp_path):
    repository = _repository(_dependency_estate(tmp_path))
    output = tmp_path / "bundle"
    with pytest.raises(NotImplementedError, match="Alias usage is not yet supported"):
        generate_item_build_bundle(
            repository,
            bindings=ItemBindings(
                (_binding("Warehouse/Reporting", "Reporting_Dev"),)
            ),
            output=Location(str(output)),
            store=LocalStore(),
            prune=False,
        )
    assert not output.exists()


def test_authored_three_part_name_is_preserved_in_payload(tmp_path):
    repository = _repository(_dependency_estate(tmp_path))
    bundle = generate_item_build_bundle(
        repository,
        bindings=ItemBindings((_binding("Warehouse/Audit", "Audit_Dev"),)),
        output=Location(str(tmp_path / "bundle")),
        store=LocalStore(),
        prune=False,
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
        prune=False,
    )
    second = generate_item_build_bundle(
        repository,
        bindings=bindings,
        output=Location(str(tmp_path / "second")),
        store=LocalStore(),
        prune=False,
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
        prune=False,
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
            "spark_table": noop,
            "folder": noop,
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
        prune=True,
        resolver=lakehouses.resolver,
    )

    prune = bundle.plan.sequences[0]
    assert prune.number == 10
    assert prune.description == "prune unmanaged objects by logical item"
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
        prune=False,
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
        prune=True,
        resolver=lakehouses.resolver,
    )

    prune = bundle.plan.sequences[0]
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
        prune=True,
        resolver=lakehouses.resolver,
    )

    prune_targets = {
        batch.target_id
        for sequence in bundle.plan.sequences
        if sequence.number == 10
        for batch in sequence.batches
    }
    assert prune_targets == {"Lakehouse-Raw--lakehouse-Raw_New"}
    assert lakehouses.store.exists(
        lakehouses.resolver.tables_root(old) / "Sales" / "Ghost"
    )


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
        prune=True,
        sql_by_item={item: _WarehouseInventory()},
    )

    actions = [
        action
        for sequence, _batch, action in bundle.plan.actions()
        if sequence.number == 10
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
        prune=False,
        catalogue=True,
        control_lakehouse=LakehouseBinding(ItemRef("Weaver_Control")),
    )

    assert bundle.plan.sequences[-3].number == 9000
    assert bundle.plan.sequences[-2].number == 9010
    assert bundle.plan.sequences[-1].number == 9020
    assert all(
        action.kind == "publish_registry"
        for batch in bundle.plan.sequences[-1].batches
        for action in batch.actions
    )
    assert len(bundle.plan.sequences[-1].batches) == 2
    registry_payloads = [
        LocalStore().read(bundle.location.join(*action.payload.split("/"))).decode()
        for batch in bundle.plan.sequences[-1].batches
        for action in batch.actions
    ]
    assert any("`item_name` = 'Raw'" in payload for payload in registry_payloads)
    assert any("`item_name` = 'Audit'" in payload for payload in registry_payloads)


def test_catalogue_requires_an_explicit_control_plane_target(tmp_path):
    repository = _repository(_estate(tmp_path))
    with pytest.raises(BuildError, match="control-plane Lakehouse"):
        generate_item_build_bundle(
            repository,
            bindings=ItemBindings((_binding("Lakehouse/Raw", "Raw_Dev"),)),
            output=Location(str(tmp_path / "bundle")),
            store=LocalStore(),
            prune=False,
            catalogue=True,
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
        prune=False,
        catalogue=True,
        control_lakehouse=control,
    )

    physical = [
        action
        for sequence, _batch, action in bundle.plan.actions()
        if sequence.number < 9000 and action.kind == "build_table"
    ]
    assert len(physical) == 10
    assert bundle.plan.sequences[-1].number == 9020
    assert bundle.plan.targets[0].logical_item_name == "_weaver"
