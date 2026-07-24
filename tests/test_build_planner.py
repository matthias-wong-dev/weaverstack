"""Generating a Lakehouse-only bundle from the build-lakehouse repository.

Generation is pure — read the repository once, project, generate definitions,
certify the snapshot — so this needs no Spark. It pins the plan the vertical
slice installs: the sequence order Folder -> Delta -> view -> view, one bound
Lakehouse target, per-action payloads that exist and hash, a shipped snapshot,
and no Warehouse target or T-SQL executor.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from weaver import FolderTarget, ItemRef, LocalHost, LocalResolver, LocalStore, Location
from weaver.build_bundle import (
    LakehouseBinding,
    TargetBindings,
    generate_build_bundle,
    load_bundle,
)

FIXTURE = Path(__file__).parent / "fixtures" / "build-lakehouse"


@pytest.fixture
def weaver_lakehouse(tmp_path):
    host = LocalHost(root=tmp_path, weaver_lakehouse="Weaver")
    store = LocalStore()
    resolver = LocalResolver(host)
    for item in ("Weaver", "Sales_LH"):
        store.make_directory(resolver.files_root(ItemRef(item)))
        store.make_directory(resolver.tables_root(ItemRef(item)))
    store.make_directory(resolver.repos_root)
    shutil.copytree(FIXTURE, (resolver.repos_root / "MyRepo").path)
    return host, store, resolver


def _generate(weaver_lakehouse, tmp_path):
    host, store, resolver = weaver_lakehouse
    bindings = TargetBindings(lakehouse=LakehouseBinding(lakehouse=ItemRef("Sales_LH")))
    output = Location(str(tmp_path / "bundle"))
    return generate_build_bundle(
        weaver_lakehouse=ItemRef("Weaver"),
        repository_name="MyRepo",
        targets=bindings,
        output=output,
        host=host,
        store=store,
    ), store, output


def _sequence_of(plan):
    """resource_node_id -> the sequence number it is built in."""

    return {
        action.resource_node_id: sequence.number
        for sequence, _, action in plan.actions()
        if action.resource_node_id is not None
    }


def test_generated_plan_has_the_expected_shape(weaver_lakehouse, tmp_path):
    bundle, _, _ = _generate(weaver_lakehouse, tmp_path)
    plan = bundle.plan

    assert plan.format_version == 1
    assert plan.repository_name == "MyRepo"
    assert plan.repository_signature
    assert len(plan.targets) == 1
    target = plan.targets[0]
    assert (target.kind, target.host_kind, target.item_id) == ("lakehouse", "local", "Sales_LH")
    assert plan.omitted_nodes == ()


def test_no_warehouse_target_or_tsql_executor(weaver_lakehouse, tmp_path):
    bundle, _, _ = _generate(weaver_lakehouse, tmp_path)
    plan = bundle.plan

    assert all(target.kind == "lakehouse" for target in plan.targets)
    executors = {action.executor for _, _, action in plan.actions()}
    assert executors <= {"spark_sql", "folder", "prune"}


def test_build_order_is_folder_then_delta_then_view_on_view(weaver_lakehouse, tmp_path):
    bundle, _, _ = _generate(weaver_lakehouse, tmp_path)
    order = _sequence_of(bundle.plan)

    assert order["folder:Raw.CustomerCsv"] < order["delta:DWG.Customer"]
    assert order["delta:DWG.Customer"] < order["delta:DWG.ActiveCustomer"]
    assert order["delta:DWG.ActiveCustomer"] < order["delta:DWG.ActiveCustomerSummary"]


def test_each_object_action_uses_the_right_executor(weaver_lakehouse, tmp_path):
    bundle, _, _ = _generate(weaver_lakehouse, tmp_path)
    executor = {
        action.resource_node_id: action.executor
        for _, _, action in bundle.plan.actions()
        if action.resource_node_id is not None
    }

    assert executor["folder:Raw.CustomerCsv"] == "folder"
    assert executor["delta:DWG.Customer"] == "spark_sql"
    assert executor["delta:DWG.ActiveCustomer"] == "spark_sql"
    assert executor["delta:DWG.ActiveCustomerSummary"] == "spark_sql"


def test_a_clean_target_needs_no_prune(weaver_lakehouse, tmp_path):
    bundle, _, _ = _generate(weaver_lakehouse, tmp_path)
    plan = bundle.plan

    # Nothing in the target to reconcile, so no prune sequence — schemas first.
    assert plan.sequences[0].number == 20
    assert not any(a.kind.startswith("prune") for _, _, a in plan.actions())
    # Only schemas holding a table or view get a database — Raw is folder-only.
    created = {action.id for _, _, action in plan.actions() if action.kind == "create_schema"}
    assert created == {"schema-DWG"}


def test_prune_freezes_a_drop_for_each_physical_orphan(weaver_lakehouse, tmp_path):
    _, store, resolver = weaver_lakehouse
    target = ItemRef("Sales_LH")
    # Seed unmanaged content in the target's storage, before the build inspects it.
    store.make_directory(resolver.tables_root(target).join("DWG", "Ghost"))     # orphan table, managed schema
    store.make_directory(resolver.tables_root(target).join("Legacy", "Thing"))  # orphan schema
    store.make_directory(resolver.folder_object(FolderTarget(lakehouse=target), "Raw", "OldFolder"))

    bundle, _, output = _generate(weaver_lakehouse, tmp_path)
    plan = bundle.plan

    prune = plan.sequences[0]
    assert (prune.number, prune.description) == (10, "prune unmanaged objects")
    # No prune_view — this build had no Spark session to read the catalog.
    assert {a.kind for _, _, a in plan.actions() if a.kind.startswith("prune")} == {
        "prune_table", "prune_schema", "prune_folder"
    }

    # The drops are frozen, and readable up front.
    frozen = {
        a.id: store.read(output.join(*a.payload.split("/"))).decode()
        for _, _, a in plan.actions()
        if a.payload is not None and a.kind.startswith("prune")
    }
    assert "DROP TABLE IF EXISTS `DWG`.`Ghost`" in frozen["prune-table-DWG.Ghost"]
    assert "DROP DATABASE IF EXISTS `Legacy` CASCADE" in frozen["prune-schema-Legacy"]
    # The unmanaged folder is a directory-removing action, identified by resource.
    prune_folders = [a for _, _, a in plan.actions() if a.kind == "prune_folder"]
    assert {a.resource_node_id for a in prune_folders} == {"folder:Raw.OldFolder"}


def test_payload_bearing_actions_reference_an_existing_payload(weaver_lakehouse, tmp_path):
    bundle, store, output = _generate(weaver_lakehouse, tmp_path)

    for _, _, action in bundle.plan.actions():
        if action.payload is None:
            # Only folder and prune actions are payload-less.
            assert action.executor in {"folder", "prune"}
            continue
        assert store.exists(output.join(*action.payload.split("/")))
        if action.resource_node_id is not None:
            assert action.resource_node_id.split(":", 1)[1] in action.payload


def test_snapshot_is_shipped_in_the_bundle(weaver_lakehouse, tmp_path):
    bundle, store, output = _generate(weaver_lakehouse, tmp_path)

    for relative in (
        "Raw__CustomerCsv.py",
        "DWG__Customer.py",
        "DWG.ActiveCustomer.spark.sql",
        "_schemas/DWG.yml",
        "data/customers.csv",
    ):
        assert store.exists(output.join("repository", *relative.split("/")))


def test_generated_bundle_reloads_and_validates(weaver_lakehouse, tmp_path):
    bundle, store, output = _generate(weaver_lakehouse, tmp_path)

    reloaded = load_bundle(output, store=store)
    assert reloaded.plan == bundle.plan
    assert reloaded.bundle_id == bundle.bundle_id


def test_generation_is_deterministic(weaver_lakehouse, tmp_path):
    first, _, _ = _generate(weaver_lakehouse, tmp_path / "a")
    second, _, _ = _generate(weaver_lakehouse, tmp_path / "b")

    assert first.bundle_id == second.bundle_id
    assert first.plan == second.plan
