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

from weaver import ItemRef, LocalHost, LocalResolver, LocalStore, Location
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


def test_prune_runs_first_then_schemas(weaver_lakehouse, tmp_path):
    bundle, _, _ = _generate(weaver_lakehouse, tmp_path)
    plan = bundle.plan

    # The target is reconciled before anything is created.
    assert plan.sequences[0].number == 10
    assert plan.sequences[0].description == "prune unmanaged objects"
    assert {a.kind for _, _, a in plan.actions() if a.executor == "prune"} == {
        "prune_views", "prune_delta", "prune_folders", "prune_schemas"
    }
    # Only schemas holding a table or view get a database — Raw is folder-only.
    created = {action.id for _, _, action in plan.actions() if action.kind == "create_schema"}
    assert created == {"schema-DWG"}


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
