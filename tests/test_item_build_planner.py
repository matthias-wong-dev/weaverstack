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


def test_item_planner_refuses_old_target_kind_prune(tmp_path):
    repository = _repository(_estate(tmp_path))
    with pytest.raises(BuildError, match="item-scoped prune"):
        generate_item_build_bundle(
            repository,
            bindings=ItemBindings((_binding("Lakehouse/Raw", "Raw_Dev"),)),
            output=Location(str(tmp_path / "bundle")),
            store=LocalStore(),
            prune=True,
        )
