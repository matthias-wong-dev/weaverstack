"""Failure and boundary paths in generation — no Spark needed.

A missing Warehouse binding must yield a coherent Lakehouse plan with the
Warehouse leaf transparently omitted; a *supplied* Warehouse binding for T-SQL
work must raise the explicit v1 boundary rather than silently omit; and the
planner must never invent a schema action for a schema no resource declares.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from weaver import ItemRef, LocalHost, LocalResolver, LocalStore, Location
from weaver.build import (
    LakehouseBinding,
    TargetBindings,
    WarehouseBinding,
    generate_build_bundle,
)
from weaver.build.models import OMIT_TARGET_UNBOUND

WAREHOUSE_FIXTURE = Path(__file__).parent / "fixtures" / "build-lakehouse-warehouse"


@pytest.fixture
def warehouse_repo(tmp_path):
    host = LocalHost(root=tmp_path, weaver_lakehouse="Weaver")
    store = LocalStore()
    resolver = LocalResolver(host)
    for item in ("Weaver", "Sales_LH"):
        store.make_directory(resolver.files_root(ItemRef(item)))
        store.make_directory(resolver.tables_root(ItemRef(item)))
    store.make_directory(resolver.repos_root)
    shutil.copytree(WAREHOUSE_FIXTURE, (resolver.repos_root / "WhRepo").path)
    return host, store, resolver, tmp_path


def _generate(warehouse_repo, targets):
    host, store, resolver, tmp_path = warehouse_repo
    return generate_build_bundle(
        weaver_lakehouse=ItemRef("Weaver"),
        repository_name="WhRepo",
        targets=targets,
        output=Location(str(tmp_path / "bundle")),
        host=host,
        store=store,
    )


def test_lakehouse_only_omits_the_warehouse_leaf_but_stays_coherent(warehouse_repo):
    bundle = _generate(
        warehouse_repo,
        TargetBindings(lakehouse=LakehouseBinding(lakehouse=ItemRef("Sales_LH"))),
    )
    plan = bundle.plan

    built = {a.resource_node_id for _, _, a in plan.actions() if a.resource_node_id}
    assert built == {"folder:Raw.CustomerCsv", "delta:DWG.Customer"}

    omitted = {node.node_id: node.reason for node in plan.omitted_nodes}
    assert omitted == {"sql:Reporting.CustomerReport": OMIT_TARGET_UNBOUND}


def test_supplying_a_warehouse_binding_for_tsql_raises_the_v1_boundary(warehouse_repo):
    targets = TargetBindings(
        lakehouse=LakehouseBinding(lakehouse=ItemRef("Sales_LH")),
        warehouse=WarehouseBinding(warehouse=ItemRef("Sales_WH")),
    )

    with pytest.raises(NotImplementedError, match="Warehouse installation"):
        _generate(warehouse_repo, targets)


def test_only_schemas_used_by_retained_resources_are_created(warehouse_repo):
    bundle = _generate(
        warehouse_repo,
        TargetBindings(lakehouse=LakehouseBinding(lakehouse=ItemRef("Sales_LH"))),
    )

    created = {
        a.id.removeprefix("schema-")
        for _, _, a in bundle.plan.actions()
        if a.kind == "create_schema"
    }
    # Reporting is declared but only the omitted Warehouse leaf uses it, so no
    # schema action is synthesised for it.
    assert created == {"Raw", "DWG"}
