"""Failure and boundary paths in generation — no Spark needed.

A build targets one physical side at a time: a Lakehouse-only binding yields a
coherent Lakehouse plan with the Warehouse leaf transparently omitted; a
Warehouse-only binding yields a T-SQL plan with the Lakehouse objects omitted;
binding both at once is refused until cross-database chaining lands; and the
planner must never invent a schema action for a schema no resource declares.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from weaver import ItemRef, LocalHost, LocalResolver, LocalStore, Location
from weaver.build_bundle import (
    LakehouseBinding,
    TargetBindings,
    WarehouseBinding,
    generate_build_bundle,
)
from weaver.build_bundle.models import OMIT_TARGET_UNBOUND

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


def _generate(warehouse_repo, targets, *, prune=True):
    host, store, resolver, tmp_path = warehouse_repo
    return generate_build_bundle(
        weaver_lakehouse=ItemRef("Weaver"),
        repository_name="WhRepo",
        targets=targets,
        output=Location(str(tmp_path / "bundle")),
        host=host,
        store=store,
        prune=prune,
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


def test_binding_both_sides_at_once_is_refused(warehouse_repo):
    from weaver.errors import BuildError

    targets = TargetBindings(
        lakehouse=LakehouseBinding(lakehouse=ItemRef("Sales_LH")),
        warehouse=WarehouseBinding(warehouse=ItemRef("Sales_WH")),
    )
    with pytest.raises(BuildError, match="separate calls"):
        _generate(warehouse_repo, targets)


def test_warehouse_only_builds_the_tsql_objects_and_omits_the_lakehouse(warehouse_repo):
    bundle = _generate(
        warehouse_repo,
        TargetBindings(warehouse=WarehouseBinding(warehouse=ItemRef("Sales_WH"))),
        prune=False,
    )
    plan = bundle.plan

    # The one Warehouse target, and the Warehouse object built through the T-SQL
    # executor; the Lakehouse objects are omitted as unbound.
    assert [t.kind for t in plan.targets] == ["warehouse"]
    built = {a.resource_node_id for _, _, a in plan.actions() if a.resource_node_id}
    assert built == {"sql:Reporting.CustomerReport"}
    executors = {a.executor for _, _, a in plan.actions()}
    assert executors == {"tsql"}

    omitted = {node.node_id: node.reason for node in plan.omitted_nodes}
    assert omitted == {
        "folder:Raw.CustomerCsv": OMIT_TARGET_UNBOUND,
        "delta:DWG.Customer": OMIT_TARGET_UNBOUND,
    }

    # The Warehouse schema is created through a plain T-SQL CREATE SCHEMA.
    schema_actions = [a for _, _, a in plan.actions() if a.kind == "create_schema"]
    assert [a.id for a in schema_actions] == ["schema-Reporting"]
    schema_sql = bundle.location.join(*schema_actions[0].payload.split("/"))
    body = warehouse_repo[1].read(schema_sql).decode("utf-8")
    assert "create schema [Reporting]" in body


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
    # Only DWG holds a table; Raw is folder-only (no database) and Reporting is
    # declared but used solely by the omitted Warehouse leaf — neither is created.
    assert created == {"DWG"}
