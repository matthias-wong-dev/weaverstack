"""The Warehouse side of the mixed estate, built in its own call.

Bound to the Warehouse alone, the same repository yields a T-SQL plan: the two
Warehouse objects in dependency order, a T-SQL schema create, and the Lakehouse
objects omitted. The report table reads the Lakehouse by hard-coded three-part
name — an external reference, so it carries no SES dependency on the Lakehouse
build and the two sides are produced independently. Behavioural execution runs
against the Play Warehouse under Fabric; here we pin the generated plan and text.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from weaver import ItemRef, LocalHost, LocalResolver, LocalStore, Location, RepositoryRef
from weaver.build_bundle import (
    TargetBindings,
    WarehouseBinding,
    generate_build_bundle,
)

FIXTURE = Path(__file__).parent / "fixtures" / "mixed-estate"

def _physical(plan):
    """Actions that build the destination, excluding the catalogue's own work.

    The catalogue is written to the Weaver Lakehouse through Spark SQL whichever
    side is being built, so a claim about *this* target's executors has to say so.
    """

    from weaver.build_bundle.models import CATALOGUE_KINDS

    return [action for _s, _b, action in plan.actions() if action.kind not in CATALOGUE_KINDS]



@pytest.fixture
def estate(tmp_path):
    host = LocalHost(root=tmp_path, weaver_lakehouse="Weaver")
    store = LocalStore()
    resolver = LocalResolver(host)
    for item in ("Weaver", "Sales_LH"):
        store.make_directory(resolver.files_root(ItemRef(item)))
        store.make_directory(resolver.tables_root(ItemRef(item)))
    store.make_directory(resolver.repos_root)
    shutil.copytree(FIXTURE, resolver.repository(RepositoryRef("Mixed")).path)
    return host, store, tmp_path


def _generate(estate):
    host, store, tmp_path = estate
    return generate_build_bundle(
        weaver_lakehouse=ItemRef("Weaver"),
        repository_name="Mixed",
        targets=TargetBindings(warehouse=WarehouseBinding(warehouse=ItemRef("Sales_WH"))),
        output=Location(str(tmp_path / "bundle")),
        host=host,
        store=store,
        prune=False,
    )


def _payload(estate, bundle, action) -> str:
    store = estate[1]
    return store.read(bundle.location.join(*action.payload.split("/"))).decode("utf-8")


def test_the_warehouse_plan_builds_both_objects_in_dependency_order(estate):
    bundle = _generate(estate)
    plan = bundle.plan

    # Plus the Weaver Lakehouse, which is where the catalogue lives.
    assert [t.kind for t in plan.targets] == ["warehouse", "lakehouse"]
    built = [a.resource_node_id for _, _, a in plan.actions() if a.resource_node_id]
    # The report is built before the view that reads it.
    assert built == ["sql:Wh.CustomerReport", "sql:Wh.ActiveReport"]
    assert {a.executor for a in _physical(plan)} == {"tsql"}

    omitted = {node.node_id for node in plan.omitted_nodes}
    assert omitted == {
        "folder:Raw.Orders",
        "delta:Sales.Customer",
        "delta:Sales.CustomerEnriched",
        "delta:Sales.ActiveCustomer",
    }


def test_the_warehouse_schema_is_created_with_tsql(estate):
    bundle = _generate(estate)
    schema_actions = [a for _, _, a in bundle.plan.actions() if a.kind == "create_schema"]
    assert [a.id for a in schema_actions] == ["schema-Wh"]
    body = _payload(estate, bundle, schema_actions[0])
    assert "create schema [Wh]" in body


def test_the_report_table_reads_the_lakehouse_by_three_part_name(estate):
    bundle = _generate(estate)
    report = next(
        a
        for _, _, a in bundle.plan.actions()
        if a.resource_node_id == "sql:Wh.CustomerReport"
    )
    script = _payload(estate, bundle, report)

    # A self-contained, inferred T-SQL build reading the Lakehouse by physical name.
    assert "[Sales_LH].[Sales].[Customer]" in script
    assert "where 1=0" in script
    assert "into #weaver_shape_Wh_CustomerReport" in script
    assert "[Wh].[CustomerReport]" in script
    assert "_Current" not in script and "_History" not in script


def test_the_report_view_is_a_create_or_alter_view(estate):
    bundle = _generate(estate)
    view = next(
        a
        for _, _, a in bundle.plan.actions()
        if a.resource_node_id == "sql:Wh.ActiveReport"
    )
    script = _payload(estate, bundle, view)
    assert script == (
        "create or alter view [Wh].[ActiveReport] as\n"
        "select CustomerId from [Wh].[CustomerReport]\n"
    )
