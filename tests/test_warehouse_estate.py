"""A self-contained Warehouse estate: tables from VALUES, then tables and views on them.

This repository needs no Lakehouse: its base tables seed themselves from literal
``VALUES``, so the whole estate is a closed Warehouse graph. Downstream tables
read the base tables by two-part name, a dimension adds a Weaver-managed identity
surrogate, and a view sits on top. Here we pin the generated plan and scripts;
because the seeds are literal, this same fixture is the one to *execute* against
the Play Warehouse on Fabric, where each script builds a real, queryable table
for targeted troubleshooting.
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

FIXTURE = Path(__file__).parent / "fixtures" / "warehouse-estate"

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
    store.make_directory(resolver.files_root(ItemRef("Weaver")))
    store.make_directory(resolver.tables_root(ItemRef("Weaver")))
    store.make_directory(resolver.repos_root)
    shutil.copytree(FIXTURE, resolver.repository(RepositoryRef("WhEstate")).path)
    return host, store, tmp_path


@pytest.fixture
def bundle(estate):
    host, store, tmp_path = estate
    return generate_build_bundle(
        weaver_lakehouse=ItemRef("Weaver"),
        repository_name="WhEstate",
        targets=TargetBindings(warehouse=WarehouseBinding(warehouse=ItemRef("Play_WH"))),
        output=Location(str(tmp_path / "bundle")),
        host=host,
        store=store,
        prune=False,
    )


def _script(estate, bundle, node_id: str) -> str:
    store = estate[1]
    action = next(
        a for _, _, a in bundle.plan.actions() if a.resource_node_id == node_id
    )
    return store.read(bundle.location.join(*action.payload.split("/"))).decode("utf-8")


def _sequence_of(bundle) -> dict[str, int]:
    return {
        a.resource_node_id: seq.number
        for seq, _, a in bundle.plan.actions()
        if a.resource_node_id is not None
    }


# --- the plan ---------------------------------------------------------------


def test_the_estate_is_warehouse_native_and_all_tsql(estate, bundle):
    # The Warehouse destination, plus the Weaver Lakehouse the catalogue is
    # written to — the control plane is a Lakehouse whichever side is built.
    assert [t.kind for t in bundle.plan.targets] == ["warehouse", "lakehouse"]
    assert {a.executor for a in _physical(bundle.plan)} == {"tsql"}
    assert bundle.plan.omitted_nodes == ()


def test_both_schemas_are_created_with_tsql(estate, bundle):
    schemas = {
        a.id: _payload(estate, bundle, a)
        for _, _, a in bundle.plan.actions()
        if a.kind == "create_schema"
    }
    assert set(schemas) == {"schema-Wh", "schema-Rpt"}
    assert "create schema [Wh]" in schemas["schema-Wh"]
    assert "create schema [Rpt]" in schemas["schema-Rpt"]


def _payload(estate, bundle, action) -> str:
    return estate[1].read(bundle.location.join(*action.payload.split("/"))).decode("utf-8")


def test_dependencies_layer_bases_then_dependents_then_the_view(estate, bundle):
    at = _sequence_of(bundle)
    # Base tables come first; the dependent table and dimension after; the view last.
    assert at["sql:Wh.Customer"] < at["sql:Wh.CustomerOrder"]
    assert at["sql:Wh.Customer"] < at["sql:Wh.CustomerDim"]
    assert at["sql:Wh.CustomerOrder"] < at["sql:Rpt.CustomerSummary"]


# --- the scripts ------------------------------------------------------------


def test_a_base_table_seeds_itself_from_values_shape_only(estate, bundle):
    script = _script(estate, bundle, "sql:Wh.Customer")
    # Inferred: it shapes the VALUES seed into a temp table and infers the types.
    assert "values" in script.lower()
    assert "'Ada Lovelace'" in script
    assert "where 1=0" in script
    assert "into #weaver_shape_Wh_Customer" in script
    assert "case bt.base_type" in script  # inferred type mapping


def test_a_declared_base_table_uses_its_declared_types(estate, bundle):
    script = _script(estate, bundle, "sql:Wh.Product")
    assert "[ProductId] int not null" in script
    assert "[ProductName] varchar(100) null" in script
    assert "[Price] decimal(10,2) null" in script
    assert "'Widget'" in script  # still validates the VALUES seed
    assert "case bt.base_type" not in script  # declared, so no type inference


def test_a_downstream_table_reads_a_base_table_by_two_part_name(estate, bundle):
    script = _script(estate, bundle, "sql:Wh.CustomerOrder")
    assert "[Wh].[Customer]" in script
    assert "[Wh].[CustomerOrder]" in script
    assert "into #weaver_shape_Wh_CustomerOrder" in script


def test_the_dimension_adds_a_weaver_managed_identity_surrogate(estate, bundle):
    script = _script(estate, bundle, "sql:Wh.CustomerDim")
    # A plain not-null bigint surrogate, added at the front, not autogenerated.
    assert (
        "select 0 as column_ordinal, N'[CustomerKey] bigint not null' as column_definition"
        in script
    )
    assert "throw 51006" in script  # collision guard
    assert " identity" not in script.lower()


def test_the_reporting_view_is_a_create_or_alter_view(estate, bundle):
    script = _script(estate, bundle, "sql:Rpt.CustomerSummary")
    assert script.startswith("create or alter view [Rpt].[CustomerSummary] as\n")
    assert "[Wh].[CustomerOrder]" in script


# --- determinism ------------------------------------------------------------


def test_regeneration_is_byte_identical(estate, bundle):
    host, store, tmp_path = estate
    again = generate_build_bundle(
        weaver_lakehouse=ItemRef("Weaver"),
        repository_name="WhEstate",
        targets=TargetBindings(warehouse=WarehouseBinding(warehouse=ItemRef("Play_WH"))),
        output=Location(str(tmp_path / "bundle-2")),
        host=host,
        store=store,
        prune=False,
    )
    assert again.plan.bundle_id == bundle.plan.bundle_id
