"""Alias-closed dependency resolution and the complete cross-engine DAG."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from weaver import Location
from weaver.errors import DiscoveryError, GraphError
from weaver.ses import ObjectId, read_repository

FIXTURE = Location(str(Path(__file__).parent / "fixtures" / "cross-engine"))


@pytest.fixture(scope="module")
def repo():
    return read_repository(FIXTURE)


# --- namespaces --------------------------------------------------------------


def test_objects_sort_into_their_native_partitions(repo):
    assert set(repo.folder_native) == {ObjectId("Raw", "Customer")}
    assert set(repo.lakehouse_native) == {
        ObjectId("Sales", "Customer"),
        ObjectId("Sales", "CustomerFeature"),
        ObjectId("Sales", "Ledger"),
    }
    assert set(repo.warehouse_native) == {
        ObjectId("Reporting", "CustomerSummary"),
        ObjectId("Sales", "Ledger"),
    }


def test_one_name_may_be_a_delta_and_a_warehouse_object(repo):
    ledger = ObjectId("Sales", "Ledger")
    assert ledger in repo.lakehouse_native
    assert ledger in repo.warehouse_native
    assert ledger not in repo.folder_native


def test_aliases_map_the_published_name_to_the_declaring_object(repo):
    assert repo.warehouse_aliases[ObjectId("Sales", "Customer")].node_id == "delta:Sales.Customer"
    assert repo.lakehouse_aliases[ObjectId("Reporting", "CustomerSummary")].node_id == (
        "sql:Reporting.CustomerSummary"
    )


# --- resolution and provenance ----------------------------------------------


def edge(repo, producer: str, consumer: str):
    matches = [
        edge
        for edge in repo.dependency_edges
        if edge.producer == producer and edge.consumer == consumer
    ]
    assert len(matches) == 1, f"expected one {producer} -> {consumer} edge, got {matches}"
    return matches[0]


def test_a_lakehouse_reference_resolves_to_a_lakehouse_native(repo):
    assert edge(repo, "folder:Raw.Customer", "delta:Sales.Customer").resolution_kind == "native"
    assert edge(repo, "delta:Sales.Customer", "delta:Sales.Ledger").resolution_kind == "native"


def test_a_warehouse_reference_closes_through_a_warehouse_alias(repo):
    crossing = edge(repo, "delta:Sales.Customer", "sql:Reporting.CustomerSummary")
    assert crossing.resolution_kind == "warehouse_alias"
    assert crossing.reference == "Sales.Customer"


def test_a_lakehouse_reference_closes_through_a_lakehouse_alias(repo):
    crossing = edge(repo, "sql:Reporting.CustomerSummary", "delta:Sales.CustomerFeature")
    assert crossing.resolution_kind == "lakehouse_alias"
    assert crossing.reference == "Reporting.CustomerSummary"


def test_nothing_is_left_unresolved(repo):
    assert repo.external_references == {}


# --- the complete DAG --------------------------------------------------------


def test_every_object_is_a_node(repo):
    assert set(repo.graph.nodes) == {
        "folder:Raw.Customer",
        "delta:Sales.Customer",
        "delta:Sales.CustomerFeature",
        "delta:Sales.Ledger",
        "sql:Reporting.CustomerSummary",
        "sql:Sales.Ledger",
    }


def test_the_loop_crosses_lakehouse_warehouse_lakehouse(repo):
    order = repo.graph.order()
    assert order.index("folder:Raw.Customer") < order.index("delta:Sales.Customer")
    assert order.index("delta:Sales.Customer") < order.index("sql:Reporting.CustomerSummary")
    assert order.index("sql:Reporting.CustomerSummary") < order.index("delta:Sales.CustomerFeature")


def test_the_order_is_deterministic(repo):
    assert read_repository(FIXTURE).graph.order() == repo.graph.order()


def test_schema_usage_is_derived_per_namespace(repo):
    assert repo.schemas_by_namespace["lakehouse"] == frozenset({"Raw", "Sales", "Reporting"})
    assert repo.schemas_by_namespace["warehouse"] == frozenset({"Reporting", "Sales"})


# --- small repositories for the remaining shapes -----------------------------


def delta(name: str, *, warehouse_alias: str | None = None, deps: list[str] | None = None) -> str:
    schema, obj = name.split(".")
    alias = f"\nWarehouse alias: {warehouse_alias}" if warehouse_alias else ""
    dependencies = ""
    if deps:
        dependencies = "\nDependencies:\n" + "".join(f"  - {dep}\n" for dep in deps)
    return textwrap.dedent(
        f'''"""
Table ID: {name}

Description: x

Lineage: y{alias}
{dependencies}
Primary key: Id

Schema:
  Id: string
"""

from weaver import Table


class {schema}__{obj}(Table):
    def read(self):
        return [], []
'''
    )


def sql_table(name: str, *, lakehouse_alias: str | None = None, body: str = "select 1 as [Id]") -> str:
    alias = f"\nLakehouse alias: {lakehouse_alias}" if lakehouse_alias else ""
    return textwrap.dedent(
        f"""/*
Table ID: {name}

Description: x

Lineage: y{alias}
*/

{body}
"""
    )


def build(tmp_path: Path, *, schemas: list[str], objects: dict[str, str]) -> Location:
    directory = tmp_path / "_schemas"
    directory.mkdir()
    for schema in schemas:
        (directory / f"{schema}.yml").write_text(f"Schema ID: {schema}\n", encoding="utf-8")
    for filename, text in objects.items():
        (tmp_path / filename).write_text(text, encoding="utf-8")
    return Location(str(tmp_path))


def test_a_warehouse_native_resolves_a_warehouse_native(tmp_path):
    root = build(
        tmp_path,
        schemas=["Reporting"],
        objects={
            "Reporting.Base.sql": sql_table("Reporting.Base"),
            "Reporting.Derived.sql": sql_table("Reporting.Derived", body="select * from Reporting.Base"),
        },
    )
    repo = read_repository(root)
    assert repo.graph.upstream_of("sql:Reporting.Derived") == ("sql:Reporting.Base",)


# --- unresolved two-part names are refused -----------------------------------


def test_an_unknown_two_part_reference_is_refused(tmp_path):
    root = build(
        tmp_path,
        schemas=["Reporting"],
        objects={"Reporting.R.sql": sql_table("Reporting.R", body="select * from Reporting.Missing")},
    )
    with pytest.raises(DiscoveryError, match="unresolved two-part reference"):
        read_repository(root)


def test_a_cross_engine_reference_without_an_alias_is_refused(tmp_path):
    """The Delta Sales.Customer publishes no Warehouse alias, so a Warehouse
    query naming it two-part cannot reach it."""
    root = build(
        tmp_path,
        schemas=["Sales", "Reporting"],
        objects={
            "Sales__Customer.py": delta("Sales.Customer"),
            "Reporting.R.sql": sql_table("Reporting.R", body="select * from Sales.Customer"),
        },
    )
    with pytest.raises(DiscoveryError, match="unresolved two-part reference"):
        read_repository(root)


def test_the_same_reference_resolves_once_the_alias_is_declared(tmp_path):
    root = build(
        tmp_path,
        schemas=["Sales", "Reporting"],
        objects={
            "Sales__Customer.py": delta("Sales.Customer", warehouse_alias="Sales.Customer"),
            "Reporting.R.sql": sql_table("Reporting.R", body="select * from Sales.Customer"),
        },
    )
    repo = read_repository(root)
    assert edge(repo, "delta:Sales.Customer", "sql:Reporting.R").resolution_kind == "warehouse_alias"


def test_an_alias_with_a_misspelled_schema_is_refused(tmp_path):
    root = build(
        tmp_path,
        schemas=["Reporting"],  # Reportng is the typo, undeclared
        objects={"Reporting.R.sql": sql_table("Reporting.R", lakehouse_alias="Reportng.R")},
    )
    with pytest.raises(DiscoveryError, match="schema 'Reportng' is not declared"):
        read_repository(root)


def test_a_reference_to_a_misspelled_alias_name_is_refused(tmp_path):
    """The Warehouse table publishes Foo.Summary; the Lakehouse table asks for
    Foo.Summry, which nothing owns."""
    root = build(
        tmp_path,
        schemas=["Foo"],
        objects={
            "Foo.Report.sql": sql_table("Foo.Report", lakehouse_alias="Foo.Summary"),
            "Foo.Feature.spark.sql": textwrap.dedent(
                """/*
Table ID: Foo.Feature

Description: x

Lineage: y

Dependencies:
  - Foo.Summry

Primary key: Id

Schema:
  Id: string
*/

select 1 as `Id` from Foo.Summry
"""
            ),
        },
    )
    with pytest.raises(DiscoveryError, match="unresolved two-part reference"):
        read_repository(root)


def test_a_three_part_name_is_external_not_refused(tmp_path):
    root = build(
        tmp_path,
        schemas=["Reporting"],
        objects={"Reporting.R.sql": sql_table("Reporting.R", body="select * from [DB].[Sales].[Order]")},
    )
    repo = read_repository(root)
    assert repo.external_references["sql:Reporting.R"] == ("DB.Sales.Order",)


def test_a_function_call_is_external_not_refused(tmp_path):
    root = build(
        tmp_path,
        schemas=["Reporting"],
        objects={
            "Reporting.R.sql": sql_table(
                "Reporting.R", body="select * from Reporting.Base cross apply Reporting.Split(1) as s"
            ),
            "Reporting.Base.sql": sql_table("Reporting.Base"),
        },
    )
    repo = read_repository(root)
    assert repo.external_references["sql:Reporting.R"] == ("Reporting.Split",)
    assert "sql:Reporting.Split" not in repo.graph.nodes


def folder(name: str) -> str:
    schema, obj = name.split(".")
    return textwrap.dedent(
        f'''"""
Folder ID: {name}

Description: x

Lineage: y

File key: "*.csv"
"""

from weaver import Folder


class {schema}__{obj}(Folder):
    def read(self):
        return self.staging_folder(), []
'''
    )


def test_a_folder_and_a_delta_table_may_not_share_a_name(tmp_path):
    root = build(
        tmp_path,
        schemas=["Sales"],
        objects={
            "Sales__Ledger.py": folder("Sales.Ledger"),
            "Sales.Ledger.spark.sql": textwrap.dedent(
                """/*
Table ID: Sales.Ledger

Description: x

Lineage: y

Dependencies:
  - Sales.Source

Primary key: Id

Schema:
  Id: string
*/

select 1 as `Id`
"""
            ),
        },
    )
    with pytest.raises(DiscoveryError, match="both a Folder .* and a Delta table"):
        read_repository(root)


def test_a_name_spelled_two_ways_across_targets_is_refused(tmp_path):
    root = build(
        tmp_path,
        schemas=["Sales"],
        objects={
            "Sales__Ledger.py": delta("Sales.Ledger"),
            "sales.ledger.sql": sql_table("sales.ledger"),
        },
    )
    with pytest.raises(DiscoveryError, match="differ only by case"):
        read_repository(root)


def test_a_delta_and_a_warehouse_table_may_share_a_name(tmp_path):
    root = build(
        tmp_path,
        schemas=["Sales"],
        objects={
            "Sales__Ledger.py": delta("Sales.Ledger"),
            "Sales.Ledger.sql": sql_table("Sales.Ledger"),
        },
    )
    repo = read_repository(root)
    assert ObjectId("Sales", "Ledger") in repo.lakehouse_native
    assert ObjectId("Sales", "Ledger") in repo.warehouse_native


# --- cycles ------------------------------------------------------------------


def test_a_same_engine_cycle_is_refused(tmp_path):
    root = build(
        tmp_path,
        schemas=["Foo"],
        objects={
            "Foo.A.sql": sql_table("Foo.A", body="select * from Foo.B"),
            "Foo.B.sql": sql_table("Foo.B", body="select * from Foo.A"),
        },
    )
    with pytest.raises(GraphError, match="cycle"):
        read_repository(root)


def test_a_cross_engine_cycle_through_aliases_is_refused(tmp_path):
    """delta A -> (warehouse alias) -> sql B -> (lakehouse alias) -> delta A."""
    root = build(
        tmp_path,
        schemas=["Foo"],
        objects={
            "Foo__A.py": delta("Foo.A", warehouse_alias="Foo.A", deps=["Foo.B"]),
            "Foo.B.sql": sql_table("Foo.B", lakehouse_alias="Foo.B", body="select * from Foo.A"),
        },
    )
    with pytest.raises(GraphError, match="cycle"):
        read_repository(root)
