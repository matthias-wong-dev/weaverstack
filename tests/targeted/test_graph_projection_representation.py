"""The graph tables restate the installed graph, one item at a time.

`_.GraphNode` and `_.GraphEdge` exist so a report that cannot resolve a
Weaver reference can still walk the graph. They are only useful if they are the
graph `Catalogue.dag()` reads, so the central claim here is parity: once a
build's rows are installed, the union of every item's graph rows is that
graph's nodes and edges.

Pure Python. The rows are the ones `desired_catalogue` publishes, and the graph
is read back from those same rows, so neither side is a restatement.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from factories import (
    built_catalogue,
    installation_row,
    item_bindings,
    registry_row,
)
from support.weaver_test import weaver_test
from support.workspaces import WORKSPACE

from weaver.build_bundle import effective_item_bindings
from weaver.build_bundle.catalogue_actions import (
    desired_catalogue,
    render_catalogue_after_build,
)
from weaver.build_bundle.planner import certifiable_identities
from weaver.catalogue.graph_rows import SEARCH_TEXT_BYTES, graph_rows
from weaver.catalogue.reconcile import publish
from weaver.catalogue.state import Catalogue
from weaver.catalogue.tables import (
    DEPENDENCY,
    GRAPH_EDGE,
    GRAPH_NODE,
    GRAPH_TABLES,
    INSTALLATION,
    REGISTRY,
    TABLE_DICTIONARY,
)
from weaver.declaration import parse_item_repository
from weaver.declaration.model import WeaverDocumentId, WeaverItemId
from weaver.locations import Location
from weaver.targets import ItemRef

#: Two items and a logical shortcut between them, with a View, a Folder, a Test
#: and an Assumption.
CROSS_ITEM = Path(__file__).parents[1] / "fixtures" / "cross-item-journey"

SALES = WeaverItemId.parse("Lakehouse/Sales")
REPORTING = WeaverItemId.parse("Warehouse/Reporting")
CUSTOMER = WeaverDocumentId.parse("Lakehouse/Sales/Tables/DWG.Customer")


@pytest.fixture(scope="module")
def repository():
    return parse_item_repository(Location(str(CROSS_ITEM)))


def _bindings(*items: WeaverItemId):
    """The named items and the catalogue item, as every real build binds it."""

    targets = {SALES: "Sales_LH", REPORTING: "Reporting_WH"}
    return effective_item_bindings(
        item_bindings(*((str(item), targets[item]) for item in items)),
        control_item=ItemRef("Weaver"),
        workspace_name=WORKSPACE,
    )


def _desired(repository, bindings, *, omitted=()):
    by_item = {binding.item: binding for binding in bindings.entries}
    return desired_catalogue(
        repository,
        certifiable_identities(repository, by_item) - set(omitted),
        {binding.item: binding.to_bound_target() for binding in bindings.entries},
    )


def _rows(catalogue: Catalogue, table) -> list[dict]:
    return [
        row for tables in catalogue.rows.values() for row in tables.get(table.name, ())
    ]


def _graphed(catalogue: Catalogue):
    """The graph rows' nodes, resolved edges and External edges."""

    nodes = {row["node_id"] for row in _rows(catalogue, GRAPH_NODE)}
    edges = {
        (
            row["upstream_node_id"],
            row["downstream_node_id"],
            row["edge_kind"],
            row["through_node_id"],
        )
        for row in _rows(catalogue, GRAPH_EDGE)
        if row["edge_kind"] != "external"
    }
    external = {
        (row["downstream_node_id"], row["upstream_node_id"])
        for row in _rows(catalogue, GRAPH_EDGE)
        if row["edge_kind"] == "external"
    }
    return nodes, edges, external


def _graph(dag, *, items=None):
    """The same three sets, read from the installed graph."""

    def wanted(identity) -> bool:
        return items is None or identity.item in items

    validations = {node.node_id for node in dag.nodes if node.is_validation}
    nodes = {node.node_id for node in dag.nodes if wanted(node.identity)}
    edges = {
        (
            str(edge.upstream),
            str(edge.downstream),
            "shortcut"
            if edge.is_shortcut
            else "validation"
            if str(edge.downstream) in validations
            else "dependency",
            None if edge.through is None else str(edge.through),
        )
        for edge in dag.edges
        if wanted(edge.downstream)
    }
    external = {
        (str(consumer), reference)
        for found in (dag.external_references, dag.unresolved_references)
        for consumer, references in found.items()
        if wanted(consumer)
        for reference in references
    }
    return nodes, edges, external


# --- parity -------------------------------------------------------------------


@weaver_test()
def test_the_graph_rows_are_the_installed_graph(repository):
    built = built_catalogue(repository, _bindings(SALES, REPORTING))

    nodes, edges, external = _graphed(built)
    expected_nodes, expected_edges, expected_external = _graph(built.dag())

    assert nodes == expected_nodes
    assert edges == expected_edges
    assert external == expected_external == set()
    # The fixture reaches every kind the claim depends on.
    kinds = {row["node_kind"] for row in _rows(built, GRAPH_NODE)}
    assert {"table", "view", "folder", "shortcut", "test", "assumption"} <= kinds
    assert {kind for _up, _down, kind, _through in edges} == {
        "dependency",
        "shortcut",
        "validation",
    }


@weaver_test()
def test_a_read_through_a_shortcut_names_the_source_and_the_shortcut(repository):
    built = built_catalogue(repository, _bindings(SALES, REPORTING))

    _nodes, edges, _external = _graphed(built)

    assert (
        str(CUSTOMER),
        "Warehouse/Reporting/Rpt.PortableCustomer",
        "shortcut",
        None,
    ) in edges
    assert (
        str(CUSTOMER),
        "Warehouse/Reporting/Rpt.CustomerReport",
        "dependency",
        "Warehouse/Reporting/Rpt.PortableCustomer",
    ) in edges


@weaver_test()
def test_an_unresolved_read_is_an_external_edge_to_what_its_author_wrote(repository):
    """A build that leaves a producer out leaves its readers unresolved."""

    installed = _desired(repository, _bindings(SALES, REPORTING), omitted={CUSTOMER})

    nodes, edges, external = _graphed(installed)
    expected = _graph(installed.dag(), items={SALES})

    sales_nodes = {node for node in nodes if node.startswith(f"{SALES}/")}
    sales_edges = {edge for edge in edges if edge[1].startswith(f"{SALES}/")}
    sales_external = {edge for edge in external if edge[0].startswith(f"{SALES}/")}
    assert (sales_nodes, sales_edges, sales_external) == expected
    assert str(CUSTOMER) not in nodes
    # A Python table's import, as its module wrote it.
    assert ("Lakehouse/Sales/Tables/DWG.Order", "Tables.DWG__Customer") in (
        sales_external
    )


@weaver_test()
def test_a_physical_read_is_an_external_edge():
    item = REPORTING
    consumer = WeaverDocumentId.parse("Warehouse/Reporting/Rpt.Customer")
    catalogue = Catalogue(
        {
            item: {
                INSTALLATION.name: (installation_row(item, "Reporting_WH"),),
                REGISTRY.name: (registry_row(consumer),),
                DEPENDENCY.name: (
                    {
                        "item_type": item.item_type,
                        "item_name": item.item_name,
                        "referencing_schema_name": "Rpt",
                        "referencing_object_name": "Customer",
                        "dependency_reference": "Ledger.dbo.Customer",
                        "signature": "dependency",
                    },
                ),
            }
        }
    )

    rows = graph_rows(catalogue, item)

    (edge,) = rows[GRAPH_EDGE.name]
    assert (edge["upstream_node_id"], edge["downstream_node_id"]) == (
        "Ledger.dbo.Customer",
        str(consumer),
    )
    assert edge["edge_kind"] == "external"
    assert edge["through_node_id"] is None


# --- per item -----------------------------------------------------------------


@weaver_test()
def test_an_items_rows_come_from_its_own_rows_alone(repository):
    """A read of another item's object names that object, installed or not.

    So the rows a build publishes for one item do not go stale when another is
    rebuilt, and a scoped build publishes the same rows as a whole one.
    """

    alone = _desired(repository, _bindings(REPORTING))
    together = built_catalogue(repository, _bindings(SALES, REPORTING))

    for table in GRAPH_TABLES:
        assert alone.rows[REPORTING][table.name] == together.rows[REPORTING][table.name]
    assert SALES not in alone.rows
    upstreams = {row["upstream_node_id"] for row in _rows(alone, GRAPH_EDGE)}
    assert str(CUSTOMER) in upstreams


@weaver_test()
def test_rows_are_published_only_for_the_items_a_build_binds(repository):
    """And a row the item no longer projects is deleted, within its scope."""

    built = built_catalogue(repository, _bindings(SALES, REPORTING))
    departed = {
        **_rows(built, GRAPH_NODE)[0],
        "item_type": "Lakehouse",
        "item_name": "Sales",
        "schema_name": "Tables/DWG",
        "object_name": "Departed",
        "node_id": "Lakehouse/Sales/Tables/DWG.Departed",
    }
    current = Catalogue(
        {
            item: {
                **tables,
                GRAPH_NODE.name: tables[GRAPH_NODE.name]
                + ((departed,) if item == SALES else ()),
            }
            for item, tables in built.rows.items()
        }
    )
    bindings = _bindings(SALES)
    by_item = {binding.item: binding for binding in bindings.entries}

    stages = render_catalogue_after_build(
        repository,
        certifiable_identities(repository, by_item),
        {binding.item: binding.to_bound_target() for binding in bindings.entries},
        catalogue_target=by_item[SALES].to_bound_target(),
        current=current,
    )

    lines = [
        line
        for stage in stages
        for content in stage.payloads.values()
        for line in json.loads(content)
        if any(f"[{table.name}]" in line for table in GRAPH_TABLES)
    ]
    (delete,) = lines
    assert delete.startswith("DELETE FROM [_].[GraphNode]")
    assert "N'Sales'" in delete and "N'Reporting'" not in delete


@weaver_test()
def test_an_unchanged_estate_publishes_no_graph_rows(repository):
    """Two separate projections of one estate agree row for row."""

    first = built_catalogue(repository, _bindings(SALES, REPORTING))
    again = built_catalogue(
        parse_item_repository(Location(str(CROSS_ITEM))), _bindings(SALES, REPORTING)
    )

    publication = publish(first, again)

    assert not any(
        f"[{table.name}]" in statement
        for statement in publication.statements
        for table in GRAPH_TABLES
    )


@weaver_test()
def test_a_catalogue_without_graph_rows_gains_them_on_the_next_build(repository):
    """An estate built before these tables existed fills them in unchanged."""

    built = built_catalogue(repository, _bindings(SALES, REPORTING))
    older = Catalogue(
        {
            item: {
                name: rows
                for name, rows in tables.items()
                if name not in {table.name for table in GRAPH_TABLES}
            }
            for item, tables in built.rows.items()
        }
    )

    publication = publish(older, built)

    touched = {
        table.name
        for statement in publication.statements
        for table in (*GRAPH_TABLES, TABLE_DICTIONARY, REGISTRY)
        if f"[{table.name}]" in statement
    }
    assert touched == {GRAPH_NODE.name, GRAPH_EDGE.name}


@weaver_test()
def test_an_item_whose_graph_fails_keeps_its_rows_and_the_build_goes_on(
    repository, monkeypatch
):
    """The projection is advisory: one item's failure warns and publishes nothing.

    The other item's rows still publish, and the failing item's existing rows
    are neither merged nor deleted.
    """

    import weaver.installed as installed
    from weaver.errors import GraphError

    real = installed.item_dag

    def failing(catalogue, item):
        if item == REPORTING:
            raise GraphError("Rpt.CustomerReport depends on itself")
        return real(catalogue, item)

    monkeypatch.setattr(installed, "item_dag", failing)
    built = built_catalogue(repository, _bindings(SALES, REPORTING))
    current = Catalogue(
        {
            item: {
                name: rows
                for name, rows in tables.items()
                # Sales has none yet; Reporting holds rows its graph no longer has.
                if item != SALES or name not in {t.name for t in GRAPH_TABLES}
            }
            for item, tables in built.rows.items()
        }
    )
    bindings = _bindings(SALES, REPORTING)
    by_item = {binding.item: binding for binding in bindings.entries}
    warned = []

    stages = render_catalogue_after_build(
        repository,
        certifiable_identities(repository, by_item),
        {binding.item: binding.to_bound_target() for binding in bindings.entries},
        catalogue_target=by_item[SALES].to_bound_target(),
        current=current,
        warn=warned.append,
    )

    assert warned == [
        "Catalogue Dashboard graph for Warehouse/Reporting was not updated: "
        "Rpt.CustomerReport depends on itself. The Build continued."
    ]
    lines = [
        line
        for stage in stages
        for content in stage.payloads.values()
        for line in json.loads(content)
        if any(f"[{table.name}]" in line for table in GRAPH_TABLES)
    ]
    assert lines, "Sales' graph rows still publish"
    assert all("N'Sales'" in line for line in lines)
    assert not any("N'Reporting'" in line for line in lines)


# --- what a node says ---------------------------------------------------------


@weaver_test()
def test_a_node_carries_its_name_item_description_and_search_text(repository):
    built = built_catalogue(repository, _bindings(SALES, REPORTING))
    by_id = {row["node_id"]: row for row in _rows(built, GRAPH_NODE)}

    customer = by_id[str(CUSTOMER)]
    assert (customer["schema_name"], customer["object_name"]) == (
        "Tables/DWG",
        "Customer",
    )
    assert customer["node_kind"] == "table"
    assert customer["label"] == "DWG.Customer"
    assert customer["item_label"] == "Lakehouse/Sales"
    assert customer["description"] == "One row per customer, typed from the raw CSV."
    assert customer["search_text"] == (
        "dwg.customer lakehouse/sales one row per customer, typed from the raw csv."
    )
    assert customer["is_internal"] is False

    assumption = by_id["Lakehouse/Sales/DWG.OrderHasCustomer"]
    assert assumption["node_kind"] == "assumption"
    assert assumption["description"] == "Every order names a customer that exists."

    shortcut = by_id["Warehouse/Reporting/Rpt.PortableCustomer"]
    assert shortcut["node_kind"] == "shortcut"
    assert shortcut["description"] is None


@weaver_test()
def test_the_catalogue_and_its_surface_are_internal(repository):
    built = built_catalogue(repository, _bindings(SALES, REPORTING))
    by_id = {row["node_id"]: row for row in _rows(built, GRAPH_NODE)}

    internal = {node for node, row in by_id.items() if row["is_internal"]}

    assert "Warehouse/_weaver/_.Registry" in internal
    assert "Warehouse/_weaver/_.GraphNode" in internal
    assert "Lakehouse/Sales/Tables/_.Log" in internal
    assert "Warehouse/Reporting/_.LoadStatus" in internal
    assert all(
        node.startswith("Warehouse/_weaver/") or "/_." in node for node in internal
    )


@weaver_test()
def test_search_text_stays_within_its_column():
    """Bounded in bytes, as the Warehouse counts them, and still valid text."""

    item = SALES
    identity = WeaverDocumentId.parse("Lakehouse/Sales/Tables/DWG.Customer")
    catalogue = Catalogue(
        {
            item: {
                INSTALLATION.name: (installation_row(item, "Sales_LH"),),
                REGISTRY.name: (registry_row(identity),),
                TABLE_DICTIONARY.name: (
                    {
                        "item_type": "Lakehouse",
                        "item_name": "Sales",
                        "schema_name": "Tables/DWG",
                        "object_name": "Customer",
                        "description": "É" * 1999,
                    },
                ),
            }
        }
    )

    (node,) = graph_rows(catalogue, item)[GRAPH_NODE.name]

    stored = node["search_text"].encode("utf-8")
    assert len(stored) <= SEARCH_TEXT_BYTES
    assert node["search_text"].startswith("dwg.customer lakehouse/sales éé")


@weaver_test()
def test_the_catalogue_dashboard_is_internal():
    from weaver.build_bundle.targets import ItemBindings, parse_build_item
    from weaver.catalogue_dashboard import DASHBOARD_ITEMS

    composed = parse_item_repository(
        Location(str(CROSS_ITEM)), catalogue_dashboard=True
    )
    bindings = effective_item_bindings(
        ItemBindings(
            item_bindings(("Lakehouse/Sales", "Sales_LH")).entries
            + tuple(
                parse_build_item(f"{item}={item.item_type}/Estate Dashboard")
                for item in DASHBOARD_ITEMS
            )
        ),
        control_item=ItemRef("Weaver"),
        workspace_name=WORKSPACE,
    )
    rows = _rows(built_catalogue(composed, bindings), GRAPH_NODE)
    dashboard = [row for row in rows if row["item_name"] == "Catalogue Dashboard"]

    assert {row["item_type"] for row in dashboard} == {"SemanticModel", "Report"}
    assert all(row["is_internal"] for row in dashboard)
    assert not any(
        row["is_internal"]
        for row in rows
        if row["item_name"] == SALES.item_name and not row["schema_name"].endswith("_")
    )
