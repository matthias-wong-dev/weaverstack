"""The lineage tables restate the installed graph, one item at a time.

`_.LineageNode` and `_.LineageEdge` exist so a report that cannot resolve a
Weaver reference can still walk the graph. They are only useful if they are the
graph `Catalogue.dag()` reads, so the central claim here is parity: once a
build's rows are installed, the union of every item's lineage rows is that
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
from weaver.catalogue.lineage_rows import SEARCH_TEXT_BYTES, lineage_rows, node_key
from weaver.catalogue.reconcile import publish
from weaver.catalogue.state import Catalogue
from weaver.catalogue.tables import (
    DEPENDENCY,
    EDGE_EXTERNAL,
    EDGE_SHORTCUT,
    INSTALLATION,
    LINEAGE_EDGE,
    LINEAGE_NODE,
    LINEAGE_TABLES,
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
    """The lineage rows' nodes, resolved edges and External edges."""

    nodes = {row["node_id"] for row in _rows(catalogue, LINEAGE_NODE)}
    edges = {
        (
            row["upstream_node_id"],
            row["downstream_node_id"],
            row["edge_kind"],
            row["through_node_id"],
        )
        for row in _rows(catalogue, LINEAGE_EDGE)
        if row["edge_kind"] != "external"
    }
    external = {
        (row["downstream_node_id"], row["upstream_node_id"])
        for row in _rows(catalogue, LINEAGE_EDGE)
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
def test_the_lineage_rows_are_the_installed_graph(repository):
    built = built_catalogue(repository, _bindings(SALES, REPORTING))

    nodes, edges, external = _graphed(built)
    expected_nodes, expected_edges, expected_external = _graph(built.dag())

    assert nodes == expected_nodes
    assert edges == expected_edges
    assert external == expected_external == set()
    # The fixture reaches every kind the claim depends on.
    kinds = {row["node_kind"] for row in _rows(built, LINEAGE_NODE)}
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

    rows = lineage_rows(catalogue, item)

    (edge,) = rows[LINEAGE_EDGE.name]
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

    for table in LINEAGE_TABLES:
        assert alone.rows[REPORTING][table.name] == together.rows[REPORTING][table.name]
    assert SALES not in alone.rows
    upstreams = {row["upstream_node_id"] for row in _rows(alone, LINEAGE_EDGE)}
    assert str(CUSTOMER) in upstreams


@weaver_test()
def test_rows_are_published_only_for_the_items_a_build_binds(repository):
    """And a row the item no longer projects is deleted, within its scope."""

    built = built_catalogue(repository, _bindings(SALES, REPORTING))
    departed = {
        **_rows(built, LINEAGE_NODE)[0],
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
                LINEAGE_NODE.name: tables[LINEAGE_NODE.name]
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
        if any(f"[{table.name}]" in line for table in LINEAGE_TABLES)
    ]
    (delete,) = lines
    assert delete.startswith("DELETE FROM [_].[LineageNode]")
    assert "N'Sales'" in delete and "N'Reporting'" not in delete


@weaver_test()
def test_an_unchanged_estate_publishes_no_lineage_rows(repository):
    """Two separate projections of one estate agree row for row."""

    first = built_catalogue(repository, _bindings(SALES, REPORTING))
    again = built_catalogue(
        parse_item_repository(Location(str(CROSS_ITEM))), _bindings(SALES, REPORTING)
    )

    publication = publish(first, again)

    assert not any(
        f"[{table.name}]" in statement
        for statement in publication.statements
        for table in LINEAGE_TABLES
    )


@weaver_test()
def test_a_catalogue_without_lineage_rows_gains_them_on_the_next_build(repository):
    """An estate built before these tables existed fills them in unchanged."""

    built = built_catalogue(repository, _bindings(SALES, REPORTING))
    older = Catalogue(
        {
            item: {
                name: rows
                for name, rows in tables.items()
                if name not in {table.name for table in LINEAGE_TABLES}
            }
            for item, tables in built.rows.items()
        }
    )

    publication = publish(older, built)

    touched = {
        table.name
        for statement in publication.statements
        for table in (*LINEAGE_TABLES, TABLE_DICTIONARY, REGISTRY)
        if f"[{table.name}]" in statement
    }
    assert touched == {LINEAGE_NODE.name, LINEAGE_EDGE.name}


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
                if item != SALES or name not in {t.name for t in LINEAGE_TABLES}
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
        "Lineage for Warehouse/Reporting was not updated: "
        "Rpt.CustomerReport depends on itself. The Build continued."
    ]
    lines = [
        line
        for stage in stages
        for content in stage.payloads.values()
        for line in json.loads(content)
        if any(f"[{table.name}]" in line for table in LINEAGE_TABLES)
    ]
    assert lines, "Sales' lineage rows still publish"
    assert all("N'Sales'" in line for line in lines)
    assert not any("N'Reporting'" in line for line in lines)


# --- what a node says ---------------------------------------------------------


@weaver_test()
def test_a_node_carries_its_name_item_description_and_search_text(repository):
    built = built_catalogue(repository, _bindings(SALES, REPORTING))
    by_id = {row["node_id"]: row for row in _rows(built, LINEAGE_NODE)}

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
    by_id = {row["node_id"]: row for row in _rows(built, LINEAGE_NODE)}

    internal = {node for node, row in by_id.items() if row["is_internal"]}

    assert "Warehouse/_weaver/_.Registry" in internal
    assert "Warehouse/_weaver/_.LineageNode" in internal
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

    (node,) = lineage_rows(catalogue, item)[LINEAGE_NODE.name]

    stored = node["search_text"].encode("utf-8")
    assert len(stored) <= SEARCH_TEXT_BYTES
    assert node["search_text"].startswith("dwg.customer lakehouse/sales éé")


@weaver_test()
def test_an_authored_model_is_never_internal_by_its_name(tmp_path):
    """Only the catalogue's own item is internal; a model named like the
    Catalogue Dashboard is an ordinary item."""

    import shutil

    from weaver.build_bundle.targets import ItemBindings, parse_build_item
    from weaver.onboarding.project import semantic_extension

    root = tmp_path / "project"
    shutil.copytree(CROSS_ITEM, root)
    model = root / "PowerBI/Catalogue Dashboard/Catalogue Dashboard.tmdl"
    model.parent.mkdir(parents=True)
    model.write_text(semantic_extension(), encoding="utf-8")
    authored = parse_item_repository(Location(str(root)))
    bindings = effective_item_bindings(
        ItemBindings(
            item_bindings(("Lakehouse/Sales", "Sales_LH")).entries
            + (
                parse_build_item(
                    "SemanticModel/Catalogue Dashboard=SemanticModel/Estate"
                ),
            )
        ),
        control_item=ItemRef("Weaver"),
        workspace_name=WORKSPACE,
    )
    rows = _rows(built_catalogue(authored, bindings), LINEAGE_NODE)

    (named,) = [row for row in rows if row["item_name"] == "Catalogue Dashboard"]
    assert not named["is_internal"]
    assert all(row["is_internal"] for row in rows if row["item_name"] == "_weaver")


# --- keys and flags -----------------------------------------------------------


#: Two workspaces, as their GUIDs. Sales and Reporting may live in either.
FIRST = "6F9619FF-8B86-D011-B42D-00C04FC964FF"
SECOND = "0e6c41d4-7b1f-4a63-9e7a-3a3c4b0d5f21"


def _keyed(built: Catalogue, workspaces):
    """Every item's lineage rows, projected with these workspaces."""

    return {
        item: lineage_rows(Catalogue({item: tables}), item, workspaces=workspaces)
        for item, tables in built.rows.items()
    }


def _keyed_rows(projected, table):
    return [row for rows in projected.values() for row in rows[table.name]]


@weaver_test()
def test_a_key_is_the_node_in_its_workspace_and_each_edge_names_both_keys(repository):
    built = built_catalogue(repository, _bindings(SALES, REPORTING))
    workspaces = {SALES: FIRST, REPORTING: SECOND}
    projected = _keyed(built, workspaces)

    owners = {}
    for row in _keyed_rows(projected, LINEAGE_NODE):
        item = WeaverItemId(row["item_type"], row["item_name"])
        owners[row["node_id"]] = workspaces.get(item)
        assert row["node_key"] == node_key(row["node_id"], workspaces.get(item))
    keys = {
        row["node_id"]: row["node_key"] for row in _keyed_rows(projected, LINEAGE_NODE)
    }
    assert len(set(keys.values())) == len(keys)
    # The GUID is canonical, so its case does not change a key.
    assert node_key("x", FIRST) == node_key("x", FIRST.lower())
    for edge in _keyed_rows(projected, LINEAGE_EDGE):
        assert edge["downstream_node_key"] == keys[edge["downstream_node_id"]]
        if edge["edge_kind"] != EDGE_EXTERNAL:
            assert edge["upstream_node_key"] == keys[edge["upstream_node_id"]]
        through = edge["through_node_id"]
        assert edge["through_node_key"] == (None if through is None else keys[through])


@weaver_test()
def test_one_object_in_two_workspaces_has_two_keys_and_a_rename_changes_neither(
    repository,
):
    built = built_catalogue(repository, _bindings(SALES, REPORTING))
    first = _keyed(built, dict.fromkeys(built.rows, FIRST))
    second = _keyed(built, dict.fromkeys(built.rows, SECOND))

    def keys(projected):
        return {
            row["node_id"]: row["node_key"]
            for row in _keyed_rows(projected, LINEAGE_NODE)
        }

    assert keys(first).keys() == keys(second).keys()
    assert not set(keys(first).values()) & set(keys(second).values())
    # A display name is no part of a key, so renaming a workspace keeps them.
    renamed = Catalogue(
        {
            item: {
                **tables,
                "Installation": tuple(
                    {**row, "workspace_name": "Renamed", "workspace_id": FIRST}
                    for row in tables.get("Installation", ())
                ),
            }
            for item, tables in built.rows.items()
        }
    )
    assert keys(_keyed(renamed, dict.fromkeys(renamed.rows, FIRST))) == keys(first)
    assert keys({item: lineage_rows(renamed, item) for item in renamed.rows}) == keys(
        first
    )


@weaver_test()
def test_a_shortcut_across_workspaces_reads_the_source_in_its_own_workspace(
    repository,
):
    """Reporting shortcuts Sales; each item is projected from its own rows."""

    built = built_catalogue(repository, _bindings(SALES, REPORTING))
    projected = _keyed(built, {SALES: FIRST, REPORTING: SECOND})
    sales = {
        row["node_id"]: row["node_key"] for row in projected[SALES][LINEAGE_NODE.name]
    }
    (crossing,) = [
        edge
        for edge in projected[REPORTING][LINEAGE_EDGE.name]
        if edge["edge_kind"] == EDGE_SHORTCUT and edge["upstream_node_id"] in sales
    ]

    assert crossing["upstream_node_key"] == sales[crossing["upstream_node_id"]]
    assert crossing["upstream_node_key"] == node_key(
        crossing["upstream_node_id"], FIRST
    )
    assert crossing["downstream_node_key"] == node_key(
        crossing["downstream_node_id"], SECOND
    )


@weaver_test()
def test_an_upstream_in_no_known_workspace_matches_no_node(repository):
    """Neither an External read nor an item no installation records is invented."""

    built = built_catalogue(repository, _bindings(SALES, REPORTING))
    projected = _keyed(built, {REPORTING: SECOND})
    reporting_keys = {
        row["node_key"] for row in projected[REPORTING][LINEAGE_NODE.name]
    }
    crossing = [
        edge
        for edge in projected[REPORTING][LINEAGE_EDGE.name]
        if edge["upstream_node_id"].startswith(f"{SALES}/")
    ]

    assert crossing
    for edge in crossing:
        assert edge["upstream_node_key"] == node_key(edge["upstream_node_id"])
        assert edge["upstream_node_key"] not in reporting_keys
    unresolved = _keyed(
        _desired(repository, _bindings(SALES, REPORTING), omitted={CUSTOMER}),
        {SALES: FIRST, REPORTING: SECOND},
    )
    external = [
        edge
        for edge in _keyed_rows(unresolved, LINEAGE_EDGE)
        if edge["edge_kind"] == EDGE_EXTERNAL
    ]
    keys = {row["node_key"] for row in _keyed_rows(unresolved, LINEAGE_NODE)}
    assert external
    assert all(edge["upstream_node_key"] not in keys for edge in external)


@weaver_test()
def test_a_build_keys_lineage_by_every_workspace_the_catalogue_records(repository):
    """The upstream item's workspace comes from the current catalogue."""

    from weaver.catalogue.lineage_rows import with_lineage_rows

    built = built_catalogue(repository, _bindings(SALES, REPORTING))

    def installed(item, guid):
        return {
            **built.rows[item],
            "Installation": tuple(
                {**row, "workspace_id": guid}
                for row in built.rows[item]["Installation"]
            ),
        }

    current = Catalogue({SALES: installed(SALES, FIRST)})
    desired = Catalogue({REPORTING: installed(REPORTING, SECOND)})
    projected = with_lineage_rows(desired, repository, current=current)

    crossing = [
        edge
        for edge in projected.rows[REPORTING][LINEAGE_EDGE.name]
        if edge["upstream_node_id"].startswith(f"{SALES}/")
    ]
    assert crossing
    assert all(
        edge["upstream_node_key"] == node_key(edge["upstream_node_id"], FIRST)
        for edge in crossing
    )


@weaver_test()
def test_the_flags_are_the_installed_nodes_own(repository):
    """Health reads them, so they are the installed graph's answers."""

    built = built_catalogue(repository, _bindings(SALES, REPORTING))
    by_id = {node.node_id: node for node in built.dag().nodes}

    flagged = {row["node_id"]: row for row in _rows(built, LINEAGE_NODE)}
    for node_id, row in flagged.items():
        node = by_id[node_id]
        assert (
            row["can_load"],
            row["is_static"],
            row["is_mirrored"],
            row["is_installed"],
        ) == (node.can_load, node.is_static, node.is_mirrored, node.is_installed)
    assert any(row["can_load"] for row in flagged.values())
    assert not all(row["can_load"] for row in flagged.values())


@weaver_test()
def test_every_certified_data_object_has_a_graph_node(repository):
    """The Dashboard reads a certified object without one as an unprojected graph."""

    built = built_catalogue(repository, _bindings(SALES, REPORTING))

    nodes = {
        (row["item_type"], row["item_name"], row["schema_name"], row["object_name"])
        for row in _rows(built, LINEAGE_NODE)
    }
    certified = {
        (row["item_type"], row["item_name"], row["schema_name"], row["object_name"])
        for row in _rows(built, REGISTRY)
        if row["object_role"] == "data"
    }
    assert certified and certified <= nodes


@weaver_test()
def test_a_model_build_deploys_is_projected_as_refreshed():
    """Its refresh is in the definition Fabric returns, read after projection."""

    item = WeaverItemId.parse("SemanticModel/Sales")
    model = WeaverDocumentId.model_root(item)
    catalogue = Catalogue(
        {
            item: {
                INSTALLATION.name: (installation_row(item, "Sales"),),
                REGISTRY.name: (registry_row(model, object_type="semantic_model"),),
            }
        }
    )

    (node,) = lineage_rows(catalogue, item)[LINEAGE_NODE.name]

    assert (node["node_kind"], node["can_load"], node["is_installed"]) == (
        "semantic_model",
        True,
        True,
    )


@weaver_test()
def test_borrowing_changes_only_what_the_mirror_marks_in_lineage(repository):
    """``lineage_statement`` corrects lineage published without ``_.Mirror``.

    Projected with an item's ``_.Mirror`` rows, a borrowed object differs from
    its projection without them in exactly the columns the statement sets, and
    the statement joins on the identity both tables store.
    """

    from weaver.catalogue.borrow import lineage_statement
    from weaver.catalogue.tables import MIRROR

    built = built_catalogue(repository, _bindings(SALES, REPORTING))
    tables = dict(built.rows[REPORTING])
    report = {"schema_name": "Rpt", "object_name": "CustomerReport"}
    borrowed = {
        "item_type": "Warehouse",
        "item_name": "Reporting",
        **report,
        "source_workspace_name": "Production",
        "source_target_name": "Reporting",
        "source_schema_name": "Rpt",
        "source_object_name": "CustomerReport",
        "physical_type": "table",
    }
    plain = lineage_rows(Catalogue({REPORTING: tables}), REPORTING)
    mirrored = lineage_rows(
        Catalogue({REPORTING: {**tables, MIRROR.name: (borrowed,)}}), REPORTING
    )

    assert plain[LINEAGE_EDGE.name] == mirrored[LINEAGE_EDGE.name]
    before = {row["node_id"]: row for row in plain[LINEAGE_NODE.name]}
    changed = {}
    for row in mirrored[LINEAGE_NODE.name]:
        differs = {
            name
            for name, value in row.items()
            if name != "signature" and before[row["node_id"]][name] != value
        }
        if differs:
            changed[(row["schema_name"], row["object_name"])] = (
                differs,
                row["is_mirrored"],
                row["can_load"],
            )
    assert changed == {
        ("Rpt", "CustomerReport"): ({"is_mirrored", "can_load"}, True, False)
    }
    statement = lineage_statement()
    assert "set [Is mirrored] = 1, [Can load] = 0" in statement
    for column in ("Item type", "Item name", "Schema name", "Object name"):
        assert f"[m].[{column}] = [n].[{column}]" in statement
