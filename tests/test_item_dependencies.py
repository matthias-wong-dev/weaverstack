"""Pure-Python tests for item-owned dependency graphs and sparse projection."""

from __future__ import annotations

import pytest
from test_item_repository import (
    _estate,
    _folder,
    _table,
    _warehouse_table,
    _write,
)

from weaver.declaration import parse_item_repository, project_bound_documents
from weaver.declaration.model import WeaverDocumentId, WeaverItemId
from weaver.errors import BuildError, GraphError
from weaver.locations import Location


def _dependency_estate(tmp_path):
    root = _estate(tmp_path)
    raw_table = _table("Sales.Customer").replace(
        "from weaver import Table",
        "from weaver import Table\nfrom .Files.Sales__Landing import Sales__Landing",
    )
    landing = _folder("Sales.Landing").replace(
        "from weaver import Folder",
        "from weaver import Folder\nfrom ..lib.csv_helpers import rows",
    )
    export = _folder("Sales.Export").replace(
        "from weaver import Folder",
        "from weaver import Folder\nfrom ..Sales__Customer import Sales__Customer",
    )
    archive = _folder("Sales.Archive").replace(
        "from weaver import Folder",
        "from weaver import Folder\nfrom .Sales__Landing import Sales__Landing",
    )
    reporting = _warehouse_table("Sales.Customer").replace(
        "select cast(1 as varchar(20)) as Id;",
        "select c.Id from Sales.PortableCustomer as c;",
    )
    audit = _warehouse_table("Sales.Change").replace(
        "select cast(1 as varchar(20)) as Id;",
        "select c.Id from Raw_LH.Sales.Customer as c;",
    )
    _write(root, "Lakehouse/Raw/Sales__Customer.py", raw_table)
    _write(root, "Lakehouse/Raw/Files/Sales__Landing.py", landing)
    _write(root, "Lakehouse/Raw/Files/Sales__Export.py", export)
    _write(root, "Lakehouse/Raw/Files/Sales__Archive.py", archive)
    _write(root, "Warehouse/Reporting/Sales.Customer.sql", reporting)
    _write(root, "Warehouse/Audit/Sales.Change.sql", audit)
    _write(
        root,
        "Warehouse/Reporting/alias.yml",
        """aliases:
  Sales.PortableCustomer: Lakehouse/Curated/Sales.Customer
""",
    )
    return root


def _edge(repository, consumer: str, reference: str):
    identity = WeaverDocumentId.parse(consumer)
    return next(
        edge
        for edge in repository.dependency_edges
        if edge.consumer == identity and edge.reference == reference
    )


def test_relative_python_imports_resolve_across_tables_and_files(tmp_path):
    repository = parse_item_repository(Location(str(_dependency_estate(tmp_path))))

    delta_to_folder = _edge(
        repository,
        "Lakehouse/Raw/Sales.Customer",
        ".Files.Sales__Landing",
    )
    folder_to_delta = _edge(
        repository,
        "Lakehouse/Raw/Files/Sales.Export",
        "..Sales__Customer",
    )
    folder_to_folder = _edge(
        repository,
        "Lakehouse/Raw/Files/Sales.Archive",
        ".Sales__Landing",
    )

    assert str(delta_to_folder.producer) == "Lakehouse/Raw/Files/Sales.Landing"
    assert str(folder_to_delta.producer) == "Lakehouse/Raw/Sales.Customer"
    assert str(folder_to_folder.producer) == "Lakehouse/Raw/Files/Sales.Landing"
    assert all(
        edge.is_within_item
        for edge in (delta_to_folder, folder_to_delta, folder_to_folder)
    )


def test_lib_import_creates_no_object_edge(tmp_path):
    repository = parse_item_repository(Location(str(_dependency_estate(tmp_path))))
    landing = WeaverDocumentId.parse("Lakehouse/Raw/Files/Sales.Landing")
    assert not any(
        edge.consumer == landing and "lib" in edge.reference
        for edge in repository.dependency_edges
    )


def test_two_part_sql_reference_resolves_through_cross_item_alias(tmp_path):
    repository = parse_item_repository(Location(str(_dependency_estate(tmp_path))))
    edge = _edge(
        repository,
        "Warehouse/Reporting/Sales.Customer",
        "Sales.PortableCustomer",
    )

    assert edge.reference == "Sales.PortableCustomer"
    assert str(edge.producer) == "Lakehouse/Curated/Sales.Customer"
    assert edge.uses_alias
    assert not edge.is_within_item


def test_the_alias_destination_is_its_own_node_between_source_and_consumer(tmp_path):
    """The graph is three hops where the published edge is two.

    ``dependency_edges`` says where the data comes from, so an alias edge names
    the source document. The graph says what must be built and in what order, and
    there the alias destination is a thing in its own right — so impact reaches a
    consumer *through* it rather than jumping the boundary.
    """

    repository = parse_item_repository(Location(str(_dependency_estate(tmp_path))))
    graph = repository.dependency_graph
    source = "Lakehouse/Curated/Sales.Customer"
    alias = "Warehouse/Reporting/Sales.PortableCustomer"
    consumer = "Warehouse/Reporting/Sales.Customer"

    assert alias in graph.nodes
    pairs = {(edge.upstream, edge.downstream) for edge in graph.edges}
    assert (source, alias) in pairs
    assert (alias, consumer) in pairs
    # The two-hop shortcut must *not* also be there, or the alias would be
    # bypassable and a build could order the consumer before it.
    assert (source, consumer) not in pairs

    assert consumer in graph.descendants(source)
    assert graph.descendants(alias) == (consumer,)


def test_an_alias_no_document_consumes_still_waits_for_its_source(tmp_path):
    """It has to be materialised after the thing it points at exists, whether or
    not anything reads it yet."""

    root = _dependency_estate(tmp_path)
    _write(
        root,
        "Warehouse/Audit/alias.yml",
        "aliases:\n  Sales.Unread: Lakehouse/Curated/Sales.Customer\n",
    )
    graph = parse_item_repository(Location(str(root))).dependency_graph

    alias = "Warehouse/Audit/Sales.Unread"
    assert alias in graph.nodes
    assert ("Lakehouse/Curated/Sales.Customer", alias) in {
        (edge.upstream, edge.downstream) for edge in graph.edges
    }


def test_published_dependency_edges_ignore_the_alias_node(tmp_path):
    """The graph gained a hop; the catalogue's dependency rows did not.

    Guards the decoupling directly: no edge may name an alias destination as a
    producer, and none may become within-item by acquiring one.
    """

    repository = parse_item_repository(Location(str(_dependency_estate(tmp_path))))
    destinations = {alias.destination for alias in repository.aliases}

    assert destinations
    assert all(
        edge.producer not in destinations for edge in repository.dependency_edges
    )

    alias_edge = _edge(
        repository, "Warehouse/Reporting/Sales.Customer", "Sales.PortableCustomer"
    )
    assert str(alias_edge.producer) == "Lakehouse/Curated/Sales.Customer"
    assert not alias_edge.is_within_item


def test_three_part_sql_reference_is_preserved_as_physical(tmp_path):
    repository = parse_item_repository(Location(str(_dependency_estate(tmp_path))))
    edge = _edge(
        repository,
        "Warehouse/Audit/Sales.Change",
        "Raw_LH.Sales.Customer",
    )

    assert edge.producer is None
    assert edge.is_physical
    assert not edge.is_within_item


def test_sparse_projection_selects_only_exact_bound_items(tmp_path):
    repository = parse_item_repository(Location(str(_dependency_estate(tmp_path))))
    selected = project_bound_documents(
        repository, [WeaverItemId.parse("Warehouse/Reporting")]
    )

    assert [source.node_id for source in selected] == [
        "Warehouse/Reporting/Sales.Customer"
    ]
    assert all(source.logical_id.item.item_name != "Curated" for source in selected)


def test_projection_requires_at_least_one_binding(tmp_path):
    repository = parse_item_repository(Location(str(_dependency_estate(tmp_path))))
    with pytest.raises(BuildError, match="at least one Weaver item"):
        project_bound_documents(repository, [])


def test_dependency_cycle_across_items_is_rejected(tmp_path):
    root = _estate(tmp_path)
    curated = _table("Sales.Customer").replace(
        "from weaver import Table",
        "from weaver import Table\nfrom Sales__Reporting import Sales__Reporting",
    )
    reporting = _warehouse_table("Sales.Customer").replace(
        "select cast(1 as varchar(20)) as Id;",
        "select c.Id from Sales.Curated as c;",
    )
    _write(root, "Lakehouse/Curated/Sales__Customer.py", curated)
    _write(root, "Warehouse/Reporting/Sales.Customer.sql", reporting)
    _write(
        root,
        "Lakehouse/Curated/alias.yml",
        "aliases:\n  Sales.Reporting: Warehouse/Reporting/Sales.Customer\n",
    )
    _write(
        root,
        "Warehouse/Reporting/alias.yml",
        "aliases:\n  Sales.Curated: Lakehouse/Curated/Sales.Customer\n",
    )
    with pytest.raises(GraphError, match="dependency cycle"):
        parse_item_repository(Location(str(root)))
