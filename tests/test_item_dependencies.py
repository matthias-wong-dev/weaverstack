"""Pure-Python tests for item-owned dependency graphs and sparse projection."""

from __future__ import annotations

import pytest

from weaver.errors import BuildError, GraphError
from weaver.locations import Location
from weaver.ses import project_bound_documents, read_weaver_repository
from weaver.ses.model import WeaverDocumentId, WeaverItemId

from test_item_repository import (
    _estate,
    _folder,
    _table,
    _warehouse_table,
    _write,
)


def _dependency_estate(tmp_path):
    root = _estate(tmp_path)
    raw_table = _table("Sales.Customer").replace(
        "from weaver import Table",
        "from weaver import Table\n"
        "from .Files.Sales__Landing import Sales__Landing",
    )
    landing = _folder("Sales.Landing").replace(
        "from weaver import Folder",
        "from weaver import Folder\nfrom ..lib.csv_helpers import rows",
    )
    export = _folder("Sales.Export").replace(
        "from weaver import Folder",
        "from weaver import Folder\n"
        "from ..Sales__Customer import Sales__Customer",
    )
    archive = _folder("Sales.Archive").replace(
        "from weaver import Folder",
        "from weaver import Folder\n"
        "from .Sales__Landing import Sales__Landing",
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
        "alias.yml",
        """aliases:
  Warehouse/Reporting/Sales.PortableCustomer: Lakehouse/Curated/Sales.Customer
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
    repository = read_weaver_repository(Location(str(_dependency_estate(tmp_path))))

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
    assert all(edge.is_within_item for edge in (delta_to_folder, folder_to_delta, folder_to_folder))


def test_lib_import_creates_no_object_edge(tmp_path):
    repository = read_weaver_repository(Location(str(_dependency_estate(tmp_path))))
    landing = WeaverDocumentId.parse("Lakehouse/Raw/Files/Sales.Landing")
    assert not any(
        edge.consumer == landing and "lib" in edge.reference
        for edge in repository.dependency_edges
    )


def test_two_part_sql_reference_resolves_through_cross_item_alias(tmp_path):
    repository = read_weaver_repository(Location(str(_dependency_estate(tmp_path))))
    edge = _edge(
        repository,
        "Warehouse/Reporting/Sales.Customer",
        "Sales.PortableCustomer",
    )

    assert edge.reference == "Sales.PortableCustomer"
    assert str(edge.producer) == "Lakehouse/Curated/Sales.Customer"
    assert edge.uses_alias
    assert not edge.is_within_item


def test_three_part_sql_reference_is_preserved_as_physical(tmp_path):
    repository = read_weaver_repository(Location(str(_dependency_estate(tmp_path))))
    edge = _edge(
        repository,
        "Warehouse/Audit/Sales.Change",
        "Raw_LH.Sales.Customer",
    )

    assert edge.producer is None
    assert edge.is_physical
    assert not edge.is_within_item


def test_sparse_projection_selects_only_exact_bound_items(tmp_path):
    repository = read_weaver_repository(Location(str(_dependency_estate(tmp_path))))
    selected = project_bound_documents(
        repository, [WeaverItemId.parse("Warehouse/Reporting")]
    )

    assert [source.node_id for source in selected] == [
        "Warehouse/Reporting/Sales.Customer"
    ]
    assert all(source.logical_id.item.item_name != "Curated" for source in selected)


def test_projection_requires_at_least_one_binding(tmp_path):
    repository = read_weaver_repository(Location(str(_dependency_estate(tmp_path))))
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
        "alias.yml",
        """aliases:
  Lakehouse/Curated/Sales.Reporting: Warehouse/Reporting/Sales.Customer
  Warehouse/Reporting/Sales.Curated: Lakehouse/Curated/Sales.Customer
""",
    )
    with pytest.raises(GraphError, match="dependency cycle"):
        read_weaver_repository(Location(str(root)))
