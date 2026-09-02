"""The repository's item-level dependency graph and its topological layers."""

from __future__ import annotations

import pytest
from support.weaver_test import weaver_test
from test_item_dependencies_declaration import _dependency_estate
from test_item_repository_declaration import _estate, _table, _warehouse_table, _write

from weaver.declaration import parse_item_repository
from weaver.declaration.model import WeaverItemId
from weaver.errors import GraphError
from weaver.locations import Location


def _layers(repository):
    return [[str(item) for item in layer] for layer in repository.item_layers]


@weaver_test()
def test_every_item_is_placed_in_exactly_one_layer(tmp_path):
    repository = parse_item_repository(Location(str(_dependency_estate(tmp_path))))
    placed = [item for layer in repository.item_layers for item in layer]

    assert sorted(placed, key=str) == sorted(
        (item.identity for item in repository.items), key=str
    )
    assert len(placed) == len(set(placed))


@weaver_test()
def test_an_shortcut_puts_its_source_item_in_an_earlier_layer(tmp_path):
    """``Warehouse/Reporting`` shortcuts ``Lakehouse/Curated``, so it comes after it."""

    repository = parse_item_repository(Location(str(_dependency_estate(tmp_path))))
    layer_of = {
        str(item): index
        for index, layer in enumerate(repository.item_layers)
        for item in layer
    }

    assert layer_of["Lakehouse/Curated"] < layer_of["Warehouse/Reporting"]


@weaver_test()
def test_an_unused_shortcut_still_orders_its_two_items(tmp_path):
    """The shortcut itself has to be materialised after its source exists.

    Nothing consumes ``Sales.Landed`` here, so no document edge exists, and the
    shortcut or view standing for it is still built in ``Curated`` over a table
    ``Raw`` produces.
    """

    root = _estate(tmp_path)
    _write(
        root,
        "Lakehouse/Curated/shortcuts.py",
        'from weaver import Shortcut\n\nSales__Landed = Shortcut(\n    shortcut_type="table",\n    target_type="logical",\n    target="Lakehouse/Raw/Tables/Sales.Customer",\n)\n',
    )
    repository = parse_item_repository(Location(str(root)))
    layer_of = {
        str(item): index
        for index, layer in enumerate(repository.item_layers)
        for item in layer
    }

    assert layer_of["Lakehouse/Raw"] < layer_of["Lakehouse/Curated"]


@weaver_test()
def test_independent_items_share_one_layer(tmp_path):
    """Every authored item together, behind the built-in item they all reach.

    Nothing here declares a relationship with anything else, so the only order is
    the one the catalogue imposes: an item whose objects Weaver loads presents the
    catalogue's `_.Bookmark`, and that is a document the built-in item owns.
    """

    from weaver.catalogue.builtin import BUILTIN_ITEM

    repository = parse_item_repository(Location(str(_estate(tmp_path))))

    assert _layers(repository) == [
        [str(BUILTIN_ITEM)],
        sorted(
            str(item.identity)
            for item in repository.items
            if item.identity != BUILTIN_ITEM
        ),
    ]


@weaver_test()
def test_an_item_cycle_is_rejected_even_when_no_document_cycle_exists(tmp_path):
    """Two items that shortcut each other's different objects.

    The document graph stays acyclic, ``Curated.Customer`` feeds
    ``Reporting.Customer``, and ``Reporting.Audit`` feeds ``Curated.Summary``,
    so nothing at document level objects. The items still cannot be ordered, and
    that is a repository fault rather than something to discover at install time.
    """

    root = _estate(tmp_path)
    _write(
        root,
        "Lakehouse/Curated/Tables/Sales__Summary.py",
        _table("Sales.Summary").replace(
            "from weaver import Table",
            "from weaver import Table\nfrom Tables.Sales__Audited import Sales__Audited",
        ),
    )
    _write(
        root,
        "Warehouse/Reporting/Sales.Customer.sql",
        _warehouse_table("Sales.Customer").replace(
            "select cast(1 as varchar(20)) as Id;",
            "select c.Id from Sales.Landed as c;",
        ),
    )
    _write(
        root,
        "Warehouse/Reporting/Sales.Audit.sql",
        _warehouse_table("Sales.Audit"),
    )
    _write(
        root,
        "Lakehouse/Curated/shortcuts.py",
        'from weaver import Shortcut\n\nSales__Audited = Shortcut(\n    shortcut_type="table",\n    target_type="logical",\n    target="Warehouse/Reporting/Sales.Audit",\n)\n',
    )
    _write(
        root,
        "Warehouse/Reporting/shortcuts.yml",
        "logical:\n  Warehouse/Reporting/Sales.Landed: Lakehouse/Curated/Tables/Sales.Customer\n",
    )

    with pytest.raises(GraphError, match="item dependency cycle"):
        parse_item_repository(Location(str(root)))


@weaver_test()
def test_a_within_item_dependency_creates_no_item_edge(tmp_path):
    """An item cannot wait for itself: the document graph already orders those.

    The built-in item is upstream and is not one of these edges. Every item
    whose objects Weaver loads reaches the catalogue's `_.Bookmark`.
    """

    from weaver.catalogue.builtin import BUILTIN_ITEM

    repository = parse_item_repository(Location(str(_dependency_estate(tmp_path))))
    raw = WeaverItemId.parse("Lakehouse/Raw")

    assert repository.item_graph.upstream_of(str(raw)) == (str(BUILTIN_ITEM),)
