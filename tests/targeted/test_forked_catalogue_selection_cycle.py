"""Selection over the catalogue a fork leaves, on one clock and on two.

A fork copies the source estate's rows and leaves the destination's own
``Warehouse/_weaver`` rows alone.
:func:`weaver.build_bundle.incremental.stale_through_shortcuts` reads a Registry
build datetime to find a pointer a producer outran, and the ``_`` surface
pointers are sourced from ``Warehouse/_weaver``. Dated on two clocks they read
as behind the catalogue tables they stand on, and the descendant walk carries
that into every object whose query reads the surface.

Pure Python, through :func:`weaver.build_bundle.incremental.select_build` and
the freshness read the planner performs before it.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path

from factories import (
    FixtureInventory,
    _write,
    installed_catalogue,
    item_bindings,
    item_id,
    schema_document,
    warehouse_table,
    warehouse_view,
)
from support.weaver_test import weaver_test
from support.workspaces import WORKSPACE

from weaver.build_bundle import effective_item_bindings
from weaver.build_bundle.incremental import select_build, stale_through_shortcuts
from weaver.catalogue.state import Catalogue
from weaver.catalogue.tables import BUILD_DATETIME, MIRROR, REGISTRY
from weaver.declaration import parse_item_repository
from weaver.declaration.metadata import ObjectId
from weaver.declaration.model import WeaverDocumentId
from weaver.locations import Location
from weaver.targets import ItemRef

ITEM = "Warehouse/Curated"
BUILTIN = "Warehouse/_weaver"
TARGET = "Curated_Dev"
SOURCE_TARGET = "Curated"

#: The instant the destination catalogue was built at, and the later one the
#: fork copied the source estate's rows in at.
CATALOGUE_BUILT_AT = datetime(2026, 3, 1, 0, 0, 0)
FORKED_AT = CATALOGUE_BUILT_AT + timedelta(minutes=1)
#: What the source estate's own builds had dated its rows to.
SOURCE_BUILT_AT = CATALOGUE_BUILT_AT - timedelta(days=8)

#: A table that reads its own bookmark, which puts it under the ``_.Bookmark``
#: surface view in the dependency graph.
BOOKMARKED = """\
declare @bookmark datetime2(6) = (
    select b.[Bookmark datetime]
      from [_].[Bookmark] as b
     where b.[Object name] = 'Order'
);
select cast(1 as int) as OrderId
"""


# --- the estate ---------------------------------------------------------------


def _estate(root: Path):
    """One Warehouse item: a bookmarked table, a view on it, and a plain table.

    The plain table reads no surface view, so it says which objects the walk
    from a ``_`` surface pointer reaches and which it leaves.
    """

    _write(root, f"{ITEM}/schemas/Sales.yml", schema_document("Sales"))
    _write(
        root,
        f"{ITEM}/Sales.Order.sql",
        warehouse_table("Sales.Order", select=BOOKMARKED, primary_key="OrderId"),
    )
    _write(
        root,
        f"{ITEM}/Sales.OrderReport.sql",
        warehouse_view(
            "Sales.OrderReport",
            select="select OrderId from [Sales].[Order]",
            depends_on="Sales.Order",
        ),
    )
    _write(
        root,
        f"{ITEM}/Sales.Region.sql",
        warehouse_table(
            "Sales.Region",
            select="select cast(1 as int) as RegionId",
            primary_key="RegionId",
        ),
    )
    return parse_item_repository(Location(str(root)))


def _object(name: str) -> WeaverDocumentId:
    return WeaverDocumentId(item_id(ITEM), ObjectId("Sales", name))


def _bindings():
    """The item and the catalogue item, as every build binds them."""

    return effective_item_bindings(
        item_bindings((ITEM, TARGET)),
        control_item=ItemRef("Weaver_Control"),
        workspace_name=WORKSPACE,
    )


#: Every relation of the item a mirror stands over.
BORROWED = ("Order", "OrderReport", "Region")


def _forked(repository, *, copied_at: datetime) -> Catalogue:
    """The destination catalogue as a fork leaves it.

    ``Warehouse/_weaver`` is the build the fork ran to make this catalogue, so
    its rows carry :data:`CATALOGUE_BUILT_AT`. Every other row was copied, and
    ``copied_at`` is what the copy dated it to.
    """

    catalogue = installed_catalogue(repository, _bindings())
    rows = {}
    for item, tables in catalogue.rows.items():
        instant = CATALOGUE_BUILT_AT if item.item_name == "_weaver" else copied_at
        rows[item] = {
            **{name: tuple(each) for name, each in tables.items()},
            REGISTRY.name: tuple(
                {**row, BUILD_DATETIME: instant.isoformat()}
                for row in tables[REGISTRY.name]
            ),
        }
    item = item_id(ITEM)
    rows[item][MIRROR.name] = tuple(
        {
            "item_type": item.item_type,
            "item_name": item.item_name,
            "schema_name": "Sales",
            "object_name": name,
            "source_workspace_name": WORKSPACE,
            "source_target_name": SOURCE_TARGET,
            "source_schema_name": "Sales",
            "source_object_name": name,
            "physical_type": "view",
        }
        for name in BORROWED
    )
    return Catalogue(rows=rows, materialised=catalogue.materialised | {MIRROR.name})


def _inventory(repository):
    """The Warehouse as a mirror leaves it: a View at each borrowed address."""

    bound = {each.item: each.to_bound_target() for each in _bindings().entries}
    inventory = FixtureInventory.from_repository(
        repository,
        item=ITEM,
        target_id=bound[item_id(ITEM)].id,
        kind="warehouse",
        target_name=TARGET,
    )
    names = {f"Sales.{name}" for name in BORROWED}
    return replace(
        inventory,
        tables=tuple(name for name in inventory.tables if name not in names),
        views=tuple(sorted(set(inventory.views) | names)),
    )


def _selection(repository, catalogue):
    """The build's decision, with freshness read as the planner reads it."""

    by_item = _bindings().by_item
    inventories = {
        item_id(ITEM): _inventory(repository),
        item_id(BUILTIN): FixtureInventory.from_repository(
            repository,
            item=BUILTIN,
            target_id=f"warehouse:{WORKSPACE}/Weaver_Control",
            kind="warehouse",
            target_name="Weaver_Control",
        ),
    }
    registered = {
        identity: document
        for identity, document in catalogue.registered.items()
        if identity.item in by_item
    }
    return select_build(
        repository,
        registered,
        selected=set(registered),
        inventories=inventories,
        stale_consumers=stale_through_shortcuts(
            repository, catalogue.registered, bound_items=by_item
        ),
        mirrored=catalogue.mirrors,
    )


# --- the surface pointer, on one clock and on two -----------------------------


@weaver_test()
def test_a_fork_dates_its_rows_so_no_surface_pointer_reads_as_behind(tmp_path):
    """The copy carries the fork's instant, which is after the catalogue build.

    Nothing in the estate changed, so an unchanged mirror stays a mirror: no
    object is selected, and the ``_.Mirror`` rows that record what is borrowed
    are left standing.
    """

    repository = _estate(tmp_path / "repo")
    catalogue = _forked(repository, copied_at=FORKED_AT)

    stale = stale_through_shortcuts(
        repository, catalogue.registered, bound_items=_bindings().by_item
    )
    selection = _selection(repository, catalogue)

    assert stale == ()
    assert selection.selected_for_build == ()
    assert set(catalogue.mirrors) == {_object(name) for name in BORROWED}


@weaver_test()
def test_the_source_estates_instants_drag_every_surface_reader_into_the_build(
    tmp_path,
):
    """The defect, as a copied ``build_datetime`` produced it.

    Dated by the source estate's builds, each ``_`` surface pointer is behind
    the catalogue table it stands on, and the walk from it reaches the
    bookmarked table and the view over it. Both are mirrors, so
    ``prohibit_rebuild`` does not hold them, and the build gives them rows of
    their own. ``Sales.Region`` reads no surface view and is left alone, which
    is what says the walk is the mechanism.
    """

    repository = _estate(tmp_path / "repo")
    catalogue = _forked(repository, copied_at=SOURCE_BUILT_AT)

    selection = _selection(repository, catalogue)

    assert _object("Order") in selection.selected_for_build
    assert _object("OrderReport") in selection.selected_for_build
    assert _object("Region") not in selection.selected_for_build
