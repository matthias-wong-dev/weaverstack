"""Selection over the estate a fork and a mirror leave, with nothing changed.

A fork copies installed state literally, build datetimes included, and a mirror
reproduces the physical estate: it borrows the data, recreates the pointers and
copies the code. What that is worth is that the next build finds nothing to do.

Two chains reach a mirrored object here. One runs through the ``_`` surface,
which is how the ACQSC objects were first dragged into a build: a T-SQL body
reading ``[_].[Bookmark]`` sits under the surface view over
``Warehouse/_weaver``. The other runs through the item's own logical shortcut,
the pointer a mirror recreates.

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
    lakehouse_table,
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
#: The item the pointer reads, which this run does not rebind.
PRODUCER = "Lakehouse/Landing"
PRODUCER_TARGET = "Landing"

#: When the source estate's own build published its rows, and the later instant
#: the destination catalogue was built at. A fork copies the first and writes
#: the second, so one catalogue holds both.
SOURCE_BUILT_AT = datetime(2026, 3, 1, 0, 0, 0)
CATALOGUE_BUILT_AT = SOURCE_BUILT_AT + timedelta(days=8)

#: A table that reads its own bookmark, which puts it under the ``_.Bookmark``
#: surface view, and reads the pointer, which puts it under that too.
BOOKMARKED = """\
declare @bookmark datetime2(6) = (
    select b.[Bookmark datetime]
      from [_].[Bookmark] as b
     where b.[Object name] = 'Order'
);
select OrderId from [Sales].[OrderDelta] where @bookmark is null or OrderId > 0
"""

#: The pointer, as ``shortcuts.yml`` declares it.
POINTER = """\
logical:
  Warehouse/Curated/Sales.OrderDelta: Lakehouse/Landing/Tables/Sales.Order
"""


# --- the estate ---------------------------------------------------------------


def _estate(root: Path):
    """One Warehouse item reading one Lakehouse item through a pointer.

    ``Sales.Region`` reads neither the surface nor the pointer, so it says
    which objects a walk reaches and which it leaves.
    """

    _write(root, f"{PRODUCER}/schemas/Sales.yml", schema_document("Sales"))
    _write(
        root,
        f"{PRODUCER}/Tables/Sales__Order.py",
        lakehouse_table("Sales.Order", columns={"OrderId": "int"}),
    )
    _write(root, f"{ITEM}/schemas/Sales.yml", schema_document("Sales"))
    _write(root, f"{ITEM}/shortcuts.yml", POINTER)
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
    """Both items and the catalogue item, as every build binds them."""

    return effective_item_bindings(
        item_bindings((ITEM, TARGET), (PRODUCER, PRODUCER_TARGET)),
        control_item=ItemRef("Weaver_Control"),
        workspace_name=WORKSPACE,
    )


#: Every relation of the Warehouse item a mirror stands over.
BORROWED = ("Order", "OrderReport", "Region")
#: The pointer the mirror recreates, which is not borrowed.
RECREATED = "OrderDelta"


def _forked(repository) -> Catalogue:
    """The destination catalogue as a fork and a mirror leave it.

    Every copied row keeps the source estate's ``build_datetime``.
    ``Warehouse/_weaver`` is the build the fork ran to make this catalogue, so
    its rows are later, and they are the one thing a fork does not copy.
    """

    catalogue = installed_catalogue(repository, _bindings())
    rows = {}
    for item, tables in catalogue.rows.items():
        instant = CATALOGUE_BUILT_AT if item.item_name == "_weaver" else SOURCE_BUILT_AT
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


def _inventory(repository, *, recreated: bool = True):
    """The Warehouse as a mirror leaves it.

    A View at each borrowed address, and one at the pointer's when the mirror
    recreated it. ``recreated=False`` is the estate a mirror that only borrows
    leaves: Registry certifies the pointer and its address holds nothing.
    """

    bound = {each.item: each.to_bound_target() for each in _bindings().entries}
    inventory = FixtureInventory.from_repository(
        repository,
        item=ITEM,
        target_id=bound[item_id(ITEM)].id,
        kind="warehouse",
        target_name=TARGET,
    )
    borrowed = {f"Sales.{name}" for name in BORROWED}
    pointer = {f"Sales.{RECREATED}"}
    views = set(inventory.views) | borrowed
    views = views | pointer if recreated else views - pointer
    return replace(
        inventory,
        tables=tuple(name for name in inventory.tables if name not in borrowed),
        views=tuple(sorted(views)),
    )


def _inventories(repository, **how):
    bound = {each.item: each.to_bound_target() for each in _bindings().entries}
    made = {item_id(ITEM): _inventory(repository, **how)}
    for item, kind in ((PRODUCER, "lakehouse"), (BUILTIN, "warehouse")):
        identity = item_id(item)
        made[identity] = FixtureInventory.from_repository(
            repository,
            item=item,
            target_id=bound[identity].id,
            kind=kind,
            target_name=bound[identity].name,
        )
    return made


def _selection(repository, catalogue, **how):
    """The build's decision, with freshness read as the planner reads it."""

    by_item = _bindings().by_item
    registered = {
        identity: document
        for identity, document in catalogue.registered.items()
        if identity.item in by_item
    }
    return select_build(
        repository,
        registered,
        selected=set(registered),
        inventories=_inventories(repository, **how),
        stale_consumers=stale_through_shortcuts(
            repository, catalogue.registered, bound_items=by_item
        ),
        mirrored=catalogue.mirrors,
    )


# --- a mirror that left the estate complete -----------------------------------


@weaver_test()
def test_an_unchanged_build_over_a_forked_estate_selects_nothing(tmp_path):
    """A literal fork leaves the next build nothing to do.

    Every copied row carries the source estate's instant, the pointer stands
    where Registry says it does, and the mirrors are left recorded.
    """

    repository = _estate(tmp_path / "repo")
    catalogue = _forked(repository)

    selection = _selection(repository, catalogue)

    assert selection.selected_for_build == ()
    assert set(catalogue.mirrors) == {_object(name) for name in BORROWED}


@weaver_test()
def test_the_catalogues_own_build_does_not_outdate_the_surface_it_made(tmp_path):
    """``Warehouse/_weaver`` is the one row a fork writes for itself.

    Its instant is later than every copied one by construction. Freshness
    leaves the catalogue item out, because every build binds it and a changed
    catalogue table is classified by signature.
    """

    repository = _estate(tmp_path / "repo")

    stale = stale_through_shortcuts(
        repository, _forked(repository).registered, bound_items=_bindings().by_item
    )

    assert stale == ()


# --- a mirror that certified a pointer it did not stand up --------------------


@weaver_test()
def test_a_pointer_the_mirror_did_not_recreate_is_new(tmp_path):
    """The defect the ACQSC run exposed, at the moment it is created.

    Registry certifies the pointer and the Warehouse holds nothing at its
    address, so the build materialises it and re-dates its Registry row. The
    next build then reads every mirrored object behind it as stale, which is
    what took the ACQSC objects out of the mirror.
    """

    repository = _estate(tmp_path / "repo")

    selection = _selection(repository, _forked(repository), recreated=False)

    assert _object(RECREATED) in selection.impact.new
    assert _object(RECREATED) in selection.selected_for_build


@weaver_test()
def test_a_pointer_re_dated_by_a_build_takes_the_mirrors_behind_it(tmp_path):
    """The second half of that path, and why recreating the pointer matters.

    The estate a build of the previous test leaves: the pointer carries that
    build's instant and every mirrored object behind it still carries the
    fork's. ``Sales.Region`` reads neither the pointer nor the surface, so it
    stays put.
    """

    repository = _estate(tmp_path / "repo")
    catalogue = _forked(repository)
    rebuilt = _rebuilt(catalogue, _object(RECREATED))

    selection = _selection(repository, rebuilt)

    assert _object("Order") in selection.selected_for_build
    assert _object("OrderReport") in selection.selected_for_build
    assert _object("Region") not in selection.selected_for_build


def _rebuilt(catalogue: Catalogue, identity) -> Catalogue:
    """The same catalogue, with one object dated by a later build."""

    later = (CATALOGUE_BUILT_AT + timedelta(days=1)).isoformat()
    rows = {}
    for item, tables in catalogue.rows.items():
        rows[item] = {
            **{name: tuple(each) for name, each in tables.items()},
            REGISTRY.name: tuple(
                {**row, BUILD_DATETIME: later}
                if (
                    item == identity.item
                    and str(row.get("object_name")) == identity.object_id.object
                )
                else row
                for row in tables[REGISTRY.name]
            ),
        }
    return Catalogue(rows=rows, materialised=catalogue.materialised)

