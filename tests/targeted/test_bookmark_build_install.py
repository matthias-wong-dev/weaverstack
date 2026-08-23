"""What a build decides about ``_.Bookmark``, as the intent it freezes.

Three claims, and they are separate because they fail separately.

Which objects can hold a bookmark at all: the ones Weaver loads, derived from
the load artefacts the item installs, so what carries a bookmark cannot drift
from what has something to run.

Which rows a build ends the life of: the keyed rows the reconciliation action
carries. Read as structured intent rather than as SQL text, because the decision
is which object's operational state is no longer current — a statement is one
rendering of that, and asserting on the rendering makes a renaming of a keyword
look like a change of lifecycle.

Where the action sits: before the first physical action. That is the safety
property and the only one whose failure is silent — an absent bookmark makes the
next load read everything, while one left in place over a recreated table makes
it read almost nothing.

Pure Python. What a build decides is a build's own decision, and every input to
it can be constructed.
"""

from __future__ import annotations

import pytest
from factories import (
    CATALOGUE_ITEM,
    ITEM,
    WAREHOUSE_ITEM,
    catalogue_inventory,
    estate_bindings,
    estate_inventories,
    full_estate,
    item_id,
    lakehouse_table,
    schema_document,
    warehouse_table,
)
from support.catalogues import LOADED_AT
from support.weaver_test import weaver_test
from support.workspaces import WORKSPACE

from weaver.build_bundle import WarehouseBinding, generate_item_build_bundle
from weaver.build_bundle.bookmarks import bookmark_invalidation
from weaver.catalogue.state import Catalogue
from weaver.catalogue.tables import BOOKMARK
from weaver.declaration import parse_item_repository
from weaver.etl import item_bookmarkable_objects
from weaver.locations import Location
from weaver.store import FilesystemStore
from weaver.targets import ItemRef

CATALOGUE = WarehouseBinding(ItemRef("Weaver"), workspace_name=WORKSPACE)


@pytest.fixture
def estate(tmp_path):
    return full_estate(tmp_path / "repo")


#: A catalogue holding no rows at all, which is every state these tests start
#: from: what a build says about bookmarks turns on whether the *table* is there,
#: and a target's inventory answers that.
EMPTY = Catalogue({})


def _inventories_over(inventories, *, holding_bookmark: bool = True):
    """These target inventories, plus the catalogue Warehouse's own."""

    with_catalogue = dict(inventories)
    with_catalogue[CATALOGUE_ITEM] = catalogue_inventory(holding=holding_bookmark)
    return with_catalogue


def _inventories(repository, *, holding_bookmark: bool = True):
    """Every target this estate binds, including the catalogue's own Warehouse.

    ``holding_bookmark`` is the distinction the bookmark stages turn on: a
    catalogue Warehouse without the table is one this bundle is creating it in,
    and the table can hold no row anything could have written.
    """

    return _inventories_over(
        estate_inventories(repository, empty=True), holding_bookmark=holding_bookmark
    )


def _bundle(
    repository, tmp_path, *, catalogue=None, inventories=None, bookmark_source=None
):
    return generate_item_build_bundle(
        repository,
        bindings=estate_bindings(),
        output=Location(str(tmp_path / "bundle")),
        store=FilesystemStore(),
        target_inventories=inventories
        if inventories is not None
        else _inventories(repository),
        catalogue=catalogue if catalogue is not None else EMPTY,
        catalogue_binding=CATALOGUE,
        bookmark_source=bookmark_source,
    )


def _bookmark_actions(bundle):
    return [
        (sequence, action)
        for sequence, _batch, action in bundle.plan.actions()
        if action.kind == "reconcile_bookmarks"
    ]


def _invalidated(invalidation, table: str = BOOKMARK.name) -> set[tuple]:
    """The rows one intent names, as ``(item type, item name, schema, object)``."""

    return {
        tuple(row[name] for name in BOOKMARK.key)
        for one in invalidation
        if one.table == table
        for row in one.rows
    }


def _decided(repository, *, items, selected_for_build, catalogue):
    """What a build over these inputs decides about ``_.Bookmark``."""

    return _invalidated(
        bookmark_invalidation(
            repository,
            items=items,
            selected_for_build=selected_for_build,
            catalogue=catalogue,
        )
    )


# --- which objects can hold one ------------------------------------------------


@weaver_test()
def test_a_loadable_table_and_folder_are_bookmarkable(estate):
    """Both, and by the same rule: Weaver loads them, so it records how far."""

    found = {str(one) for one in item_bookmarkable_objects(estate, item=item_id(ITEM))}

    assert "Lakehouse/Sales/DWG.Customer" in found
    assert "Lakehouse/Sales/Files/Raw.CustomerCsv" in found


@weaver_test()
def test_a_view_is_not_bookmarkable(estate):
    """A view holds no rows and has no load, so there is nothing to be far into."""

    found = {str(one) for one in item_bookmarkable_objects(estate, item=item_id(ITEM))}

    assert not [one for one in found if "ActiveCustomer" in one]


@weaver_test()
def test_a_runtime_artefact_is_not_bookmarkable(estate):
    """The module and the procedure are what *does* the loading."""

    found = {str(one) for one in item_bookmarkable_objects(estate, item=item_id(ITEM))}

    assert not [one for one in found if "_/Load" in one or "lib" in one]


@weaver_test()
def test_a_table_weaver_does_not_load_is_not_bookmarkable(tmp_path):
    """``Has load procedure: false`` means something else populates it."""

    root = tmp_path / "repo"
    for relative, text in {
        f"{WAREHOUSE_ITEM}/schemas/Sales.yml": schema_document("Sales"),
        f"{WAREHOUSE_ITEM}/Sales.Customer.sql": warehouse_table("Sales.Customer"),
        f"{WAREHOUSE_ITEM}/Sales.External.sql": warehouse_table(
            "Sales.External", has_load_procedure=False
        ),
    }.items():
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    repository = parse_item_repository(Location(str(root)))

    found = {
        str(one)
        for one in item_bookmarkable_objects(repository, item=item_id(WAREHOUSE_ITEM))
    }

    assert found == {"Warehouse/Reporting/Sales.Customer"}


@weaver_test()
def test_the_catalogue_item_holds_no_bookmarks(estate):
    """Its tables are written by catalogue DML, never loaded."""

    assert (
        bookmark_invalidation(
            estate,
            items=(item_id("Warehouse/_weaver"),),
            selected_for_build={item_id(ITEM)},
            catalogue=_holding("_.Bookmark", item="Warehouse/_weaver"),
        )
        == ()
    )


# --- which rows a build ends the life of ---------------------------------------


def _holding(*qualified: str, item: str = ITEM, repository=None) -> Catalogue:
    """A catalogue holding a bookmark row for each of these objects.

    What a build reads and decides from. ``_.Bookmark`` is read like any other
    catalogue table now, so a test says which rows are there rather than which
    rows a statement should have kept.
    """

    owner = item_id(item)
    rows = tuple(
        {
            "item_type": owner.item_type,
            "item_name": owner.item_name,
            "schema_name": schema,
            "object_name": name,
            "bookmark_datetime": LOADED_AT,
        }
        for schema, _, name in (one.rpartition(".") for one in qualified)
    )
    return Catalogue({owner: {"Bookmark": rows}})


@weaver_test()
def test_a_build_with_nothing_to_do_says_nothing_about_bookmarks(estate):
    """An unchanged repository produces an empty bundle, bookmarks included.

    Every row belongs to an object this build keeps, so nothing is obsolete.
    """

    assert (
        bookmark_invalidation(
            estate,
            items=(item_id(ITEM),),
            selected_for_build=(),
            catalogue=_holding("DWG.Customer", "Files/Raw.CustomerCsv"),
        )
        == ()
    )


@weaver_test()
def test_a_build_of_objects_that_hold_no_bookmark_says_nothing_either(estate):
    """Only an object that can hold a bookmark can cost one.

    A view here, and a validation in a real estate: both carry an ordinary
    ``Schema.Object`` identity and neither has a load. Selecting one leaves every
    row where it was.
    """

    view = next(
        identity
        for identity in estate.source_documents
        if identity.item == item_id(ITEM) and "ActiveCustomer" in str(identity)
    )

    assert (
        bookmark_invalidation(
            estate,
            items=(item_id(ITEM),),
            selected_for_build={view},
            catalogue=_holding("DWG.Customer", "Files/Raw.CustomerCsv"),
        )
        == ()
    )


@weaver_test()
def test_a_catalogue_holding_no_rows_has_nothing_to_invalidate(estate, tmp_path):
    """A first build, and the one case that needed a special gate before.

    The table arrives with this bundle, so the read found no rows and there is
    nothing to remove. Nothing is skipped for a stated reason: the action is
    absent because the set of obsolete rows is empty.
    """

    bundle = _bundle(estate, tmp_path)

    assert _bookmark_actions(bundle) == []
    assert bundle.plan.runtime_state == ()


@weaver_test()
def test_a_rebuild_ends_the_incarnation_of_what_it_replaces(estate, tmp_path):
    """Every loadable object rebuilt, so every row it had goes."""

    bundle = _bundle(
        estate, tmp_path, catalogue=_holding("DWG.Customer", "Files/Raw.CustomerCsv")
    )

    assert _invalidated(bundle.plan.runtime_state) == {
        ("Lakehouse", "Sales", "DWG", "Customer"),
        ("Lakehouse", "Sales", "Files/Raw", "CustomerCsv"),
    }


@weaver_test()
def test_only_current_state_is_invalidated(estate, tmp_path):
    """``_.Bookmark`` is current state. Nothing historical is named."""

    bundle = _bundle(
        estate, tmp_path, catalogue=_holding("DWG.Customer", "Files/Raw.CustomerCsv")
    )

    assert {one.table for one in bundle.plan.runtime_state} == {BOOKMARK.name}


# --- where the action sits -----------------------------------------------------


@weaver_test()
def test_bookmarks_are_reconciled_before_the_first_physical_action(estate, tmp_path):
    """The safety property: an invalidation that failed to happen is silent."""

    bundle = _bundle(
        estate, tmp_path, catalogue=_holding("DWG.Customer", "Files/Raw.CustomerCsv")
    )
    physical = {
        "create_schema",
        "build_table",
        "build_view",
        "build_folder",
        "build_procedure",
        "write_file",
        "drop_table",
        "prune_table",
    }
    kinds = [action.kind for _sequence, _batch, action in bundle.plan.actions()]

    assert kinds.index("reconcile_bookmarks") < min(
        index for index, kind in enumerate(kinds) if kind in physical
    )


@weaver_test()
def test_invalidation_is_one_action(estate, tmp_path):
    """One lifecycle decision, so one action carrying the whole intent."""

    bundle = _bundle(
        estate, tmp_path, catalogue=_holding("DWG.Customer", "Files/Raw.CustomerCsv")
    )
    (_sequence, action), *rest = _bookmark_actions(bundle)

    assert not rest
    assert action.executor == "runtime_state"


@weaver_test()
def test_the_action_carries_the_intent_the_plan_states(estate, tmp_path):
    """What runs and what the plan says it means are one computation.

    A summary the planner writes about its own plan proves nothing on its own;
    this is what makes it prove something.
    """

    from weaver.catalogue.runtime_state import read_invalidation

    bundle = _bundle(
        estate, tmp_path, catalogue=_holding("DWG.Customer", "Files/Raw.CustomerCsv")
    )
    (_sequence, action), *_rest = _bookmark_actions(bundle)
    carried = read_invalidation(bundle.store.read(bundle.location / action.payload))

    assert carried == bundle.plan.runtime_state


# --- scope ---------------------------------------------------------------------


@weaver_test()
def test_an_object_left_alone_keeps_its_row(estate):
    """One object rebuilt, and only its row is named."""

    assert _decided(
        estate,
        items=(item_id(ITEM),),
        selected_for_build={document_id_of(estate, "DWG.Customer")},
        catalogue=_holding("DWG.Customer", "Files/Raw.CustomerCsv"),
    ) == {("Lakehouse", "Sales", "DWG", "Customer")}


@weaver_test()
def test_a_row_the_repository_no_longer_declares_goes(estate, tmp_path):
    """Nothing declares it as something Weaver loads, so nothing keeps it.

    A row left behind by an earlier failure goes the same way, and for the same
    reason: what keeps a row is a declaration, not the row's own existence.
    """

    smaller = _without_the_folder(tmp_path)

    assert _decided(
        smaller,
        items=(item_id(ITEM),),
        selected_for_build=(),
        catalogue=_holding("DWG.Customer", "Files/Raw.CustomerCsv"),
    ) == {("Lakehouse", "Sales", "Files/Raw", "CustomerCsv")}


@weaver_test()
def test_an_unrelated_item_is_outside_the_scope_that_prunes(estate):
    """A build maintains the items it was pointed at and no others.

    A row belonging to an item this build does not name is not its to remove,
    even though the table is shared.
    """

    assert _decided(
        estate,
        items=(item_id(ITEM),),
        selected_for_build={one for one in estate.source_documents},
        catalogue=_two_items(),
    ) == {("Lakehouse", "Sales", "DWG", "Customer")}


@weaver_test()
def test_both_items_of_one_build_are_one_intent(estate):
    """Bound items share the table, so addressing them separately is round trips."""

    invalidation = bookmark_invalidation(
        estate,
        items=(item_id(ITEM), item_id(WAREHOUSE_ITEM)),
        selected_for_build={one for one in estate.source_documents},
        catalogue=_two_items(),
    )
    (one,) = invalidation

    assert _invalidated(invalidation) == {
        ("Lakehouse", "Sales", "DWG", "Customer"),
        ("Warehouse", "Reporting", "Rpt", "Customer"),
    }
    assert one.table == BOOKMARK.name


def _two_items() -> Catalogue:
    """A catalogue holding one bookmark row for each of two items."""

    return Catalogue(
        {
            item_id(ITEM): {
                "Bookmark": (
                    {
                        "item_type": "Lakehouse",
                        "item_name": "Sales",
                        "schema_name": "DWG",
                        "object_name": "Customer",
                        "bookmark_datetime": LOADED_AT,
                    },
                )
            },
            item_id(WAREHOUSE_ITEM): {
                "Bookmark": (
                    {
                        "item_type": "Warehouse",
                        "item_name": "Reporting",
                        "schema_name": "Rpt",
                        "object_name": "Customer",
                        "bookmark_datetime": LOADED_AT,
                    },
                )
            },
        }
    )


# --- it is never dropped -------------------------------------------------------


@weaver_test()
def test_a_catalogue_table_cannot_be_dropped_by_a_managed_drop():
    """It holds installed state no declaration reproduces.

    Every catalogue table declares ``Prohibit rebuild``, so selection never
    offers one. This is the guard behind that declaration, and it is at the
    renderer because there the resource is known by its identity — an installer
    would have to read it back out of SQL.
    """

    from weaver.build_bundle.physical import _refuse_protected
    from weaver.errors import BuildError

    for name in ("Bookmark", "Log", "Registry"):
        with pytest.raises(BuildError) as raised:
            _refuse_protected("_", name, f"Warehouse/_weaver/_.{name}")
        assert "cannot be dropped" in str(raised.value)

    # And nothing else is spared by it.
    _refuse_protected("Sales", "Customer", "Lakehouse/Sales/Sales.Customer")


@weaver_test()
def test_prune_spares_the_catalogue_table_and_not_the_local_reference():
    """One name, two things, and the difference decides the lifecycle.

    ``_.Bookmark`` is the catalogue's own table in the catalogue Warehouse and a
    view over it everywhere else. The table is never prune's to remove; the view
    has the ordinary lifecycle of the keep-set it is in, so it goes when the
    item's last loadable object does.
    """

    from weaver.catalogue.tables import is_protected

    assert is_protected("_", "Bookmark")
    assert is_protected("_", "bookmark")
    assert not is_protected("Sales", "Bookmark")


# --- where the Lakehouse reference points --------------------------------------


@weaver_test()
def test_the_source_is_the_catalogue_tables_own_delta_directory():
    """A Warehouse publishes each table as a Delta directory a shortcut can read."""

    from weaver.build_bundle.shortcut_sources import read_bookmark_source

    class _Item:
        workspace_id = "ws-1"
        id = "item-1"
        name = "Weaver"

    class _Resolver:
        def warehouse(self, target):
            return _Item()

    source = read_bookmark_source(resolver=_Resolver(), catalogue="Weaver")

    assert source.path == "Tables/_/Bookmark"
    assert source.item_id == "item-1"


@weaver_test()
def test_the_build_that_creates_the_table_also_points_at_it(estate, tmp_path):
    """One build, and the graph orders it: the table, then the reference to it.

    The reference reads a document the built-in item owns, so the item graph puts
    that item first — the same edge a declared shortcut's source item gets. A
    build that installed the table and deferred the reference would not converge
    in one pass, and the build after it would not be a no-op.
    """

    from weaver.build_bundle.shortcuts import ResolvedShortcutSource

    source = ResolvedShortcutSource(
        workspace_id="ws-1",
        item_id="item-1",
        item_name="Weaver",
        path="Tables/_/Bookmark",
    )

    from weaver.build_bundle import effective_item_bindings

    # The catalogue item bound as every real build binds it, so the bundle holds
    # the table as well as the references to it.
    bindings = effective_item_bindings(
        estate_bindings(), control_item=ItemRef("Weaver"), workspace_name=WORKSPACE
    )
    inventories = _inventories(estate, holding_bookmark=False)
    inventories[CATALOGUE_ITEM] = catalogue_inventory(
        holding=False,
        target_id=next(
            binding.to_bound_target().id
            for binding in bindings.entries
            if binding.item == CATALOGUE_ITEM
        ),
    )

    creating = generate_item_build_bundle(
        estate,
        bindings=bindings,
        output=Location(str(tmp_path / "bundle")),
        store=FilesystemStore(),
        target_inventories=inventories,
        catalogue=EMPTY,
        catalogue_binding=CATALOGUE,
        bookmark_source=source,
    )

    assert _references(creating, "Lakehouse")
    assert _references(creating, "Warehouse")
    # And each comes after the catalogue table it points at, in the same bundle.
    order = [action.id for _sequence, _batch, action in creating.plan.actions()]
    table = order.index("object-Warehouse--_weaver--_.Bookmark")
    assert table < order.index(_references(creating, "Lakehouse")[0])
    assert table < order.index(_references(creating, "Warehouse")[0])


def _references(bundle, item_type: str) -> list[str]:
    """The bookmark-reference actions one bundle installs, per kind of item."""

    wanted = f"bookmark-reference-{item_type}"
    return [
        action.id
        for _sequence, _batch, action in bundle.plan.actions()
        if action.id.startswith(wanted)
    ]


# --- fixtures ------------------------------------------------------------------


def document_id_of(repository, qualified: str):
    """One declared identity by its ``Schema.Object``, as the repository holds it."""

    return next(
        identity
        for identity in repository.source_documents
        if str(identity).endswith(f"/{qualified}")
    )


def _without_the_folder(tmp_path):
    """The same estate with the Folder removed from the repository."""

    root = tmp_path / "smaller"
    for relative, text in {
        f"{ITEM}/schemas/DWG.yml": schema_document("DWG"),
        f"{ITEM}/DWG__Customer.py": lakehouse_table("DWG.Customer"),
    }.items():
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    return parse_item_repository(Location(str(root)))


__all__: tuple = ()
