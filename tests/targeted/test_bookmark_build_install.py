"""What a build does to ``_.Bookmark``, as the statements it freezes.

Three claims, and they are separate because they fail separately.

Which objects can hold a bookmark at all: the ones Weaver loads, derived from
the load artefacts the item installs, so what carries a bookmark cannot drift
from what has something to run.

When the statements are issued: when the build acts, not on every run, because
an unchanged repository produces an empty bundle and that property is what makes
an idle build cheap.

Where they sit: before the first physical action. That is the safety property
and the only one whose failure is silent — a reset makes the next load read
everything, while a bookmark left advanced over a recreated table makes it read
almost nothing.

Pure Python. The statements are what a build decides, and every input to that
decision can be constructed.
"""

from __future__ import annotations

import json

import pytest
from factories import (
    ITEM,
    WAREHOUSE_ITEM,
    estate_bindings,
    estate_inventories,
    full_estate,
    item_id,
    lakehouse_table,
    schema_document,
    warehouse_table,
)
from support.weaver_test import weaver_test
from support.workspaces import WORKSPACE

from weaver.build_bundle import WarehouseBinding, generate_item_build_bundle
from weaver.build_bundle.bookmarks import bookmark_statements
from weaver.catalogue.state import Catalogue
from weaver.catalogue.tables import BOOKMARK_SENTINEL_TEXT, CATALOGUE_TABLES
from weaver.declaration import parse_item_repository
from weaver.etl import item_bookmarkable_objects
from weaver.locations import Location
from weaver.store import FilesystemStore
from weaver.targets import ItemRef

CATALOGUE = WarehouseBinding(ItemRef("Weaver"), workspace_name=WORKSPACE)


@pytest.fixture
def estate(tmp_path):
    return full_estate(tmp_path / "repo")


#: A catalogue holding no rows but every table. The distinction is the one the
#: bookmark stage turns on: a catalogue holding *nothing* is one the same bundle
#: is creating, and has no bookmarks to reconcile.
EMPTY = Catalogue(
    {}, present_tables=frozenset(table.name for table in CATALOGUE_TABLES)
)


def _bundle(repository, tmp_path, *, catalogue=None, inventories=None):
    return generate_item_build_bundle(
        repository,
        bindings=estate_bindings(),
        output=Location(str(tmp_path / "bundle")),
        store=FilesystemStore(),
        target_inventories=inventories
        if inventories is not None
        else estate_inventories(repository, empty=True),
        catalogue=catalogue if catalogue is not None else EMPTY,
        catalogue_binding=CATALOGUE,
    )


def _bookmark_actions(bundle):
    return [
        (sequence, action)
        for sequence, _batch, action in bundle.plan.actions()
        if action.kind == "reconcile_bookmarks"
    ]


def _statements(bundle) -> list[str]:
    """The statements the bookmark action carries, read from its payload."""

    (_sequence, action), *rest = _bookmark_actions(bundle)
    assert not rest, "one action reconciles bookmarks, not several"
    return json.loads(bundle.store.read(bundle.location / action.payload))


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
        bookmark_statements(
            estate,
            items=(item_id("Warehouse/_weaver"),),
            selected_for_build={item_id(ITEM)},
        )
        == ()
    )


# --- when the statements are issued -------------------------------------------


@weaver_test()
def test_a_build_with_nothing_to_do_says_nothing_about_bookmarks(estate):
    """An unchanged repository produces an empty bundle, bookmarks included."""

    assert (
        bookmark_statements(estate, items=(item_id(ITEM),), selected_for_build=()) == ()
    )


@weaver_test()
def test_a_first_build_resets_every_loadable_object_it_installs(estate, tmp_path):
    statements = _statements(_bundle(estate, tmp_path))
    merge = [one for one in statements if one.startswith("MERGE")]

    assert len(merge) == 1
    assert BOOKMARK_SENTINEL_TEXT in merge[0]
    assert "N'DWG', N'Customer'" in merge[0]
    # The Folder keeps its `Files/` prefix, so it is not the table of that name.
    assert "N'Files/Raw', N'CustomerCsv'" in merge[0]


@weaver_test()
def test_a_build_that_changes_one_object_resets_only_that_one(estate, tmp_path):
    """An unchanged object keeps the bookmark it has, or every build reloads it."""

    installed = _installed(estate)
    changed = _with_changed_customer(tmp_path)

    statements = _statements(
        _bundle(
            changed,
            tmp_path / "second",
            catalogue=installed,
            inventories=estate_inventories(changed),
        )
    )
    merge = "".join(one for one in statements if one.startswith("MERGE"))

    assert "N'DWG', N'Customer'" in merge
    assert "N'Files/Raw', N'CustomerCsv'" not in merge


@weaver_test()
def test_a_build_creating_the_catalogue_reconciles_no_bookmarks(estate, tmp_path):
    """The bootstrap case: the table this would write is in the same bundle.

    Not a silent skip. A catalogue holding nothing has never had anything
    installed into it, so there is no row to reset and none to prune.
    """

    bundle = _bundle(estate, tmp_path, catalogue=Catalogue({}))

    assert _bookmark_actions(bundle) == []


# --- where they sit ------------------------------------------------------------


@weaver_test()
def test_bookmarks_are_reconciled_before_the_first_physical_action(estate, tmp_path):
    """The safety property: a reset that failed to happen is the silent case."""

    bundle = _bundle(estate, tmp_path)
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
def test_the_reset_and_the_prune_are_one_action(estate, tmp_path):
    """One statement each, in one batch: this is one decision about one table."""

    statements = _statements(_bundle(estate, tmp_path))

    assert len([one for one in statements if one.startswith("DELETE")]) == 1
    assert len([one for one in statements if one.startswith("MERGE")]) == 1


@weaver_test()
def test_the_batch_refuses_to_run_without_the_table_it_maintains(estate, tmp_path):
    """A build that quietly did no bookmark work would leave the next load wrong."""

    statements = _statements(_bundle(estate, tmp_path))

    assert "object_id(N'[_].[Bookmark]', N'U') is null" in statements[0]
    assert "throw 51030" in statements[0]


# --- scope ---------------------------------------------------------------------


@weaver_test()
def test_the_prune_keeps_every_object_the_repository_still_declares(estate):
    statements = bookmark_statements(
        estate,
        items=(item_id(ITEM),),
        selected_for_build={one for one in estate.source_documents},
    )
    delete = next(one for one in statements if one.startswith("DELETE"))

    assert "N'DWG', N'Customer'" in delete
    assert "N'Files/Raw', N'CustomerCsv'" in delete


@weaver_test()
def test_an_object_the_repository_no_longer_declares_is_pruned(estate, tmp_path):
    """Its key is absent from the keep-set, so the anti-join removes the row."""

    smaller = _without_the_folder(tmp_path)
    statements = bookmark_statements(
        smaller,
        items=(item_id(ITEM),),
        selected_for_build={one for one in smaller.source_documents},
    )
    delete = next(one for one in statements if one.startswith("DELETE"))

    assert "N'DWG', N'Customer'" in delete
    assert "N'Files/Raw', N'CustomerCsv'" not in delete


@weaver_test()
def test_an_unrelated_item_is_outside_the_scope_that_prunes(estate):
    """A build maintains the items it reconciles and no others."""

    statements = bookmark_statements(
        estate,
        items=(item_id(ITEM),),
        selected_for_build={one for one in estate.source_documents},
    )
    delete = next(one for one in statements if one.startswith("DELETE"))

    assert "N'Lakehouse' AND [Item name] = N'Sales'" in delete
    assert "Reporting" not in delete


@weaver_test()
def test_both_items_of_one_build_are_one_scoped_statement(estate):
    """Bound items share the table, so addressing them separately is round trips."""

    statements = bookmark_statements(
        estate,
        items=(item_id(ITEM), item_id(WAREHOUSE_ITEM)),
        selected_for_build={one for one in estate.source_documents},
    )
    delete = next(one for one in statements if one.startswith("DELETE"))

    assert "N'Sales'" in delete and "N'Reporting'" in delete


# --- fixtures ------------------------------------------------------------------


def _installed(repository) -> Catalogue:
    """The catalogue a successful build of this estate leaves behind."""

    from weaver.build_bundle.catalogue_actions import desired_catalogue
    from weaver.build_bundle.planner import certifiable_identities
    from weaver.catalogue.tables import CATALOGUE_TABLES

    bindings = estate_bindings()
    by_item = {binding.item: binding for binding in bindings.entries}
    state = desired_catalogue(
        repository,
        certifiable_identities(repository, by_item),
        {binding.item: binding.to_bound_target() for binding in bindings.entries},
    )
    return Catalogue(
        rows=state.rows,
        present_tables=frozenset(table.name for table in CATALOGUE_TABLES),
    )


def _with_changed_customer(tmp_path):
    """The same estate with one table's source edited, and nothing else."""

    root = tmp_path / "changed"
    full_estate(root)
    path = root / f"{ITEM}/DWG__Customer.py"
    path.write_text(
        lakehouse_table("DWG.Customer").replace("Description:", "Description: edited,"),
        encoding="utf-8",
    )
    return parse_item_repository(Location(str(root)))


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
