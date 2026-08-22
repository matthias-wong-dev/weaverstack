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
from weaver.catalogue.tables import CATALOGUE_TABLES
from weaver.declaration import parse_item_repository
from weaver.etl import item_bookmarkable_objects
from weaver.locations import Location
from weaver.store import FilesystemStore
from weaver.targets import ItemRef

CATALOGUE = WarehouseBinding(ItemRef("Weaver"), workspace_name=WORKSPACE)


@pytest.fixture
def estate(tmp_path):
    return full_estate(tmp_path / "repo")


#: A catalogue holding no rows but every table, `_.Bookmark` included. That last
#: is the distinction the bookmark stage turns on: a catalogue without the table
#: is one this bundle is creating it in, and it can hold no row anything could
#: have written.
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
def test_a_first_build_invalidates_every_loadable_object_it_installs(estate, tmp_path):
    """It is building all of them, so none of their histories survives.

    The keep-set is empty, which the renderer turns into a plain scoped delete:
    nothing in these items keeps a bookmark.
    """

    statements = _statements(_bundle(estate, tmp_path))
    delete = [one for one in statements if one.startswith("DELETE")]

    assert len(delete) == 1
    assert "NOT EXISTS" not in delete[0]
    assert not [one for one in statements if one.startswith("MERGE")]


@weaver_test()
def test_a_build_that_changes_one_object_invalidates_only_that_one(estate, tmp_path):
    """An unchanged object keeps the bookmark it has, or every build reloads it.

    The keep-set is what survives, so the object being rebuilt is *absent* from
    it and the one left alone is in it.
    """

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
    keep = next(one for one in statements if one.startswith("DELETE"))

    assert "N'DWG', N'Customer'" not in keep
    assert "N'Files/Raw', N'CustomerCsv'" in keep


@pytest.mark.parametrize(
    "present",
    [
        pytest.param(frozenset(), id="bootstrap"),
        pytest.param(
            frozenset(
                table.name for table in CATALOGUE_TABLES if table.name != "Bookmark"
            ),
            id="upgrade",
        ),
    ],
)
@weaver_test()
def test_a_build_creating_the_table_reconciles_no_bookmarks(estate, tmp_path, present):
    """The table this would write is arriving in the same bundle.

    Every build binds the built-in item, so a catalogue without `_.Bookmark` gets
    it from this build — whether that catalogue is empty or is an older estate
    being upgraded. Not a silent skip: a table nothing could ever have written to
    has no row to reset and none to prune.
    """

    bundle = _bundle(estate, tmp_path, catalogue=Catalogue({}, present_tables=present))

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
def test_invalidation_is_one_statement(estate, tmp_path):
    """One decision about one table, so one statement in one action."""

    statements = _statements(_bundle(estate, tmp_path))

    assert len([one for one in statements if one.startswith("DELETE")]) == 1
    assert not [one for one in statements if one.startswith("MERGE")]


@weaver_test()
def test_the_batch_refuses_to_run_without_the_table_it_maintains(estate, tmp_path):
    """A build that quietly did no bookmark work would leave the next load wrong."""

    statements = _statements(_bundle(estate, tmp_path))

    assert "object_id(N'[_].[Bookmark]', N'U') is null" in statements[0]
    assert "throw 51030" in statements[0]


# --- scope ---------------------------------------------------------------------


@weaver_test()
def test_an_object_left_alone_keeps_its_bookmark(estate):
    """Nothing selected, one object removed: the untouched ones stay in the keep-set."""

    statements = bookmark_statements(
        estate,
        items=(item_id(ITEM),),
        selected_for_build=(),
        removed={document_id_of(estate, "DWG.Summary")},
    )
    keep = next(one for one in statements if one.startswith("DELETE"))

    assert "N'DWG', N'Customer'" in keep
    assert "N'Files/Raw', N'CustomerCsv'" in keep


@weaver_test()
def test_an_object_the_repository_no_longer_declares_loses_its_row(estate, tmp_path):
    """Its key is absent from the keep-set, so the anti-join removes the row."""

    smaller = _without_the_folder(tmp_path)
    statements = bookmark_statements(
        smaller,
        items=(item_id(ITEM),),
        selected_for_build=(),
        removed={document_id_of(estate, "Files/Raw.CustomerCsv")},
    )
    keep = next(one for one in statements if one.startswith("DELETE"))

    assert "N'DWG', N'Customer'" in keep
    assert "N'Files/Raw', N'CustomerCsv'" not in keep


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


# --- fixtures ------------------------------------------------------------------


def document_id_of(repository, qualified: str):
    """One declared identity by its ``Schema.Object``, as the repository holds it."""

    return next(
        identity
        for identity in repository.source_documents
        if str(identity).endswith(f"/{qualified}")
    )


def _installed(repository) -> Catalogue:
    """The catalogue a successful build of this estate leaves behind."""

    from weaver.build_bundle.catalogue_actions import desired_catalogue
    from weaver.build_bundle.planner import certifiable_identities

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
