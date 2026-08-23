"""A build's effect on the estate *and* on the catalogue's runtime state.

One round trip, and the two halves of state a build leaves behind:

.. code-block:: text

    Inventory + Catalogue
        -> apply the build plan
            -> resulting Inventory + Catalogue

The physical half already had this — :meth:`TargetInventory.update_using` — and
the operational half now has it too, so what a rebuild means for an object's
bookmark is proven as resulting state rather than by reading a DELETE statement.

The distinction the whole runtime model rests on is here: an object rebuilt keeps
existing and loses its *current* operational state, while the estate's history of
what happened to it is untouched. Both are asserted on one plan, because a change
that got one right and the other wrong would look correct from either side alone.
"""

from __future__ import annotations

import pytest
from factories import (
    CATALOGUE_ITEM,
    ITEM,
    catalogue_inventory,
    estate_bindings,
    estate_inventories,
    full_estate,
    installed_catalogue,
    item_id,
    lakehouse_table,
)
from support.catalogues import LOADED_AT
from support.weaver_test import weaver_test
from support.workspaces import WORKSPACE

from weaver.build_bundle import WarehouseBinding, generate_item_build_bundle
from weaver.catalogue.state import Catalogue
from weaver.catalogue.tables import BOOKMARK, LOG
from weaver.declaration import parse_item_repository
from weaver.locations import Location
from weaver.store import FilesystemStore
from weaver.targets import ItemRef

CATALOGUE = WarehouseBinding(ItemRef("Weaver"), workspace_name=WORKSPACE)

CUSTOMER = ("DWG", "Customer")
CSV = ("Files/Raw", "CustomerCsv")


@pytest.fixture
def estate(tmp_path):
    return full_estate(tmp_path / "repo")


def _bookmark_row(schema: str, name: str) -> dict:
    return {
        "item_type": "Lakehouse",
        "item_name": "Sales",
        "schema_name": schema,
        "object_name": name,
        "bookmark_datetime": LOADED_AT,
    }


def _log_row(schema: str, name: str) -> dict:
    return {
        "log_sk": f"{schema}.{name}",
        "workflow_id": "workflow-1",
        "task_type": "load",
        "schema_name": schema,
        "object_name": name,
        "result": "succeeded",
    }


def _operational(repository) -> Catalogue:
    """The catalogue a built and loaded estate holds: what is installed, and
    what has happened to it."""

    installed = installed_catalogue(repository, estate_bindings())
    rows = {item: dict(tables) for item, tables in installed.rows.items()}
    rows[item_id(ITEM)][BOOKMARK.name] = (
        _bookmark_row(*CUSTOMER),
        _bookmark_row(*CSV),
    )
    rows[item_id(ITEM)][LOG.name] = (_log_row(*CUSTOMER), _log_row(*CSV))
    return Catalogue(rows=rows)


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


def _applied(repository, catalogue, tmp_path):
    """Generate against this state, then apply the plan to both halves of it."""

    inventories = dict(estate_inventories(repository))
    inventories[CATALOGUE_ITEM] = catalogue_inventory(holding=True)
    bundle = generate_item_build_bundle(
        repository,
        bindings=estate_bindings(),
        output=Location(str(tmp_path / "bundle")),
        store=FilesystemStore(),
        target_inventories=inventories,
        catalogue=catalogue,
        catalogue_binding=CATALOGUE,
    )
    reached = {
        item: inventory.update_using(bundle.plan)
        for item, inventory in inventories.items()
    }
    return bundle, reached, catalogue.update_using(bundle.plan)


def _keys(catalogue, table) -> set[tuple]:
    return {
        tuple(row.get(name) for name in ("schema_name", "object_name"))
        for row in catalogue.table_rows(table)
    }


# --- one object rebuilt -------------------------------------------------------


@weaver_test()
def test_a_rebuilt_object_survives_and_its_current_state_does_not(estate, tmp_path):
    """The object is still there; its bookmark is not.

    Which is the whole lifecycle rule in one assertion. A bookmark row means "a
    clean load has run for this object's current incarnation", and the rebuild
    ended that incarnation.
    """

    catalogue = _operational(estate)
    changed = _with_changed_customer(tmp_path)

    _bundle, reached, after = _applied(changed, catalogue, tmp_path)

    assert "DWG.Customer" in reached[item_id(ITEM)].tables
    assert CUSTOMER not in _keys(after, BOOKMARK)


@weaver_test()
def test_an_object_the_build_left_alone_keeps_its_current_state(estate, tmp_path):
    """The Folder was not rebuilt, so how far it has been loaded still holds."""

    catalogue = _operational(estate)
    changed = _with_changed_customer(tmp_path)

    _bundle, _reached, after = _applied(changed, catalogue, tmp_path)

    assert CSV in _keys(after, BOOKMARK)


@weaver_test()
def test_history_survives_the_rebuild(estate, tmp_path):
    """``_.Log`` records what happened, and a rebuild does not unhappen it."""

    catalogue = _operational(estate)
    changed = _with_changed_customer(tmp_path)

    _bundle, _reached, after = _applied(changed, catalogue, tmp_path)

    assert _keys(after, LOG) == {CUSTOMER, CSV}


# --- nothing changed ----------------------------------------------------------


@weaver_test()
def test_a_build_with_nothing_to_do_leaves_both_halves_alone(estate, tmp_path):
    """An idle build is idle in the catalogue too, not only in the estate."""

    catalogue = _operational(estate)

    bundle, _reached, after = _applied(estate, catalogue, tmp_path)

    assert bundle.plan.runtime_state == ()
    assert _keys(after, BOOKMARK) == {CUSTOMER, CSV}
    assert _keys(after, LOG) == {CUSTOMER, CSV}


@weaver_test()
def test_applying_a_plan_leaves_the_catalogue_it_was_read_from_alone(estate, tmp_path):
    """A prediction, so the state it was made from is still there to compare."""

    catalogue = _operational(estate)
    changed = _with_changed_customer(tmp_path)

    _bundle, _reached, after = _applied(changed, catalogue, tmp_path)

    assert CUSTOMER in _keys(catalogue, BOOKMARK)
    assert after is not catalogue


__all__: tuple = ()
