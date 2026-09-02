"""A build's effect on the estate and on the catalogue's runtime state.

.. code-block:: text

    Inventory + Catalogue
        -> apply the build plan
            -> resulting Inventory + Catalogue

Both halves against the same plan, because a change that got one right and the
other wrong would look correct from either side alone.
"""

from __future__ import annotations

import pytest
from factories import (
    CATALOGUE_ITEM,
    ITEM,
    catalogue_inventory,
    estate_bindings,
    folder_document,
    installed_catalogue,
    item_id,
    lakehouse_table,
    lakehouse_test,
    schema_document,
    target_inventory,
    warehouse_table,
)
from factories import (
    WAREHOUSE_ITEM as OTHER_ITEM,
)
from factories import (
    FixtureInventory as _FixtureInventory,
)
from support.catalogues import LOADED_AT
from support.weaver_test import weaver_test
from support.workspaces import WORKSPACE

from weaver.build_bundle import WarehouseBinding, generate_item_build_bundle
from weaver.catalogue.state import Catalogue
from weaver.catalogue.tables import (
    BOOKMARK,
    LOAD_STATISTIC,
    LOAD_STATUS,
    LOG,
    TEST_STATUS,
)
from weaver.declaration import parse_item_repository
from weaver.locations import Location
from weaver.store import FilesystemStore
from weaver.targets import ItemRef

CATALOGUE = WarehouseBinding(ItemRef("Weaver"), workspace_name=WORKSPACE)

#: The three objects the estate declares, as their current-state rows key them.
#: The four keys, as every operational table stores them. A Lakehouse data
#: object names its area and a validation names none, so the Test sits beside
#: the table it reconciles without colliding with it.
CUSTOMER = ("Tables/DWG", "Customer")
CSV = ("Files/Raw", "CustomerCsv")
RECONCILE = ("DWG", "CustomerReconcile")

#: One object in the other item, which is a Warehouse and has no areas, so
#: scope can be asserted.
OTHER = ("Sales", "Customer")


def _estate(root):
    """A loadable table, a loadable folder, a validation, and a second item.

    Everything the invalidation rules distinguish between, in the smallest
    repository that holds all of it.
    """

    files = {
        f"{ITEM}/schemas/DWG.yml": schema_document("DWG"),
        f"{ITEM}/schemas/Raw.yml": schema_document("Raw"),
        f"{ITEM}/Tables/DWG__Customer.py": lakehouse_table("DWG.Customer"),
        f"{ITEM}/Files/Raw__CustomerCsv.py": folder_document("Raw.CustomerCsv"),
        f"{ITEM}/tests/DWG__CustomerReconcile.py": lakehouse_test(
            "DWG.CustomerReconcile"
        ),
        f"{OTHER_ITEM}/schemas/Sales.yml": schema_document("Sales"),
        f"{OTHER_ITEM}/Sales.Customer.sql": warehouse_table("Sales.Customer"),
    }
    for relative, text in files.items():
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    return parse_item_repository(Location(str(root)))


@pytest.fixture
def estate(tmp_path):
    return _estate(tmp_path / "repo")


# --- the state a built and run estate holds -----------------------------------


def _current(table, item, schema: str, name: str) -> dict:
    """One current-state row, keyed as the Registry keys the object."""

    row = {
        "item_type": item.item_type,
        "item_name": item.item_name,
        "schema_name": schema,
        "object_name": name,
    }
    if table is BOOKMARK:
        return {**row, "bookmark_datetime": LOADED_AT}
    if table is TEST_STATUS:
        return {**row, "test_type": "test", "result": "succeeded"}
    return {**row, "result": "succeeded"}


def _history(table, schema: str, name: str) -> dict:
    """One historical row, keyed by its own surrogate."""

    key = table.key[0]
    return {
        key: f"{table.name}:{schema}.{name}",
        "workflow_id": "workflow-1",
        "schema_name": schema,
        "object_name": name,
        **({"task_type": "load", "result": "succeeded"} if table is LOG else {}),
    }


def _operational(repository) -> Catalogue:
    """The catalogue a built, loaded and tested estate holds, in every table."""

    installed = installed_catalogue(repository, estate_bindings())
    rows = {item: dict(tables) for item, tables in installed.rows.items()}
    own = rows[item_id(ITEM)]
    own[BOOKMARK.name] = (
        _current(BOOKMARK, item_id(ITEM), *CUSTOMER),
        _current(BOOKMARK, item_id(ITEM), *CSV),
    )
    own[LOAD_STATUS.name] = (
        _current(LOAD_STATUS, item_id(ITEM), *CUSTOMER),
        _current(LOAD_STATUS, item_id(ITEM), *CSV),
    )
    own[TEST_STATUS.name] = (_current(TEST_STATUS, item_id(ITEM), *RECONCILE),)
    own[LOG.name] = (
        _history(LOG, *CUSTOMER),
        _history(LOG, *CSV),
        _history(LOG, *RECONCILE),
    )
    own[LOAD_STATISTIC.name] = (
        _history(LOAD_STATISTIC, *CUSTOMER),
        _history(LOAD_STATISTIC, *CSV),
    )
    other = rows[item_id(OTHER_ITEM)]
    other[BOOKMARK.name] = (_current(BOOKMARK, item_id(OTHER_ITEM), *OTHER),)
    other[LOAD_STATUS.name] = (_current(LOAD_STATUS, item_id(OTHER_ITEM), *OTHER),)
    return Catalogue(rows=rows)


def _inventories(repository):
    bound = {b.item: b.to_bound_target() for b in estate_bindings().entries}
    made = {}
    for item, kind in (
        (ITEM, "lakehouse"),
        (OTHER_ITEM, "warehouse"),
    ):
        identity = item_id(item)
        made[identity] = _FixtureInventory.from_repository(
            repository,
            item=item,
            target_id=bound[identity].id,
            kind=kind,
            target_name=bound[identity].name,
        )
    made[CATALOGUE_ITEM] = catalogue_inventory(holding=True)
    return made


def _applied(repository, catalogue, tmp_path, *, items=None):
    """Generate against this state, then apply the plan to both halves of it."""

    inventories = _inventories(repository)
    bindings = estate_bindings()
    if items is not None:
        bindings = type(bindings)(
            tuple(entry for entry in bindings.entries if str(entry.item) in items)
        )
    bundle = generate_item_build_bundle(
        repository,
        bindings=bindings,
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


def _changed(tmp_path, relative: str, text: str, *, name: str = "changed"):
    """The same estate with one source edited, and nothing else.

    The directory name becomes the repository name, so it is one logical name.
    """

    root = tmp_path / name
    _estate(root)
    path = root / relative
    path.write_text(text, encoding="utf-8")
    return parse_item_repository(Location(str(root)))


def _keys(catalogue, table) -> set[tuple]:
    return {
        tuple(row.get(name) for name in ("schema_name", "object_name"))
        for row in catalogue.table_rows(table)
    }


def _rebuilt_table(tmp_path):
    return _changed(
        tmp_path,
        f"{ITEM}/Tables/DWG__Customer.py",
        lakehouse_table("DWG.Customer").replace("Description:", "Description: edited,"),
        name="rebuilt_table",
    )


def _rebuilt_validation(tmp_path):
    return _changed(
        tmp_path,
        f"{ITEM}/tests/DWG__CustomerReconcile.py",
        lakehouse_test("DWG.CustomerReconcile").replace(
            "Description:", "Description: edited,"
        ),
        name="rebuilt_validation",
    )


# --- rebuilding a loadable object ---------------------------------------------


@weaver_test()
def test_a_rebuilt_object_survives_and_its_current_state_does_not(estate, tmp_path):
    """The object is still there; its bookmark and load status are not."""

    catalogue = _operational(estate)

    _bundle, reached, after = _applied(_rebuilt_table(tmp_path), catalogue, tmp_path)

    assert "DWG.Customer" in reached[item_id(ITEM)].tables
    assert CUSTOMER not in _keys(after, BOOKMARK)
    assert CUSTOMER not in _keys(after, LOAD_STATUS)


@weaver_test()
def test_rebuilding_a_loadable_object_leaves_a_validation_alone(estate, tmp_path):
    """The validation was not rebuilt, so a status dropped would read as
    "never run"."""

    catalogue = _operational(estate)

    _bundle, _reached, after = _applied(_rebuilt_table(tmp_path), catalogue, tmp_path)

    assert RECONCILE in _keys(after, TEST_STATUS)


@weaver_test()
def test_an_object_the_build_left_alone_keeps_its_current_state(estate, tmp_path):
    """The Folder was not rebuilt, so how far it has been loaded still holds."""

    catalogue = _operational(estate)

    _bundle, _reached, after = _applied(_rebuilt_table(tmp_path), catalogue, tmp_path)

    assert CSV in _keys(after, BOOKMARK)
    assert CSV in _keys(after, LOAD_STATUS)


@weaver_test()
def test_history_survives_the_rebuild(estate, tmp_path):
    """History records what happened, and a rebuild does not unhappen it."""

    catalogue = _operational(estate)

    _bundle, _reached, after = _applied(_rebuilt_table(tmp_path), catalogue, tmp_path)

    assert _keys(after, LOG) == {CUSTOMER, CSV, RECONCILE}
    assert _keys(after, LOAD_STATISTIC) == {CUSTOMER, CSV}


# --- rebuilding a validation ---------------------------------------------------


@weaver_test()
def test_a_rebuilt_validation_loses_its_status_and_nothing_else(estate, tmp_path):
    """The other direction: rebuilding it says nothing about a bookmark."""

    catalogue = _operational(estate)

    _bundle, _reached, after = _applied(
        _rebuilt_validation(tmp_path), catalogue, tmp_path
    )

    assert RECONCILE not in _keys(after, TEST_STATUS)
    assert _keys(after, BOOKMARK) == {CUSTOMER, CSV, OTHER}
    assert _keys(after, LOAD_STATUS) == {CUSTOMER, CSV, OTHER}


@weaver_test()
def test_history_survives_a_rebuilt_validation(estate, tmp_path):
    """Nothing invalidates history, whichever population was rebuilt."""

    catalogue = _operational(estate)

    _bundle, _reached, after = _applied(
        _rebuilt_validation(tmp_path), catalogue, tmp_path
    )

    assert _keys(after, LOG) == {CUSTOMER, CSV, RECONCILE}
    assert _keys(after, LOAD_STATISTIC) == {CUSTOMER, CSV}


# --- scope ---------------------------------------------------------------------


@weaver_test()
def test_an_item_this_build_was_not_pointed_at_keeps_its_state(estate, tmp_path):
    """The tables are shared, so a wider reconciliation would take another
    build's rows."""

    catalogue = _operational(estate)

    _bundle, _reached, after = _applied(
        _rebuilt_table(tmp_path), catalogue, tmp_path, items={ITEM}
    )

    assert OTHER in _keys(after, BOOKMARK)
    assert OTHER in _keys(after, LOAD_STATUS)


@weaver_test()
def test_a_build_with_nothing_to_do_leaves_every_table_alone(estate, tmp_path):
    """An idle build is idle in the catalogue too, not only in the estate."""

    catalogue = _operational(estate)

    bundle, _reached, after = _applied(estate, catalogue, tmp_path)

    assert bundle.plan.runtime_state == ()
    assert _keys(after, BOOKMARK) == {CUSTOMER, CSV, OTHER}
    assert _keys(after, LOAD_STATUS) == {CUSTOMER, CSV, OTHER}
    assert _keys(after, TEST_STATUS) == {RECONCILE}
    assert _keys(after, LOG) == {CUSTOMER, CSV, RECONCILE}
    assert _keys(after, LOAD_STATISTIC) == {CUSTOMER, CSV}


@weaver_test()
def test_applying_a_plan_leaves_the_catalogue_it_was_read_from_alone(estate, tmp_path):
    """A prediction, so the state it was made from is still there to compare."""

    catalogue = _operational(estate)

    _bundle, _reached, after = _applied(_rebuilt_table(tmp_path), catalogue, tmp_path)

    assert CUSTOMER in _keys(catalogue, BOOKMARK)
    assert after is not catalogue


# --- an object the repository stopped declaring --------------------------------


@weaver_test()
def test_a_removed_validation_loses_its_status(estate, tmp_path):
    """What keeps a row is a declaration, not the row's own existence."""

    catalogue = _operational(estate)
    root = tmp_path / "smaller"
    _estate(root)
    (root / f"{ITEM}/tests/DWG__CustomerReconcile.py").unlink()
    smaller = parse_item_repository(Location(str(root)))

    _bundle, _reached, after = _applied(smaller, catalogue, tmp_path)

    assert RECONCILE not in _keys(after, TEST_STATUS)
    assert _keys(after, BOOKMARK) == {CUSTOMER, CSV, OTHER}


@weaver_test()
def test_an_empty_target_does_not_invalidate_what_it_never_held(tmp_path):
    """A first build reads no rows, so the action is absent rather than empty."""

    repository = _estate(tmp_path / "first")
    inventories = {
        item: target_inventory(
            target_id=inventory.target_id,
            kind=inventory.kind,
            target_name=inventory.target_name,
        )
        for item, inventory in _inventories(repository).items()
        if item != CATALOGUE_ITEM
    }
    inventories[CATALOGUE_ITEM] = catalogue_inventory(holding=False)
    bundle = generate_item_build_bundle(
        repository,
        bindings=estate_bindings(),
        output=Location(str(tmp_path / "bundle")),
        store=FilesystemStore(),
        target_inventories=inventories,
        catalogue=Catalogue({}),
        catalogue_binding=CATALOGUE,
    )

    assert bundle.plan.runtime_state == ()


__all__: tuple = ()
