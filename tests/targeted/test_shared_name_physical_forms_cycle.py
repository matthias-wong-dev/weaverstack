"""One logical name changing independently in every physical namespace.

The lifecycle uses ``Sales.Thing`` as a Lakehouse table, a Lakehouse Files
folder, and a Warehouse relation at the same time. Each changes form in one
build, then the resulting estate is fed back through the ordinary bundle
planner. The shared spelling makes any collapsed identity remove or retain the
wrong object.
"""

from __future__ import annotations

from factories import (
    SPARK_TABLE,
    _write,
    folder_document,
    item_bindings,
    lakehouse_table,
    physical_folder_shortcut,
    schema_document,
    target_inventory,
    warehouse_table,
)
from support.weaver_test import weaver_test
from support.workspaces import WORKSPACE

from weaver.build_bundle import WarehouseBinding, generate_item_build_bundle
from weaver.build_bundle.catalogue_actions import desired_catalogue
from weaver.build_bundle.planner import certifiable_identities
from weaver.build_bundle.shortcuts import ResolvedShortcutSource
from weaver.catalogue.state import Catalogue
from weaver.declaration import parse_item_repository
from weaver.declaration.model import WeaverDocumentId, WeaverItemId
from weaver.locations import Location
from weaver.store import FilesystemStore
from weaver.targets import ItemRef

LAKEHOUSE = "Lakehouse/Raw"
WAREHOUSE = "Warehouse/Reporting"
QUALIFIED = "Sales.Thing"


def _warehouse_view() -> str:
    return f"""\
/*
View ID: {QUALIFIED}

Description: The changed Warehouse relation.

Lineage: The lifecycle fixture.
*/
select cast(1 as int) as CustomerId
"""


def _initial_repository(root):
    for relative, text in {
        f"{LAKEHOUSE}/schemas/Sales.yml": schema_document("Sales"),
        f"{LAKEHOUSE}/Sales__Thing.py": lakehouse_table(QUALIFIED),
        f"{LAKEHOUSE}/Files/Sales__Thing.py": folder_document(QUALIFIED),
        f"{WAREHOUSE}/schemas/Sales.yml": schema_document("Sales"),
        f"{WAREHOUSE}/Sales.Thing.sql": warehouse_table(QUALIFIED),
    }.items():
        _write(root, relative, text)
    return parse_item_repository(Location(str(root)))


def _changed_repository(root):
    for relative, text in {
        f"{LAKEHOUSE}/schemas/Sales.yml": schema_document("Sales"),
        f"{LAKEHOUSE}/Sales.Thing.sql": SPARK_TABLE.format(object_id=QUALIFIED),
        f"{WAREHOUSE}/schemas/Sales.yml": schema_document("Sales"),
        f"{WAREHOUSE}/Sales.Thing.sql": _warehouse_view(),
    }.items():
        _write(root, relative, text)
    _write(
        root,
        *physical_folder_shortcut(
            LAKEHOUSE,
            name=QUALIFIED,
            target="Lakehouse/Source/Files/Sales/Thing",
            workspace="Upstream",
        ),
    )
    return parse_item_repository(Location(str(root)))


def _bindings():
    return item_bindings((LAKEHOUSE, "Raw_LH"), (WAREHOUSE, "Reporting_WH"))


def _bound(bindings):
    return {
        item: binding.to_bound_target() for item, binding in bindings.by_item.items()
    }


def _empty_inventories(bindings):
    bound = _bound(bindings)
    return {
        WeaverItemId.parse(LAKEHOUSE): target_inventory(
            target_id=bound[WeaverItemId.parse(LAKEHOUSE)].id,
            kind="lakehouse",
            target_name="Raw_LH",
        ),
        WeaverItemId.parse(WAREHOUSE): target_inventory(
            target_id=bound[WeaverItemId.parse(WAREHOUSE)].id,
            kind="warehouse",
            target_name="Reporting_WH",
        ),
    }


def _installed_catalogue(repository, bindings):
    return desired_catalogue(
        repository,
        certifiable_identities(repository, bindings.by_item),
        _bound(bindings),
    )


def _build(repository, root, *, inventories, catalogue, shortcut_sources=None):
    bindings = _bindings()
    return generate_item_build_bundle(
        repository,
        bindings=bindings,
        output=Location(str(root / "bundle")),
        store=FilesystemStore(),
        target_inventories=inventories,
        catalogue=catalogue,
        catalogue_binding=WarehouseBinding(
            ItemRef("Weaver_Control"), workspace_name=WORKSPACE
        ),
        shortcut_sources=shortcut_sources,
    )


def _actions(bundle):
    return [action for _sequence, _batch, action in bundle.plan.actions()]


def _apply(bundle, inventories):
    return {
        item: inventory.update_using(bundle.plan)
        for item, inventory in inventories.items()
    }


def _shared_name(values):
    return tuple(value for value in values if value.casefold() == QUALIFIED.casefold())


@weaver_test()
def test_shared_names_change_all_three_physical_forms_and_reach_a_fixed_point(
    tmp_path,
):
    """Tables, Files, and Warehouse relations retain separate lifecycles."""

    initial = _initial_repository(tmp_path / "initial")
    changed = _changed_repository(tmp_path / "changed")
    bindings = _bindings()
    lakehouse = WeaverItemId.parse(LAKEHOUSE)
    warehouse = WeaverItemId.parse(WAREHOUSE)
    table_id = WeaverDocumentId.parse(f"{LAKEHOUSE}/{QUALIFIED}")
    files_id = WeaverDocumentId.parse(f"{LAKEHOUSE}/Files/{QUALIFIED}")
    warehouse_id = WeaverDocumentId.parse(f"{WAREHOUSE}/{QUALIFIED}")

    empty = _empty_inventories(bindings)
    first = _build(
        initial,
        tmp_path / "first",
        inventories=empty,
        catalogue=Catalogue({}),
    )
    after_first = _apply(first, empty)
    initial_catalogue = _installed_catalogue(initial, bindings)

    assert _shared_name(after_first[lakehouse].tables) == (QUALIFIED,)
    assert _shared_name(after_first[lakehouse].folders) == (QUALIFIED,)
    assert _shared_name(after_first[warehouse].tables) == (QUALIFIED,)
    assert after_first[lakehouse].runtime_references
    assert initial_catalogue.registered[files_id].object_role == "data"

    shortcut_sources = {
        f"{LAKEHOUSE}/Sales__Thing": ResolvedShortcutSource(
            workspace_id="workspace-source",
            item_id="lakehouse-source",
            item_name="Source",
            path="Files/Sales/Thing",
        )
    }
    second = _build(
        changed,
        tmp_path / "second",
        inventories=after_first,
        catalogue=initial_catalogue,
        shortcut_sources=shortcut_sources,
    )
    after_second = _apply(second, after_first)
    changed_catalogue = _installed_catalogue(changed, bindings)
    actions = _actions(second)

    folder_drop = next(
        action
        for action in actions
        if action.kind == "drop_folder" and action.resource_node_id == str(files_id)
    )
    table_drop = next(
        action
        for action in actions
        if action.kind == "drop_table" and action.resource_node_id == str(table_id)
    )
    warehouse_drop = next(
        action
        for action in actions
        if action.kind == "drop_table" and action.resource_node_id == str(warehouse_id)
    )
    shortcut_create = next(
        action
        for action in actions
        if action.kind == "create_shortcut" and action.executor == "shortcut"
    )

    assert folder_drop.executor == "folder"
    assert folder_drop.payload is None
    assert table_drop.executor == "spark_sql"
    assert warehouse_drop.executor == "tsql"
    assert actions.index(folder_drop) < actions.index(shortcut_create)

    assert _shared_name(after_second[lakehouse].tables) == (QUALIFIED,)
    assert _shared_name(after_second[lakehouse].folders) == (QUALIFIED,)
    assert _shared_name(after_second[warehouse].tables) == ()
    assert _shared_name(after_second[warehouse].views) == (QUALIFIED,)
    assert (
        after_second[lakehouse].runtime_references
        == after_first[lakehouse].runtime_references
    )
    assert changed_catalogue.registered[table_id].object_type == "table"
    assert changed_catalogue.registered[table_id].object_role == "data"
    assert changed_catalogue.registered[files_id].object_type == "folder"
    assert changed_catalogue.registered[files_id].object_role == "shortcut"
    assert changed_catalogue.registered[warehouse_id].object_type == "view"
    assert changed_catalogue.registered[warehouse_id].object_role == "data"

    third = _build(
        changed,
        tmp_path / "third",
        inventories=after_second,
        catalogue=changed_catalogue,
        shortcut_sources=shortcut_sources,
    )

    assert _actions(third) == []
    assert third.plan.target_changes == {}
