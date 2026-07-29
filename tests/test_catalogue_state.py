"""Catalogue reconciliation trusts only claims proved by physical inventory."""

from __future__ import annotations

from weaver.build_bundle.prune import TargetInventory
from weaver.catalogue import (
    CATALOGUE_TABLES,
    FOLDER_DICTIONARY,
    REGISTRY,
    TABLE_DICTIONARY,
)
from weaver.catalogue.state import CatalogueState, reconcile_catalogue_state
from weaver.declaration.model import WeaverItemId


ITEM = WeaverItemId.parse("Lakehouse/Sales")


def _row(name: str, *, object_type: str = "table"):
    return {
        "item_type": "Lakehouse",
        "item_name": "Sales",
        "schema_name": "Sales",
        "object_name": name,
        "object_type": object_type,
    }


def _folder_row(name: str):
    return {**_row(name, object_type="folder"), "schema_name": "Files/Sales"}


def _state(*rows):
    registry = tuple(dict(row) for row in rows)
    return CatalogueState(
        status="valid",
        rows={
            ITEM: {
                REGISTRY.name: registry,
                TABLE_DICTIONARY.name: tuple(
                    row for row in registry if row["object_type"] in {"table", "view"}
                ),
                FOLDER_DICTIONARY.name: tuple(
                    row for row in registry if row["object_type"] == "folder"
                ),
            }
        },
        present_tables=frozenset(table.name for table in CATALOGUE_TABLES),
        missing_tables=frozenset(),
    )


def _inventory(*tables, folders=(), views=()):
    return TargetInventory(
        target_id="sales",
        kind="lakehouse",
        target_name="Sales_Dev",
        tables=tuple(f"Sales.{name}" for name in tables),
        folders=tuple(f"Sales.{name}" for name in folders),
        views=tuple(f"Sales.{name}" for name in views),
    )


def test_valid_rows_remain_and_stale_object_metadata_is_removed():
    result = reconcile_catalogue_state(
        _state(_row("Current"), _row("Missing")),
        inventories={ITEM: _inventory("Current")},
    )

    assert [row["object_name"] for row in result.rows[ITEM][REGISTRY.name]] == [
        "Current"
    ]
    assert [
        row["object_name"] for row in result.rows[ITEM][TABLE_DICTIONARY.name]
    ] == ["Current"]
    assert result.stale_objects == ("Lakehouse/Sales/Sales.Missing",)
    assert any("TableDictionary" in statement for statement in result.delete_dml)
    assert any("Registry" in statement for statement in result.delete_dml)


def test_same_named_folder_and_table_keep_the_four_part_catalogue_identity():
    result = reconcile_catalogue_state(
        _state(_row("Customer"), _folder_row("Customer")),
        inventories={ITEM: _inventory(folders=("Customer",))},
    )

    assert [
        row["object_type"] for row in result.rows[ITEM][REGISTRY.name]
    ] == ["folder"]
    assert result.rows[ITEM][TABLE_DICTIONARY.name] == ()
    assert [
        row["object_type"] for row in result.rows[ITEM][FOLDER_DICTIONARY.name]
    ] == ["folder"]
    assert result.stale_objects == ("Lakehouse/Sales/Sales.Customer",)
    assert result.delete_dml
    assert all("'Sales'" in statement for statement in result.delete_dml)
    assert all("'Files/Sales'" not in statement for statement in result.delete_dml)


def test_missing_folder_does_not_remove_same_named_table():
    result = reconcile_catalogue_state(
        _state(_row("Customer"), _folder_row("Customer")),
        inventories={ITEM: _inventory("Customer")},
    )

    assert [
        row["object_type"] for row in result.rows[ITEM][REGISTRY.name]
    ] == ["table"]
    assert [
        row["object_type"] for row in result.rows[ITEM][TABLE_DICTIONARY.name]
    ] == ["table"]
    assert result.rows[ITEM][FOLDER_DICTIONARY.name] == ()
    assert result.stale_objects == ("Lakehouse/Sales/Files/Sales.Customer",)
    assert result.delete_dml
    assert all("'Files/Sales'" in statement for statement in result.delete_dml)


def test_physical_objects_without_catalogue_rows_generate_no_deletes():
    result = reconcile_catalogue_state(
        _state(), inventories={ITEM: _inventory("Unregistered")}
    )
    assert result.delete_dml == ()
    assert result.stale_objects == ()
