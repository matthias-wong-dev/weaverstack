"""Catalogue reconciliation trusts only claims proved by physical inventory."""

from __future__ import annotations

from weaver.build_bundle.prune import TargetInventory
from weaver.catalogue import CATALOGUE_TABLES, REGISTRY, TABLE_DICTIONARY
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


def _state(*rows):
    dictionaries = tuple(dict(row) for row in rows)
    return CatalogueState(
        status="valid",
        rows={
            ITEM: {
                REGISTRY.name: dictionaries,
                TABLE_DICTIONARY.name: dictionaries,
            }
        },
        present_tables=frozenset(table.name for table in CATALOGUE_TABLES),
        missing_tables=frozenset(),
    )


def _inventory(*tables):
    return TargetInventory(
        target_id="sales",
        kind="lakehouse",
        target_name="Sales_Dev",
        tables=tuple(f"Sales.{name}" for name in tables),
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


def test_physical_objects_without_catalogue_rows_generate_no_deletes():
    result = reconcile_catalogue_state(
        _state(), inventories={ITEM: _inventory("Unregistered")}
    )
    assert result.delete_dml == ()
    assert result.stale_objects == ()
