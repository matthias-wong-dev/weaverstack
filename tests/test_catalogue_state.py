"""Catalogue reconciliation trusts only claims proved by physical inventory."""

from __future__ import annotations

import pytest

from weaver.build_bundle.catalogue_actions import _claim_statements
from weaver.build_bundle.prune import TargetInventory
from weaver.catalogue import (
    CATALOGUE_TABLES,
    FOLDER_DICTIONARY,
    REGISTRY,
    TABLE_DICTIONARY,
)
from weaver.catalogue.claims import CatalogueClaim, CatalogueClaimRule
from weaver.catalogue.state import Catalogue, reconcile_catalogue_state
from weaver.declaration.model import WeaverDocumentId, WeaverItemId
from weaver.errors import BuildError


ITEM = WeaverItemId.parse("Lakehouse/Sales")


def _row(name: str, *, object_type: str = "table"):
    return {
        "item_type": "Lakehouse",
        "item_name": "Sales",
        "schema_name": "Sales",
        "object_name": name,
        "object_type": object_type,
        "object_role": "data",
        "signature": f"signature-{name}-{object_type}",
    }


def _folder_row(name: str):
    return {**_row(name, object_type="folder"), "schema_name": "Files/Sales"}


def _state(*rows):
    registry = tuple(dict(row) for row in rows)
    return Catalogue(
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

    assert [row["object_name"] for row in result.catalogue.rows[ITEM][REGISTRY.name]] == [
        "Current"
    ]
    assert [
        row["object_name"] for row in result.catalogue.rows[ITEM][TABLE_DICTIONARY.name]
    ] == ["Current"]
    assert result.stale_objects == ("Lakehouse/Sales/Sales.Missing",)
    assert {claim.rule.table.name for claim in result.stale_claims} >= {
        "TableDictionary",
        "Registry",
    }


def test_same_named_folder_and_table_keep_the_four_part_catalogue_identity():
    result = reconcile_catalogue_state(
        _state(_row("Customer"), _folder_row("Customer")),
        inventories={ITEM: _inventory(folders=("Customer",))},
    )

    assert [
        row["object_type"] for row in result.catalogue.rows[ITEM][REGISTRY.name]
    ] == ["folder"]
    assert result.catalogue.rows[ITEM][TABLE_DICTIONARY.name] == ()
    assert [
        row["object_type"] for row in result.catalogue.rows[ITEM][FOLDER_DICTIONARY.name]
    ] == ["folder"]
    assert result.stale_objects == ("Lakehouse/Sales/Sales.Customer",)
    assert result.stale_claims
    assert all(not claim.identity.is_files for claim in result.stale_claims)


def test_missing_folder_does_not_remove_same_named_table():
    result = reconcile_catalogue_state(
        _state(_row("Customer"), _folder_row("Customer")),
        inventories={ITEM: _inventory("Customer")},
    )

    assert [
        row["object_type"] for row in result.catalogue.rows[ITEM][REGISTRY.name]
    ] == ["table"]
    assert [
        row["object_type"] for row in result.catalogue.rows[ITEM][TABLE_DICTIONARY.name]
    ] == ["table"]
    assert result.catalogue.rows[ITEM][FOLDER_DICTIONARY.name] == ()
    assert result.stale_objects == ("Lakehouse/Sales/Files/Sales.Customer",)
    assert result.stale_claims
    assert all(claim.identity.is_files for claim in result.stale_claims)


def test_physical_objects_without_catalogue_rows_generate_no_deletes():
    result = reconcile_catalogue_state(
        _state(), inventories={ITEM: _inventory("Unregistered")}
    )
    assert result.stale_claims == ()
    assert result.stale_objects == ()


def test_folder_claims_do_not_infer_ownership_of_table_dictionary_rows():
    state = _state(_folder_row("Archive"))
    state.rows[ITEM][TABLE_DICTIONARY.name] = (_row("Archive"),)
    result = reconcile_catalogue_state(state, inventories={ITEM: _inventory()})

    assert result.catalogue.rows[ITEM][TABLE_DICTIONARY.name] == (_row("Archive"),)
    assert TABLE_DICTIONARY.name not in {
        claim.rule.table.name for claim in result.stale_claims
    }


def test_registry_rejects_an_unsupported_installed_object_type():
    with pytest.raises(BuildError, match="unsupported object_type 'procedure'"):
        reconcile_catalogue_state(
            _state(_row("Load", object_type="procedure")),
            inventories={ITEM: _inventory()},
        )


def test_claim_deletion_uses_the_rule_predicate_columns():
    identity = WeaverDocumentId.parse("Lakehouse/Sales/Sales.Customer")
    rule = CatalogueClaimRule(
        REGISTRY,
        predicate_columns=("owned_schema", "owned_object"),
    )

    statement = _claim_statements((CatalogueClaim(identity, rule),))[0]

    assert "`owned_schema` = 'Sales'" in statement
    assert "`owned_object` = 'Customer'" in statement
    assert "`schema_name`" not in statement
    assert "`object_name`" not in statement
