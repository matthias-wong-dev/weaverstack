"""Pure-Python tests for logical repository, item and document identity."""

from __future__ import annotations

import pytest

from weaver.declaration import WeaverDocument
from weaver.declaration.metadata import SesDocument
from weaver.declaration.model import (
    WeaverDocumentId,
    WeaverItem,
    WeaverItemId,
    WeaverRepository,
    WeaverSchemaId,
)
from weaver.errors import DiscoveryError, IdentityError
from weaver.targets import DeltaTarget


def test_item_and_document_identities_round_trip_exactly():
    item = WeaverItemId.parse("Lakehouse/Raw")
    table = WeaverDocumentId.parse("Lakehouse/Raw/Sales.Customer")
    folder = WeaverDocumentId.parse("Lakehouse/Raw/Files/Sales.Customer")

    assert str(item) == "Lakehouse/Raw"
    assert str(table) == "Lakehouse/Raw/Sales.Customer"
    assert str(folder) == "Lakehouse/Raw/Files/Sales.Customer"
    assert table != folder


def test_same_object_name_in_distinct_typed_items_is_distinct():
    lakehouse = WeaverDocumentId.parse("Lakehouse/Raw/Sales.Customer")
    warehouse = WeaverDocumentId.parse("Warehouse/Reporting/Sales.Customer")

    assert lakehouse != warehouse
    assert len({lakehouse, warehouse}) == 2


@pytest.mark.parametrize(
    "text",
    [
        "lakehouse/Raw",
        "LAKEHOUSE/Raw",
        "SemanticModel/Reporting",
        "Lakehouse",
        "Lakehouse/Raw/Extra",
    ],
)
def test_item_type_and_shape_are_strict(text):
    with pytest.raises(IdentityError):
        WeaverItemId.parse(text)


def test_files_documents_only_belong_to_lakehouses():
    with pytest.raises(IdentityError, match="only belong to a Lakehouse"):
        WeaverDocumentId.parse("Warehouse/Reporting/Files/Sales.Export")


def test_lookup_is_exact_case():
    item_id = WeaverItemId.parse("Lakehouse/Raw")
    item = WeaverItem(
        item_id,
        schemas=(WeaverSchemaId.parse("Lakehouse/Raw/Sales"),),
        documents=(WeaverDocumentId.parse("Lakehouse/Raw/Sales.Customer"),),
    )
    repository = WeaverRepository("Estate", (item,))

    assert repository["Lakehouse/Raw"]["Sales.Customer"].object_id.object == "Customer"
    with pytest.raises(DiscoveryError):
        repository["Lakehouse/raw"]
    with pytest.raises(DiscoveryError):
        item["sales.Customer"]


def test_case_only_duplicate_declarations_are_rejected():
    item_id = WeaverItemId.parse("Lakehouse/Raw")
    with pytest.raises(DiscoveryError, match="differ only by case"):
        WeaverItem(
            item_id,
            documents=(
                WeaverDocumentId.parse("Lakehouse/Raw/Sales.Customer"),
                WeaverDocumentId.parse("Lakehouse/Raw/sales.Customer"),
            ),
        )


def test_logical_identity_is_independent_of_physical_binding():
    logical = WeaverItemId.parse("Lakehouse/Curated")
    development = {logical: DeltaTarget.parse("Curated_Dev")}
    production = {logical: DeltaTarget.parse("Curated_Prod")}

    assert next(iter(development)) == next(iter(production)) == logical
    assert development[logical] != production[logical]


def test_weaver_document_is_the_canonical_metadata_name_during_transition():
    assert SesDocument is WeaverDocument
    assert WeaverDocument.__name__ == "WeaverDocument"
