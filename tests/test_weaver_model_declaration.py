"""Pure-Python tests for logical repository, item and document identity."""

from __future__ import annotations

import pytest
from support.weaver_test import weaver_test

from weaver.declaration import WeaverDocument
from weaver.declaration.metadata import ObjectId, SesDocument
from weaver.declaration.model import (
    OBJECT_SHAPE,
    VALIDATION_SHAPE,
    WeaverDocumentId,
    WeaverItem,
    WeaverItemId,
    WeaverRepository,
    WeaverSchemaId,
)
from weaver.errors import DiscoveryError, IdentityError
from weaver.targets import DeltaTarget


@weaver_test()
def test_item_and_document_identities_round_trip_exactly():
    item = WeaverItemId.parse("Lakehouse/Raw")
    table = WeaverDocumentId.parse("Lakehouse/Raw/Tables/Sales.Customer")
    folder = WeaverDocumentId.parse("Lakehouse/Raw/Files/Sales.Customer")

    assert str(item) == "Lakehouse/Raw"
    assert str(table) == "Lakehouse/Raw/Tables/Sales.Customer"
    assert str(folder) == "Lakehouse/Raw/Files/Sales.Customer"
    assert table != folder


@weaver_test()
def test_same_object_name_in_distinct_typed_items_is_distinct():
    lakehouse = WeaverDocumentId.parse("Lakehouse/Raw/Tables/Sales.Customer")
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
@weaver_test()
def test_item_type_and_shape_are_strict(text):
    with pytest.raises(IdentityError):
        WeaverItemId.parse(text)


@weaver_test()
def test_files_documents_only_belong_to_lakehouses():
    with pytest.raises(IdentityError, match="is a Lakehouse area"):
        WeaverDocumentId.parse("Warehouse/Reporting/Files/Sales.Export")
    with pytest.raises(IdentityError, match="only belong to a Lakehouse"):
        WeaverDocumentId(
            WeaverItemId.parse("Warehouse/Reporting"),
            ObjectId("Sales", "Export"),
            is_files=True,
        )


@weaver_test()
def test_lookup_is_exact_case():
    item_id = WeaverItemId.parse("Lakehouse/Raw")
    item = WeaverItem(
        item_id,
        schemas=(WeaverSchemaId.parse("Lakehouse/Raw/Sales"),),
        documents=(WeaverDocumentId.parse("Lakehouse/Raw/Tables/Sales.Customer"),),
    )
    repository = WeaverRepository("Estate", (item,))

    assert (
        repository["Lakehouse/Raw"]["Tables/Sales.Customer"].object_id.object
        == "Customer"
    )
    with pytest.raises(DiscoveryError):
        repository["Lakehouse/raw"]
    with pytest.raises(DiscoveryError):
        item["sales.Customer"]


@weaver_test()
def test_case_only_duplicate_declarations_are_rejected():
    item_id = WeaverItemId.parse("Lakehouse/Raw")
    with pytest.raises(DiscoveryError, match="differ only by case"):
        WeaverItem(
            item_id,
            documents=(
                WeaverDocumentId.parse("Lakehouse/Raw/Tables/Sales.Customer"),
                WeaverDocumentId.parse("Lakehouse/Raw/Tables/sales.Customer"),
            ),
        )


@weaver_test()
def test_logical_identity_is_independent_of_physical_binding():
    logical = WeaverItemId.parse("Lakehouse/Curated")
    development = {logical: DeltaTarget.parse("Curated_Dev")}
    production = {logical: DeltaTarget.parse("Curated_Prod")}

    assert next(iter(development)) == next(iter(production)) == logical
    assert development[logical] != production[logical]


@weaver_test()
def test_weaver_document_is_the_canonical_metadata_name_during_transition():
    assert SesDocument is WeaverDocument
    assert WeaverDocument.__name__ == "WeaverDocument"


# --- the two Lakehouse areas ---------------------------------------------------


@weaver_test()
def test_a_lakehouse_data_identity_names_its_area():
    """A Fabric Lakehouse holds its tables under Tables and the rest under Files."""

    table = WeaverDocumentId.parse("Lakehouse/Raw/Tables/Sales.Customer")
    folder = WeaverDocumentId.parse("Lakehouse/Raw/Files/Sales.Customer")

    assert (table.area, folder.area) == ("Tables", "Files")
    assert (table.is_files, folder.is_files) == (False, True)
    assert table.relative == "Tables/Sales.Customer"
    assert folder.relative == "Files/Sales.Customer"


@weaver_test()
def test_one_schema_object_may_sit_in_both_areas():
    table = WeaverDocumentId.parse("Lakehouse/Raw/Tables/Sales.Customer")
    folder = WeaverDocumentId.parse("Lakehouse/Raw/Files/Sales.Customer")

    assert table != folder
    assert len({table, folder}) == 2
    assert table.object_id == folder.object_id


@weaver_test()
def test_a_warehouse_relation_names_no_area():
    warehouse = WeaverDocumentId.parse("Warehouse/Reporting/Sales.Customer")

    assert warehouse.area is None
    assert warehouse.shape == OBJECT_SHAPE
    assert str(warehouse) == "Warehouse/Reporting/Sales.Customer"


@weaver_test()
def test_a_lakehouse_area_may_not_be_written_for_a_warehouse():
    with pytest.raises(IdentityError):
        WeaverDocumentId.parse("Warehouse/Reporting/Tables/Sales.Customer")


@weaver_test()
def test_the_runtime_shapes_are_unchanged():
    module = WeaverDocumentId.parse("Lakehouse/Raw/file:_/Load/lib/dates.py")
    procedure = WeaverDocumentId.parse(
        "Warehouse/Reporting/procedure:_/Load Sales.Customer"
    )

    assert (module.area, procedure.area) == (None, None)
    assert str(module) == "Lakehouse/Raw/file:_/Load/lib/dates.py"
    assert str(procedure) == "Warehouse/Reporting/procedure:_/Load Sales.Customer"


@weaver_test()
def test_a_validation_identity_acquires_no_area():
    """A Test materialises nothing, so it sits in neither area and names neither."""

    validation = WeaverDocumentId.validation(
        WeaverItemId.parse("Lakehouse/Raw"), ObjectId("Sales", "CustomerCount")
    )

    assert validation.shape == VALIDATION_SHAPE
    assert validation.area is None
    assert str(validation) == "Lakehouse/Raw/Sales.CustomerCount"
    assert WeaverDocumentId.parse(str(validation)) == validation


@weaver_test()
def test_a_warehouse_validation_keeps_the_object_shape():
    """A Warehouse has no areas, so its validation is spelled as its relations are."""

    validation = WeaverDocumentId.validation(
        WeaverItemId.parse("Warehouse/Reporting"), ObjectId("Sales", "CustomerCount")
    )

    assert validation.shape == OBJECT_SHAPE
    assert WeaverDocumentId.parse(str(validation)) == validation


@weaver_test()
def test_the_validation_shape_is_refused_for_a_warehouse():
    with pytest.raises(IdentityError, match="occupies no Lakehouse area"):
        WeaverDocumentId(
            WeaverItemId.parse("Warehouse/Reporting"),
            ObjectId("Sales", "CustomerCount"),
            shape=VALIDATION_SHAPE,
        )


@weaver_test()
def test_the_old_lakehouse_data_spelling_is_not_a_table_identity():
    """It is the validation namespace now, and it is not a Table or a View."""

    parsed = WeaverDocumentId.parse("Lakehouse/Raw/Sales.Customer")

    assert parsed.shape == VALIDATION_SHAPE
    assert parsed != WeaverDocumentId.parse("Lakehouse/Raw/Tables/Sales.Customer")


@weaver_test()
def test_an_item_relative_lakehouse_identity_names_its_area():
    item = WeaverItemId.parse("Lakehouse/Raw")

    table = WeaverDocumentId.parse_local(item, "Tables/Sales.Customer")
    folder = WeaverDocumentId.parse_local(item, "Files/Sales.Customer")

    assert str(table) == "Lakehouse/Raw/Tables/Sales.Customer"
    assert str(folder) == "Lakehouse/Raw/Files/Sales.Customer"
    assert WeaverDocumentId.parse_local(item, table.relative) == table
