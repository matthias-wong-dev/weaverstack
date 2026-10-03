"""Logical semantic sources retain exact Weaver relation identities."""

from support.weaver_test import weaver_test

from weaver.declaration.model import WeaverItemId
from weaver.declaration.repository import parse_item_repository
from weaver.locations import Location


@weaver_test()
def test_source_directives_are_separate_from_native_properties(tmp_path):
    folder = tmp_path / "SemanticModel/Reporting"
    folder.mkdir(parents=True)
    (folder / "addon.yml").write_text(
        "tables:\n"
        "  Sales:\n"
        "    .source: Warehouse/Serving/Cake.Sales\n"
        "    description: Sales facts\n"
        "  Customer:\n"
        "    .source: Lakehouse/Curated/Cake.Customer\n"
        "  OtherCustomer:\n"
        "    .source: Lakehouse/Curated/Tables/Cake.Customer\n",
        encoding="utf-8",
    )
    repository = parse_item_repository(Location(tmp_path.as_posix()))
    contribution = repository.semantic_models[
        WeaverItemId.parse("SemanticModel/Reporting")
    ]
    assert contribution.source_references == {
        "Sales": "Warehouse/Serving/Cake.Sales",
        "Customer": "Lakehouse/Curated/Tables/Cake.Customer",
        "OtherCustomer": "Lakehouse/Curated/Tables/Cake.Customer",
    }
    assert contribution.requested["tables"] == [
        {"name": "Sales", "description": "Sales facts"},
        {"name": "Customer"},
        {"name": "OtherCustomer"},
    ]
