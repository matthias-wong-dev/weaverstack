"""The native Parcel model can begin locally, then read the Warehouse schema."""

from support.weaver_test import weaver_test

from weaver.declaration.model import WeaverItemId
from weaver.declaration.repository import parse_item_repository
from weaver.initialise import _generated_files, _write
from weaver.locations import Location
from weaver.onboarding.project import ProjectRequest
from weaver.semantic_models import TmdlDefinition


@weaver_test()
def test_native_parcel_current_status_matches_warehouse_columns(tmp_path):
    files = _generated_files(
        ProjectRequest(
            workspace="Parcel Development",
            catalogue=None,
            environment=None,
            example="Parcel",
            semantic_model="Parcel",
        )
    )
    _write(tmp_path, files)
    repository = parse_item_repository(Location(str(tmp_path)))
    contribution = repository.semantic_models[
        WeaverItemId.parse("SemanticModel/Parcel")
    ]
    table = TmdlDefinition(contribution.parts).model.tables["ParcelStatus"]
    assert {column.name for column in table.columns} == {"Parcel ID", "Status", "Depot"}
    assert {column.dataType for column in table.columns} == {"string"}
    assert {column.name: column.sourceColumn for column in table.columns} == {
        "Parcel ID": "Parcel ID",
        "Status": "Status",
        "Depot": "Depot",
    }
    assert "#table" in table.partitions["ParcelStatus"].source
    assert "P-1001" in table.partitions["ParcelStatus"].source
    assert not contribution.source_references
    assert "Delivery" in TmdlDefinition(contribution.parts).model.tables
    assert "Depot" in TmdlDefinition(contribution.parts).model.tables
