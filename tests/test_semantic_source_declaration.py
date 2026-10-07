"""Shared native sources acquire managed lineage during environment resolution."""

from support.weaver_test import weaver_test

from weaver.declaration.model import WeaverItemId
from weaver.declaration.repository import parse_item_repository
from weaver.locations import Location


@weaver_test()
def test_shared_source_declarations_remain_native_until_environment_resolution(
    tmp_path,
):
    folder = tmp_path / "SemanticModel/Reporting"
    folder.mkdir(parents=True)
    content = """expression 'Warehouse/Serving' = Sql.Database("server", "database")

expression 'Lakehouse/Curated' = Sql.Database("server", "database")

/// Sales facts
table Sales
    partition Sales = entity
        mode: directLake
        source
            schemaName: Cake
            entityName: Sales
            expressionSource: 'Warehouse/Serving'

table Customer
    partition Customer = entity
        mode: directLake
        source
            schemaName: Cake
            entityName: Customer
            expressionSource: 'Lakehouse/Curated'
"""
    (folder / f"{folder.name}.tmdl").write_text(content, encoding="utf-8")
    repository = parse_item_repository(Location(tmp_path.as_posix()))
    contribution = repository.semantic_models[
        WeaverItemId.parse("SemanticModel/Reporting")
    ]
    assert contribution.source_references == {}
    assert contribution.source_bindings == {}
    assert contribution.dependencies == ()
    tables = {t["name"]: t for t in contribution.requested["tables"]}
    assert tables["Sales"]["description"] == "Sales facts"
    assert (
        tables["Sales"]["partitions"][0]["source"]["expressionSource"]
        == "Warehouse/Serving"
    )
    assert (
        tables["Customer"]["partitions"][0]["source"]["expressionSource"]
        == "Lakehouse/Curated"
    )
    assert (folder / f"{folder.name}.tmdl").read_text(encoding="utf-8") == content
