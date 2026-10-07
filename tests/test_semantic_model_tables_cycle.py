"""Typed semantic catalogue rows follow the model's Build lifecycle."""

from dataclasses import replace

from support.semantic_models import probe_model
from support.weaver_test import weaver_test
from test_semantic_model_build_cycle import ITEM, ROOT, prepared

from weaver.build_bundle.catalogue_actions import desired_catalogue
from weaver.catalogue.claims import (
    CatalogueClaim,
    claim_rules_for_object_type,
    without_claims,
)
from weaver.catalogue.semantic import project_semantic_model
from weaver.catalogue.state import Catalogue
from weaver.catalogue.tables import CATALOGUE_TABLES
from weaver.declaration.repository import parse_item_repository
from weaver.locations import Location


@weaver_test()
def test_typed_semantic_rows_keep_descriptions_native_metadata_and_claim_ownership(
    tmp_path,
):
    root, _, bindings, _, _ = prepared(tmp_path, True)
    (root / str(ITEM) / f"{ITEM.item_name}.tmdl").write_text(
        "/// Sales model\nmodel Model\n\n/// Sales transactions\nref table Sales\n\t/// Product identity\n\tcolumn ProductId\n\n\t/// Total revenue\n\tmeasure Revenue\n",
        encoding="utf-8",
    )
    repository = parse_item_repository(Location(root.as_posix()))
    target = replace(
        bindings.by_item[ITEM].to_bound_target(),
        workspace_id="workspace-id",
        item_id="model-id",
    )
    desired = desired_catalogue(repository, {ROOT}, {ITEM: target})
    assert "SemanticModel" not in desired.rows[ITEM]
    observed = probe_model()
    observed["model"]["description"] = "Sales model"
    sales = next(t for t in observed["model"]["tables"] if t["name"] == "Sales")
    sales["description"] = "Sales transactions"
    next(c for c in sales["columns"] if c["name"] == "ProductId")["description"] = (
        "Product identity"
    )
    next(m for m in sales["measures"] if m["name"] == "Revenue")["description"] = (
        "Total revenue"
    )
    projection = project_semantic_model(
        ITEM, repository.semantic_models[ITEM], deployed=observed
    )
    projected = Catalogue({**desired.rows, ITEM: {**desired.rows[ITEM], **projection}})
    catalogue = Catalogue.from_mapping(projected.to_mapping())
    rows = catalogue.rows[ITEM]
    semantic_tables = {
        "SemanticModel",
        "SemanticModelTable",
        "SemanticModelMeasure",
        "SemanticModelRelationship",
        "SemanticModelColumn",
    }
    assert {
        t.name for t in CATALOGUE_TABLES if t.name.startswith("Semantic")
    } == semantic_tables
    assert rows["SemanticModel"][0]["description"] == "Sales model"
    assert set(rows["SemanticModel"][0]) == {
        "item_type",
        "item_name",
        "description",
        "signature",
    }
    sales = next(r for r in rows["SemanticModelTable"] if r["table_name"] == "Sales")
    assert sales["description"] == "Sales transactions"
    assert (
        sales["table_ordinal"]
        == repository.semantic_models[ITEM].table_names.index("Sales") + 1
    )
    measure = next(
        r for r in rows["SemanticModelMeasure"] if r["measure_name"] == "Revenue"
    )
    assert measure["table_name"] == "Sales"
    assert measure["description"] == "Total revenue"
    assert measure["expression"]
    column = next(
        r
        for r in rows["SemanticModelColumn"]
        if r["table_name"] == "Sales" and r["column_name"] == "ProductId"
    )
    assert column["description"] == "Product identity"
    assert column["data_type"] == "int64"
    relationship = rows["SemanticModelRelationship"][0]
    assert relationship["from_table"] == "Sales"
    assert relationship["to_table"] == "Product"
    assert relationship["from_column"] == relationship["to_column"] == "ProductId"
    assert relationship["from_cardinality"] == "many"
    assert relationship["to_cardinality"] == "one"
    assert relationship["cross_filtering_behavior"] == "oneDirection"
    assert relationship["is_active"] is True
    for name in semantic_tables:
        for row in rows[name]:
            assert row["signature"] == repository.semantic_models[ITEM].signature
            assert not {"definition", "properties", "provenance"} & set(row)
    pruned = without_claims(
        catalogue,
        [
            CatalogueClaim(ROOT, rule)
            for rule in claim_rules_for_object_type("semantic_model")
        ],
    )
    assert not pruned.registered
    assert all(not pruned.rows[ITEM][name] for name in semantic_tables)
