"""The catalogue stores native semantic definitions and their child projection."""

import json

from support.weaver_test import weaver_test

from weaver.build_bundle.catalogue_actions import desired_catalogue
from weaver.build_bundle.targets import parse_build_item
from weaver.catalogue.semantic import project_semantic_model
from weaver.catalogue.state import Catalogue
from weaver.declaration.model import WeaverDocumentId, WeaverItemId
from weaver.declaration.repository import parse_item_repository
from weaver.locations import Location


@weaver_test()
def test_semantic_catalogue_roundtrips_model_root_and_projects_native_children(
    tmp_path,
):
    from weaver.catalogue.claims import (
        CatalogueClaim,
        claim_rules_for_object_type,
        without_claims,
    )
    from weaver.catalogue.tables import (
        SEMANTIC_MODEL,
        SEMANTIC_MODEL_MEASURE,
        SEMANTIC_TABLES,
    )

    folder = tmp_path / "SemanticModel" / "Reporting"
    folder.mkdir(parents=True)
    (folder / "addon.yml").write_text(
        'tables:\n  Calendar:\n    .dax: ROW("Year", 2026)\n    measures:\n      Answer:\n        expression: "1"\n',
        encoding="utf-8",
    )
    repository = parse_item_repository(Location(tmp_path.as_posix()))
    item = WeaverItemId.parse("SemanticModel/Reporting")
    root = WeaverDocumentId.model_root(item)
    from dataclasses import replace

    target = replace(
        parse_build_item(
            "SemanticModel/Reporting=SemanticModel/Reporting_Dev"
        ).to_bound_target(),
        workspace_id="workspace-id",
        item_id="model-id",
    )
    desired = desired_catalogue(repository, {root}, {item: target})
    assert SEMANTIC_MODEL.name not in desired.rows[item]
    observed = {
        "model": {
            "culture": "en-US",
            "tables": [
                {
                    "name": "Calendar",
                    "partitions": [
                        {
                            "name": "Calendar",
                            "source": {
                                "type": "calculated",
                                "expression": 'ROW("Year", 2026)',
                            },
                        }
                    ],
                    "columns": [
                        {"name": "Year", "dataType": "int64", "sourceColumn": "[Year]"}
                    ],
                    "measures": [{"name": "Answer", "expression": "1"}],
                }
            ],
        }
    }
    projection = project_semantic_model(
        item, repository.semantic_models[item], deployed=observed
    )
    catalogue = Catalogue({**desired.rows, item: {**desired.rows[item], **projection}})
    restored = Catalogue.from_mapping(catalogue.to_mapping())
    assert restored.registered[root].object_type == "semantic_model"
    assert restored.dag().node(root).target.kind == "semanticmodel"
    assert restored.dag().node(root).artefact is None
    rows = restored.rows[item]
    assert rows["Installation"][0]["workspace_id"] == "workspace-id"
    assert rows["Installation"][0]["item_id"] == "model-id"
    assert json.loads(rows[SEMANTIC_MODEL.name][0]["definition"]) == observed
    (measure,) = rows[SEMANTIC_MODEL_MEASURE.name]
    assert measure["table_name"] == "Calendar"
    assert measure["measure_name"] == "Answer"
    assert json.loads(measure["properties"])["expression"] == "1"
    assert (
        json.loads(measure["provenance"])["expression"]["source"]
        == "SemanticModel/Reporting/addon.yml"
    )
    pruned = without_claims(
        restored,
        [
            CatalogueClaim(root, rule)
            for rule in claim_rules_for_object_type("semantic_model")
        ],
    )
    assert not pruned.registered
    assert not pruned.rows[item][SEMANTIC_MODEL.name]
    assert all(not pruned.rows[item][table.name] for table in SEMANTIC_TABLES)
