"""The catalogue stores native semantic definitions and their child projection."""

import json

from support.weaver_test import weaver_test

from weaver.build_bundle.catalogue_actions import desired_catalogue
from weaver.build_bundle.targets import parse_build_item
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
        SEMANTIC_MODEL_DICTIONARY,
        SEMANTIC_OBJECT_DICTIONARY,
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
    catalogue = desired_catalogue(repository, {root}, {item: target})
    restored = Catalogue.from_mapping(catalogue.to_mapping())
    assert restored.registered[root].object_type == "semantic_model"
    assert restored.dag().node(root).target.kind == "semanticmodel"
    assert restored.dag().node(root).artefact is None
    rows = restored.rows[item]
    assert rows["Installation"][0]["workspace_id"] == "workspace-id"
    assert rows["Installation"][0]["item_id"] == "model-id"
    assert (
        json.loads(rows[SEMANTIC_MODEL_DICTIONARY.name][0]["definition"])
        == repository.semantic_models[item].model
    )
    children = {r["semantic_path"]: r for r in rows[SEMANTIC_OBJECT_DICTIONARY.name]}
    measure = children["/model/tables/Calendar/measures/Answer"]
    assert measure["semantic_kind"] == "measure"
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
    assert not pruned.rows[item][SEMANTIC_MODEL_DICTIONARY.name]
    assert not pruned.rows[item][SEMANTIC_OBJECT_DICTIONARY.name]
