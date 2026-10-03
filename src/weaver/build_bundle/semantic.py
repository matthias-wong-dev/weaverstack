"""Semantic model actions in the ordinary Build barriers."""

from ..catalogue.semantic import json_text
from ..catalogue.state import Catalogue
from ..catalogue.tables import SEMANTIC_MODEL_DICTIONARY, SEMANTIC_OBJECT_DICTIONARY
from ..declaration.model import WeaverDocumentId
from .models import BuildBatch, InstallAction
from .payloads import sha256_hex
from .stages import BUILD, CATALOGUE, PlannedStage

SEMANTIC_TABLES = (SEMANTIC_MODEL_DICTIONARY, SEMANTIC_OBJECT_DICTIONARY)


def bind_semantic_target(target, inventory):
    from dataclasses import replace

    from .targets import SEMANTIC_MODEL_TARGET

    if target.kind != SEMANTIC_MODEL_TARGET or inventory is None:
        return target
    return replace(
        target,
        workspace_id=inventory.workspace_id,
        item_id=inventory.item_id or target.item_id,
    )


def semantic_stage(repository, item, target, *, catalogue_target=None):
    contribution = repository.semantic_models[item]
    publishing = catalogue_target is not None
    executor = "semantic_catalogue" if publishing else "semantic_model"
    filename = f"{target.id}.{executor}.json"
    content = (
        json_text(
            {
                "model": contribution.model,
                "properties": contribution.properties,
                "provenance": dict(contribution.provenance),
                "source_references": dict(contribution.source_references),
                "source_bindings": dict(contribution.source_bindings),
                "signature": contribution.signature,
                "target_id": target.id,
                "item": str(item),
                "allow_purge_data": False,
            }
        )
        + "\n"
    ).encode("utf-8")
    action = InstallAction(
        id=f"{executor}-{target.id}",
        kind=executor,
        executor=executor,
        resource_node_id=str(item),
        payload=filename,
        payload_sha256=sha256_hex(content),
    )
    return PlannedStage(
        phase=CATALOGUE if publishing else BUILD,
        slug="semantic-catalogue" if publishing else "build-objects",
        index=0,
        description="Publish deployed semantic definitions"
        if publishing
        else "Build item documents",
        payloads={filename: content},
        batches=(
            BuildBatch(
                id=action.id,
                target_id=catalogue_target.id if publishing else target.id,
                actions=(action,),
            ),
        ),
    )


def publication_catalogues(current, desired, selected_models):
    """Retain observed dictionaries until installation supplies replacement rows."""
    rows = {item: dict(tables) for item, tables in desired.rows.items()}
    before = {item: dict(tables) for item, tables in current.rows.items()}
    for item, tables in rows.items():
        if item.item_type != "SemanticModel":
            continue
        for table in SEMANTIC_TABLES:
            if WeaverDocumentId.parse(str(item)) in selected_models:
                tables.pop(table.name, None)
                before.get(item, {}).pop(table.name, None)
            else:
                tables[table.name] = current.rows.get(item, {}).get(table.name, ())
    return Catalogue(before), Catalogue(rows)
