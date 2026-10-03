"""Catalogue projections of native semantic-model definitions."""

from __future__ import annotations

import json

from ..semantic_models.compiler import escape, leaf_properties
from .tables import (
    DEPENDENCY,
    REGISTRY,
    ROLE_DATA,
    SEMANTIC_MODEL_DICTIONARY,
    SEMANTIC_OBJECT_DICTIONARY,
)

_COLLECTION_KINDS = {
    "tables": "table",
    "columns": "column",
    "measures": "measure",
    "partitions": "partition",
    "relationships": "relationship",
    "roles": "role",
    "tablePermissions": "tablePermission",
    "hierarchies": "hierarchy",
    "levels": "level",
    "annotations": "annotation",
    "calculationItems": "calculationItem",
    "perspectives": "perspective",
    "cultures": "culture",
}


def json_text(value):
    return json.dumps(
        value,
        sort_keys=True,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    )


def project_semantic_model(item, contribution, *, deployed=None):
    model = contribution.model if deployed is None else deployed
    common = {
        "item_type": item.item_type,
        "item_name": item.item_name,
        "schema_name": "",
        "object_name": "",
        "signature": contribution.signature,
    }
    provenance = {
        p: contribution.provenance.get(
            p, {"source": "semantic engine", "reason": "readback"}
        )
        for p in leaf_properties(model)
    }
    objects = []

    def visit(node, path, kind):
        if not isinstance(node, dict):
            return
        if kind is not None:
            local = {
                p[len(path) + 1 :]: origin
                for p, origin in provenance.items()
                if p.startswith(path + "/")
            }
            objects.append(
                {
                    **common,
                    "semantic_path": path,
                    "semantic_kind": kind,
                    "properties": json_text(node),
                    "provenance": json_text(local),
                    "source_binding": (
                        json_text(contribution.source_bindings[node["name"]])
                        if kind == "table"
                        and node["name"] in contribution.source_bindings
                        else None
                    ),
                }
            )
        for key, value in node.items():
            child_path = f"{path}/{escape(key)}"
            if isinstance(value, list) and key in _COLLECTION_KINDS:
                for child in value:
                    if isinstance(child, dict) and isinstance(child.get("name"), str):
                        visit(
                            child,
                            f"{child_path}/{escape(child['name'])}",
                            _COLLECTION_KINDS[key],
                        )
            elif isinstance(value, dict):
                visit(
                    value,
                    child_path,
                    "calculationGroup" if key == "calculationGroup" else None,
                )

    visit(model["model"], "/model", "model")
    from ..semantic_models.references import source_identity
    from .claims import catalogue_columns

    dependencies = []
    for table, reference in sorted(contribution.source_references.items()):
        producer = source_identity(reference)
        schema, name = catalogue_columns(producer)
        dependencies.append(
            {
                "item_type": item.item_type,
                "item_name": item.item_name,
                "referencing_schema_name": "",
                "referencing_object_name": table,
                "dependency_reference": str(producer),
                "referenced_item_type": producer.item.item_type,
                "referenced_item_name": producer.item.item_name,
                "referenced_schema_name": schema,
                "referenced_object_name": name,
                "signature": contribution.signature,
            }
        )
    return {
        DEPENDENCY.name: tuple(dependencies),
        REGISTRY.name: (
            {**common, "object_type": "semantic_model", "object_role": ROLE_DATA},
        ),
        SEMANTIC_MODEL_DICTIONARY.name: (
            {
                **common,
                "definition": json_text(model),
                "properties": json_text(contribution.properties),
                "provenance": json_text(provenance),
            },
        ),
        SEMANTIC_OBJECT_DICTIONARY.name: tuple(
            sorted(objects, key=lambda r: r["semantic_path"])
        ),
    }
