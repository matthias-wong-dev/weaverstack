"""Catalogue projections of native semantic-model definitions."""

from __future__ import annotations

import json

from ..semantic_models.compiler import escape, leaf_properties
from ..semantic_models.deployed import _RELATIONSHIP_DEFAULTS
from .tables import (
    DEPENDENCY,
    REGISTRY,
    ROLE_DATA,
    SEMANTIC_MODEL,
    SEMANTIC_MODEL_COLUMN,
    SEMANTIC_MODEL_MEASURE,
    SEMANTIC_MODEL_RELATIONSHIP,
    SEMANTIC_MODEL_TABLE,
)


def json_text(value):
    return json.dumps(
        value,
        sort_keys=True,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    )


def _text(value):
    return "\n".join(value) if isinstance(value, list) else value


def project_semantic_model(item, contribution, *, deployed=None):
    from ..semantic_models.lineage import dependency_references, observed_bindings

    model = {"model": {}} if deployed is None else deployed
    bindings = dict(contribution.source_bindings)
    if deployed is not None and contribution.expression_sources:
        bindings = {
            **observed_bindings(deployed, contribution.expression_sources),
            **{
                name: value
                for name, value in contribution.source_bindings.items()
                if name in contribution.source_references
            },
        }
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

    def metadata(node, path):
        return {
            **common,
            "properties": json_text(node),
            "provenance": json_text(
                {
                    p[len(path) + 1 :]: origin
                    for p, origin in provenance.items()
                    if p.startswith(path + "/")
                }
            ),
        }

    tables, columns, measures, relationships = [], [], [], []
    for table in model["model"].get("tables", ()):
        name = table["name"]
        path = f"/model/tables/{escape(name)}"
        tables.append(
            {
                **metadata(table, path),
                "table_name": name,
                "description": _text(table.get("description")),
                "source_binding": json_text(bindings[name])
                if name in bindings
                else None,
            }
        )
        for column in table.get("columns", ()):
            columns.append(
                {
                    **metadata(column, f"{path}/columns/{escape(column['name'])}"),
                    "table_name": name,
                    "column_name": column["name"],
                    "description": _text(column.get("description")),
                    "data_type": column.get("dataType"),
                    "column_type": column.get("type"),
                    "source_column": column.get("sourceColumn"),
                    "expression": _text(column.get("expression")),
                }
            )
        for measure in table.get("measures", ()):
            measures.append(
                {
                    **metadata(measure, f"{path}/measures/{escape(measure['name'])}"),
                    "table_name": name,
                    "measure_name": measure["name"],
                    "description": _text(measure.get("description")),
                    "expression": _text(measure.get("expression")),
                    "format_string": measure.get("formatString"),
                }
            )
    for relationship in model["model"].get("relationships", ()):
        relationships.append(
            {
                **metadata(
                    relationship,
                    f"/model/relationships/{escape(relationship['name'])}",
                ),
                "relationship_name": relationship["name"],
                **{
                    stored: relationship.get(native, _RELATIONSHIP_DEFAULTS.get(native))
                    for stored, native in (
                        ("from_table", "fromTable"),
                        ("from_column", "fromColumn"),
                        ("to_table", "toTable"),
                        ("to_column", "toColumn"),
                        ("from_cardinality", "fromCardinality"),
                        ("to_cardinality", "toCardinality"),
                        ("cross_filtering_behavior", "crossFilteringBehavior"),
                        ("is_active", "isActive"),
                    )
                },
            }
        )

    from ..semantic_models.references import source_identity
    from .claims import catalogue_columns

    dependencies = []
    for table, reference in dependency_references(
        contribution.source_references, bindings
    ):
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
    projected = {
        DEPENDENCY.name: tuple(dependencies),
        REGISTRY.name: (
            {**common, "object_type": "semantic_model", "object_role": ROLE_DATA},
        ),
        SEMANTIC_MODEL.name: (
            {
                **common,
                "description": _text(model["model"].get("description")),
                "definition": json_text(model),
                "properties": json_text(contribution.properties),
                "provenance": json_text(provenance),
            },
        ),
        **{
            table.name: tuple(
                sorted(rows, key=lambda row: tuple(row[k] for k in table.key))
            )
            for table, rows in (
                (SEMANTIC_MODEL_TABLE, tables),
                (SEMANTIC_MODEL_COLUMN, columns),
                (SEMANTIC_MODEL_MEASURE, measures),
                (SEMANTIC_MODEL_RELATIONSHIP, relationships),
            )
        },
    }
    return (
        projected
        if deployed is not None
        else {key: projected[key] for key in (DEPENDENCY.name, REGISTRY.name)}
    )
