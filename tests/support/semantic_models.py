"""Explicit semantic REST boundary fixtures; never a TMDL interpreter."""

import json
from pathlib import Path

from support.workspaces import _identifier


def fixture_parts():
    folder = (
        Path(__file__).parents[1] / "fixtures/semantic_model/Probe/Probe.SemanticModel"
    )
    return {
        p.relative_to(folder).as_posix(): p.read_bytes()
        for p in folder.rglob("*")
        if p.is_file()
    }


def probe_model():
    """TMSL recorded from the fixed Probe model, without platform identity parts."""
    return json.loads(
        (
            Path(__file__).parents[1] / "fixtures/semantic_model/observed-probe.json"
        ).read_text()
    )


def source_model(
    physical="Serving_Dev",
    *,
    relations=None,
    descriptions=None,
    notes=True,
    lakehouse=False,
):
    relations = (
        relations
        if relations is not None
        else {"Sales": "Sales", "SalesAgain": "Sales", "Summary": "Summary"}
    )
    descriptions = (
        descriptions
        if descriptions is not None
        else {"Sales": "Sales description", "Summary": "Summary description"}
    )
    database = _identifier("SQLEndpoint" if lakehouse else "Warehouse", physical)
    server = (
        "lake.datawarehouse.fabric.microsoft.com"
        if lakehouse
        else "serving.datawarehouse.fabric.microsoft.com"
    )
    model = {
        "culture": "en-US",
        "defaultPowerBIDataSourceVersion": "powerBI_V3",
        "tables": [],
        "expressions": [],
    }
    for table_name, object_name in relations.items():
        expression = "WeaverSource/" + table_name
        table = {
            "name": table_name,
            "columns": [
                {"name": "Id", "dataType": "int64", "sourceColumn": "Id"},
                {"name": "Label", "dataType": "string", "sourceColumn": "Label"},
            ],
            "partitions": [
                {
                    "name": table_name,
                    "mode": "directLake",
                    "source": {
                        "type": "entity",
                        "schemaName": "Cake",
                        "entityName": object_name,
                        "expressionSource": expression,
                    },
                }
            ],
        }
        if object_name in descriptions:
            table["description"] = descriptions[object_name]
        if notes and object_name == "Sales":
            table["columns"][0]["description"] = "Sales key"
        model["tables"].append(table)
        model["expressions"].append(
            {
                "name": expression,
                "kind": "m",
                "expression": f'Sql.Database("{server}", "{database}")',
            }
        )
    return {"compatibilityLevel": 1606, "model": model}
