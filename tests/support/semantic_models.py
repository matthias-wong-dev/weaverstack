"""Explicit semantic REST boundary fixtures; never a TMDL interpreter."""

import json
from pathlib import Path


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
    database = physical
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
        expression = "Lakehouse/Curated" if lakehouse else "Warehouse/Serving"
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
        model["expressions"] = [
            {
                "name": expression,
                "kind": "m",
                "expression": f'Sql.Database("{server}", "{database}")',
            }
        ]
    return {"compatibilityLevel": 1606, "model": model}


def shared_source_tmdl(
    *, logical="Warehouse/Serving", relations=None, descriptions=None, notes=True
):
    """Explicit native authoring fixture, independent of the recorded response."""
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
    text = f'expression \'{logical}\' = Sql.Database("previous", "database")\n'
    for table, relation in relations.items():
        description = descriptions.get(relation)
        text += (
            "\n" + (f"/// {description}\n" if description else "") + f"table {table}\n"
        )
        if notes and relation == "Sales":
            text += "\t/// Sales key\n"
        text += "\tcolumn Id\n\t\tdataType: int64\n\t\tsourceColumn: Id\n"
        text += "\n\tcolumn Label\n\t\tdataType: string\n\t\tsourceColumn: Label\n"
        text += f"\n\tpartition {table} = entity\n\t\tmode: directLake\n\t\tsource\n\t\t\tschemaName: Cake\n\t\t\tentityName: {relation}\n\t\t\texpressionSource: '{logical}'\n"
    return text
