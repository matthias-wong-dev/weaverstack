import json
from dataclasses import replace
from types import SimpleNamespace

from support.weaver_test import weaver_test
from test_powerbi_project_declaration import parse, write

from weaver.build_bundle.semantic import semantic_stage
from weaver.catalogue.semantic import project_semantic_model
from weaver.declaration.model import WeaverItemId
from weaver.semantic_models.binding import bind_semantic_sources
from weaver.semantic_models.definition import decode_parts
from weaver.semantic_models.source import SemanticContribution

ITEM = WeaverItemId("SemanticModel", "Revenue")
REFERENCE = "Warehouse/Serving/Cake.Sales"


def source_bound_annotation(tmp_path, body, *, tables=""):
    write(
        tmp_path,
        "PowerBI/Sales/Revenue.tmdl",
        "model Model\n\tannotation ACME.AfterSource = true\n\n"
        f"table Sales\n\tannotation Weaver.Source = {REFERENCE}\n" + tables,
    )
    write(
        tmp_path,
        "SemanticModel/annotations/ACME__AfterSource.py",
        "from weaver.semantic_models import Annotation\n"
        "class ACME__AfterSource(Annotation):\n"
        '    scopes = {"model"}\n'
        "    def apply(self, target):\n"
        '        if "Id" not in target.tables["Sales"].columns:\n'
        "            return\n" + body,
    )
    repository = parse(tmp_path)
    source = {
        "reference": REFERENCE,
        "server": "serving.datawarehouse.fabric.microsoft.com",
        "database": "Serving_Dev",
        "schema": "Cake",
        "object": "Sales",
        "source_columns": [{"column_name": "Id", "data_type": "bigint"}],
        "description": "Sales facts",
        "column_notes": {"Id": "Sales key"},
    }
    bound = bind_semantic_sources(repository, {REFERENCE: source}, {ITEM: None})
    return repository.semantic_models[ITEM], bound


def transported_contribution(repository):
    target = SimpleNamespace(id="model", kind="semanticmodel")
    stage = semantic_stage(repository, ITEM, target)
    (payload,) = stage.payloads.values()
    spec = json.loads(payload)
    return spec, SemanticContribution(
        parts=decode_parts(spec["definition"]),
        sources={},
        provenance=spec["provenance"],
        requested=spec["requested"],
        absent=tuple(tuple(tuple(pair) for pair in path) for path in spec["absent"]),
        source_references=spec["source_references"],
        source_bindings=spec["source_bindings"],
        table_order=tuple(spec["table_order"]),
    )


@weaver_test()
def test_source_generated_columns_allow_final_annotation_table_ordinals(tmp_path):
    initial, repository = source_bound_annotation(
        tmp_path,
        '        if "Metadata" not in target.tables:\n'
        '            table = target.tables.add("Metadata")\n'
        '            table.columns.add("Value", dataType="int64", sourceColumn="[Value]")\n'
        '            table.partitions.add("Metadata", "calculated", mode="import").set_expression("source", "{1}")\n',
    )
    assert initial.table_names == ("Sales",)
    final = repository.semantic_models[ITEM]
    assert b"column 'Id'" in final.parts["definition/tables/Sales.tmdl"]
    assert b"table Metadata" in final.parts["definition/tables/Metadata.tmdl"]
    assert final.table_names == ("Sales", "Metadata")
    spec, transported = transported_contribution(repository)
    assert spec["table_order"] == ["Sales", "Metadata"]
    assert list(transported.parts) == sorted(transported.parts)
    assert transported.table_names == final.table_names
    assert transported.signature == spec["signature"] == final.signature
    assert replace(final, table_order=("Sales",)).signature != final.signature
    # Authored offline readback input isolates projection from service behaviour.
    rows = project_semantic_model(
        ITEM,
        transported,
        deployed={"model": {"tables": [{"name": "Metadata"}, {"name": "Sales"}]}},
    )
    assert {
        r["table_name"]: r["table_ordinal"] for r in rows["SemanticModelTable"]
    } == {
        "Sales": 1,
        "Metadata": 2,
    }
    assert final.source_references == initial.source_references == {"Sales": REFERENCE}
    assert final.source_bindings == transported.source_bindings
    assert final.dependencies == initial.dependencies == (("Sales", REFERENCE),)
    (edge,) = rows["Dependency"]
    assert edge["referencing_object_name"] == "Sales"
    assert edge["dependency_reference"] == REFERENCE
    assert edge["referenced_item_type"] == "Warehouse"
    assert edge["referenced_item_name"] == "Serving"
    assert edge["referenced_schema_name"] == "Cake"
    assert edge["referenced_object_name"] == "Sales"
    sales = next(r for r in rows["SemanticModelTable"] if r["table_name"] == "Sales")
    assert sales["source_mode"] == "directLake"
    assert sales["source_access"] == "sql"
    assert final.source_bindings["Sales"]["description"] == "Sales facts"
    assert b"/// Sales facts" in final.parts["definition/tables/Sales.tmdl"]
    assert b"/// Sales key" in final.parts["definition/tables/Sales.tmdl"]


@weaver_test()
def test_source_bound_annotation_removal_keeps_surviving_declaration_order(tmp_path):
    initial, repository = source_bound_annotation(
        tmp_path,
        '        if "Middle" in target.tables:\n'
        '            target.tables["Middle"].remove()\n',
        tables="\ntable Zebra\n\ntable Middle\n\ntable Alpha\n",
    )
    assert initial.table_names == ("Sales", "Zebra", "Middle", "Alpha")
    final = repository.semantic_models[ITEM]
    assert final.table_names == ("Sales", "Zebra", "Alpha")
    assert not any(b"table Middle" in part for part in final.parts.values())
    spec, transported = transported_contribution(repository)
    assert spec["table_order"] == ["Sales", "Zebra", "Alpha"]
    assert transported.signature == final.signature
    assert transported.source_references == final.source_references
    assert transported.source_bindings == final.source_bindings
    rows = project_semantic_model(
        ITEM,
        transported,
        deployed={
            "model": {
                "tables": [{"name": name} for name in ("Alpha", "Zebra", "Sales")]
            }
        },
    )
    assert {
        r["table_name"]: r["table_ordinal"] for r in rows["SemanticModelTable"]
    } == {
        "Sales": 1,
        "Zebra": 2,
        "Alpha": 3,
    }
    assert final.dependencies == initial.dependencies
