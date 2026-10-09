from dataclasses import replace

from support.weaver_test import weaver_test
from test_powerbi_project_declaration import parse, write

from weaver.catalogue.semantic import project_semantic_model
from weaver.declaration.model import WeaverItemId
from weaver.semantic_models.binding import bind_semantic_sources

ITEM = WeaverItemId("SemanticModel", "Executive")
REFERENCE = "Warehouse/Serving/Cake.Sales"


@weaver_test()
def test_composed_source_final_annotations_metadata_and_order_have_one_owner(tmp_path):
    write(
        tmp_path,
        "PowerBI/Sales/Normal.tmdl",
        "model Model\n\tannotation ACME.Final = true\n\ntable Sales\n\tannotation Weaver.Source = "
        + REFERENCE
        + "\n\ntable Zebra\n\ntable Alpha\n",
    )
    write(tmp_path, "PowerBI/policy.tmdl", "model Model\n\tculture: en-AU\n")
    write(
        tmp_path,
        "PowerBI/Sales/Executive.tmdl",
        "model Model\n\tannotation Weaver.BaseSemanticModels = Normal\n\ntable Middle\n",
    )
    write(
        tmp_path,
        "PowerBI/annotations/ACME__Final.py",
        "from weaver.semantic_models import Annotation\nclass ACME__Final(Annotation):\n    scopes = {'model'}\n    def apply(self, target):\n        assert set(t.name for t in target.tables) == {'Sales', 'Zebra', 'Alpha', 'Middle'}\n        assert 'Id' in target.tables['Sales'].columns\n        target.tables.add('Metadata').partitions.add('Metadata', 'calculated', mode='import').set_expression('source', '{1}')\n",
    )
    repository = parse(tmp_path)
    prepared = repository.semantic_models[ITEM]
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
    final = bound.semantic_models[ITEM]
    assert final.table_names == ("Sales", "Zebra", "Alpha", "Middle", "Metadata")
    assert b"column 'Id'" in final.parts["definition/tables/Sales.tmdl"]
    assert b"/// Sales facts" in final.parts["definition/tables/Sales.tmdl"]
    assert b"/// Sales key" in final.parts["definition/tables/Sales.tmdl"]
    assert final.source_references == prepared.source_references == {"Sales": REFERENCE}
    assert not bound.semantic_models[
        WeaverItemId("SemanticModel", "Normal")
    ].source_bindings
    assert final.signature != prepared.signature
    edited_metadata = {**source, "description": "Changed facts"}
    changed = bind_semantic_sources(
        repository, {REFERENCE: edited_metadata}, {ITEM: None}
    ).semantic_models[ITEM]
    assert changed.signature != final.signature
    rows = project_semantic_model(
        ITEM,
        final,
        deployed={"model": {"tables": [{"name": n} for n in final.table_names]}},
    )
    assert {
        r["table_name"]: r["table_ordinal"] for r in rows["SemanticModelTable"]
    } == {n: i for i, n in enumerate(final.table_names, 1)}
    assert len(rows["Dependency"]) == 1
    assert (
        len([r for r in rows["Registry"] if r["object_role"] == "source"]) == 4
    )  # base, policy, target and implementation
    assert (
        replace(final, table_order=tuple(reversed(final.table_names))).signature
        != final.signature
    )
    import json
    from types import SimpleNamespace

    from weaver.build_bundle.semantic import semantic_stage
    from weaver.semantic_models.definition import decode_parts

    stage = semantic_stage(
        bound, ITEM, SimpleNamespace(id="model", kind="semanticmodel")
    )
    (payload,) = stage.payloads.values()
    spec = json.loads(payload)
    assert spec["table_order"] == list(final.table_names)
    assert spec["signature"] == final.signature
    assert decode_parts(spec["definition"]) == final.parts
