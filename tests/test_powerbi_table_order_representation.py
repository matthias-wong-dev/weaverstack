from support.weaver_test import weaver_test
from test_powerbi_project_declaration import native, parse, write

from weaver.catalogue.semantic import project_semantic_model
from weaver.declaration.model import WeaverItemId


@weaver_test()
def test_effective_table_order_survives_projection_sort_and_observed_order(tmp_path):
    native(tmp_path)
    prefix = "PowerBI/Sales/Revenue.SemanticModel/definition/"
    write(
        tmp_path,
        prefix + "model.tmdl",
        "model Model\n\tref table Zebra\n\tref table Alpha\n",
    )
    write(tmp_path, prefix + "tables/Alpha.tmdl", "table Alpha\n")
    write(tmp_path, prefix + "tables/Zebra.tmdl", "table Zebra\n")
    write(tmp_path, "PowerBI/policy.tmdl", "table Policy\n")
    write(tmp_path, "PowerBI/Sales/Revenue.tmdl", "table Named\n")
    item = WeaverItemId("SemanticModel", "Revenue")
    contribution = parse(tmp_path).semantic_models[item]
    observed = {
        "model": {
            "tables": [{"name": n} for n in ["Alpha", "Named", "Policy", "Zebra"]]
        }
    }
    rows = project_semantic_model(item, contribution, deployed=observed)[
        "SemanticModelTable"
    ]
    assert [r["table_name"] for r in rows] == ["Alpha", "Named", "Policy", "Zebra"]
    assert {r["table_name"]: r["table_ordinal"] for r in rows} == {
        "Zebra": 1,
        "Alpha": 2,
        "Policy": 3,
        "Named": 4,
    }
    write(
        tmp_path,
        prefix + "model.tmdl",
        "model Model\n\tref table Alpha\n\tref table Zebra\n",
    )
    reordered = parse(tmp_path).semantic_models[item]
    assert reordered.signature != contribution.signature
    assert reordered.table_names == ("Alpha", "Zebra", "Policy", "Named")


@weaver_test()
def test_final_annotations_and_sorted_part_codec_preserve_effective_order(tmp_path):
    from dataclasses import replace

    write(
        tmp_path,
        "PowerBI/Sales/Revenue.tmdl",
        "model Model\n\tannotation ACME.FinalTables = true\n\ntable Zebra\n\ntable Alpha\n",
    )
    write(
        tmp_path,
        "SemanticModel/annotations/ACME__FinalTables.py",
        'from weaver.semantic_models import Annotation\nclass ACME__FinalTables(Annotation):\n    scopes = {"model"}\n    def apply(self, target):\n        target.tables["Alpha"].remove()\n        target.tables.add("Last")\n',
    )
    item = WeaverItemId("SemanticModel", "Revenue")
    contribution = parse(tmp_path).semantic_models[item]
    assert contribution.table_order == ("Zebra", "Last")
    transported = replace(contribution, parts=dict(sorted(contribution.parts.items())))
    assert transported.table_names == ("Zebra", "Last")
    assert transported.signature == contribution.signature
    rows = project_semantic_model(
        item,
        transported,
        deployed={"model": {"tables": [{"name": "Last"}, {"name": "Zebra"}]}},
    )["SemanticModelTable"]
    assert {r["table_name"]: r["table_ordinal"] for r in rows} == {
        "Zebra": 1,
        "Last": 2,
    }


@weaver_test()
def test_extension_only_reordering_changes_effective_signature(tmp_path):
    path = "PowerBI/Sales/Revenue.tmdl"
    write(tmp_path, path, "table Zebra\n\ntable Alpha\n")
    item = WeaverItemId("SemanticModel", "Revenue")
    before = parse(tmp_path).semantic_models[item]
    write(tmp_path, path, "table Alpha\n\ntable Zebra\n")
    after = parse(tmp_path).semantic_models[item]
    assert before.table_names == ("Zebra", "Alpha")
    assert after.table_names == ("Alpha", "Zebra")
    assert before.signature != after.signature
    assert after.signature == parse(tmp_path).semantic_models[item].signature


@weaver_test()
def test_semantic_metadata_uses_real_keys_and_whole_model_claims(tmp_path):
    from weaver.catalogue.claims import (
        CatalogueClaim,
        claim_rules_for_object_type,
        without_claims,
    )
    from weaver.catalogue.state import Catalogue
    from weaver.catalogue.tables import (
        DEPENDENCY,
        INSTALLATION,
        REGISTRY,
        SEMANTIC_TABLES,
    )
    from weaver.declaration.model import WeaverDocumentId

    native(tmp_path)
    write(
        tmp_path, "PowerBI/Sales/Revenue.tmdl", "table Calendar\n\tmeasure Answer = 1\n"
    )
    item = WeaverItemId("SemanticModel", "Revenue")
    contribution = parse(tmp_path).semantic_models[item]
    projection = project_semantic_model(
        item,
        contribution,
        deployed={
            "model": {
                "tables": [
                    {
                        "name": "Calendar",
                        "columns": [{"name": "Year"}],
                        "measures": [{"name": "Answer"}],
                    }
                ],
                "relationships": [{"name": "YearLink"}],
            }
        },
    )
    for table in SEMANTIC_TABLES:
        assert "schema_name" not in table.column_names
        assert "object_name" not in table.column_names
        assert table.key[:2] == ("item_type", "item_name")
        assert projection[table.name]
        assert all(
            "schema_name" not in r and "object_name" not in r
            for r in projection[table.name]
        )
    assert {"schema_name", "object_name"} <= set(REGISTRY.column_names)
    assert {"referencing_schema_name", "referencing_object_name"} <= set(
        DEPENDENCY.column_names
    )
    assert "item_id" in INSTALLATION.column_names
    other = WeaverItemId("SemanticModel", "Other")
    catalogue = Catalogue(
        {
            item: projection,
            other: {
                name: tuple({**r, "item_name": "Other"} for r in rows)
                for name, rows in projection.items()
            },
        }
    )
    claims = [
        CatalogueClaim(WeaverDocumentId.model_root(item), rule)
        for rule in claim_rules_for_object_type("semantic_model")
    ]
    pruned = without_claims(catalogue, claims)
    assert all(not pruned.rows[item][table.name] for table in SEMANTIC_TABLES)
    assert pruned.rows[other] == catalogue.rows[other]
