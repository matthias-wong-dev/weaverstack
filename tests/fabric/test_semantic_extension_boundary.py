"""Partial native TMDL produces a deployable model through the REST boundary."""

import json

from support.weaver_test import weaver_test
from test_semantic_extension_representation import ITEM, ORG, base_parts
from test_semantic_model_boundary import (
    restored_semantic_model as restored_semantic_model,
)

from weaver.semantic_models.definition import decode_model, encode_parts
from weaver.semantic_models.deployed import verify_requested
from weaver.semantic_models.extensions import merge_extensions


@weaver_test(remote=True, resources={"rest"})
def test_native_extension_merge_deploys_and_reads_back(restored_semantic_model):
    model = restored_semantic_model
    before = base_parts()
    layers = [(ORG, "SemanticModel/extension.tmdl")]
    for local in (False, True):
        if local:
            layers.append((ITEM, "Sales/extension.tmdl"))
        merged = merge_extensions(before, layers)
        touched = {
            "definition/model.tmdl",
            "definition/tables/Sales.tmdl",
            "definition/expressions.tmdl",
        }
        assert {p: merged.parts[p] for p in before if p not in touched} == {
            p: v for p, v in before.items() if p not in touched
        }
        model.update_definition(
            encode_parts(merged.parts), allow_purge_data=True, timeout=300
        )
        observed = decode_model(model.get_definition())
        verify_requested(merged.requested, observed)
        native = observed["model"]
        assert native.get("discourageImplicitMeasures", False) is not local
        tables = {table["name"]: table for table in native["tables"]}
        assert set(tables) == {"Sales", "Product", "Helper"}
        sales = tables["Sales"]
        assert sales.get("isHidden", False) is not local
        product_id = next(c for c in sales["columns"] if c["name"] == "ProductId")
        assert product_id.get("isHidden", False) is local
        assert (
            next(m for m in sales["measures"] if m["name"] == "Double Revenue")[
                "expression"
            ]
            == "[Revenue] * 2"
        )
        assert {c["name"] for c in tables["Helper"]["columns"]} == {"Value"}
        reporting = next(p for p in native["perspectives"] if p["name"] == "Reporting")
        assert {t["name"] for t in reporting["tables"]} == {"Sales"}
        assert native["relationships"][0]["fromTable"] == "Sales"
        source = next(e for e in native["expressions"] if e["name"] == "DataSource1")
        if local:
            assert source["expression"] == "2"
            assert sales["description"] == "Reporting sales."
        print(
            json.dumps(
                {
                    "native_extension_probe": {
                        "layer": "item" if local else "organisation",
                        "readback_verified": True,
                        "perspective": reporting,
                        "tables": sorted(tables),
                        "unrelated_bytes_preserved": True,
                    }
                }
            )
        )
    refreshed = model.refresh(timeout=300)
    assert refreshed["status"] == "Completed" and refreshed["request_id"]
    assert base_parts() == before
    print(json.dumps({"native_extension_probe_refresh": refreshed}, default=str))
