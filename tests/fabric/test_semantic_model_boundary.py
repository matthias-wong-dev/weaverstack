"""Fixed-item semantic definition, refresh and DAX through the desktop Session."""

import hashlib
import json
import time
from pathlib import Path

import pytest
from support.weaver_test import weaver_test

from weaver.fabric.client import FabricError
from weaver.operations.doctor import doctor
from weaver.semantic_models.definition import decode_model, encode_parts
from weaver.semantic_models.extensions import apply_extensions
from weaver.semantic_models.source import SemanticContribution

FIXTURE = Path(__file__).parents[1] / "fixtures" / "semantic_model" / "Probe"


def _source_hashes():
    return {
        path.relative_to(FIXTURE).as_posix(): hashlib.sha256(
            path.read_bytes()
        ).hexdigest()
        for path in sorted(FIXTURE.rglob("*"))
        if path.is_file()
    }


def _settle_refreshes(model):
    deadline = time.monotonic() + 600
    while time.monotonic() < deadline:
        history = model.power_bi.get_json(f"{model.dataset_path}/refreshes?$top=1")
        entries = history["value"]
        if not entries or entries[0]["status"] in {
            "Completed",
            "Failed",
            "Cancelled",
            "Disabled",
        }:
            return
        time.sleep(2)
    pytest.fail(
        "Semantic model refresh is unsettled; definition restoration was not attempted"
    )


@pytest.fixture
def restored_semantic_model(fixed_semantic_model, tmp_path):
    model = fixed_semantic_model
    _settle_refreshes(model)
    original = model.get_definition()
    backup = tmp_path / "original-definition.json"
    backup.write_text(json.dumps(original), encoding="utf-8")
    print(f"Semantic model definition backup: {backup}")
    try:
        yield model
    finally:
        _settle_refreshes(model)
        model.update_definition(original, allow_purge_data=True, timeout=300)
        restored = decode_model(model.get_definition())
        assert restored == decode_model(original), (
            "Original semantic model definition was not restored"
        )
        print(
            f"Restored semantic model {model.model_id}; definition matches the backup"
        )


@weaver_test(remote=True, resources={"rest"})
def test_pbip_definition_mutation_refresh_and_dax_round_trip(restored_semantic_model):
    model = restored_semantic_model
    source_hashes = _source_hashes()
    folder = FIXTURE / "Probe.SemanticModel"
    parts = {"definition.pbism": (folder / "definition.pbism").read_bytes()}
    parts.update(
        {
            p.relative_to(folder).as_posix(): p.read_bytes()
            for p in (folder / "definition").rglob("*.tmdl")
        }
    )
    model.update_definition(encode_parts(parts), allow_purge_data=True, timeout=300)
    deployed = decode_model(model.get_definition())
    assert deployed["compatibilityLevel"] == 1606
    tables = {table["name"]: table for table in deployed["model"]["tables"]}
    assert set(tables) == {"Sales", "Product"}
    assert tables["Sales"]["measures"][0]["expression"] == "SUM(Sales[Amount])"
    relationship = deployed["model"]["relationships"][0]
    assert (
        relationship["fromTable"],
        relationship["fromColumn"],
        relationship["toTable"],
        relationship["toColumn"],
    ) == (
        "Sales",
        "ProductId",
        "Product",
        "ProductId",
    )
    refreshed = model.refresh(timeout=300)
    assert refreshed["status"] == "Completed" and refreshed["request_id"]
    assert model.query_dax(
        'EVALUATE ROW("Revenue", [Revenue], "SalesRows", COUNTROWS(Sales), "Products", COUNTROWS(Product))'
    ) == [{"[Revenue]": 20, "[SalesRows]": 2, "[Products]": 2}]
    assert model.query_dax(
        'EVALUATE ROW("FilteredRevenue", CALCULATE([Revenue], Product[ProductId] = 10), "ReverseFilterProducts", CALCULATE(COUNTROWS(Product), Sales[Id] = 1))'
    ) == [{"[FilteredRevenue]": 12.5, "[ReverseFilterProducts]": 2}]

    mutated = apply_extensions(
        SemanticContribution(parts, {}, {}),
        "Probe",
        [
            (
                b"model Model\n\tdiscourageImplicitMeasures\n\ntable _Measure\n\tpartition _Measure = calculated\n\t\tsource = INFO.VIEW.MEASURES()\n",
                "Reporting.tmdl",
            )
        ],
    )
    model.update_definition(
        encode_parts(mutated.parts), allow_purge_data=True, timeout=300
    )
    deployed = decode_model(model.get_definition())
    assert deployed["model"]["discourageImplicitMeasures"] is True
    measure_table = next(
        table for table in deployed["model"]["tables"] if table["name"] == "_Measure"
    )
    assert {"Name", "Table", "Expression"} <= {
        column["name"] for column in measure_table["columns"]
    }
    refreshed = model.refresh(timeout=300)
    assert refreshed["status"] == "Completed"
    assert model.query_dax(
        'EVALUATE ROW("Revenue", [Revenue], "Measures", COUNTROWS(\'_Measure\'))'
    ) == [{"[Revenue]": 20, "[Measures]": 1}]
    assert _source_hashes() == source_hashes


@weaver_test(remote=True, resources={"rest"})
def test_focused_doctor_uses_only_rest(
    semantic_model_session, fixed_semantic_model_name
):
    report = doctor(
        workspace=semantic_model_session.workspace.workspace,
        semantic_model=fixed_semantic_model_name,
        session=semantic_model_session,
    )
    assert report.succeeded, report.to_mapping()
    assert all(check.passed for check in report.checks), report.to_mapping()
    names = {check.name for check in report.checks}
    assert {
        "Power BI authentication",
        "Semantic model definition",
        "Semantic model DAX",
    } <= names
    assert "Semantic model refresh" not in names
    assert not {"OneLake", "Warehouse TDS", "Fabric Spark / Livy"} & names
    print(json.dumps(report.to_mapping(), indent=2))


@weaver_test(remote=True, resources={"rest"})
def test_dax_engine_error_is_not_a_successful_empty_result(fixed_semantic_model):
    assert fixed_semantic_model.query_dax('EVALUATE ROW("Value", 1)') == [
        {"[Value]": 1}
    ]
    with pytest.raises(FabricError) as rejected:
        fixed_semantic_model.query_dax("EVALUATE weavertest_absent_semantic_table")
    assert rejected.value.status_code == 400
    print(f"Expected DAX rejection: {rejected.value}")
