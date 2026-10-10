"""Fixed-item semantic definition, refresh and DAX through the desktop Session."""

import json
import time

import pytest
from support.semantic_fixture_source import ConfiguredSemanticSource
from support.weaver_test import weaver_test

from weaver.fabric.client import FabricError
from weaver.operations.doctor import doctor


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
def restored_semantic_model(fixed_semantic_model, semantic_model_session, tmp_path):
    model = fixed_semantic_model
    _settle_refreshes(model)
    source = ConfiguredSemanticSource.capture(model)
    original = source.original
    backup = tmp_path / "original-definition.json"
    backup.write_text(json.dumps(original), encoding="utf-8")
    print(f"Semantic model definition backup: {backup}")
    source.attach(semantic_model_session)
    try:
        yield model
    finally:
        source.detach(semantic_model_session)
        source.restore(_settle_refreshes)
        print("SEMANTIC_SOURCE_EVIDENCE " + json.dumps(source.evidence))


@pytest.fixture
def scratch_model(scratch_semantic_model):
    """The scratch model, settled before and after a test reshapes it."""

    _settle_refreshes(scratch_semantic_model)
    yield scratch_semantic_model
    _settle_refreshes(scratch_semantic_model)


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
