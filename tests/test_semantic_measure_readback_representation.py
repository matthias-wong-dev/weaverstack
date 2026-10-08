"""Known measure-table readback types do not require invalid native declarations."""

from copy import deepcopy

import pytest
from support.weaver_test import weaver_test
from test_semantic_annotation_declaration import ITEM
from test_semantic_annotation_phase_cycle import parse, project

from weaver.errors import InstallError
from weaver.semantic_models.annotation import apply_annotations
from weaver.semantic_models.builtin_annotations import MEASURE_TABLE_COLUMNS
from weaver.semantic_models.deployed import verify_requested


def compiled_measure_table(tmp_path):
    root = project(
        tmp_path,
        "table Metric\n\tannotation Weaver.MeasureTable = true\n\tmeasure Base = 1\n",
    )
    return apply_annotations(parse(root).semantic_models[ITEM])


@weaver_test()
def test_measure_table_generated_types_are_exact_readback_expectations(tmp_path):
    compiled = compiled_measure_table(tmp_path)
    table = next(t for t in compiled.requested["tables"] if t["name"] == "Metric")
    assert {c["name"] for c in table["columns"]} == {
        name for name, _ in MEASURE_TABLE_COLUMNS
    }
    assert all(
        c["type"] == "calculatedTableColumn" and c["dataType"] == "string"
        for c in table["columns"]
    )
    native = b"\n".join(compiled.parts.values())
    assert b"dataType:" not in native and b"type: calculatedTableColumn" not in native
    verify_requested(
        compiled.requested,
        {"model": deepcopy(compiled.requested)},
        owned=compiled.owned,
    )


@pytest.mark.parametrize("field,value", [("type", "data"), ("dataType", "int64")])
@weaver_test()
def test_wrong_measure_table_service_type_is_refused(tmp_path, field, value):
    compiled = compiled_measure_table(tmp_path)
    actual = {"model": deepcopy(compiled.requested)}
    next(t for t in actual["model"]["tables"] if t["name"] == "Metric")["columns"][0][
        field
    ] = value
    with pytest.raises(InstallError, match=field):
        verify_requested(compiled.requested, actual, owned=compiled.owned)
