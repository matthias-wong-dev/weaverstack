"""Native annotation transformations through public Build, readback and Load."""

import json
import shutil

import pytest
from support.weaver_test import weaver_test
from test_semantic_model_public_cycle import (
    ITEM,
    PBIP,
    SCOPE,
)
from test_semantic_model_public_cycle import (
    semantic_build_context as semantic_build_context,
)

import weaver
from weaver.catalogue.reader import read_table
from weaver.catalogue.tables import (
    DEPENDENCY,
    LOAD_STATUS,
    LOG,
    SEMANTIC_MODEL,
    SEMANTIC_MODEL_COLUMN,
    SEMANTIC_MODEL_MEASURE,
    SEMANTIC_MODEL_TABLE,
    TABLE_DICTIONARY,
)
from weaver.semantic_models.definition import decode_model

SOURCE = "Warehouse/_weaver/_.TableDictionary"
SOURCE_TEXT = """model Model
    annotation Weaver.AutoHideColumns = "Schema*"
    annotation Weaver.AutoHideForeignKeys = true

table Objects
    annotation Weaver.Source = Warehouse/_weaver/_.TableDictionary

    column 'Object type'
        annotation Weaver.Exclude = true

    measure Rows = COUNTROWS(Objects)
        formatString: #,##0

    measure One = 1
        formatString: 0

table Reference
    annotation Weaver.Source = Warehouse/_weaver/_.TableDictionary

relationship ObjectNames
    fromColumn: Objects.'Item name'
    toColumn: Reference.'Item name'
    fromCardinality: many
    toCardinality: many
    crossFilteringBehavior: bothDirections

table Metric
    isHidden
    annotation Weaver.MeasureTable = true

    measure Value
        annotation Weaver.Switch = ```
            Objects[Rows]
            Objects[One]
            ```

table Obsolete
    annotation Weaver.Exclude = true
    measure Old = 1
"""
METRIC_TEXT = """table Metric
    isHidden
    annotation Weaver.MeasureTable = true

    measure Value
        annotation Weaver.Switch = Sales[Revenue]
"""
OVERLAY_TEXT = """model Model
    annotation Weaver.AutoHideColumns = "Amount"
    annotation Weaver.AutoHideForeignKeys = true

ref table Sales
    column Id
        annotation Weaver.Exclude = true
"""


def _project(folder, form):
    folder.mkdir(parents=True)
    if form == "source-extension":
        target = folder / "extension.tmdl"
        target.write_text(SOURCE_TEXT, encoding="utf-8")
        return target
    shutil.copytree(PBIP, folder, dirs_exist_ok=True)
    definition = folder / "Probe.SemanticModel/definition"
    if form == "pbip":
        model = definition / "model.tmdl"
        model.write_text(
            model.read_text(encoding="utf-8").replace(
                "model Model\n",
                "model Model\n\tannotation Weaver.AutoHideColumns = Amount\n\tannotation Weaver.AutoHideForeignKeys = true\n",
                1,
            ),
            encoding="utf-8",
        )
        sales = definition / "tables/Sales.tmdl"
        sales.write_text(
            sales.read_text(encoding="utf-8").replace(
                "column Id\n", "column Id\n\t\tannotation Weaver.Exclude = true\n", 1
            ),
            encoding="utf-8",
        )
        (definition / "tables/Metric.tmdl").write_text(METRIC_TEXT, encoding="utf-8")
        return model
    (folder.parent / "extension.tmdl").write_text(
        'model Model\n\tannotation Weaver.AutoHideColumns = "Product*"\n',
        encoding="utf-8",
    )
    target = folder / "extension.tmdl"
    target.write_text(OVERLAY_TEXT + "\n" + METRIC_TEXT, encoding="utf-8")
    return target


@weaver_test(remote=True, resources={"rest", "tds"})
@pytest.mark.parametrize("form", ["source-extension", "pbip", "overlays"])
def test_public_annotation_build_readback_load_and_fixed_point(
    semantic_build_context, tmp_path, form
):
    context = semantic_build_context
    folder = tmp_path / "project" / str(ITEM)
    edit_target = _project(folder, form)
    project = folder.parent.parent
    before = {p: p.read_bytes() for p in project.rglob("*") if p.is_file()}
    selection = f"{ITEM}=SemanticModel/{context.target}"
    built = weaver.build(project, items=selection, session=context.session)
    assert built.succeeded, built.errors
    actual = decode_model(context.model.get_definition())["model"]
    tables = {t["name"]: t for t in actual["tables"]}
    assert "Metric" in tables and tables["Metric"].get("isHidden") is True
    metric = tables["Metric"]
    expression = metric["partitions"][0]["source"]["expression"]
    assert (
        "\n".join(expression) if isinstance(expression, list) else expression
    ).strip() == "INFO.VIEW.MEASURES()"
    value = next(m for m in metric["measures"] if m["name"] == "Value")
    rendered_value = value["expression"]
    rendered_format = value["formatStringDefinition"]["expression"]
    assert "SELECTEDVALUE('Metric'[Name])" in (
        "\n".join(rendered_value)
        if isinstance(rendered_value, list)
        else rendered_value
    )
    assert "SWITCH(" in (
        "\n".join(rendered_format)
        if isinstance(rendered_format, list)
        else rendered_format
    )
    assert any(a["name"] == "Weaver.Switch" for a in value["annotations"])
    assert any(a["name"] == "Weaver.MeasureTable" for a in metric["annotations"])
    assert {a["name"] for a in actual["annotations"]} >= {
        "Weaver.AutoHideColumns",
        "Weaver.AutoHideForeignKeys",
    }
    if form == "source-extension":
        assert "Obsolete" not in tables
        assert "Object type" not in {c["name"] for c in tables["Objects"]["columns"]}
        for name in ("Objects", "Reference"):
            columns = {c["name"]: c for c in tables[name]["columns"]}
            assert columns["Schema name"]["isHidden"] is True
            assert columns["Item name"]["isHidden"] is True
            assert tables[name]["description"] == TABLE_DICTIONARY.description
            assert any(c.get("description") for c in columns.values())
            assert any(
                a["name"] == "Weaver.Source" for a in tables[name]["annotations"]
            )
        assert len(actual["expressions"]) == 1
        dependencies = read_table(
            context.connection, DEPENDENCY, predicate=SCOPE.predicate
        )
        assert {
            (r["referencing_object_name"], r["dependency_reference"])
            for r in dependencies
            if r["dependency_reference"] == SOURCE
        } == {("Objects", SOURCE), ("Reference", SOURCE)}
    else:
        sales = {c["name"]: c for c in tables["Sales"]["columns"]}
        assert "Id" not in sales
        assert sales["Amount"]["isHidden"] is True
        assert sales["ProductId"]["isHidden"] is True
    for table in (
        SEMANTIC_MODEL,
        SEMANTIC_MODEL_TABLE,
        SEMANTIC_MODEL_COLUMN,
        SEMANTIC_MODEL_MEASURE,
    ):
        assert read_table(context.connection, table, predicate=SCOPE.predicate)
    loaded = weaver.load(str(ITEM), session=context.session)
    assert loaded.succeeded, loaded.to_mapping()
    assert len(loaded.nodes) == 1 and loaded.nodes[0].result.status == "Completed"
    request_id = loaded.nodes[0].result.request_id
    assert (
        request_id
        and loaded.nodes[0].result.start_time
        and loaded.nodes[0].result.end_time
    )
    assert any(
        request_id in row["details"] for row in read_table(context.connection, LOG)
    )
    status = read_table(context.connection, LOAD_STATUS, predicate=SCOPE.predicate)
    assert len(status) == 1 and status[0]["result"] == "succeeded"
    repeated = weaver.build(project, items=selection, session=context.session)
    assert (
        repeated.succeeded
        and repeated.installation_report.action_counts()["total"] == 0
    )
    assert (
        read_table(context.connection, LOAD_STATUS, predicate=SCOPE.predicate) == status
    )
    assert {p: p.read_bytes() for p in before} == before
    edit_target.write_text(
        edit_target.read_text(encoding="utf-8").replace(
            "model Model", "/// Annotation lifecycle change\nmodel Model", 1
        ),
        encoding="utf-8",
    )
    changed = weaver.build(project, items=selection, session=context.session)
    assert (
        changed.succeeded and changed.installation_report.action_counts()["total"] > 0
    ), changed.errors
    assert (
        read_table(context.connection, LOAD_STATUS, predicate=SCOPE.predicate)[0][
            "result"
        ]
        == "pending"
    )
    assert weaver.load(str(ITEM), session=context.session).succeeded
    final = weaver.build(project, items=selection, session=context.session)
    assert final.succeeded and final.installation_report.action_counts()["total"] == 0
    assert not {"livy", "onelake"} & {
        e.resource for e in context.session.telemetry.events()
    }
    print(
        json.dumps(
            {
                "annotation_lifecycle": {
                    "form": form,
                    "build": True,
                    "readback": True,
                    "load": True,
                    "unchanged_zero_actions": True,
                    "changed_pending": True,
                    "reload": True,
                    "final_zero_actions": True,
                    "no_livy_or_onelake": True,
                }
            },
            default=str,
        )
    )
