"""Native annotation transformations through public Build, readback and Load."""

import json

import pytest
from support.semantic_projects import ITEM, ROOT, SCOPE, annotation_project
from support.weaver_test import weaver_test
from test_semantic_model_public_cycle import (
    scratch_build_context as scratch_build_context,
)

import weaver
from weaver.catalogue.reader import read_table
from weaver.catalogue.render import InstallationScope
from weaver.catalogue.tables import (
    COLUMN_DICTIONARY,
    DEPENDENCY,
    LOAD_STATUS,
    LOG,
    SEMANTIC_MODEL,
    SEMANTIC_MODEL_COLUMN,
    SEMANTIC_MODEL_MEASURE,
    SEMANTIC_MODEL_TABLE,
    TABLE_DICTIONARY,
)
from weaver.semantic_models.builtin_annotations import MEASURE_TABLE_SOURCE
from weaver.semantic_models.definition import decode_model

SOURCE = "Warehouse/_weaver/_.TableDictionary"


@weaver_test(remote=True, resources={"rest", "tds"})
@pytest.mark.parametrize("form", ["source-extension", "pbip", "overlays"])
def test_public_annotation_build_readback_load_and_fixed_point(
    scratch_build_context, tmp_path, form
):
    context = scratch_build_context
    folder = tmp_path / "project" / str(ITEM)
    annotation_project(folder, form)
    edit_target = (
        folder / "Probe.SemanticModel/definition/model.tmdl"
        if form == "pbip"
        else folder / f"{folder.name}.tmdl"
    )
    project = folder.parent.parent
    before = {p: p.read_bytes() for p in project.rglob("*") if p.is_file()}
    selection = f"{ITEM}=SemanticModel/{context.target}"
    source_description = None
    source_column_notes = {}
    if form == "source-extension":
        source_scope = InstallationScope("Warehouse", "_weaver")
        source_predicate = "[Schema name] = N'_' AND [Object name] = N'TableDictionary'"
        (source_row,) = read_table(
            context.connection,
            TABLE_DICTIONARY,
            scope=source_scope,
            predicate=source_predicate,
        )
        source_description = source_row["description"]
        source_column_notes = {
            row["column_name"]: row["description"]
            for row in read_table(
                context.connection,
                COLUMN_DICTIONARY,
                scope=source_scope,
                predicate=source_predicate,
            )
            if row["description"]
        }
        assert source_description and source_column_notes
    built = weaver.build(project, items=selection, session=context.session)
    assert built.succeeded, built.errors
    actual = decode_model(context.model.get_definition())["model"]
    tables = {t["name"]: t for t in actual["tables"]}
    assert "Metric" in tables and tables["Metric"].get("isHidden") is True
    metric = tables["Metric"]
    expression = metric["partitions"][0]["source"]["expression"]
    assert (
        "\n".join(expression) if isinstance(expression, list) else expression
    ).strip() == MEASURE_TABLE_SOURCE
    value = next(m for m in metric["measures"] if m["name"] == "Value")
    rendered_value = value["expression"]
    rendered_format = value["formatStringDefinition"]["expression"]
    assert "SELECTEDVALUE('Metric'[Measure name])" in (
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
            assert tables[name]["description"] == source_description
            propagated = {
                key: note for key, note in source_column_notes.items() if key in columns
            }
            assert propagated
            assert {
                key: columns[key].get("description") for key in propagated
            } == propagated
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
    loaded = context.load(str(ITEM), session=context.session)
    assert loaded.succeeded, loaded.to_mapping()
    assert len(loaded.nodes) == 1 and loaded.nodes[0].result.status == "Completed"
    request_id = loaded.nodes[0].result.request_id
    assert (
        request_id
        and loaded.nodes[0].result.start_time
        and loaded.nodes[0].result.end_time
    )
    assert any(
        request_id in (row["details"] or "")
        for row in read_table(context.connection, LOG)
    )
    status = read_table(context.connection, LOAD_STATUS, predicate=SCOPE.predicate)
    assert len(status) == 1 and status[0]["result"] == "succeeded"
    (definition,) = read_table(context.connection, SEMANTIC_MODEL, scope=SCOPE)
    repeated = weaver.build(project, items=selection, session=context.session)
    assert repeated.succeeded, repeated.errors
    assert repeated.selection.selected_for_build == (ROOT,)
    assert not repeated.selection.impact.changed
    assert repeated.installation_report.action_counts()["total"] > 0
    assert (
        read_table(context.connection, SEMANTIC_MODEL, scope=SCOPE)[0]["signature"]
        == definition["signature"]
    )
    assert (
        read_table(context.connection, LOAD_STATUS, predicate=SCOPE.predicate)[0][
            "result"
        ]
        == "pending"
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
    assert context.load(str(ITEM), session=context.session).succeeded
    final = weaver.build(project, items=selection, session=context.session)
    assert final.succeeded, final.errors
    assert final.selection.selected_for_build == (ROOT,)
    assert not final.selection.impact.changed
    assert final.installation_report.action_counts()["total"] > 0
    assert (
        read_table(context.connection, LOAD_STATUS, scope=SCOPE)[0]["result"]
        == "pending"
    )
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
                    "unchanged_eager_stable_signature": True,
                    "changed_pending": True,
                    "reload": True,
                    "final_eager_pending_refresh": True,
                    "no_livy_or_onelake": True,
                }
            },
            default=str,
        )
    )
