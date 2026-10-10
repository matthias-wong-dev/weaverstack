"""A generated Direct Lake model through Build, Load, DAX and both wipes."""

import hashlib
import json
from contextlib import contextmanager

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
    CURRENT_STATE_TABLES,
    DEPENDENCY,
    INSTALLATION,
    LOAD_STATUS,
    LOG,
    PROJECTED_TABLES,
    SEMANTIC_MODEL,
    SEMANTIC_MODEL_COLUMN,
    SEMANTIC_MODEL_MEASURE,
    SEMANTIC_MODEL_TABLE,
    TABLE_DICTIONARY,
)
from weaver.catalogue.tsql import literal
from weaver.semantic_models.builtin_annotations import MEASURE_TABLE_SOURCE
from weaver.semantic_models.definition import decode_model
from weaver.semantic_models.wipe import connection_signature

SOURCE = "Warehouse/_weaver/_.TableDictionary"
SOURCE_SCOPE = InstallationScope("Warehouse", "_weaver")
# Rows, One and Value; Obsolete's measure is excluded.
AUTHORED_MEASURES = 3


@contextmanager
def phase(name):
    try:
        yield
    except AssertionError as error:
        raise AssertionError(f"{name}: {error}") from error


def _fingerprint(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, default=str).encode()
    ).hexdigest()


def _text(value):
    return "\n".join(value) if isinstance(value, list) else value


@weaver_test(remote=True, resources={"rest", "tds"})
def test_generated_direct_lake_build_load_and_both_wipes(
    scratch_build_context, tmp_path
):
    context = scratch_build_context
    connection = context.connection
    model = context.model
    folder = tmp_path / "project" / str(ITEM)
    annotation_project(folder, "source-extension")
    project = folder.parent.parent
    selection = f"{ITEM}=SemanticModel/{context.target}"
    target = f"SemanticModel/{context.target}"
    source_predicate = "[Schema name] = N'_' AND [Object name] = N'TableDictionary'"

    def build(label):
        result = weaver.build(project, items=selection, session=context.session)
        assert result.succeeded, f"{label}: {result.errors}"
        return result

    def load_status():
        (status,) = read_table(connection, LOAD_STATUS, scope=SCOPE)
        return status["result"]

    def owned_rows_empty():
        for table in (*PROJECTED_TABLES, *CURRENT_STATE_TABLES):
            assert not read_table(connection, table, scope=SCOPE), table.name

    with phase("build"):
        (source_row,) = read_table(
            connection, TABLE_DICTIONARY, scope=SOURCE_SCOPE, predicate=source_predicate
        )
        source_column_notes = {
            row["column_name"]: row["description"]
            for row in read_table(
                connection,
                COLUMN_DICTIONARY,
                scope=SOURCE_SCOPE,
                predicate=source_predicate,
            )
            if row["description"]
        }
        assert source_row["description"] and source_column_notes
        build("build")
        actual = decode_model(model.get_definition())["model"]
        tables = {t["name"]: t for t in actual["tables"]}
        metric = tables["Metric"]
        assert metric.get("isHidden") is True
        assert (
            _text(metric["partitions"][0]["source"]["expression"]).strip()
            == MEASURE_TABLE_SOURCE
        )
        value = next(m for m in metric["measures"] if m["name"] == "Value")
        assert "SELECTEDVALUE('Metric'[Measure name])" in _text(value["expression"])
        assert "SWITCH(" in _text(value["formatStringDefinition"]["expression"])
        assert any(a["name"] == "Weaver.Switch" for a in value["annotations"])
        assert any(a["name"] == "Weaver.MeasureTable" for a in metric["annotations"])
        assert {a["name"] for a in actual["annotations"]} >= {
            "Weaver.AutoHideColumns",
            "Weaver.AutoHideForeignKeys",
        }
        assert "Obsolete" not in tables
        assert "Object type" not in {c["name"] for c in tables["Objects"]["columns"]}
        for name in ("Objects", "Reference"):
            columns = {c["name"]: c for c in tables[name]["columns"]}
            assert columns["Schema name"]["isHidden"] is True, name
            assert columns["Item name"]["isHidden"] is True, name
            assert tables[name]["description"] == source_row["description"], name
            propagated = {
                key: note for key, note in source_column_notes.items() if key in columns
            }
            assert propagated, name
            assert {
                key: columns[key].get("description") for key in propagated
            } == propagated, name
            assert any(
                a["name"] == "Weaver.Source" for a in tables[name]["annotations"]
            ), name
        assert len(actual["expressions"]) == 1
        for table in (
            SEMANTIC_MODEL,
            SEMANTIC_MODEL_TABLE,
            SEMANTIC_MODEL_COLUMN,
            SEMANTIC_MODEL_MEASURE,
        ):
            assert read_table(connection, table, scope=SCOPE), table.name
        assert {
            (row["referencing_object_name"], row["dependency_reference"])
            for row in read_table(connection, DEPENDENCY, scope=SCOPE)
            if row["dependency_reference"] == SOURCE
        } == {("Objects", SOURCE), ("Reference", SOURCE)}

    with phase("load"):
        loaded = context.load(str(ITEM), session=context.session)
        assert loaded.succeeded, loaded.to_mapping()
        (node,) = loaded.nodes
        assert node.result.status == "Completed"
        assert node.result.request_id
        assert node.result.start_time and node.result.end_time
        logs = read_table(
            connection,
            LOG,
            predicate=f"[Workflow ID] = {literal(loaded.workflow_id)}",
        )
        assert any(node.result.request_id in (row["details"] or "") for row in logs)
        assert load_status() == "succeeded"
        before_connections = model.get_connections()
        assert len(before_connections) == 1
        assert before_connections[0]["connectivityType"] == "Automatic"
        assert before_connections[0]["connectionDetails"]["type"] == "SQL"
        binding_before = connection_signature(before_connections)

    with phase("dax"):
        (row,) = model.query_dax('EVALUATE ROW("N", COUNTROWS(Metric))')
        assert row["[N]"] >= AUTHORED_MEASURES

    with phase("unchanged build"):
        (definition,) = read_table(connection, SEMANTIC_MODEL, scope=SCOPE)
        unchanged = build("unchanged build")
        assert unchanged.selection.selected_for_build == (ROOT,)
        assert not unchanged.selection.impact.changed
        assert unchanged.installation_report.action_counts()["total"] > 0
        (after,) = read_table(connection, SEMANTIC_MODEL, scope=SCOPE)
        assert after["signature"] == definition["signature"]
        assert load_status() == "pending"

    with phase("preserve wipe plan"):
        before = decode_model(model.get_definition())
        source_installation = read_table(connection, INSTALLATION, scope=SOURCE_SCOPE)
        assert source_installation
        source_before = _fingerprint(
            read_table(connection, TABLE_DICTIONARY, scope=SOURCE_SCOPE)
        )
        item_path = f"workspaces/{model.workspace_id}/items/{model.model_id}"
        item_before = model.fabric.get_json(item_path)
        connections_before = _fingerprint(model.fabric.paged("connections"))
        plan = weaver.plan_wipe(
            target, preserve_data_source=True, session=context.session
        )
        assert [str(t) for t in plan.targets] == [target]
        assert plan.catalogue_action == "unbind" and not plan.empties_the_catalogue
        preview = weaver.wipe(plan=plan, dry_run=True, session=context.session)
        assert preview.dry_run
        assert decode_model(model.get_definition()) == before
        assert connection_signature(model.get_connections()) == binding_before

    with phase("preserve wipe"):
        capture = []
        execute = context.session.execute_mutation

        def observe(plan, payloads=None, **options):
            capture.append(plan)
            return execute(plan, payloads, **options)

        context.session.execute_mutation = observe
        try:
            wiped = weaver.wipe(plan=plan, session=context.session)
        finally:
            context.session.execute_mutation = execute
        assert wiped.emptied == (target,)
        assert wiped.unbound["logical_items"] == [str(ITEM)]
        (executed,) = capture
        assert [action.executor for _, _, action in executed.actions()] == [
            "tsql_batch",
            "semantic_wipe",
            "tsql_batch",
        ]
        assert executed.execution.spark_home_target_id is None
        observed = decode_model(model.get_definition())["model"]
        assert not any(
            observed.get(kind)
            for kind in ("relationships", "roles", "perspectives", "cultures")
        )
        (anchor,) = observed["tables"]
        assert anchor["name"] == "__WeaverSource" and anchor["isHidden"] is True
        assert not (
            anchor.get("columns") or anchor.get("measures") or anchor.get("hierarchies")
        )
        (partition,) = anchor["partitions"]
        assert partition["mode"] == "directLake"
        assert connection_signature(model.get_connections()) == binding_before
        assert model.refresh(timeout=300)["status"] == "Completed"
        assert connection_signature(model.get_connections()) == binding_before
        owned_rows_empty()
        assert (
            read_table(connection, INSTALLATION, scope=SOURCE_SCOPE)
            == source_installation
        )
        assert (
            _fingerprint(read_table(connection, TABLE_DICTIONARY, scope=SOURCE_SCOPE))
            == source_before
        )
        item_after = model.fabric.get_json(item_path)
        assert (item_after["id"], item_after["displayName"], item_after["type"]) == (
            item_before["id"],
            item_before["displayName"],
            item_before["type"],
        )
        assert _fingerprint(model.fabric.paged("connections")) == connections_before

    with phase("repeated preserve wipe"):
        repeated = weaver.wipe(
            target, preserve_data_source=True, session=context.session
        )
        assert repeated.emptied == wiped.emptied

    with phase("plain wipe"):
        build("rebuild before plain wipe")
        plain = weaver.wipe(target, session=context.session)
        assert plain.emptied == (target,)
        observed = decode_model(model.get_definition())["model"]
        assert not observed.get("tables") and not observed.get("expressions")
        assert not model.get_connections()
        owned_rows_empty()

    with phase("rebuild"):
        build("rebuild")
        assert load_status() == "pending"
        assert context.load(str(ITEM), session=context.session).succeeded
        assert connection_signature(model.get_connections()) == binding_before
        assert not {"livy", "onelake"} & {
            event.resource for event in context.session.telemetry.events()
        }
    print(
        "SEMANTIC_DIRECT_LAKE_EVIDENCE "
        + json.dumps(
            {
                "model_id": model.model_id,
                "source_fingerprint": source_before,
                "connection_signature": binding_before,
                "measure_rows": row["[N]"],
            },
            sort_keys=True,
            default=str,
        )
    )
