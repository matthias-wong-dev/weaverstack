"""Semantic refresh participates in the shared graph and settlement policy."""

from dataclasses import replace

import pytest
from support.catalogues import Recording
from support.weaver_test import weaver_test
from test_semantic_model_load_cycle import (
    COMPLETED,
    ITEM,
    REQUEST_ID,
    ROOT,
    WORKSPACE_ID,
    answer_installed,
    installed_rows,
    refresh_session,
)
from test_semantic_model_rest_boundary import Client, response

import weaver
from weaver.catalogue.state import Catalogue
from weaver.declaration.model import WeaverDocumentId, WeaverItemId
from weaver.fabric.semantic_model import SemanticModelClient
from weaver.graph import Graph
from weaver.installed import InstalledEdge
from weaver.load_plan import load_dag
from weaver.run import Runner, RunRequest, RunState
from weaver.run.record import LOAD_TASK, RunRecord


@weaver_test()
@pytest.mark.parametrize("failure", [False, True])
@pytest.mark.parametrize("fault_tolerant", [False, True])
def test_semantic_nodes_keep_known_graph_order_and_runner_failure_policy(
    monkeypatch, failure, fault_tolerant
):
    rows = installed_rows()
    child = WeaverItemId.parse("SemanticModel/Summary")
    independent = WeaverItemId.parse("SemanticModel/Unrelated")
    for index, item in enumerate((child, independent), 1):
        rows[item] = {
            table: tuple({**row, "item_name": item.item_name} for row in values)
            for table, values in rows[ITEM].items()
        }
        rows[item]["Installation"][0].update(
            target_name=item.item_name,
            item_id=f"11111111-2222-3333-4444-{index:012d}",
        )
    writer = Recording()
    catalogue = Catalogue(rows, writer=writer)
    installed = catalogue.dag()
    # No .source lineage is inferred. This supplied edge tests graph mechanics.
    assert not installed.edges
    edge = InstalledEdge(ROOT, WeaverDocumentId.model_root(child), "known dependency")
    supplied = replace(
        installed,
        edges=(edge,),
        graph=Graph(installed.by_id, ((str(edge.upstream), str(edge.downstream)),)),
    )
    items = (ITEM, child, independent)
    planned = load_dag(supplied, items=items)
    ids = {node.logical_id: node.node_id for node in planned.nodes}
    expected_edge = (ids[ROOT], ids[WeaverDocumentId.model_root(child)])
    assert planned.edges == (expected_edge,)

    polls = ({"status": "Failed"} if failure else COMPLETED,)
    session, client, _ = refresh_session(monkeypatch, *polls, rows=rows)
    with session:
        for item in (child, independent):
            model_id = rows[item]["Installation"][0]["item_id"]
            session.answer_semantic_model(
                WORKSPACE_ID,
                model_id,
                SemanticModelClient(
                    WORKSPACE_ID,
                    model_id,
                    fabric=Client(),
                    power_bi=Client(
                        response({}, 202, {"x-ms-request-id": REQUEST_ID}),
                        response(COMPLETED),
                    ),
                ),
            )
        runner = Runner(
            RunState(catalogue),
            RunRequest.load(items, fault_tolerant=fault_tolerant),
            workspace=session.workspace,
        )
        runner._graph = replace(runner.plan(), edges=planned.edges)
        record = RunRecord("graph-refresh", LOAD_TASK, catalogue)
        result = runner.run(session=session, on_node=record.settled)
        record.flush()
        assert [node.logical_id for node in result.nodes] == list(
            map(str, (ROOT, child, independent))
        )
        statuses = [node.status for node in result.nodes]
        expected = (
            ["succeeded"] * 3
            if not failure
            else ["failed", "succeeded", "succeeded"]
            if fault_tolerant
            else ["failed", "blocked", "pending"]
        )
        assert statuses == expected
        assert [row["result"] for row in writer.rows("LoadStatus")] == expected
        assert len(writer.rows("Log")) == 3
        assert not writer.rows("Bookmark") and not writer.rows("LoadStatistic")
        assert writer.flushes == 1
        assert not session.python and not session.spark_sql


@weaver_test()
def test_semantic_dry_run_and_table_flags_leave_bookmarks_untouched(monkeypatch):
    session, client, _ = refresh_session(monkeypatch, COMPLETED)
    with session:
        report = weaver.load(str(ITEM), session=session, dry_run=True)
        assert report.succeeded and report.nodes[0].status == "validated"
        assert not client.calls
        assert not any("MERGE" in sql or "INSERT INTO" in sql for sql in session.tsql)
        report = weaver.load(
            str(ITEM), session=session, reload=True, ignore_stability_threshold=True
        )
        assert report.succeeded
        assert not any(
            "[_].[Bookmark]" in sql and ("MERGE" in sql or "INSERT INTO" in sql)
            for sql in session.tsql
        )


@weaver_test()
def test_changed_dictionary_signature_requires_build_before_load(monkeypatch):
    from weaver.errors import LoadError

    rows = installed_rows()
    rows[ITEM]["SemanticModelDictionary"][0]["signature"] = "uncertified-definition"
    session, client, _ = refresh_session(monkeypatch, COMPLETED)
    with session:
        answer_installed(session, rows)
        with pytest.raises(LoadError, match="Build"):
            weaver.load(str(ITEM), session=session)
        assert not client.calls
