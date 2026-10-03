"""Installed semantic refresh uses public Load and central run recording."""

import json
from types import SimpleNamespace

import pytest
from support.weaver_test import weaver_test
from test_semantic_model_build_cycle import session_for
from test_semantic_model_rest_boundary import Client, response

import weaver
from weaver.catalogue.connection import catalogue_connection
from weaver.catalogue.reader import read_table
from weaver.catalogue.state import Catalogue
from weaver.catalogue.tables import CATALOGUE_TABLES, READABLE_TABLES
from weaver.declaration.model import WeaverDocumentId, WeaverItemId
from weaver.fabric.semantic_model import SemanticModelClient

ITEM = WeaverItemId.parse("SemanticModel/Reporting")
ROOT = WeaverDocumentId.model_root(ITEM)
WORKSPACE_ID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
MODEL_ID = "11111111-2222-3333-4444-555555555555"
REQUEST_ID = "99999999-8888-7777-6666-555555555555"
COMPLETED = {
    "status": "Completed",
    "startTime": "2026-01-01T01:00:00Z",
    "endTime": "2026-01-01T01:00:02Z",
}


def installed_rows():
    identity = {
        "item_type": ITEM.item_type,
        "item_name": ITEM.item_name,
        "schema_name": "",
        "object_name": "",
    }
    return {
        ITEM: {
            "Installation": (
                {
                    "item_type": ITEM.item_type,
                    "item_name": ITEM.item_name,
                    "target_name": "Reporting_Dev",
                    "workspace_id": WORKSPACE_ID,
                    "item_id": MODEL_ID,
                },
            ),
            "Registry": (
                {
                    **identity,
                    "object_type": "semantic_model",
                    "object_role": "data",
                    "signature": "compiled-definition",
                },
            ),
            "SemanticModelDictionary": (
                {
                    **identity,
                    "signature": "compiled-definition",
                    "definition": '{"model":{"culture":"en-US"}}',
                    "properties": "{}",
                    "provenance": "{}",
                },
            ),
        }
    }


def answer_installed(session, rows):
    session.answer_tsql(
        "SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = N'_'",
        [
            {"TABLE_NAME": table.name, "COLUMN_NAME": column.public_name}
            for table in CATALOGUE_TABLES
            for column in table.columns
        ],
    )
    connection = catalogue_connection(session)
    for table in READABLE_TABLES:
        before = len(session.tsql)
        read_table(connection, table)
        for statement in session.tsql[before:]:
            if f"FROM [_].[{table.name}]" in statement:
                session.answer_tsql(
                    statement,
                    [
                        row
                        for tables in rows.values()
                        for row in tables.get(table.name, ())
                    ],
                )
    session.calls.clear()


def refresh_session(monkeypatch, *polls, rows=None):
    from weaver.fabric import semantic_model

    clock = SimpleNamespace(now=0)

    def sleep(seconds):
        clock.now += seconds

    monkeypatch.setattr(
        semantic_model,
        "time",
        SimpleNamespace(monotonic=lambda: clock.now, sleep=sleep),
    )
    power_bi = Client(
        response({}, 202, {"x-ms-request-id": REQUEST_ID}),
        *(
            response(body, 202 if body["status"] != "Completed" else 200)
            for body in polls
        ),
    )
    session = session_for()
    model = SemanticModelClient(
        WORKSPACE_ID, MODEL_ID, fabric=Client(), power_bi=power_bi
    )
    session.answer_semantic_model(WORKSPACE_ID, MODEL_ID, model)
    answer_installed(session, installed_rows() if rows is None else rows)
    return session, power_bi, clock


@weaver_test()
def test_public_load_refreshes_installed_model_and_records_terminal_evidence(
    monkeypatch,
):
    with refresh_session(monkeypatch, {"status": "InProgress"}, COMPLETED)[
        0
    ] as session:
        report = weaver.load(str(ITEM), session=session)
        assert len(report.nodes) == 1, report.to_mapping()
        node = report.nodes[0]
        assert report.succeeded and node.status == "succeeded" and node.executed
        assert node.logical_id == str(ROOT)
        assert node.primitive_kind == "semantic_refresh"
        assert (
            node.dispatch_location
            == f"groups/{WORKSPACE_ID}/datasets/{MODEL_ID}/refreshes"
        )
        assert node.result.request_id == REQUEST_ID
        assert node.result.status == "Completed"
        assert node.result.start_time == COMPLETED["startTime"]
        assert node.result.end_time == COMPLETED["endTime"]
        assert node.started_at and node.finished_at
        assert not hasattr(node.result, "rows_inserted")
        assert not hasattr(node.result, "bookmark_datetime")
        writes = [s for s in session.tsql if "MERGE" in s or "INSERT INTO" in s]
        assert any("[_].[LoadStatus]" in s and "Succeeded" in s for s in writes)
        log = next(s for s in writes if "[_].[Log]" in s)
        assert REQUEST_ID in log and '"status": "Completed"' in log
        assert "SemanticModel" in log
        assert "N'SemanticModel'" in log
        assert not any(
            "[_].[Bookmark]" in s or "[_].[LoadStatistic]" in s for s in writes
        )
        assert not session.python and not session.spark_sql
        assert (
            json.loads(json.dumps(report.to_mapping()))["nodes"][0]["rows"][
                "request_id"
            ]
            == REQUEST_ID
        )
        assert Catalogue(installed_rows()).dag().node(ROOT).can_load


@weaver_test()
@pytest.mark.parametrize(
    "missing", ["Installation", "Registry", "SemanticModelDictionary"]
)
def test_public_load_refuses_an_uninstalled_or_uncertified_model(monkeypatch, missing):
    from weaver.errors import WeaverError

    rows = installed_rows()
    rows[ITEM].pop(missing)
    with refresh_session(monkeypatch, COMPLETED, rows=rows)[0] as session:
        with pytest.raises(WeaverError, match="[Bb]uild"):
            weaver.load(str(ITEM), session=session)
        assert not any(call.kind == "semantic_model" for call in session.calls)


@weaver_test()
@pytest.mark.parametrize("field", ["workspace_id", "item_id"])
@pytest.mark.parametrize(
    "value", [None, "Reporting_Dev", " " + MODEL_ID, MODEL_ID + " "]
)
def test_public_load_refuses_missing_or_malformed_frozen_ids(monkeypatch, field, value):
    from weaver.errors import LoadError

    rows = installed_rows()
    binding = rows[ITEM]["Installation"][0]
    binding[field] = value
    session, client, _ = refresh_session(monkeypatch, COMPLETED, rows=rows)
    session.answer_semantic_model(
        binding["workspace_id"],
        binding["item_id"],
        SemanticModelClient(WORKSPACE_ID, MODEL_ID, fabric=Client(), power_bi=client),
    )
    with session:
        with pytest.raises(LoadError, match="[Bb]uild"):
            weaver.load(str(ITEM), session=session)
        assert not client.calls


@weaver_test()
def test_public_load_uses_frozen_typed_ids_without_name_resolution(monkeypatch):
    import requests

    from weaver.fabric.auth import FABRIC_SCOPE, TokenProvider
    from weaver.fabric.client import FabricClient
    from weaver.fabric.resolution import FabricResolver
    from weaver.sessions import Session, TestSession
    from weaver.workspaces import Workspace

    sent = []
    scopes = []

    class Credential:
        def get_token(self, scope):
            scopes.append(scope)
            return SimpleNamespace(token="test-token", expires_on=1)

    def send(method, url, **kwargs):
        sent.append((method, url, kwargs))
        answer = requests.Response()
        answer.status_code = 202 if method == "POST" else 200
        answer.headers = {"x-ms-request-id": REQUEST_ID}
        answer._content = json.dumps({} if method == "POST" else COMPLETED).encode()
        return answer

    class BoundarySession(TestSession):
        semantic_model = Session.semantic_model

    monkeypatch.setattr(requests, "request", send)
    workspace = Workspace(workspace="RenamedWorkspace", catalogue="Warehouse/Catalogue")
    client = FabricClient(token=TokenProvider(FABRIC_SCOPE, cred=Credential()))
    resolver = FabricResolver(workspace, client=client)
    with BoundarySession(workspace=workspace, resolver=resolver) as session:
        answer_installed(session, installed_rows())
        report = weaver.load(str(ITEM), session=session)
        assert report.succeeded
        base = f"https://api.powerbi.com/v1.0/myorg/groups/{WORKSPACE_ID}/datasets/{MODEL_ID}/refreshes"
        assert [(method, url) for method, url, _ in sent] == [
            ("POST", base),
            ("GET", f"{base}/{REQUEST_ID}"),
        ]
        assert set(scopes) == {"https://analysis.windows.net/powerbi/api/.default"}
        assert not session.python and not session.spark_sql


@weaver_test()
@pytest.mark.parametrize("outcome", ["Failed", "Cancelled", "timeout", "poll_error"])
def test_public_load_records_refresh_failure_evidence_before_raising(
    monkeypatch, outcome
):
    from weaver.errors import LoadError
    from weaver.fabric.client import FabricError

    pending = {"status": "InProgress", "startTime": COMPLETED["startTime"]}
    polls = (
        [pending] * 450
        if outcome == "timeout"
        else [
            pending,
            {
                "status": outcome,
                "messages": [{"type": "Error", "message": "engine failure"}],
            },
        ]
    )
    session, client, clock = refresh_session(monkeypatch, *polls)
    if outcome == "poll_error":
        request = client.request

        def disconnected(method, path, **kwargs):
            if method == "GET":
                raise FabricError("poll disconnected")
            return request(method, path, **kwargs)

        client.request = disconnected
    with session:
        with pytest.raises(LoadError) as raised:
            weaver.load(str(ITEM), session=session)
        report = raised.value.report
        assert report.status == "failed"
        (node,) = report.nodes
        assert node.executed and node.status == "failed"
        assert node.result.request_id == REQUEST_ID
        assert not node.result.succeeded
        assert node.result.error_message
        if outcome == "timeout":
            assert clock.now == 900
            assert node.result.status == "InProgress"
            assert "did not complete" in node.result.error_message
        elif outcome != "poll_error":
            assert node.result.status == outcome
            assert "engine failure" in node.result.error_message
        writes = [s for s in session.tsql if "MERGE" in s or "INSERT INTO" in s]
        assert any("[_].[LoadStatus]" in s and "Failed" in s for s in writes)
        assert any("[_].[Log]" in s and REQUEST_ID in s for s in writes)
        assert not any(
            "[_].[LoadStatistic]" in s or "[_].[Bookmark]" in s for s in writes
        )
        assert sum(c[0] == "POST" for c in client.calls) == 1


@weaver_test()
def test_stale_load_reads_model_root_status_through_the_shared_identity_codec(
    monkeypatch,
):
    from datetime import datetime, timezone

    from weaver.catalogue.tables import LOAD_STATUS

    rows = installed_rows()
    rows[ITEM]["LoadStatus"] = (
        {
            "item_type": ITEM.item_type,
            "item_name": ITEM.item_name,
            "schema_name": "",
            "object_name": "",
            "result": "succeeded",
            "completed_datetime": datetime(2026, 1, 1, tzinfo=timezone.utc),
        },
    )
    with refresh_session(monkeypatch, COMPLETED, rows=rows)[0] as session:
        before = len(session.tsql)
        read_table(catalogue_connection(session), LOAD_STATUS)
        for statement in session.tsql[before:]:
            if "FROM [_].[LoadStatus]" in statement:
                session.answer_tsql(statement, list(rows[ITEM]["LoadStatus"]))
        report = weaver.load(
            str(ITEM), session=session, stale=True, as_of="2025-12-31T00:00:00Z"
        )
        assert report.succeeded and not report.nodes
        report = weaver.load(
            str(ITEM), session=session, stale=True, as_of="2026-01-02T00:00:00Z"
        )
        assert report.succeeded and len(report.nodes) == 1


@weaver_test()
def test_refresh_report_round_trip_preserves_evidence_without_counts(monkeypatch):
    from weaver.load_report import LoadRunReport
    from weaver.operations.load import _completion_document

    with refresh_session(monkeypatch, COMPLETED)[0] as session:
        report = weaver.load(str(ITEM), session=session)
        restored = LoadRunReport.from_mapping(
            json.loads(json.dumps(report.to_mapping()))
        )
        assert restored.to_mapping() == report.to_mapping()
        assert _completion_document(report)["rows"] == {}


@weaver_test()
@pytest.mark.parametrize("json_output", [False, True])
@pytest.mark.parametrize("failed", [False, True])
def test_cli_load_uses_public_semantic_execution_without_spark(
    monkeypatch, capsys, json_output, failed
):
    from weaver_cli.main import build_parser

    body = {"status": "Failed"} if failed else COMPLETED
    with refresh_session(monkeypatch, body)[0] as session:
        args = build_parser().parse_args(
            [
                "load",
                "--item",
                str(ITEM),
                "--non-interactive",
                *(["--json"] if json_output else []),
            ]
        )
        args.session = session
        assert not {"livy", "onelake"} & args.requires(args)
        assert args.handler(args) == (1 if failed else 0)
        output = capsys.readouterr().out
        if json_output:
            payload = json.loads(output)
            node = (payload["report"] if failed else payload)["nodes"][0]
            assert node["rows"]["request_id"] == REQUEST_ID
            assert "rows_inserted" not in node["rows"]
        else:
            assert str(ITEM).split("/")[0] in output
            assert "  Rows" not in output
        assert not session.python and not session.spark_sql
