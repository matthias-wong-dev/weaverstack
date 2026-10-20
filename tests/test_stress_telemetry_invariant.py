from __future__ import annotations

import importlib.util
import json
import sys
from contextlib import nullcontext
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from support.weaver_test import weaver_test

TELEMETRY = Path(__file__).resolve().parents[1] / "examples" / "stress" / "telemetry.py"
RUNNER = Path(__file__).resolve().parents[1] / "examples" / "stress" / "run.py"


def _telemetry():
    spec = importlib.util.spec_from_file_location("stress_telemetry", TELEMETRY)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _runner():
    spec = importlib.util.spec_from_file_location("stress_run", RUNNER)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    sys.path.insert(0, str(RUNNER.parent))
    try:
        spec.loader.exec_module(module)
    finally:
        sys.path.pop(0)
    return module


@weaver_test()
def test_evidence_run_retains_complete_reports_and_session_telemetry(tmp_path):
    telemetry = _telemetry()
    clock = iter(
        (
            datetime(2026, 10, 20, 5, 0, tzinfo=timezone.utc),
            datetime(2026, 10, 20, 6, 0, tzinfo=timezone.utc),
        )
    )
    evidence = telemetry.EvidenceRun(tmp_path, run_id="hero", now=lambda: next(clock))
    evidence.start(
        command="measure",
        options={"cycles": 3, "keep": False},
        identity={"git_commit": "abc", "weaver_import": "/checkout/src/weaver"},
    )
    report = SimpleNamespace(
        to_mapping=lambda: {"status": "succeeded", "nodes": [1]},
        installation_report=SimpleNamespace(
            to_mapping=lambda: {"status": "succeeded", "sequences": [1]}
        ),
    )
    session = SimpleNamespace(
        telemetry=SimpleNamespace(to_mapping=lambda: {"events": [{"resource": "livy"}]})
    )

    evidence.report("first load", "estate", report)
    evidence.session("first load", "estate", session)
    evidence.finish(status="succeeded")

    manifest = json.loads((tmp_path / "hero" / "manifest.json").read_text())
    retained_report = json.loads(
        (tmp_path / "hero" / "weaver" / "estate-first-load-report.json").read_text()
    )
    retained_session = json.loads(
        (tmp_path / "hero" / "weaver" / "estate-first-load-session.json").read_text()
    )

    assert manifest["run_id"] == "hero"
    assert manifest["status"] == "succeeded"
    assert manifest["started_at"] == "2026-10-20T05:00:00+00:00"
    assert manifest["finished_at"] == "2026-10-20T06:00:00+00:00"
    assert manifest["reports"] == ["weaver/estate-first-load-report.json"]
    assert manifest["sessions"] == ["weaver/estate-first-load-session.json"]
    assert retained_report == {
        "status": "succeeded",
        "nodes": [1],
        "installation_report": {"status": "succeeded", "sequences": [1]},
    }
    assert retained_session == {"events": [{"resource": "livy"}]}


@weaver_test()
def test_telemetry_config_rejects_a_kql_expression_as_a_table_name():
    telemetry = _telemetry()
    mapping = {
        "capacity_id": "842e9714-d0ae-412f-9555-993a4259450f",
        "query_service_uri": "https://cluster.kusto.fabric.microsoft.com",
        "database": "WeaverStressTelemetry",
        "overview_table": "CapacityOverviewEvents | take 1",
        "operation_table": "CapacityOperationEvents",
    }

    with pytest.raises(ValueError, match="overview_table"):
        telemetry.TelemetryConfig.from_mapping(mapping)


@weaver_test()
@pytest.mark.parametrize(
    "uri",
    [
        "https://api.fabric.microsoft.com",
        "https://cluster.kusto.fabric.microsoft.com.evil.example",
        "https://user@cluster.kusto.fabric.microsoft.com",
        "https://cluster.kusto.fabric.microsoft.com:443",
        "https://cluster.kusto.fabric.microsoft.com/query",
        "https://cluster.kusto.fabric.microsoft.com?redirect=evil",
        "https://cluster.kusto.fabric.microsoft.com#fragment",
    ],
)
def test_telemetry_config_rejects_non_kusto_query_origins(uri):
    telemetry = _telemetry()

    with pytest.raises(ValueError, match="query_service_uri"):
        telemetry.TelemetryConfig.from_mapping(
            {
                "capacity_id": "842e9714-d0ae-412f-9555-993a4259450f",
                "query_service_uri": uri,
                "database": "WeaverStressTelemetry",
                "overview_table": "CapacityOverviewEvents",
                "operation_table": "CapacityOperationEvents",
            }
        )


@weaver_test()
@pytest.mark.parametrize(
    "uri",
    [
        "https://trd-fv94c0atv5j9aufjea.z8.kusto.fabric.microsoft.com",
        "https://cluster.region.kusto.windows.net/",
    ],
)
def test_telemetry_config_accepts_approved_kusto_query_origins(uri):
    telemetry = _telemetry()

    config = telemetry.TelemetryConfig.from_mapping(
        {
            "capacity_id": "842E9714-D0AE-412F-9555-993A4259450F",
            "query_service_uri": uri,
            "database": "WeaverStressTelemetry",
            "overview_table": "CapacityOverviewEvents",
            "operation_table": "CapacityOperationEvents",
        }
    )

    assert config.capacity_id == "842e9714-d0ae-412f-9555-993a4259450f"
    assert config.query_service_uri == uri.rstrip("/")


@weaver_test()
@pytest.mark.parametrize(
    "run_id", ["../escape", "nested/run", "nested\\run", ".", ".."]
)
def test_evidence_run_rejects_run_ids_that_are_not_one_safe_component(tmp_path, run_id):
    telemetry = _telemetry()

    with pytest.raises(ValueError, match="run_id"):
        telemetry.EvidenceRun(tmp_path, run_id=run_id)


@weaver_test()
def test_kusto_time_preserves_fractional_utc_precision():
    telemetry = _telemetry()

    assert (
        telemetry._kusto_time(
            datetime(2026, 10, 20, 5, 0, 0, 123456, tzinfo=timezone.utc)
        )
        == "2026-10-20T05:00:00.123456Z"
    )


@weaver_test()
@pytest.mark.parametrize("value", ["", ".", "..", "app/escape", "app\\escape"])
def test_spark_identifiers_reject_empty_dot_and_path_segments(value):
    telemetry = _telemetry()

    assert not telemetry._safe_api_segment(value)


@weaver_test()
@pytest.mark.parametrize("value", ["livy-1", "application_123_0001", "attempt.1"])
def test_spark_identifiers_accept_opaque_safe_segments(value):
    telemetry = _telemetry()

    assert telemetry._safe_api_segment(value)


@weaver_test()
def test_kusto_query_fails_when_completion_reports_errors_after_primary_result():
    telemetry = _telemetry()

    class Credential:
        def get_token(self, _scope):
            return SimpleNamespace(token="secret")

    class Response:
        status_code = 200

        def json(self):
            return [
                {
                    "FrameType": "DataTable",
                    "TableKind": "PrimaryResult",
                    "Columns": [{"ColumnName": "value"}],
                    "Rows": [[1]],
                },
                {
                    "FrameType": "DataSetCompletion",
                    "HasErrors": True,
                    "Cancelled": False,
                    "OneApiErrors": [{"error": {"message": "partial failure"}}],
                },
            ]

    client = telemetry.KustoClient(
        query_service_uri="https://cluster.kusto.fabric.microsoft.com",
        database="WeaverStressTelemetry",
        credential=Credential(),
        request=lambda *_args, **_kwargs: Response(),
    )

    with pytest.raises(telemetry.TelemetryError, match="partial failure"):
        client.query("CapacityOverviewEvents | take 1")


@weaver_test()
def test_kusto_query_returns_named_rows_for_the_utc_window():
    telemetry = _telemetry()
    calls = []

    class Credential:
        def get_token(self, scope):
            assert scope == "https://kusto.kusto.windows.net/.default"
            return SimpleNamespace(token="secret")

    class Response:
        status_code = 200
        content = b"yes"

        def json(self):
            return [
                {
                    "FrameType": "DataTable",
                    "TableKind": "PrimaryResult",
                    "Columns": [
                        {"ColumnName": "type"},
                        {"ColumnName": "data"},
                    ],
                    "Rows": [
                        ["Microsoft.Fabric.Capacity.Summary", {"capacityUnitMs": 4}]
                    ],
                }
            ]

    def request(method, url, **kwargs):
        calls.append((method, url, kwargs))
        if len(calls) == 1:
            import requests

            raise requests.exceptions.SSLError("connection reset")
        return Response()

    client = telemetry.KustoClient(
        query_service_uri="https://cluster.kusto.fabric.microsoft.com",
        database="WeaverStressTelemetry",
        credential=Credential(),
        request=request,
    )
    rows = client.window(
        "CapacityOverviewEvents",
        datetime(2026, 10, 20, 5, 0, tzinfo=timezone.utc),
        datetime(2026, 10, 20, 6, 0, tzinfo=timezone.utc),
    )

    assert rows == [
        {
            "type": "Microsoft.Fabric.Capacity.Summary",
            "data": {"capacityUnitMs": 4},
        }
    ]
    method, url, options = calls[0]
    assert method == "POST"
    assert url == "https://cluster.kusto.fabric.microsoft.com/v2/rest/query"
    assert options["headers"]["Authorization"] == "Bearer secret"
    assert options["json"] == {
        "db": "WeaverStressTelemetry",
        "csl": "CapacityOverviewEvents\n"
        "| where EventProcessedUtcTime between "
        "(datetime(2026-10-20T05:00:00Z) .. datetime(2026-10-20T06:00:00Z))\n"
        "| order by EventProcessedUtcTime asc",
    }


@weaver_test()
def test_kusto_query_pages_retains_large_ordered_results():
    telemetry = _telemetry()
    queries = []
    data = [
        ["2026-10-20T05:00:00Z", "a"],
        ["2026-10-20T05:00:01Z", "b"],
        ["2026-10-20T05:00:02Z", "c"],
        ["2026-10-20T05:00:03Z", "d"],
        ["2026-10-20T05:00:04Z", "e"],
    ]

    class Credential:
        def get_token(self, _scope):
            return SimpleNamespace(token="secret")

    class Response:
        status_code = 200

        def __init__(self, columns, rows):
            self.columns = columns
            self.rows = rows

        def json(self):
            return [
                {
                    "TableKind": "PrimaryResult",
                    "Columns": [{"ColumnName": column} for column in self.columns],
                    "Rows": self.rows,
                }
            ]

    def request(_method, _url, **kwargs):
        query = kwargs["json"]["csl"]
        queries.append(query)
        if query.endswith("| count"):
            return Response(["Count"], [[len(data)]])
        for lower, upper in ((1, 2), (3, 4), (5, 5)):
            if f"between ({lower} .. {upper})" in query:
                return Response(["EventTime", "id"], data[lower - 1 : upper])
        raise AssertionError(query)

    client = telemetry.KustoClient(
        query_service_uri="https://cluster.kusto.fabric.microsoft.com",
        database="WeaverStressTelemetry",
        credential=Credential(),
        request=request,
    )
    rows = client.query_pages(
        "CapacityOperationEvents\n| extend EventTime=todatetime(['time'])",
        page_size=2,
        ingested_before=datetime(2026, 10, 20, 6, 0, tzinfo=timezone.utc),
    )

    assert [row["id"] for row in rows] == ["a", "b", "c", "d", "e"]
    page_queries = [query for query in queries if not query.endswith("| count")]
    assert "between (1 .. 2)" in page_queries[0]
    assert "between (3 .. 4)" in page_queries[1]
    assert "between (5 .. 5)" in page_queries[2]
    assert all("project-away _TelemetryRow" in query for query in page_queries)
    assert all(
        "EventProcessedUtcTime <= datetime(2026-10-20T06:00:00Z)" in query
        for query in queries
    )


@weaver_test()
def test_kusto_query_pages_retries_when_late_ingestion_changes_the_count():
    telemetry = _telemetry()
    data = [["a"], ["b"], ["c"], ["d"]]
    count_calls = 0

    class Credential:
        def get_token(self, _scope):
            return SimpleNamespace(token="secret")

    class Response:
        status_code = 200

        def __init__(self, columns, rows):
            self.columns = columns
            self.rows = rows

        def json(self):
            return [
                {
                    "TableKind": "PrimaryResult",
                    "Columns": [{"ColumnName": column} for column in self.columns],
                    "Rows": self.rows,
                }
            ]

    def request(_method, _url, **kwargs):
        nonlocal count_calls
        query = kwargs["json"]["csl"]
        if query.endswith("| count"):
            count_calls += 1
            if count_calls == 2:
                data.insert(0, ["late"])
            return Response(["Count"], [[len(data)]])
        if "between (1 .. 2)" in query:
            return Response(["id"], data[:2])
        if "between (3 .. 4)" in query:
            return Response(["id"], data[2:4])
        if "between (5 .. 5)" in query:
            return Response(["id"], data[4:5])
        raise AssertionError(query)

    client = telemetry.KustoClient(
        query_service_uri="https://cluster.kusto.fabric.microsoft.com",
        database="WeaverStressTelemetry",
        credential=Credential(),
        request=request,
    )
    rows = client.query_pages(
        "CapacityOperationEvents\n| extend EventTime=todatetime(['time'])",
        page_size=2,
    )

    assert [row["id"] for row in rows] == ["late", "a", "b", "c", "d"]
    assert count_calls == 4


@weaver_test()
def test_spark_capture_retains_session_resource_usage_and_history(tmp_path):
    telemetry = _telemetry()
    config = telemetry.TelemetryConfig.from_mapping(
        {
            "capacity_id": "842e9714-d0ae-412f-9555-993a4259450f",
            "query_service_uri": "https://cluster.kusto.fabric.microsoft.com",
            "database": "WeaverStressTelemetry",
            "overview_table": "CapacityOverviewEvents",
            "operation_table": "CapacityOperationEvents",
        }
    )

    class Credential:
        def get_token(self, scope):
            return SimpleNamespace(token="secret")

    class Response:
        status_code = 200

        def __init__(self, payload):
            self.payload = payload

        def json(self):
            return self.payload

    def request(method, url, **_kwargs):
        assert method == "GET"
        if url.endswith("/livySessions"):
            return Response({"value": [], "continuationToken": "next page"})
        if url.endswith("/livySessions?continuationToken=next+page"):
            return Response(
                {
                    "value": [
                        {
                            "livyId": "livy-1",
                            "sparkApplicationId": "application_1",
                            "submittedDateTime": "2026-10-20T05:10:00Z",
                            "endDateTime": "2026-10-20T05:50:00Z",
                            "state": "Succeeded",
                        }
                    ]
                }
            )
        if url.endswith("/applications/application_1"):
            return Response({"attempts": [{"attemptId": "1"}, {"attemptId": "3"}]})
        if url.endswith("/3/resourceUsage"):
            return Response({"data": {"timestamps": [1, 2]}})
        payloads = {
            "/3/jobs": [{"jobId": 1}],
            "/3/stages": [{"stageId": 2}],
            "/3/allexecutors": [{"id": "driver"}],
            "/3/sql": [{"id": 3}],
        }
        return Response(
            next(
                payload for suffix, payload in payloads.items() if url.endswith(suffix)
            )
        )

    evidence = telemetry.EvidenceRun(tmp_path, run_id="hero")
    evidence.start(command="measure", options={}, identity={})
    capture = telemetry.TelemetryCapture(
        config, credential=Credential(), request=request
    )
    result = capture.spark(
        evidence,
        targets=[
            {
                "workspace_id": "workspace-1",
                "workspace_name": "Stress",
                "lakehouse_id": "lakehouse-1",
                "lakehouse_name": "Lake",
            }
        ],
        started_at=datetime(2026, 10, 20, 5, 0, tzinfo=timezone.utc),
        finished_at=datetime(2026, 10, 20, 6, 0, tzinfo=timezone.utc),
    )

    app = tmp_path / "hero" / "spark" / "application_1"
    assert json.loads((app / "session.json").read_text())["livyId"] == "livy-1"
    assert json.loads((app / "resource-usage.json").read_text())["data"][
        "timestamps"
    ] == [1, 2]
    assert json.loads((app / "jobs.json").read_text()) == [{"jobId": 1}]
    assert json.loads((app / "stages.json").read_text()) == [{"stageId": 2}]
    assert json.loads((app / "executors.json").read_text()) == [{"id": "driver"}]
    assert json.loads((app / "sql.json").read_text()) == [{"id": 3}]
    assert result["applications"] == 1
    assert result["errors"] == []
    assert result["targets"][0]["applications"] == 1
    assert result["targets"][0]["apps"] == [
        {
            "application_id": "application_1",
            "attempt": 3,
            "layers": {
                "resourceUsage": 2,
                "jobs": 1,
                "stages": 1,
                "executors": 1,
                "sql": 1,
            },
            "errors": [],
        }
    ]


@weaver_test()
def test_fabric_pagination_accepts_only_fabric_continuation_uris():
    telemetry = _telemetry()
    requested = []

    class Credential:
        def get_token(self, _scope):
            return SimpleNamespace(token="secret")

    class Response:
        status_code = 200

        def __init__(self, payload):
            self.payload = payload

        def json(self):
            return self.payload

    pages = iter(
        [
            {
                "value": [{"id": "first"}],
                "continuationUri": "https://api.fabric.microsoft.com/v1/workspaces?continuationToken=abc",
            },
            {
                "value": [{"id": "second"}],
                "continuationUri": "https://evil.example/steal",
            },
        ]
    )

    def request(_method, url, **kwargs):
        requested.append((url, kwargs["headers"]["Authorization"]))
        return Response(next(pages))

    capture = telemetry.TelemetryCapture(
        telemetry.TelemetryConfig.from_mapping(
            {
                "capacity_id": "842e9714-d0ae-412f-9555-993a4259450f",
                "query_service_uri": "https://cluster.kusto.fabric.microsoft.com",
                "database": "WeaverStressTelemetry",
                "overview_table": "CapacityOverviewEvents",
                "operation_table": "CapacityOperationEvents",
            }
        ),
        credential=Credential(),
        request=request,
    )

    with pytest.raises(telemetry.TelemetryError, match="continuation"):
        capture._fabric_list("workspaces")

    assert requested == [
        ("https://api.fabric.microsoft.com/v1/workspaces", "Bearer secret"),
        (
            "https://api.fabric.microsoft.com/v1/workspaces?continuationToken=abc",
            "Bearer secret",
        ),
    ]


@weaver_test()
def test_capacity_capture_writes_overview_and_operation_jsonl(tmp_path):
    telemetry = _telemetry()
    config = telemetry.TelemetryConfig.from_mapping(
        {
            "capacity_id": "842e9714-d0ae-412f-9555-993a4259450f",
            "query_service_uri": "https://cluster.kusto.fabric.microsoft.com",
            "database": "WeaverStressTelemetry",
            "overview_table": "CapacityOverviewEvents",
            "operation_table": "CapacityOperationEvents",
        }
    )

    class Credential:
        def get_token(self, _scope):
            return SimpleNamespace(token="secret")

    class Response:
        status_code = 200

        def __init__(self, event_type, *, count=False):
            self.event_type = event_type
            self.count = count

        def json(self):
            if self.count:
                return [
                    {
                        "TableKind": "PrimaryResult",
                        "Columns": [{"ColumnName": "Count"}],
                        "Rows": [[1]],
                    }
                ]
            return [
                {
                    "TableKind": "PrimaryResult",
                    "Columns": [{"ColumnName": "type"}, {"ColumnName": "data"}],
                    "Rows": [[self.event_type, {"capacityId": config.capacity_id}]],
                }
            ]

    def request(method, _url, **kwargs):
        assert method == "POST"
        query = kwargs["json"]["csl"]
        assert "extend EventTime = todatetime(['time'])" in query
        assert "where EventTime between" in query
        event_type = (
            "Microsoft.Fabric.Capacity.Summary"
            if "CapacityOverviewEvents" in query
            else "Microsoft.Fabric.CapacityOperationEvents.Operation"
        )
        return Response(event_type, count=query.endswith("| count"))

    evidence = telemetry.EvidenceRun(tmp_path, run_id="hero")
    evidence.start(command="measure", options={}, identity={})
    result = telemetry.TelemetryCapture(
        config, credential=Credential(), request=request
    ).capacity(
        evidence,
        started_at=datetime(2026, 10, 20, 5, 0, tzinfo=timezone.utc),
        finished_at=datetime(2026, 10, 20, 6, 0, tzinfo=timezone.utc),
    )

    overview = [
        json.loads(line)
        for line in (tmp_path / "hero" / "capacity" / "overview.jsonl")
        .read_text()
        .splitlines()
    ]
    operations = [
        json.loads(line)
        for line in (tmp_path / "hero" / "capacity" / "operations.jsonl")
        .read_text()
        .splitlines()
    ]
    assert overview[0]["type"] == "Microsoft.Fabric.Capacity.Summary"
    assert operations[0]["type"] == "Microsoft.Fabric.CapacityOperationEvents.Operation"
    assert result == {"overview": 1, "operations": 1, "errors": []}


@weaver_test()
def test_discovery_resolves_project_lakehouses_and_warehouses(tmp_path):
    telemetry = _telemetry()
    workspace_config = tmp_path / "estate.yml"
    workspace_config.write_text(
        """workspace: Stress
catalogue: Warehouse/Catalogue
targets:
  Lakehouse/Lake: PhysicalLake
  Warehouse/Core: PhysicalCore
  Warehouse/Mart: PhysicalMart
""",
        encoding="utf-8",
    )
    config = telemetry.TelemetryConfig.from_mapping(
        {
            "capacity_id": "842e9714-d0ae-412f-9555-993a4259450f",
            "query_service_uri": "https://cluster.kusto.fabric.microsoft.com",
            "database": "WeaverStressTelemetry",
            "overview_table": "CapacityOverviewEvents",
            "operation_table": "CapacityOperationEvents",
        }
    )

    class Credential:
        def get_token(self, _scope):
            return SimpleNamespace(token="secret")

    class Response:
        status_code = 200

        def __init__(self, payload):
            self.payload = payload

        def json(self):
            return self.payload

    def request(_method, url, **_kwargs):
        if url.endswith("/workspaces"):
            return Response(
                {
                    "value": [],
                    "continuationUri": "https://api.fabric.microsoft.com/v1/workspaces?continuationToken=ws",
                }
            )
        if url.endswith("/workspaces?continuationToken=ws"):
            return Response({"value": [{"id": "w1", "displayName": "Stress"}]})
        if url.endswith("/workspaces/w1"):
            return Response(
                {
                    "id": "w1",
                    "displayName": "Stress",
                    "capacityId": config.capacity_id.upper(),
                }
            )
        if url.endswith("/workspaces/w1/items"):
            return Response(
                {
                    "value": [
                        {
                            "id": "l1",
                            "displayName": "PhysicalLake",
                            "type": "Lakehouse",
                        },
                        {"id": "c1", "displayName": "Catalogue", "type": "Warehouse"},
                    ],
                    "continuationToken": "items page",
                }
            )
        if url.endswith("/workspaces/w1/items?continuationToken=items+page"):
            return Response(
                {
                    "value": [
                        {
                            "id": "w2",
                            "displayName": "PhysicalCore",
                            "type": "Warehouse",
                        },
                        {
                            "id": "w3",
                            "displayName": "PhysicalMart",
                            "type": "Warehouse",
                        },
                    ]
                }
            )
        raise AssertionError(url)

    discovered = telemetry.TelemetryCapture(
        config, credential=Credential(), request=request
    ).discover({"estate": workspace_config})

    assert discovered["spark"] == [
        {
            "project": "estate",
            "workspace_id": "w1",
            "workspace_name": "Stress",
            "lakehouse_id": "l1",
            "lakehouse_name": "PhysicalLake",
        }
    ]
    assert [target["warehouse_name"] for target in discovered["warehouses"]] == [
        "Catalogue",
        "PhysicalCore",
        "PhysicalMart",
    ]


@weaver_test()
def test_warehouse_capture_retains_query_insights_for_each_target(tmp_path):
    telemetry = _telemetry()
    queries = []
    endpoints = []

    class Executor:
        def __init__(self, endpoint):
            self.endpoint = endpoint

        def query(self, statement, parameters):
            queries.append((statement, parameters))
            return [
                {
                    "distributed_statement_id": f"statement-{self.endpoint.warehouse_id}",
                    "end_time": datetime(2026, 10, 20, 5, 30),
                }
            ]

    class Session:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def sql_executor(self, endpoint):
            endpoints.append(endpoint)
            return Executor(endpoint)

    def session_factory(*, workspace_config):
        assert workspace_config == str(tmp_path / "estate.yml")
        return Session()

    config = telemetry.TelemetryConfig.from_mapping(
        {
            "capacity_id": "842e9714-d0ae-412f-9555-993a4259450f",
            "query_service_uri": "https://cluster.kusto.fabric.microsoft.com",
            "database": "WeaverStressTelemetry",
            "overview_table": "CapacityOverviewEvents",
            "operation_table": "CapacityOperationEvents",
        }
    )
    evidence = telemetry.EvidenceRun(tmp_path, run_id="hero")
    evidence.start(command="measure", options={}, identity={})

    class Credential:
        def get_token(self, _scope):
            return SimpleNamespace(token="secret")

    class Response:
        status_code = 200

        def __init__(self, payload):
            self.payload = payload

        def json(self):
            return self.payload

    def request(_method, url, **_kwargs):
        warehouse_id = "core-id" if "/core-id" in url else "mart-id"
        if url.endswith("/connectionString"):
            return Response({"connectionString": "server.fabric.microsoft.com"})
        return Response(
            {
                "id": warehouse_id,
                "displayName": f"Renamed-{warehouse_id}",
                "type": "Warehouse",
            }
        )

    result = telemetry.TelemetryCapture(
        config, credential=Credential(), request=request
    ).warehouses(
        evidence,
        targets=[
            {
                "project": "estate",
                "workspace_config": str(tmp_path / "estate.yml"),
                "workspace_id": "w1",
                "workspace_name": "Stress",
                "warehouse_id": "core-id",
                "warehouse_name": "A B",
            },
            {
                "project": "estate",
                "workspace_config": str(tmp_path / "estate.yml"),
                "workspace_id": "w1",
                "workspace_name": "Stress",
                "warehouse_id": "mart-id",
                "warehouse_name": "A-B",
            },
        ],
        started_at=datetime(2026, 10, 20, 5, 0, tzinfo=timezone.utc),
        finished_at=datetime(2026, 10, 20, 6, 0, tzinfo=timezone.utc),
        session_factory=session_factory,
    )

    core = [
        json.loads(line)
        for line in (tmp_path / "hero" / "warehouse" / "estate-a-b-core-id.jsonl")
        .read_text()
        .splitlines()
    ]
    mart = [
        json.loads(line)
        for line in (tmp_path / "hero" / "warehouse" / "estate-a-b-mart-id.jsonl")
        .read_text()
        .splitlines()
    ]
    assert core == [
        {
            "distributed_statement_id": "statement-core-id",
            "end_time": "2026-10-20 05:30:00",
        }
    ]
    assert mart == [
        {
            "distributed_statement_id": "statement-mart-id",
            "end_time": "2026-10-20 05:30:00",
        }
    ]
    assert len(queries) == 2
    assert queries[0][1] == (
        datetime(2026, 10, 20, 6, 0),
        datetime(2026, 10, 20, 5, 0),
    )
    assert "start_time <= ? and end_time >= ?" in queries[0][0]
    assert [(item.warehouse_id, item.database) for item in endpoints] == [
        ("core-id", "Renamed-core-id"),
        ("mart-id", "Renamed-mart-id"),
    ]
    assert result["warehouses"] == 2
    assert result["requests"] == 2
    assert result["errors"] == []
    assert [target["requests"] for target in result["targets"]] == [1, 1]


@weaver_test()
def test_measure_arguments_accept_telemetry_evidence_options(tmp_path):
    runner = _runner()
    options = runner.arguments(
        [
            str(tmp_path),
            "measure",
            "--telemetry-config",
            str(tmp_path / "telemetry.yml"),
            "--evidence",
            str(tmp_path / "evidence"),
            "--run-id",
            "hero",
            "--telemetry-settle-seconds",
            "0",
        ]
    )

    assert options.telemetry_config == tmp_path / "telemetry.yml"
    assert options.evidence == tmp_path / "evidence"
    assert options.run_id == "hero"
    assert options.telemetry_settle_seconds == 0


@weaver_test()
def test_stress_retains_the_complete_load_report_when_evidence_is_enabled(
    tmp_path, monkeypatch
):
    runner = _runner()
    (tmp_path / "plan.json").write_text(
        json.dumps({"estate": {"objects": 3}, "options": {"scale": 1}}),
        encoding="utf-8",
    )
    retained = []
    evidence = SimpleNamespace(
        report=lambda step, project, report: retained.append((step, project, report))
    )
    report = SimpleNamespace(status="succeeded", nodes=())
    monkeypatch.setattr(runner.weaver, "load", lambda *_args, **_kwargs: report)
    stress = runner.Stress(
        tmp_path, tmp_path / "results.jsonl", retries=0, evidence=evidence
    )

    stress.load("estate", ["Lakehouse/Lake"], object(), "first load")

    assert retained == [("first load", "estate", report)]


@weaver_test()
def test_stress_retains_session_telemetry_when_the_session_closes(tmp_path):
    runner = _runner()
    (tmp_path / "plan.json").write_text(
        json.dumps({"estate": {"objects": 3}, "options": {"scale": 1}}),
        encoding="utf-8",
    )
    retained = []
    evidence = SimpleNamespace(
        session=lambda step, project, session: retained.append((step, project, session))
    )
    opened = object()
    stress = runner.Stress(
        tmp_path, tmp_path / "results.jsonl", retries=0, evidence=evidence
    )
    stress.session = lambda _name: nullcontext(opened)

    with stress.captured_session("estate", "initial cycle") as session:
        assert session is opened

    assert retained == [("initial cycle", "estate", opened)]


@weaver_test()
def test_require_capture_checks_each_target_and_required_spark_layer():
    runner = _runner()
    result = {
        "discovery": {"spark_targets": 2, "warehouse_targets": 2, "errors": []},
        "capacity": {"overview": 1, "operations": 1, "errors": []},
        "spark": {
            "applications": 1,
            "errors": [],
            "targets": [
                {
                    "project": "estate",
                    "lakehouse_id": "lake-a",
                    "lakehouse_name": "LakeA",
                    "applications": 1,
                    "apps": [
                        {
                            "application_id": "app-a",
                            "attempt": 2,
                            "layers": {
                                "resourceUsage": 1,
                                "jobs": 1,
                                "stages": 1,
                                "executors": 0,
                                "sql": 0,
                            },
                            "errors": [],
                        }
                    ],
                    "errors": [],
                },
                {
                    "project": "source",
                    "lakehouse_id": "lake-b",
                    "lakehouse_name": "LakeB",
                    "applications": 0,
                    "apps": [],
                    "errors": [],
                },
            ],
        },
        "warehouses": {
            "warehouses": 2,
            "requests": 1,
            "errors": [],
            "targets": [
                {
                    "project": "estate",
                    "warehouse_id": "warehouse-a",
                    "warehouse_name": "Core",
                    "requests": 1,
                    "errors": [],
                },
                {
                    "project": "estate",
                    "warehouse_id": "warehouse-b",
                    "warehouse_name": "Mart",
                    "requests": 0,
                    "errors": [],
                },
            ],
        },
    }

    with pytest.raises(SystemExit) as raised:
        runner._require_capture(result)

    message = str(raised.value)
    assert "app-a executors" in message
    assert "LakeB" in message
    assert "Mart" in message
    assert "app-a sql" not in message


@weaver_test()
def test_collection_attempts_spark_and_warehouse_after_capacity_failure(
    tmp_path, monkeypatch
):
    telemetry = _telemetry()
    config = telemetry.TelemetryConfig.from_mapping(
        {
            "capacity_id": "842e9714-d0ae-412f-9555-993a4259450f",
            "query_service_uri": "https://cluster.kusto.fabric.microsoft.com",
            "database": "WeaverStressTelemetry",
            "overview_table": "CapacityOverviewEvents",
            "operation_table": "CapacityOperationEvents",
        }
    )
    capture = telemetry.TelemetryCapture(config, credential=object(), request=object())
    calls = []
    spark_targets = [{"lakehouse_id": "lake-a"}]
    warehouse_targets = [{"warehouse_id": "warehouse-a"}]
    monkeypatch.setattr(
        capture,
        "discover",
        lambda _projects: {"spark": spark_targets, "warehouses": warehouse_targets},
    )

    def fail_capacity(*_args, **_kwargs):
        calls.append("capacity")
        raise telemetry.TelemetryError("capacity unavailable")

    def collect_spark(*_args, **kwargs):
        calls.append(("spark", kwargs["targets"]))
        return {"applications": 0, "targets": [], "errors": []}

    def collect_warehouses(*_args, **kwargs):
        calls.append(("warehouses", kwargs["targets"]))
        return {"warehouses": 0, "requests": 0, "targets": [], "errors": []}

    monkeypatch.setattr(capture, "capacity", fail_capacity)
    monkeypatch.setattr(capture, "spark", collect_spark)
    monkeypatch.setattr(capture, "warehouses", collect_warehouses)
    retained = []
    evidence = SimpleNamespace(
        capture=lambda name, value: retained.append((name, value))
    )

    result = capture.collect(
        evidence,
        projects={},
        started_at=datetime(2026, 10, 20, 5, 0, tzinfo=timezone.utc),
        finished_at=datetime(2026, 10, 20, 6, 0, tzinfo=timezone.utc),
    )

    assert calls == [
        "capacity",
        ("spark", spark_targets),
        ("warehouses", warehouse_targets),
    ]
    assert result["capacity"] == {
        "overview": 0,
        "operations": 0,
        "errors": [{"error": "TelemetryError: capacity unavailable"}],
    }
    assert retained == [("collection", result)]


@weaver_test()
def test_measure_main_collects_telemetry_and_closes_the_manifest(tmp_path, monkeypatch):
    runner = _runner()
    (tmp_path / "plan.json").write_text(
        json.dumps({"estate": {"objects": 3}, "options": {"scale": 1}}),
        encoding="utf-8",
    )
    (tmp_path / "source.yml").write_text("workspace: Stress\n", encoding="utf-8")
    (tmp_path / "estate.yml").write_text("workspace: Stress\n", encoding="utf-8")
    telemetry_path = tmp_path / "telemetry.yml"
    telemetry_path.write_text("capacity_id: ignored\n", encoding="utf-8")
    calls = []
    started = datetime(2026, 10, 20, 5, 0, tzinfo=timezone.utc)

    class Evidence:
        def __init__(self, root, *, run_id):
            calls.append(("evidence", root, run_id))
            self.started_at = started

        def start(self, **kwargs):
            calls.append(("start", kwargs))

        def finish(self, *, status):
            calls.append(("finish", status))

    class Stress:
        def __init__(self, _folder, _results, _retries, *, evidence):
            assert isinstance(evidence, Evidence)

        def measure(self, cycles, keep):
            calls.append(("measure", cycles, keep))

    class Config:
        @classmethod
        def load(cls, path):
            calls.append(("config", path))
            return cls()

    class Capture:
        def __init__(self, config):
            assert isinstance(config, Config)

        def collect(self, evidence, *, projects, started_at, finished_at):
            calls.append(("collect", projects, started_at, finished_at))
            return {
                "discovery": {
                    "spark_targets": 1,
                    "warehouse_targets": 1,
                    "errors": [],
                },
                "capacity": {"overview": 1, "operations": 1, "errors": []},
                "spark": {
                    "applications": 1,
                    "errors": [],
                    "targets": [
                        {
                            "lakehouse_name": "Lake",
                            "applications": 1,
                            "errors": [],
                            "apps": [
                                {
                                    "application_id": "app-1",
                                    "layers": {
                                        "resourceUsage": 1,
                                        "jobs": 1,
                                        "stages": 1,
                                        "executors": 1,
                                        "sql": 0,
                                    },
                                    "errors": [],
                                }
                            ],
                        }
                    ],
                },
                "warehouses": {
                    "warehouses": 1,
                    "requests": 1,
                    "errors": [],
                    "targets": [
                        {
                            "warehouse_name": "Core",
                            "requests": 1,
                            "errors": [],
                        }
                    ],
                },
            }

    monkeypatch.setattr(runner, "EvidenceRun", Evidence)
    monkeypatch.setattr(runner, "Stress", Stress)
    monkeypatch.setattr(runner, "TelemetryConfig", Config)
    monkeypatch.setattr(runner, "TelemetryCapture", Capture)
    monkeypatch.setattr(
        runner.time, "sleep", lambda seconds: calls.append(("sleep", seconds))
    )
    monkeypatch.setattr(
        runner,
        "_identity",
        lambda _folder: {"git_commit": "abc", "weaver_import": "/checkout/weaver"},
    )

    result = runner.main(
        [
            str(tmp_path),
            "measure",
            "--telemetry-config",
            str(telemetry_path),
            "--run-id",
            "hero",
            "--telemetry-settle-seconds",
            "0",
        ]
    )

    assert result == 0
    assert ("measure", 2, False) in calls
    assert ("sleep", 0) in calls
    assert calls[-1] == ("finish", "succeeded")


@weaver_test()
def test_measure_main_marks_manifest_failed_when_telemetry_setup_fails(
    tmp_path, monkeypatch
):
    runner = _runner()
    (tmp_path / "plan.json").write_text(
        json.dumps({"estate": {"objects": 3}, "options": {"scale": 1}}),
        encoding="utf-8",
    )
    telemetry_path = tmp_path / "telemetry.yml"
    telemetry_path.write_text("invalid: true\n", encoding="utf-8")
    calls = []

    class Evidence:
        def __init__(self, _root, *, run_id):
            assert run_id == "hero"

        def start(self, **_kwargs):
            calls.append("start")

        def finish(self, *, status):
            calls.append(("finish", status))

    class Config:
        @classmethod
        def load(cls, _path):
            calls.append("config")
            raise ValueError("bad telemetry config")

    monkeypatch.setattr(runner, "EvidenceRun", Evidence)
    monkeypatch.setattr(runner, "TelemetryConfig", Config)
    monkeypatch.setattr(runner, "_identity", lambda _folder: {})

    with pytest.raises(ValueError, match="bad telemetry config"):
        runner.main(
            [
                str(tmp_path),
                "measure",
                "--telemetry-config",
                str(telemetry_path),
                "--run-id",
                "hero",
            ]
        )

    assert calls == ["start", "config", ("finish", "failed")]
