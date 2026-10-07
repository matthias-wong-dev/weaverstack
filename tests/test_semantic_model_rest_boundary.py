"""Semantic-model REST request and response contracts."""

from types import SimpleNamespace

import pytest
from support.weaver_test import weaver_test

from weaver.fabric.client import FabricError


@pytest.mark.parametrize(
    "failure", ["throttled", "poll_transport", "submit_connection"]
)
@weaver_test()
def test_refresh_deadline_bounds_real_transport_retries(monkeypatch, failure):
    import json

    import requests
    from urllib3.exceptions import NewConnectionError

    from weaver.fabric import client as transport
    from weaver.fabric import semantic_model

    clock = SimpleNamespace(now=0)
    sent = []

    def sleep(seconds):
        clock.now += seconds

    timer = SimpleNamespace(monotonic=lambda: clock.now, sleep=sleep)
    monkeypatch.setattr(semantic_model, "time", timer)
    monkeypatch.setattr(transport, "time", timer)

    def send(method, url, **kwargs):
        sent.append((method, clock.now, kwargs["timeout"]))
        if failure == "submit_connection" or (
            failure == "poll_transport" and method == "GET"
        ):
            clock.now += min(4, kwargs["timeout"])
            if failure == "submit_connection":
                raise requests.ConnectionError(NewConnectionError(None, "not sent"))
            raise requests.ReadTimeout("poll timed out")
        answer = requests.Response()
        answer._content = json.dumps({}).encode()
        answer.status_code = 202 if method == "POST" else 429
        answer.headers = {
            "x-ms-request-id": "11111111-2222-3333-4444-555555555555",
            "Retry-After": "120",
        }
        return answer

    monkeypatch.setattr(requests, "request", send)
    client = transport.FabricClient(
        token="test-token", api_base_url=semantic_model.POWER_BI_API
    )
    model = semantic_model.SemanticModelClient(
        "workspace-id", "model-id", fabric=Client(), power_bi=client
    )
    with pytest.raises(FabricError):
        model.refresh(timeout=10)
    assert clock.now <= 10
    assert all(start < 10 and allowance <= 10 - start for _, start, allowance in sent)
    if failure != "submit_connection":
        assert sum(method == "POST" for method, _, _ in sent) == 1
    if failure == "throttled":
        assert len(sent) == 2


class Client:
    def __init__(self, *responses):
        self.timeout = 60
        self.responses = list(responses)
        self.calls = []

    def wait_for_operation(self, accepted, **kwargs):
        self.calls.append(("WAIT", accepted.status_code, kwargs))
        return {"status": "Succeeded"}

    def get_json(self, path):
        return self.request("GET", path, expected=(200,)).json()

    def request(self, method, path, **kwargs):
        self.calls.append((method, path, kwargs))
        # A refresh first reads data sources; unscripted, they are all bound.
        if (method, path.rsplit("/", 1)[-1]) == ("GET", "datasources") and not (
            self.responses and getattr(self.responses[0], "datasources", False)
        ):
            return SimpleNamespace(status_code=200, json=lambda: {"value": []})
        return self.responses.pop(0)


@pytest.mark.parametrize(
    "body",
    [
        {"error": {"message": "Query rejected"}},
        {
            "results": [
                {
                    "error": {"message": "Row limit exceeded"},
                    "tables": [{"rows": [{"[Value]": 1}]}],
                }
            ]
        },
        {
            "results": [
                {
                    "tables": [
                        {
                            "error": {"message": "Value limit exceeded"},
                            "rows": [{"[Value]": 1}],
                        }
                    ]
                }
            ]
        },
        {},
        {"results": []},
        {"results": [{"tables": []}]},
        {"results": [{"tables": [{}, {}]}]},
        {"results": [{"tables": [{}]}, {"tables": [{}]}]},
        {"results": [{"tables": [{"rows": [1]}]}]},
    ],
)
@weaver_test()
def test_dax_refuses_partial_or_malformed_results(body):
    from weaver.fabric.semantic_model import SemanticModelClient

    power_bi = Client(response(body))
    model = SemanticModelClient(
        "workspace-id", "model-id", fabric=Client(), power_bi=power_bi
    )
    with pytest.raises(FabricError, match="DAX"):
        model.query_dax("EVALUATE Sales")
    assert len(power_bi.calls) == 1


@pytest.mark.parametrize("asynchronous", [False, True])
@weaver_test()
def test_definition_read_requests_tmsl_and_retrieves_completed_operation_result(
    asynchronous,
):
    from weaver.fabric.semantic_model import SemanticModelClient

    definition = {
        "parts": [
            {"path": "model.bim", "payloadType": "InlineBase64", "payload": "e30="}
        ]
    }
    body = {"definition": definition}
    fabric = Client(
        *(
            [response({}, 202, {"x-ms-operation-id": "operation-id"}), response(body)]
            if asynchronous
            else [response(body)]
        )
    )
    model = SemanticModelClient(
        "workspace-id", "model-id", fabric=fabric, power_bi=Client()
    )
    assert model.get_definition(timeout=30) == definition
    assert fabric.calls[0] == (
        "POST",
        "workspaces/workspace-id/semanticModels/model-id/getDefinition?format=TMSL",
        {"expected": (200, 202)},
    )
    if asynchronous:
        assert fabric.calls[1:] == [
            ("WAIT", 202, {"timeout": 30}),
            ("GET", "operations/operation-id/result", {"expected": (200,)}),
        ]
    else:
        assert len(fabric.calls) == 1


@weaver_test()
def test_refresh_tracks_the_submitted_request_through_the_canonical_endpoint(
    monkeypatch,
):
    from weaver.fabric import semantic_model

    clock = SimpleNamespace(now=0)
    sleeps = []

    def sleep(seconds):
        sleeps.append(seconds)
        clock.now += seconds

    monkeypatch.setattr(
        semantic_model,
        "time",
        SimpleNamespace(monotonic=lambda: clock.now, sleep=sleep),
        raising=False,
    )
    request_id = "11111111-2222-3333-4444-555555555555"
    path = "groups/workspace-id/datasets/model-id/refreshes"
    power_bi = Client(
        response(
            {},
            202,
            {
                "x-ms-request-id": request_id,
                "Location": f"https://regional.analysis.windows.net/v1.0/myorg/{path}/{request_id}",
            },
        ),
        response({"status": "Unknown", "extendedStatus": "NotStarted"}, 202),
        response(
            {
                "status": "Completed",
                "objects": [{"table": "Sales", "status": "Completed"}],
            }
        ),
    )
    model = semantic_model.SemanticModelClient(
        "workspace-id", "model-id", fabric=Client(), power_bi=power_bi
    )
    outcome = model.refresh(timeout=10, poll_interval=2)
    assert outcome["status"] == "Completed" and outcome["request_id"] == request_id
    assert sleeps == [2]
    assert power_bi.calls == [
        (
            "POST",
            path,
            {
                "payload": {
                    "type": "full",
                    "commitMode": "transactional",
                    "retryCount": 0,
                    "timeout": "00:00:10",
                },
                "expected": (202,),
                "retry_transient": False,
                "timeout": 10,
                "deadline": 10,
            },
        ),
        (
            "GET",
            f"{path}/{request_id}",
            {"expected": (200, 202), "timeout": 10, "deadline": 10},
        ),
        (
            "GET",
            f"{path}/{request_id}",
            {"expected": (200, 202), "timeout": 8, "deadline": 10},
        ),
    ]


@pytest.mark.parametrize(
    "status", ["Failed", "Cancelled", "Disabled", "Unrecognised", None]
)
@weaver_test()
def test_refresh_reports_terminal_or_invalid_status_without_replay(monkeypatch, status):
    from weaver.fabric import semantic_model

    def sleep(seconds):
        pytest.fail("A terminal or invalid refresh status must not be polled again")

    monkeypatch.setattr(
        semantic_model, "time", SimpleNamespace(monotonic=lambda: 0, sleep=sleep)
    )
    request_id = "11111111-2222-3333-4444-555555555555"
    power_bi = Client(
        response({}, 202, {"x-ms-request-id": request_id}),
        response(
            {
                "status": status,
                "messages": [{"type": "Error", "message": "Partition rejected"}],
            }
        ),
    )
    model = semantic_model.SemanticModelClient(
        "workspace-id", "model-id", fabric=Client(), power_bi=power_bi
    )
    with pytest.raises(FabricError, match=request_id) as error:
        model.refresh(timeout=10)
    if status == "Failed":
        assert "Partition rejected" in str(error.value)
    assert [call[0] for call in power_bi.calls] == ["POST", "GET"]


@weaver_test()
def test_refresh_connection_failure_preserves_service_evidence_and_names_the_owner_action():
    from weaver.fabric.semantic_model import SemanticModelClient, SemanticRefreshError

    request_id = "11111111-2222-3333-4444-555555555555"
    body = {
        "status": "Failed",
        "serviceExceptionJson": '{"errorCode":"Premium_ASWL_Error","errorDescription":"Default connection has no explicit credentials"}',
    }
    power_bi = Client(
        response({}, 202, {"x-ms-request-id": request_id}), response(body)
    )
    fabric = Client()
    model = SemanticModelClient(
        "workspace-id", "model-id", fabric=fabric, power_bi=power_bi
    )
    with pytest.raises(SemanticRefreshError) as rejected:
        model.refresh(timeout=10)
    message = str(rejected.value)
    assert "Premium_ASWL_Error" in message
    assert "connection owner" in message and "grant access" in message
    assert "Fabric settings" in message
    assert [call[0] for call in power_bi.calls] == ["POST", "GET"]
    assert not fabric.calls


@pytest.mark.parametrize(
    "request_id", [None, "bad/id", " 11111111-2222-3333-4444-555555555555"]
)
@weaver_test()
def test_refresh_rejects_missing_or_malformed_operation_identity(request_id):
    from weaver.fabric.semantic_model import SemanticModelClient

    power_bi = Client(response({}, 202, {"x-ms-request-id": request_id}))
    model = SemanticModelClient(
        "workspace-id", "model-id", fabric=Client(), power_bi=power_bi
    )
    with pytest.raises(FabricError, match="request ID"):
        model.refresh(timeout=10)
    assert len(power_bi.calls) == 1


@weaver_test()
def test_refresh_does_not_accept_completion_observed_after_its_deadline(monkeypatch):
    from weaver.fabric import semantic_model

    clock = SimpleNamespace(now=0)
    monkeypatch.setattr(
        semantic_model,
        "time",
        SimpleNamespace(monotonic=lambda: clock.now, sleep=lambda _: None),
    )

    class SlowClient(Client):
        def request(self, method, path, **kwargs):
            answer = super().request(method, path, **kwargs)
            if method == "GET":
                clock.now = 11
            return answer

    request_id = "11111111-2222-3333-4444-555555555555"
    client = SlowClient(
        response({}, 202, {"x-ms-request-id": request_id}),
        response({"status": "Completed"}),
    )
    model = semantic_model.SemanticModelClient(
        "workspace-id", "model-id", fabric=Client(), power_bi=client
    )
    with pytest.raises(FabricError, match="did not complete within"):
        model.refresh(timeout=10)
    assert len(client.calls) == 2


@pytest.mark.parametrize(
    "options",
    [
        {"timeout": 0},
        {"timeout": -1},
        {"timeout": float("nan")},
        {"timeout": 86400},
        {"poll_interval": 0},
        {"poll_interval": float("inf")},
    ],
)
@weaver_test()
def test_refresh_validates_wait_limits_before_submission(options):
    from weaver.errors import ConfigError
    from weaver.fabric.semantic_model import SemanticModelClient

    client = Client()
    model = SemanticModelClient(
        "workspace-id", "model-id", fabric=Client(), power_bi=client
    )
    with pytest.raises(ConfigError, match="refresh"):
        model.refresh(**options)
    assert not client.calls


@weaver_test()
def test_refresh_pending_status_reaches_a_bounded_timeout(monkeypatch):
    from weaver.fabric import semantic_model

    clock = SimpleNamespace(now=0)

    def sleep(seconds):
        clock.now += seconds

    monkeypatch.setattr(
        semantic_model,
        "time",
        SimpleNamespace(monotonic=lambda: clock.now, sleep=sleep),
    )
    request_id = "11111111-2222-3333-4444-555555555555"
    client = Client(
        response({}, 202, {"x-ms-request-id": request_id}),
        response({"status": "Unknown"}, 202),
    )
    model = semantic_model.SemanticModelClient(
        "workspace-id", "model-id", fabric=Client(), power_bi=client
    )
    with pytest.raises(FabricError, match=request_id):
        model.refresh(timeout=1, poll_interval=2)
    assert clock.now == 1
    assert len(client.calls) == 2


@pytest.mark.parametrize("allow_purge", [False, True])
@weaver_test()
def test_definition_update_is_non_purging_by_default_and_waits_for_completion(
    allow_purge,
):
    from weaver.fabric.semantic_model import SemanticModelClient

    definition = {"format": "TMSL", "parts": []}
    client = Client(response({}, 202, {"x-ms-operation-id": "operation-id"}))
    model = SemanticModelClient(
        "workspace-id", "model-id", fabric=client, power_bi=Client()
    )
    options = {"allow_purge_data": True} if allow_purge else {}
    model.update_definition(definition, timeout=30, **options)
    assert client.calls == [
        (
            "POST",
            "workspaces/workspace-id/semanticModels/model-id/updateDefinition",
            {
                "payload": {
                    "definition": definition,
                    "options": {"allowPurgeData": allow_purge},
                },
                "expected": (200, 202),
                "retry_transient": False,
            },
        ),
        ("WAIT", 202, {"timeout": 30}),
    ]


def response(body, status=200, headers=None):
    return SimpleNamespace(
        status_code=status, headers=headers or {}, content=b"json", json=lambda: body
    )


@weaver_test()
def test_dax_uses_the_power_bi_client_and_preserves_result_column_names():
    from weaver.fabric.semantic_model import SemanticModelClient

    rows = [{"Sales[Amount]": 12.5, "[Revenue]": None}]
    power_bi = Client(response({"results": [{"tables": [{"rows": rows}]}]}))
    fabric = Client()
    model = SemanticModelClient(
        "workspace-id", "model-id", fabric=fabric, power_bi=power_bi
    )
    assert model.query_dax('EVALUATE ROW("Revenue", BLANK())') == rows
    assert not fabric.calls
    assert power_bi.calls == [
        (
            "POST",
            "groups/workspace-id/datasets/model-id/executeQueries",
            {
                "payload": {
                    "queries": [{"query": 'EVALUATE ROW("Revenue", BLANK())'}],
                    "serializerSettings": {"includeNulls": True},
                },
                "expected": (200,),
            },
        )
    ]
