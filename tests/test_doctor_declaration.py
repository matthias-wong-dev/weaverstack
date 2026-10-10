"""Workspace discovery and probe outcomes through the Session contract."""

from types import SimpleNamespace

import pytest
from support.weaver_test import weaver_test

from weaver.errors import CommandError
from weaver.operations.doctor import ERROR, FAILED, MISSING, NOT_TESTED, OK, doctor
from weaver.sessions.testing import TestSession


class Client:
    def __init__(self, items=(), visible=None, failure=None):
        self.items = items
        self.visible = (
            [{"id": "ws", "displayName": "Analytics"}] if visible is None else visible
        )
        self.failure = failure
        self.paths = []

    def authenticate(self):
        if self.failure == "auth":
            raise RuntimeError("secret token must not appear")
        return {"path": "Azure CLI"}

    def paged(self, path):
        self.paths.append(path)
        if self.failure == "rest":
            raise TimeoutError("REST timeout")
        return self.visible if path == "workspaces" else self.items


def item(name, kind):
    return {"id": name, "displayName": name, "type": kind}


def session(client, *, exists=True):
    seen = []

    def read(path):
        seen.append(path)
        if isinstance(exists, Exception):
            raise exists
        return exists

    from weaver.fabric.resolution import FabricResolver
    from weaver.workspaces import Workspace

    resolver = FabricResolver(Workspace(workspace="Analytics"), client=client)
    return TestSession(resolver=resolver, store=SimpleNamespace(exists=read)), seen


@weaver_test()
def test_named_semantic_model_checks_definition_and_dax_without_other_transports():
    from weaver.semantic_models.definition import encode_definition

    calls = []
    semantic = SimpleNamespace(
        power_bi=SimpleNamespace(authenticate=lambda: {"path": "Azure CLI"}),
        get_definition=lambda: (
            calls.append("definition") or encode_definition({"model": {}})
        ),
        query_dax=lambda query: calls.append(query) or [{"[Value]": 1}],
    )
    client = Client(
        [
            item("Reporting", "SemanticModel"),
            item("Landing", "Lakehouse"),
            item("Curated", "Warehouse"),
        ]
    )
    opened, files = session(client)
    opened.answer_semantic_model("Analytics", "Reporting", semantic)
    report = doctor(workspace="Analytics", semantic_model="Reporting", session=opened)
    assert report.succeeded and all(check.passed for check in report.checks)
    assert [check.name for check in report.checks[-3:]] == [
        "Power BI authentication",
        "Semantic model definition",
        "Semantic model DAX",
    ]
    assert [check.via for check in report.checks[-3:]] == [
        "SemanticModel/Reporting"
    ] * 3
    assert report.checks[-3].detail == "Azure CLI"
    assert calls == ["definition", 'EVALUATE ROW("Value", 1)']
    assert not files and not opened.spark_sql and not opened.tsql
    assert [call.kind for call in opened.calls] == ["semantic_model"]


@weaver_test()
def test_power_bi_auth_failure_is_redacted_and_dax_does_not_retry_sign_in():
    from weaver.semantic_models.definition import encode_definition

    calls = []

    def fail():
        calls.append("authenticate")
        raise RuntimeError("secret token must not appear")

    semantic = SimpleNamespace(
        power_bi=SimpleNamespace(authenticate=fail),
        get_definition=lambda: encode_definition({"model": {}}),
        query_dax=lambda query: calls.append("query"),
    )
    opened, _ = session(Client([item("Reporting", "SemanticModel")]))
    opened.answer_semantic_model("Analytics", "Reporting", semantic)
    report = doctor(workspace="Analytics", semantic_model="Reporting", session=opened)
    assert not report.succeeded
    assert [check.status for check in report.checks[-3:]] == [ERROR, OK, NOT_TESTED]
    assert "secret token" not in str(report.to_mapping())
    assert calls == ["authenticate"]


@weaver_test()
def test_missing_semantic_model_does_not_probe_a_same_named_warehouse():
    opened, files = session(Client([item("Reporting", "Warehouse")]))
    report = doctor(workspace="Analytics", semantic_model="Reporting", session=opened)
    assert not report.succeeded
    assert report.checks[-1].status == MISSING
    assert report.checks[-1].via == "SemanticModel/Reporting"
    assert not files and not opened.calls


@weaver_test()
def test_discovery_probes_each_transport_once_without_project_or_environment(
    tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "workspace-config.yml").write_text("invalid project config")
    client = Client(
        [
            item("Zebra", "Lakehouse"),
            item("Landing", "Lakehouse"),
            item("ZuluCatalogue", "Warehouse"),
            item("Curated", "Warehouse"),
        ]
    )
    opened, files = session(client)
    report = doctor(workspace="Analytics", session=opened)
    assert report.succeeded and all(c.status == OK for c in report.checks)
    assert report.authentication == {"path": "Azure CLI"}
    assert report.checks[1].detail == "1 workspaces visible"
    assert client.paths == ["workspaces", "workspaces/ws/items"]
    assert len(files) == 1 and str(files[0]).endswith("/Landing.Lakehouse/Files")
    assert opened.spark_sql == ("SELECT 1",)
    assert opened.tsql == ("SELECT 1",)
    assert [c.via for c in report.checks[-3:]] == [
        "Lakehouse/Landing",
        "Warehouse/Curated",
        "Lakehouse/Landing",
    ]
    assert all(not scope.workspace.environment for scope in opened._scopes.values())


@weaver_test()
def test_inapplicable_probe_items_are_not_tested_without_failing_workspace():
    opened, files = session(Client())
    report = doctor(workspace="Analytics", session=opened)
    assert report.succeeded
    assert [c.status for c in report.checks[-3:]] == [NOT_TESTED] * 3
    assert [c.detail for c in report.checks[-3:]] == [
        "No Lakehouse exists in this workspace.",
        "No Warehouse exists in this workspace.",
        "No Lakehouse exists in this workspace.",
    ]
    assert not files and not opened.calls


@pytest.mark.parametrize(
    "visible,status", [([], FAILED), ([{"id": "x", "displayName": "Other"}], MISSING)]
)
@weaver_test()
def test_unusable_workspace_stops_before_endpoint_probes(visible, status):
    client = Client(visible=visible)
    opened, files = session(client)
    report = doctor(workspace="Analytics", session=opened)
    assert not report.succeeded and report.checks[-1].status == status
    assert not files and not opened.calls
    assert client.paths == ["workspaces"]


@pytest.mark.parametrize("failure", ["auth", "rest"])
@weaver_test()
def test_authentication_and_rest_errors_are_independent(failure):
    opened, _ = session(Client(failure=failure))
    report = doctor(workspace="Analytics", session=opened)
    assert not report.succeeded and report.checks[-1].status == ERROR
    assert "secret token" not in str(report.to_mapping())
    if failure == "rest":
        assert report.checks[0].status == OK


@pytest.mark.parametrize(
    "answer,status",
    [
        (False, FAILED),
        (PermissionError("denied"), FAILED),
        (TimeoutError("timed out"), ERROR),
    ],
)
@weaver_test()
def test_onelake_negative_result_and_errors_leave_other_probes_running(answer, status):
    opened, _ = session(
        Client([item("Landing", "Lakehouse"), item("Curated", "Warehouse")]),
        exists=answer,
    )
    report = doctor(workspace="Analytics", session=opened)
    assert report.checks[3].status == status
    assert report.checks[-1].status == OK
    assert not report.succeeded


@pytest.mark.parametrize(
    "method,probe",
    [("query_tsql", "Warehouse TDS"), ("execute_spark_sql", "Fabric Spark / Livy")],
)
@pytest.mark.parametrize(
    "error,status",
    [
        (CommandError("query rejected"), FAILED),
        (TimeoutError("transport timeout"), ERROR),
    ],
)
@weaver_test()
def test_query_rejection_differs_from_transport_failure(
    monkeypatch, method, probe, error, status
):
    opened, _ = session(
        Client([item("Landing", "Lakehouse"), item("Curated", "Warehouse")])
    )

    def fail(*a, **k):
        raise error

    monkeypatch.setattr(opened, method, fail)
    report = doctor(workspace="Analytics", session=opened)
    assert next(c for c in report.checks if c.name == probe).status == status


@weaver_test()
def test_json_retains_workspace_listing_and_whole_result():
    opened, _ = session(Client())
    result = doctor(workspace="Analytics", session=opened).to_mapping()
    assert result["workspaces"] == [{"id": "ws", "displayName": "Analytics"}]
    assert (
        result["workspace"] == "Analytics"
        and result["authentication"]["path"] == "Azure CLI"
    )
    assert all(
        set(c) == {"name", "status", "detail", "via", "remedy"}
        for c in result["checks"]
    )


@pytest.mark.parametrize(
    "code,status",
    [(401, FAILED), (403, FAILED), (404, MISSING), (500, ERROR), (None, ERROR)],
)
@weaver_test()
def test_rest_listing_rejections_use_endpoint_classification(monkeypatch, code, status):
    from weaver.fabric.client import FabricError

    client = Client()
    opened, files = session(client)

    def reject(path):
        raise FabricError("REST rejected", status_code=code)

    monkeypatch.setattr(client, "paged", reject)
    report = doctor(workspace="Analytics", session=opened)
    assert report.checks[-1].name == "Fabric REST"
    assert report.checks[-1].status == status
    assert report.checks[0].status == OK
    assert not files and not opened.calls
