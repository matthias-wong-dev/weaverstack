from types import SimpleNamespace

import pytest
from support.weaver_test import weaver_test

from weaver.fabric.resolution import FabricResolver
from weaver.sessions.console import ConsoleSession
from weaver.workspaces import Workspace


class Transport:
    def __init__(self):
        self.calls = []
        self.definition = {"parts": []}

    def paged(self, path):
        self.calls.append(("LIST", path))
        if path == "workspaces":
            return [{"id": "workspace-id", "displayName": "Analytics"}]
        return [
            {"id": "warehouse-id", "displayName": "Executive", "type": "Warehouse"},
            {"id": "report-id", "displayName": "Executive", "type": "Report"},
        ]

    def request(self, method, path, **options):
        self.calls.append((method, path, options))
        return SimpleNamespace(
            status_code=202,
            headers={"x-ms-operation-id": "operation-id"},
        )

    def wait_for_operation(self, response, **options):
        self.calls.append(("WAIT", options))
        return {"status": "Succeeded"}

    def get_json(self, path):
        self.calls.append(("GET", path))
        return {"definition": self.definition}


@weaver_test()
def test_report_session_resolves_typed_target_reads_async_updates_once():
    transport = Transport()
    workspace = Workspace(workspace="Analytics")
    resolver = FabricResolver(workspace, client=transport)
    with ConsoleSession(workspace=workspace, resolver=resolver) as session:
        report = session.report_item("Executive")
        assert session.report_item("Executive") is report
        assert (report.workspace_id, report.report_id) == ("workspace-id", "report-id")
        assert report.get_definition(timeout=30) == transport.definition
        report.update_definition(transport.definition, timeout=30)
        assert not session.scope(workspace).livy.acquired
    assert transport.calls == [
        ("LIST", "workspaces"),
        ("LIST", "workspaces/workspace-id/items?type=Report"),
        (
            "POST",
            "workspaces/workspace-id/reports/report-id/getDefinition",
            {"expected": (200, 202)},
        ),
        ("WAIT", {"timeout": 30}),
        ("GET", "operations/operation-id/result"),
        (
            "POST",
            "workspaces/workspace-id/reports/report-id/updateDefinition?updateMetadata=false",
            {
                "payload": {"definition": transport.definition},
                "expected": (200, 202),
                "retry_transient": False,
            },
        ),
        ("WAIT", {"timeout": 30}),
    ]


@weaver_test()
def test_report_session_refuses_wrong_bound_type_before_resolution():
    from weaver.errors import ConfigError
    from weaver.fabric.resources import Item

    workspace = Workspace(workspace="Analytics")
    transport = Transport()
    with ConsoleSession(
        workspace=workspace, resolver=FabricResolver(workspace, client=transport)
    ) as session:
        with pytest.raises(ConfigError, match="Report"):
            session.report_item(
                Item(
                    id="report-id",
                    name="Executive",
                    type="Warehouse",
                    workspace_id="workspace-id",
                )
            )
    assert transport.calls == []
