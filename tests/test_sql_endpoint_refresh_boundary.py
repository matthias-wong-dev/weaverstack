"""SQL endpoint refresh execution and Fabric REST plumbing."""

from __future__ import annotations

from types import SimpleNamespace

from support.weaver_test import weaver_test

from weaver.build_bundle.executors.base import InstallationContext, ResolvedTarget
from weaver.build_bundle.executors.sql_endpoint_refresh import (
    SqlEndpointRefreshExecutor,
)
from weaver.build_bundle.models import InstallAction
from weaver.build_bundle.targets import BoundTarget
from weaver.fabric.client import FabricClient
from weaver.fabric.resolution import FabricResolver
from weaver.fabric.resources import SQL_ENDPOINT, Item, refresh_sql_endpoint_metadata
from weaver.store import FilesystemStore
from weaver.targets import ItemRef
from weaver.workspaces import Workspace


def _action():
    return InstallAction(
        id="refresh-application-sql-endpoint-Sales",
        kind="refresh_sql_endpoint",
        resource_node_id=None,
        executor="sql_endpoint_refresh",
        payload=None,
        payload_sha256=None,
    )


def _context(resolver):
    bound = BoundTarget(id="lakehouse-Sales", kind="lakehouse", item_id="Sales")
    return InstallationContext(
        resolver=resolver,
        store=FilesystemStore(),
        target=ResolvedTarget(bound=bound, lakehouse=ItemRef("Sales")),
    )


@weaver_test()
def test_executor_performs_the_refresh_selected_by_the_bundle():
    class Resolver:
        def __init__(self):
            self.refreshed = []

        def refresh_sql_endpoint(self, item):
            self.refreshed.append(item.name)
            return {"status": "Succeeded", "lakehouse": item.name}

    resolver = Resolver()

    details = SqlEndpointRefreshExecutor().execute(_action(), None, _context(resolver))

    assert resolver.refreshed == ["Sales"]
    assert details == {"status": "Succeeded", "lakehouse": "Sales"}


class _RefreshClient:
    def __init__(self, response):
        self.response = response
        self.requests = []

    def request(self, method, path, *, payload, expected):
        self.requests.append((method, path, payload, expected))
        return self.response

    def wait_for_operation(self, response):
        assert response is self.response
        return {"status": "Succeeded"}


@weaver_test()
def test_fabric_refresh_posts_the_endpoint_action_and_awaits_it():
    response = SimpleNamespace(
        status_code=202,
        content=b"",
        headers={"x-ms-operation-id": "operation-id"},
    )
    client = _RefreshClient(response)
    endpoint = Item(
        id="endpoint-id",
        name="Sales",
        type=SQL_ENDPOINT,
        workspace_id="workspace-id",
    )

    result = refresh_sql_endpoint_metadata(endpoint, client=client)

    assert client.requests == [
        (
            "POST",
            "workspaces/workspace-id/sqlEndpoints/endpoint-id/refreshMetadata",
            {"recreateTables": False},
            (200, 202),
        )
    ]
    assert result == {
        "lakehouse": "Sales",
        "sql_endpoint_id": "endpoint-id",
        "operation_id": "operation-id",
        "status": "Succeeded",
    }


@weaver_test()
def test_fabric_resolver_uses_the_typed_endpoint_paired_with_the_lakehouse():
    response = SimpleNamespace(status_code=200, content=b"{}", headers={})

    class Client(_RefreshClient):
        def paged(self, path):
            if path == "workspaces":
                return [{"id": "workspace-id", "displayName": "Analytics"}]
            assert path == "workspaces/workspace-id/items?type=SQLEndpoint"
            return [
                {
                    "id": "endpoint-id",
                    "displayName": "Sales",
                    "type": "SQLEndpoint",
                }
            ]

    client = Client(response)
    resolver = FabricResolver(Workspace(workspace="Analytics"), client=client)

    result = resolver.refresh_sql_endpoint(ItemRef("Sales"))

    assert result["sql_endpoint_id"] == "endpoint-id"
    assert client.requests[0][1] == (
        "workspaces/workspace-id/sqlEndpoints/endpoint-id/refreshMetadata"
    )


@weaver_test()
def test_fabric_client_waits_for_a_long_running_refresh(monkeypatch):
    accepted = SimpleNamespace(
        status_code=202,
        content=b"",
        headers={
            "Location": "https://api.fabric.microsoft.com/v1/operations/op",
            "x-ms-operation-id": "op",
            "Retry-After": "0",
        },
    )
    completed = SimpleNamespace(
        status_code=200,
        content=b"{}",
        headers={},
        json=lambda: {"status": "Succeeded", "percentComplete": 100},
    )
    client = FabricClient(token="token")
    calls = []

    def request(method, path, *, expected):
        calls.append((method, path, expected))
        return completed

    monkeypatch.setattr(client, "request", request)
    monkeypatch.setattr("weaver.fabric.client.time.sleep", lambda _seconds: None)

    result = client.wait_for_operation(accepted)

    assert result["status"] == "Succeeded"
    assert calls == [
        (
            "GET",
            "https://api.fabric.microsoft.com/v1/operations/op",
            (200,),
        )
    ]
