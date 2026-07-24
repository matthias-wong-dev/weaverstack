"""Typed Warehouse resolution to the common SQL endpoint."""

from __future__ import annotations

from types import SimpleNamespace

from weaver import FabricHost, WarehouseTarget
from weaver.fabric import FabricResolver, FabricSessionResolver


class Client:
    def __init__(self):
        self.json_paths = []

    def paged(self, path):
        if path == "workspaces":
            return [{"id": "workspace-id", "displayName": "Analytics"}]
        assert path == "workspaces/workspace-id/items?type=Warehouse"
        return [
            {
                "id": "warehouse-id",
                "displayName": "Reporting",
                "type": "Warehouse",
            }
        ]

    def get_json(self, path):
        self.json_paths.append(path)
        return {
            "connectionString": (
                "Server=tcp:warehouse.fabric.microsoft.com,1433;"
                "Encrypt=yes;TrustServerCertificate=no;"
            )
        }


def test_desktop_resolution_uses_the_typed_connection_string_endpoint():
    client = Client()
    resolver = FabricResolver(
        FabricHost(workspace="Analytics"),
        client=client,
    )

    endpoint = resolver.sql_endpoint(WarehouseTarget.parse("Reporting"))

    assert endpoint.server == "warehouse.fabric.microsoft.com"
    assert endpoint.database == "Reporting"
    assert endpoint.workspace_id == "workspace-id"
    assert endpoint.warehouse_id == "warehouse-id"
    assert client.json_paths == [
        "workspaces/workspace-id/warehouses/warehouse-id/connectionString"
    ]


def test_session_resolution_uses_session_context_and_session_authenticated_rest():
    client = Client()
    runtime = SimpleNamespace(
        context={
            "currentWorkspaceName": "Analytics",
            "currentWorkspaceId": "workspace-id",
        }
    )
    resolver = FabricSessionResolver(
        FabricHost(workspace="Analytics"),
        runtime=runtime,
        lakehouse=object(),
        client=client,
    )

    endpoint = resolver.sql_endpoint(WarehouseTarget.parse("Reporting"))

    assert endpoint.pool_key[:2] == ("workspace-id", "warehouse-id")
    assert endpoint.server == "warehouse.fabric.microsoft.com"
