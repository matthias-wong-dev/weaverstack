"""Semantic execution is resolved and owned by the Session."""

from types import SimpleNamespace

import pytest
from support.weaver_test import weaver_test

from weaver.fabric.auth import POWER_BI_SCOPE
from weaver.fabric.resolution import FabricResolver
from weaver.sessions.console import ConsoleSession
from weaver.workspaces import Workspace


@weaver_test()
def test_notebook_semantic_client_uses_native_power_bi_token_without_spark(monkeypatch):
    import sys

    from weaver.fabric.session import FabricSessionResolver
    from weaver.sessions.notebook import NotebookSession

    audiences = []
    credentials = SimpleNamespace(
        getToken=lambda audience: audiences.append(audience) or "native-token"
    )
    monkeypatch.setitem(
        sys.modules, "notebookutils", SimpleNamespace(credentials=credentials)
    )
    workspace = Workspace(workspace="Analytics")
    inventory = Inventory()
    from weaver.fabric.client import FabricClient

    client = FabricClient(token=lambda: credentials.getToken("pbi"))
    monkeypatch.setattr(client, "paged", inventory.paged)
    resolver = FabricSessionResolver(
        workspace,
        runtime=SimpleNamespace(
            context={
                "currentWorkspaceName": "Analytics",
                "currentWorkspaceId": "workspace-id",
            }
        ),
        lakehouse=object(),
        client=client,
    )
    with NotebookSession(workspace=workspace, resolver=resolver) as session:
        model = session.semantic_model("Reporting")
        assert model.model_id == "model-id"
        model.power_bi.authenticate()
        assert audiences == ["pbi"]
        assert model.power_bi.telemetry is session.telemetry
        assert session.scope(workspace)._spark is None
    assert inventory.paths == ["workspaces/workspace-id/items?type=SemanticModel"]


class Inventory:
    def __init__(self):
        self.paths = []

    def paged(self, path):
        self.paths.append(path)
        if path == "workspaces":
            return [{"id": "workspace-id", "displayName": "Analytics"}]
        return [
            {"id": "warehouse-id", "displayName": "Reporting", "type": "Warehouse"},
            {"id": "model-id", "displayName": "Reporting", "type": "SemanticModel"},
        ]


@weaver_test()
def test_console_semantic_client_reuses_injected_credential_and_typed_resolution():
    scopes = []

    class Credential:
        def get_token(self, scope):
            scopes.append(scope)
            return SimpleNamespace(token="test-token", expires_on=1)

    workspace = Workspace(workspace="Analytics")
    inventory = Inventory()
    resolver = FabricResolver(workspace, client=inventory)
    credential = Credential()
    with ConsoleSession(
        workspace=workspace, resolver=resolver, credential=credential
    ) as session:
        model = session.semantic_model("Reporting")
        assert session.semantic_model("Reporting") is model
        assert model.workspace_id == "workspace-id" and model.model_id == "model-id"
        assert model.fabric is inventory
        model.power_bi.authenticate()
        assert scopes == [POWER_BI_SCOPE]
        assert model.power_bi.telemetry is session.telemetry
        assert model.power_bi._token_source._credential() is credential
        assert not session.scope(workspace).livy.acquired
    assert inventory.paths == [
        "workspaces",
        "workspaces/workspace-id/items?type=SemanticModel",
    ]


@weaver_test()
def test_console_borrowed_resolver_keeps_one_identity_across_api_audiences(monkeypatch):
    import json

    import requests

    from weaver.fabric import auth
    from weaver.fabric.client import FabricClient

    acquired = []

    class Credential:
        def __init__(self, name):
            self.name = name

        def get_token(self, scope):
            acquired.append((self.name, scope))
            return SimpleNamespace(token="test-token", expires_on=1)

    principal = Credential("selected")
    monkeypatch.setattr(auth, "credential", lambda: Credential("ambient"))
    inventory = Inventory()

    def respond(method, url, **kwargs):
        response = requests.Response()
        response.status_code = 200
        path = url.removeprefix("https://api.fabric.microsoft.com/v1/")
        response._content = json.dumps({"value": inventory.paged(path)}).encode()
        return response

    monkeypatch.setattr(requests, "request", respond)
    workspace = Workspace(workspace="Analytics")
    client = FabricClient(token=auth.TokenProvider(auth.FABRIC_SCOPE, principal))
    resolver = FabricResolver(workspace, client=client)
    with ConsoleSession(workspace=workspace, resolver=resolver) as session:
        model = session.semantic_model("Reporting")
        model.power_bi.authenticate()
        assert {name for name, _ in acquired} == {"selected"}
        assert {scope for _, scope in acquired} == {auth.FABRIC_SCOPE, POWER_BI_SCOPE}


@pytest.mark.parametrize("prewarmed", [False, True])
@weaver_test()
def test_console_borrowed_opaque_token_requires_explicit_power_bi_credential(
    monkeypatch, prewarmed
):
    from weaver.errors import CommandError
    from weaver.fabric.client import FabricClient

    workspace = Workspace(workspace="Analytics")
    client = FabricClient(token="opaque-test-token")
    inventory = Inventory()
    monkeypatch.setattr(client, "paged", inventory.paged)
    resolver = FabricResolver(workspace, client=client)
    with ConsoleSession(workspace=workspace, resolver=resolver) as session:
        if prewarmed:
            monkeypatch.setattr("weaver.fabric.auth.credential", lambda: object())
            session.scope(workspace).token_provider()
        with pytest.raises(CommandError, match="credential"):
            session.semantic_model("Reporting")
    assert inventory.paths == []


@weaver_test()
def test_notebook_reuses_resolvers_native_credential_for_power_bi(monkeypatch):
    import sys

    from weaver.fabric.session import FabricSessionResolver
    from weaver.sessions.notebook import NotebookSession

    acquired = []
    selected = SimpleNamespace(
        getToken=lambda scope: acquired.append(("selected", scope)) or "test-token"
    )
    ambient = SimpleNamespace(
        getToken=lambda scope: acquired.append(("ambient", scope)) or "test-token"
    )
    monkeypatch.setitem(
        sys.modules, "notebookutils", SimpleNamespace(credentials=ambient)
    )
    workspace = Workspace(workspace="Analytics")
    resolver = FabricSessionResolver(
        workspace,
        runtime=SimpleNamespace(
            context={
                "currentWorkspaceName": "Analytics",
                "currentWorkspaceId": "workspace-id",
            }
        ),
        lakehouse=object(),
        credentials=selected,
    )
    monkeypatch.setattr(resolver.client, "paged", Inventory().paged)
    with NotebookSession(workspace=workspace, resolver=resolver) as session:
        model = session.semantic_model("Reporting")
        model.fabric.authenticate()
        model.power_bi.authenticate()
    assert acquired == [("selected", "pbi"), ("selected", "pbi")]
