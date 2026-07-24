"""Desktop and Fabric-session authentication remain explicit and separate."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from weaver import FabricHost, WarehouseTarget
from weaver.errors import CommandError
from weaver.fabric.auth import SQL_SCOPE
from weaver.fabric.sql import (
    FABRIC_SQL_AUDIENCE,
    desktop_sql_pool,
    fabric_sql_pool,
)
from weaver.sql import SqlEndpoint


class Resolver:
    def sql_endpoint(self, target):
        return SqlEndpoint("server.example", target.warehouse.name)


def test_desktop_authentication_uses_the_injected_credential_and_sql_scope(monkeypatch):
    credential = object()
    calls = []

    def token(scope, supplied):
        calls.append((scope, supplied))
        return "desktop-token"

    monkeypatch.setattr("weaver.fabric.sql.get_token", token)
    pool = desktop_sql_pool(
        WarehouseTarget.parse("Reporting"),
        FabricHost(workspace="Analytics"),
        resolver=Resolver(),
        credential=credential,
        connection_factory=lambda endpoint, authentication: (
            authentication.connection_arguments(),
            SimpleNamespace(close=lambda: None),
        )[1],
    )
    with pool.lease():
        pass

    assert calls == [(SQL_SCOPE, credential)]
    pool.close()


def test_fabric_authentication_uses_notebookutils_not_the_desktop_chain(monkeypatch):
    from weaver.fabric.session import FabricSessionResolver

    calls = []
    credentials = SimpleNamespace(
        getToken=lambda audience: calls.append(audience) or "fabric-token"
    )
    resolver = object.__new__(FabricSessionResolver)
    resolver._credentials = credentials
    resolver.sql_endpoint = Resolver().sql_endpoint

    pool = fabric_sql_pool(
        WarehouseTarget.parse("Reporting"),
        FabricHost(workspace="Analytics"),
        resolver=resolver,
        credentials=credentials,
        connection_factory=lambda endpoint, authentication: (
            authentication.connection_arguments(),
            SimpleNamespace(close=lambda: None),
        )[1],
    )
    with pool.lease():
        pass

    assert calls == [FABRIC_SQL_AUDIENCE]
    pool.close()


def test_fabric_authentication_fails_clearly_outside_fabric():
    with pytest.raises(CommandError, match="only inside"):
        fabric_sql_pool(
            WarehouseTarget.parse("Reporting"),
            FabricHost(workspace="Analytics"),
        )
