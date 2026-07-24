"""LivySession chooses its bootstrap from the host, without touching Fabric.

A host that names a ``fabric_environment`` attaches that Environment and boots
with a plain ``import weaver``; a host without one falls back to shipping the
package into the Lakehouse. Both branches are exercised here with a fake
resolver, so no workspace or capacity is needed.
"""

from __future__ import annotations

import types

import pytest

from weaver import FabricHost
from weaver.fabric import livy
from weaver.fabric.livy import LivySession, environment_bootstrap
from weaver.fabric.resources import Item


class _FakeResolver:
    def __init__(self):
        self.workspace = types.SimpleNamespace(id="ws1", name="WS")
        self.client = object()

    def resolve(self, item, *, item_type):
        return Item(id="lh1", name=item.name, type=item_type, workspace_id="ws1")

    def spark_root(self, item):
        return "abfss://ws1@onelake.dfs.fabric.microsoft.com/lh1"


def test_environment_bootstrap_only_imports_weaver():
    boot = environment_bootstrap()
    assert "import weaver" in boot
    assert "notebookutils" not in boot
    assert "sys.path" not in boot


def test_a_host_with_an_environment_attaches_it(monkeypatch):
    monkeypatch.setattr(
        "weaver.fabric.resources.find_item",
        lambda ws, name, *, item_type, client: Item("env99", name, item_type, ws.id),
    )
    host = FabricHost(workspace="WS", weaver_lakehouse="Weaver", fabric_environment="Weaver")

    session = LivySession.for_host(host, resolver=_FakeResolver(), token="t")

    assert session.environment_id == "env99"
    assert "import weaver" in session.bootstrap
    assert "notebookutils" not in session.bootstrap


def test_start_attaches_the_environment_as_a_spark_conf(monkeypatch):
    import json

    calls = []

    def fake_call(method, url, token, payload=None, expected=(200, 201, 202)):
        calls.append((method, url, payload))
        if method == "POST" and url.endswith("/sessions"):
            return {"id": 7}
        if method == "GET":
            return {"state": "idle"}
        return {}

    monkeypatch.setattr(livy, "_call", fake_call)
    session = LivySession("ws1", "lh1", token="t", environment_id="env99", bootstrap=None)
    session.start()

    create = next(p for m, u, p in calls if m == "POST" and u.endswith("/sessions"))
    assert "environmentId" not in create
    details = json.loads(create["conf"]["spark.fabric.environmentDetails"])
    assert details == {"id": "env99"}


def test_start_without_an_environment_sends_no_conf(monkeypatch):
    calls = []

    def fake_call(method, url, token, payload=None, expected=(200, 201, 202)):
        calls.append((method, url, payload))
        if method == "POST":
            return {"id": 7}
        return {"state": "idle"}

    monkeypatch.setattr(livy, "_call", fake_call)
    session = LivySession("ws1", "lh1", token="t", bootstrap=None)
    session.start()

    create = next(p for m, u, p in calls if m == "POST" and u.endswith("/sessions"))
    assert "conf" not in create


def test_a_host_without_an_environment_is_an_error():
    from weaver.errors import CommandError

    host = FabricHost(workspace="WS", weaver_lakehouse="Weaver")

    with pytest.raises(CommandError, match="fabric_environment"):
        LivySession.for_host(host, resolver=_FakeResolver(), token="t")
