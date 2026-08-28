"""LivySession attaches its Environment from the workspace, without touching Fabric.

A workspace that names an ``environment`` attaches it; one that does not starts
on the workspace's default runtime, which is all a body of Spark SQL needs.
Starting the session asserts no Weaver install. That is
:meth:`~weaver.fabric.livy.LivySession.ensure_weaver`, submitted by a crossing
that carries a body importing Weaver, and the one place a missing Environment
is refused.

Exercised with a fake resolver, so no workspace or capacity is needed.
"""

from __future__ import annotations

import types

import pytest
from support.weaver_test import weaver_test

from weaver.fabric import client, livy
from weaver.fabric.livy import LivySession, environment_bootstrap
from weaver.fabric.resources import Item
from weaver.workspaces import Workspace


def _spark_workspace(**changes) -> Workspace:
    """A workspace that can start Spark: a catalogue, and a Lakehouse to live in."""

    from weaver.declaration.model import WeaverItemId
    from weaver.workspaces import TargetDeclaration

    return Workspace(
        workspace="WS",
        catalogue="Warehouse/Weaver",
        targets={WeaverItemId.parse("Lakehouse/Sales"): TargetDeclaration("Sales_LH")},
        **changes,
    )


class _FakeResolver:
    def __init__(self):
        self.workspace = types.SimpleNamespace(id="ws1", name="WS")
        self.client = object()

    def resolve(self, item, *, item_type):
        return Item(id="lh1", name=item.name, type=item_type, workspace_id="ws1")

    def spark_root(self, item):
        return "abfss://ws1@onelake.dfs.fabric.microsoft.com/lh1"


@weaver_test()
def test_environment_bootstrap_only_imports_weaver():
    boot = environment_bootstrap()
    assert "import weaver" in boot
    assert "notebookutils" not in boot
    assert "sys.path" not in boot


@weaver_test()
def test_a_workspace_with_an_environment_attaches_it(monkeypatch):
    monkeypatch.setattr(
        "weaver.fabric.resources.find_item",
        lambda ws, name, *, item_type, client: Item("env99", name, item_type, ws.id),
    )
    workspace = _spark_workspace(environment="Weaver")

    session = LivySession.for_workspace(workspace, resolver=_FakeResolver(), token="t")

    assert session.environment_id == "env99"
    # Only the emit helper: a session carries Spark SQL as readily as a program,
    # and asserting the install here would put a publish in front of both.
    assert "import weaver" not in session.bootstrap
    assert "def emit(" in session.bootstrap


@weaver_test()
def test_a_qualified_environment_resolves_in_its_owning_workspace(monkeypatch):
    seen = {}
    monkeypatch.setattr(
        "weaver.fabric.resources.find_workspace",
        lambda name, *, client: types.SimpleNamespace(id="owner1", name=name),
    )

    def find(ws, name, *, item_type, client):
        seen.update(workspace=ws.name, name=name)
        return Item("env99", name, item_type, ws.id)

    monkeypatch.setattr("weaver.fabric.resources.find_item", find)
    workspace = _spark_workspace(environment="Platform/Weaver")

    session = LivySession.for_workspace(workspace, resolver=_FakeResolver(), token="t")

    assert session.environment_id == "env99"
    assert session.environment_reference == "Platform/Weaver"
    assert seen == {"workspace": "Platform", "name": "Weaver"}


@weaver_test()
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
    session = LivySession(
        "ws1", "lh1", token="t", environment_id="env99", bootstrap=None
    )
    session.start()

    create = next(p for m, u, p in calls if m == "POST" and u.endswith("/sessions"))
    assert "environmentId" not in create
    details = json.loads(create["conf"]["spark.fabric.environmentDetails"])
    assert details == {"id": "env99"}


@weaver_test()
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


@weaver_test()
def test_a_workspace_without_an_environment_starts_on_the_default_runtime():
    """A build's statements import nothing, so they need no published wheel.

    Refusing here would put a five-minute publish in front of `weaver build`,
    which submits Spark SQL and never `import weaver`.
    """

    session = LivySession.for_workspace(
        _spark_workspace(), resolver=_FakeResolver(), token="t"
    )

    assert session.environment_id is None


@weaver_test()
def test_importing_weaver_without_an_environment_is_an_error(monkeypatch):
    """The need is stated where it arises: an Environment is what carries Weaver."""

    from weaver.errors import CommandError

    session = LivySession.for_workspace(
        _spark_workspace(), resolver=_FakeResolver(), token="t"
    )
    monkeypatch.setattr(
        type(session), "run", lambda self, code, **kw: pytest.fail("submitted anyway")
    )

    with pytest.raises(CommandError, match="--environment"):
        session.ensure_weaver()


@weaver_test()
def test_a_caller_supplying_its_own_bootstrap_needs_no_environment(monkeypatch):
    """A caller can hand the session the Weaver it should import.

    The pytest harness stages a wheel built from the checkout and passes a
    bootstrap that puts it on ``sys.path``, so a source change reaches Fabric
    for a wheel build and an upload. Publishing takes minutes. The Environment
    still carries the dependencies, and it is no longer what supplies Weaver.
    """

    submitted: list[str] = []
    session = LivySession.for_workspace(
        _spark_workspace(),
        resolver=_FakeResolver(),
        token="t",
        bootstrap="emit = print\n",
        weaver_bootstrap="import weaver_from_a_wheel\n",
    )

    # Start-up code of the caller's own, so the injection reaches bodies that
    # never go near `ensure_weaver`.
    assert session.bootstrap.startswith("emit = print")
    monkeypatch.setattr(
        type(session), "run", lambda self, code, **kw: submitted.append(code)
    )

    session.ensure_weaver()
    session.ensure_weaver()

    assert submitted == ["import weaver_from_a_wheel\n"]


@weaver_test()
def test_a_workspace_configuring_no_lakehouse_cannot_start_spark():
    """Fabric creates a Spark session against a Lakehouse, so one has to exist.

    The catalogue is a Warehouse, so the
    home comes from the workspace's own Lakehouses, and a workspace that
    configures none is doing Warehouse work, which needs no Spark at all.
    """

    from weaver.errors import CommandError

    workspace = Workspace(
        workspace="WS", catalogue="Warehouse/Weaver", environment="Weaver"
    )

    with pytest.raises(CommandError, match="needs a Lakehouse to attach to"):
        LivySession.for_workspace(workspace, resolver=_FakeResolver(), token="t")


# --- a long build outlives a broken connection --------------------------------


@weaver_test()
def test_a_read_is_retried_when_the_connection_fails(monkeypatch):
    """A build polls for as long as it runs, so one refused connection is likely.

    The work being watched is unaffected by a connection that never arrived, so
    asking again gets the answer rather than failing a ten-minute build.
    """

    import requests

    attempts = []

    def flaky(method, url, **kwargs):
        attempts.append(method)
        if len(attempts) < 3:
            raise requests.exceptions.ConnectionError("connection refused")
        return types.SimpleNamespace(
            status_code=200, content=b"{}", json=lambda: {"state": "idle"}
        )

    monkeypatch.setattr(client, "time", types.SimpleNamespace(sleep=lambda _: None))
    monkeypatch.setattr(requests, "request", flaky)

    assert livy._call("GET", "https://example/sessions/1", "t") == {"state": "idle"}
    assert len(attempts) == 3


@weaver_test()
def test_a_submission_is_retried_when_it_never_reached_fabric(monkeypatch):
    """A connection that was never established carries nothing.

    The server has not seen the request, so sending it again cannot run a
    statement twice, which is what makes a refused connection safe to repeat
    for a POST as well as a read.
    """

    import requests
    from urllib3.exceptions import NewConnectionError

    attempts = []

    def flaky(method, url, **kwargs):
        attempts.append(method)
        if len(attempts) < 2:
            raise requests.exceptions.ConnectionError(
                NewConnectionError(None, "connection refused")
            )
        return types.SimpleNamespace(
            status_code=200, content=b"{}", json=lambda: {"id": 7}
        )

    monkeypatch.setattr(client, "time", types.SimpleNamespace(sleep=lambda _: None))
    monkeypatch.setattr(requests, "request", flaky)

    assert livy._call("POST", "https://example/sessions", "t", payload={}) == {"id": 7}
    assert attempts == ["POST", "POST"]


@weaver_test()
def test_a_submission_that_left_this_machine_is_not_retried(monkeypatch):
    """Once the request is on the wire, whether Fabric acted on it is unknowable.

    Sending it again could start a second session or run a statement twice, so
    the failure is reported instead.
    """

    import requests

    attempts = []

    def broke(method, url, **kwargs):
        attempts.append(method)
        raise requests.exceptions.ConnectionError("connection reset")

    monkeypatch.setattr(requests, "request", broke)

    with pytest.raises(livy.LivyError, match="could not be reached"):
        livy._call("POST", "https://example/sessions", "t", payload={})

    assert attempts == ["POST"]


@weaver_test()
def test_a_read_that_keeps_failing_says_so(monkeypatch):
    import requests

    def refused(method, url, **kwargs):
        raise requests.exceptions.ConnectionError("connection refused")

    monkeypatch.setattr(client, "time", types.SimpleNamespace(sleep=lambda _: None))
    monkeypatch.setattr(requests, "request", refused)

    with pytest.raises(livy.LivyError, match="could not be reached"):
        livy._call("GET", "https://example/sessions/1", "t")
