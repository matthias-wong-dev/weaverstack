"""``weaver.initialise()`` reaches Fabric through the Session, as build does.

Inside a Fabric notebook the documented call names no workspace, no client and
no credential, and has to run on the notebook's own identity. Nothing here
reaches Fabric.
"""

from __future__ import annotations

import sys
import types
from types import SimpleNamespace

import pytest
from support.weaver_test import weaver_test

import weaver
from weaver.fabric import resources

WORKSPACE = "Weaver Example"


class _Workspace:
    id = "ws-1"
    name = WORKSPACE


class _Definition:
    """An Environment definition with Weaver already in it."""

    def custom_libraries(self):
        return ()

    def external_libraries(self):
        return "dependencies:\n  - pip:\n      - weaverstack\n"


@pytest.fixture
def notebook(monkeypatch):
    """A process running inside the Fabric workspace it addresses."""

    tokens = []
    module = types.ModuleType("notebookutils")
    module.runtime = SimpleNamespace(
        context={
            "currentWorkspaceName": WORKSPACE,
            "currentWorkspaceId": "ws-1",
        }
    )
    module.lakehouse = SimpleNamespace(
        get=lambda name, *, workspaceId: SimpleNamespace(
            id=f"id-{name}", displayName=name
        )
    )
    module.credentials = SimpleNamespace(
        getToken=lambda audience: tokens.append(audience) or "notebook-token"
    )
    module.fs = SimpleNamespace()
    monkeypatch.setitem(sys.modules, "notebookutils", module)
    for name in ("runtime", "lakehouse", "credentials", "fs"):
        monkeypatch.setitem(sys.modules, f"notebookutils.{name}", getattr(module, name))
    return SimpleNamespace(tokens=tokens)


@pytest.fixture
def fabric(monkeypatch, notebook):
    """A workspace already holding everything, recording the client each
    control-plane helper was handed."""

    seen = []
    held = [
        SimpleNamespace(id="id-Catalogue", name="Catalogue", type=resources.WAREHOUSE),
        SimpleNamespace(id="id-Landing", name="Landing", type=resources.LAKEHOUSE),
        SimpleNamespace(id="id-Curated", name="Curated", type=resources.WAREHOUSE),
        SimpleNamespace(id="id-Weaver", name="Weaver", type=resources.ENVIRONMENT),
    ]

    def find_workspace(name, *, client=None):
        seen.append(client)
        return _Workspace()

    def list_items(workspace, *, item_type=None, client=None):
        seen.append(client)
        return tuple(held)

    monkeypatch.setattr(resources, "find_workspace", find_workspace)
    monkeypatch.setattr(resources, "list_items", list_items)
    monkeypatch.setattr(
        "weaver.fabric.environment.read_definition",
        lambda item, *, client=None: seen.append(client) or _Definition(),
    )
    monkeypatch.setattr(
        "weaver.fabric.environment.publish_state",
        lambda item, *, client=None: seen.append(client) or "Success",
    )
    return SimpleNamespace(seen=seen, held=held)


@pytest.fixture
def no_desktop_credential(monkeypatch):
    def refuse(*arguments, **keywords):
        raise AssertionError(
            "a desktop credential was constructed inside a Fabric session"
        )

    monkeypatch.setattr("azure.identity.DefaultAzureCredential", refuse)
    monkeypatch.setattr("azure.identity.AzureCliCredential", refuse)
    monkeypatch.setattr("azure.identity.InteractiveBrowserCredential", refuse)


def _initialise(tmp_path, **kwargs):
    """The documented notebook call: a repository and item names, and no more."""

    return weaver.initialise(
        tmp_path,
        catalogue="Catalogue",
        environment="Weaver",
        lakehouse="Landing",
        warehouse="Curated",
        **kwargs,
    )


@weaver_test()
def test_a_notebook_run_uses_the_notebook_identity(
    tmp_path, fabric, notebook, no_desktop_credential
):
    report = _initialise(tmp_path)

    assert report.workspace == WORKSPACE
    assert fabric.seen and all(client is not None for client in fabric.seen)
    # A token is asked for per request, so the identity is read off the client.
    assert fabric.seen[0]._token_source() == "notebook-token"
    assert notebook.tokens == ["pbi"]


@weaver_test()
def test_a_notebook_run_selects_the_notebook_session(
    tmp_path, fabric, notebook, monkeypatch
):
    from weaver.sessions.notebook import NotebookSession

    opened = []
    original = NotebookSession.__init__

    def counted(self, *arguments, **keywords):
        opened.append(self)
        original(self, *arguments, **keywords)

    monkeypatch.setattr(NotebookSession, "__init__", counted)

    _initialise(tmp_path)

    assert len(opened) == 1


@weaver_test()
def test_every_helper_is_handed_the_same_client(tmp_path, fabric, notebook):
    _initialise(tmp_path)

    assert len({id(client) for client in fabric.seen}) == 1


@weaver_test()
def test_an_injected_client_still_wins(tmp_path, fabric, notebook):
    supplied = object()

    _initialise(tmp_path, client=supplied)

    assert set(fabric.seen) == {supplied}
    assert notebook.tokens == []
