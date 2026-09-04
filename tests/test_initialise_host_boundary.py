"""``weaver.initialise()`` reaches Fabric through the Session, as build does.

The documented notebook call names no workspace, no client and no credential:

.. code-block:: python

    weaver.initialise(
        Path("builtin") / "repository",
        catalogue="Catalogue",
        environment="Weaver",
        lakehouse="Landing",
        warehouse="Curated",
        example=True,
    )

Inside Fabric that has to run on the notebook's own identity. It reached
``DefaultAzureCredential`` instead, because the session resolver's REST client
was built on first use and ``initialise`` read the raw field before anything had
built one. It passed the ``None`` on, and each Fabric resource helper answers a
``None`` with a plain ``FabricClient``.

So the claim here is the boundary rather than the credential: `initialise` asks
the resolver for its REST capability, and inside Fabric the resolver answers
with the notebook's identity. Nothing here reaches Fabric.
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
    """A process running inside the Fabric workspace it addresses.

    ``notebookutils`` is what says so, and every host decision below reads it:
    the workspace `initialise` discovers, the Session `session_for` selects, and
    the identity the session resolver's REST client carries.
    """

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
    """A workspace already holding everything, so nothing is created.

    Each helper records the client it was handed, which is the subject: a
    ``None`` reaching one of these is what built a desktop credential inside
    Fabric.
    """

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
    """Anything reaching for the library default fails the test where it did."""

    def refuse(*arguments, **keywords):
        raise AssertionError(
            "DefaultAzureCredential was constructed inside a Fabric session"
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
    """The regression, as a user meets it: no workspace, no client, no credential."""

    report = _initialise(tmp_path)

    assert report.workspace == WORKSPACE
    assert fabric.seen, "no Fabric control-plane read happened"
    assert all(client is not None for client in fabric.seen), (
        "a helper was handed None and built a client of its own"
    )
    # The client is built once and asks for a token per request, so the identity
    # it carries is read from it rather than from a crossing nothing made here.
    assert fabric.seen[0]._token_source() == "notebook-token"
    assert notebook.tokens == ["pbi"]


@weaver_test()
def test_a_notebook_run_selects_the_notebook_session(
    tmp_path, fabric, notebook, monkeypatch
):
    """`initialise` opens the Session the host offers, as build and load do."""

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
def test_every_helper_is_handed_the_session_resolver_client(tmp_path, fabric, notebook):
    """One client for the whole run, so what it asked of Fabric is counted
    against the Session that owns it."""

    _initialise(tmp_path)

    assert len({id(client) for client in fabric.seen}) == 1


@weaver_test()
def test_an_injected_client_still_wins(tmp_path, fabric, notebook):
    """A caller who supplied one is not overridden by the host's."""

    supplied = object()

    _initialise(tmp_path, client=supplied)

    assert set(fabric.seen) == {supplied}
    assert notebook.tokens == [], "a session client was built beside the injected one"
