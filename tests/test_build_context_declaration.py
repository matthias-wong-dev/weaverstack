"""How `weaver.build` decides which workspace and Weaver catalogue it means.

The public build has to work from a Fabric notebook, where a caller reasonably
supplies only a repository and its bindings, and from a desktop, where nothing
can be inferred and a missing value has to be said out loud. One resolution
order serves both:

    explicit argument → typed Workspace → configuration → notebook context

The part worth testing is the *precedence*, because every step of it is a value
that would otherwise be plausible. A configured catalogue and an attached
default Lakehouse are both real Lakehouses; picking the wrong one writes a
catalogue into somewhere that works, and is wrong in a way nothing complains
about.

The other half is where failure lands. Missing context must be a Weaver sentence
before any session starts, not a Py4J traceback from inside one.
"""

from __future__ import annotations

import sys
import types

import pytest

import weaver
import weaver.operations.build
import weaver.operations.workspace
from weaver.errors import BuildError, CommandError
from weaver.workspaces import Workspace


@pytest.fixture
def in_notebook(monkeypatch):
    """A Fabric session whose context names a workspace and a default Lakehouse."""

    module = types.ModuleType("notebookutils")
    module.runtime = types.SimpleNamespace(
        context={
            "currentWorkspaceName": "Analytics",
            "currentWorkspaceId": "ws-id",
            "defaultLakehouseId": "lh-id",
            "defaultLakehouseName": "AttachedWeaver",
        }
    )
    monkeypatch.setitem(sys.modules, "notebookutils", module)
    return module


class Halt(Exception):
    """Raised in place of doing the build, once resolution has happened."""


@pytest.fixture
def captured(monkeypatch):
    """Stop each build at the platform seam and report what it resolved."""

    seen = {}

    def capture(name):
        def _stop(workspace, **kwargs):
            seen["mode"] = name
            seen["workspace"] = workspace
            seen["bindings"] = kwargs.get("bindings")
            raise Halt(name)

        return _stop

    # One seam, because there is one build: what the Session answers is what
    # differs between a notebook and a desktop, not which algorithm runs.
    monkeypatch.setattr(weaver.operations.build, "_run_build", capture("build"))
    # Preflight is a different claim — that a build proves its items exist before
    # opening anything — and has its own tests below.
    monkeypatch.setattr(weaver.operations.build, "_preflight", lambda *a, **k: None)
    return seen


@pytest.fixture
def repository(tmp_path):
    from test_item_repository_declaration import _schema, _table, _write

    root = tmp_path / "Estate"
    _write(root, "Lakehouse/Sales/schemas/DWG.yml", _schema("DWG"))
    _write(root, "Lakehouse/Sales/DWG__Customer.py", _table("DWG.Customer"))
    return root


def _build(repository, **kwargs):
    return weaver.build(str(repository), bind="Lakehouse/Sales_LH=Sales", **kwargs)


# --- notebook inference -------------------------------------------------------


def test_a_notebook_infers_the_current_workspace(in_notebook, captured, repository):
    """The catalogue is given so that only the workspace is in question."""

    with pytest.raises(Halt):
        _build(repository, catalogue="Warehouse/Weaver")

    assert captured["workspace"].workspace == "Analytics"


# --- explicit values win ------------------------------------------------------


def test_an_operation_given_a_resolved_workspace_says_to_open_a_session(
    repository, tmp_path
):
    """Operations take names. A resolved Workspace goes through a Session.

    One way in rather than two: a Workspace argument and a Session argument
    would both carry a context, and an operation given each would have to pick
    between them.
    """

    workspace = Workspace(workspace="Demo", catalogue="Warehouse/Configured")

    with pytest.raises(CommandError, match="weaver.session"):
        _build(repository, workspace=workspace)


def test_an_explicit_catalogue_overrides_the_sessions_workspace(
    captured, repository, desktop_credential
):
    """The Session supplies the context; an argument still outranks it."""

    with weaver.session(workspace="Demo", catalogue="Warehouse/Configured") as session:
        with pytest.raises(Halt):
            _build(repository, session=session, catalogue="Warehouse/Chosen")

    assert captured["workspace"].catalogue == "Warehouse/Chosen"


def test_the_sessions_workspace_supplies_the_catalogue_when_no_argument_does(
    captured, repository, desktop_credential
):
    with weaver.session(workspace="Demo", catalogue="Warehouse/Configured") as session:
        with pytest.raises(Halt):
            _build(repository, session=session)

    assert captured["workspace"].catalogue == "Warehouse/Configured"


def test_a_desktop_caller_needs_no_workspace_object(captured, repository, tmp_path):
    """`workspace=` and `catalogue=` alone are a complete desktop context."""

    with pytest.raises(Halt):
        _build(repository, workspace="Analytics", catalogue="Warehouse/Weaver")

    assert captured["mode"] == "build"
    assert captured["workspace"].workspace == "Analytics"
    assert captured["workspace"].catalogue == "Warehouse/Weaver"


# --- and missing context is a sentence ----------------------------------------


def test_no_context_outside_fabric_names_what_to_supply(repository, monkeypatch):
    monkeypatch.delitem(sys.modules, "notebookutils", raising=False)
    monkeypatch.setattr(
        weaver.operations.workspace,
        "_current_fabric_workspace",
        lambda: (_ for _ in ()).throw(
            CommandError("give workspace or workspace_config outside a Fabric notebook")
        ),
    )

    with pytest.raises(CommandError, match="outside a Fabric notebook"):
        _build(repository)


def test_a_workspace_without_a_catalogue_says_both_ways_to_give_one(
    repository, tmp_path
):
    with pytest.raises(CommandError) as raised:
        _build(repository, workspace="Demo")

    message = str(raised.value)
    assert "catalogue=" in message
    assert "workspace configuration" in message


def test_a_resolved_workspace_and_a_configuration_file_is_refused_by_the_session(
    tmp_path, desktop_credential
):
    """The same rule, now stated once where a Workspace is accepted at all."""

    config = tmp_path / "ws.yml"
    config.write_text("workspace: Other\n", encoding="utf-8")

    with pytest.raises(CommandError, match="nothing to add"):
        weaver.session(
            workspace=Workspace(workspace="Demo", catalogue="Warehouse/Weaver"),
            workspace_config=config,
        )


# --- the Livy session is never reached by a build that cannot succeed ----------


def test_a_failed_preflight_does_not_create_a_livy_session(
    repository, monkeypatch, tmp_path
):
    """The whole point of preflight, stated as the call that must not happen."""

    from weaver.fabric import preflight as preflight_module

    def refuse(*args, **kwargs):
        raise preflight_module.PreflightError(
            "Fabric build preflight failed in workspace 'Analytics':\n"
            "- Weaver catalogue 'Weaver' was not found"
        )

    monkeypatch.setattr(preflight_module, "preflight_fabric_targets", refuse)

    import weaver.fabric as fabric

    def explode(*args, **kwargs):
        raise AssertionError("a Livy session was created after preflight failed")

    monkeypatch.setattr(fabric.LivySession, "for_workspace", explode)

    with pytest.raises(preflight_module.PreflightError, match="was not found"):
        _build(
            repository,
            workspace="Analytics",
            catalogue="Warehouse/Weaver",
            environment="WeaverEnv",
        )


def test_a_desktop_build_needs_no_environment(repository, monkeypatch):
    """A build's Spark SQL imports nothing, so it needs no published wheel.

    Refusing here put a publish in front of every build, including a
    Warehouse-only one that starts no Spark session at all.
    """

    from weaver.fabric import preflight as preflight_module

    seen = {}

    def record(*args, **kwargs):
        seen.update(kwargs)
        raise Halt()

    monkeypatch.setattr(preflight_module, "preflight_fabric_targets", record)

    with pytest.raises(Halt):
        _build(repository, workspace="Analytics", catalogue="Warehouse/Weaver")

    assert seen["environment"] is None


def test_a_repository_error_is_reported_before_any_fabric_call(tmp_path, monkeypatch):
    """Repository errors come first: they need no workspace to be true."""

    from weaver.fabric import preflight as preflight_module

    def explode(*args, **kwargs):
        raise AssertionError("Fabric was contacted before the repository parsed")

    monkeypatch.setattr(preflight_module, "preflight_fabric_targets", explode)

    empty = tmp_path / "Empty"
    empty.mkdir()

    with pytest.raises(BuildError):
        weaver.build(
            str(empty),
            bind="Lakehouse/Sales_LH=Sales",
            workspace="Analytics",
            catalogue="Warehouse/Weaver",
            environment="WeaverEnv",
        )
