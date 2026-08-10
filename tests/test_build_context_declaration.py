"""How `weaver.build` decides which workspace and Weaver Lakehouse it means.

The public build has to work from a Fabric notebook, where a caller reasonably
supplies only a repository and its bindings, and from a desktop, where nothing
can be inferred and a missing value has to be said out loud. One resolution
order serves both:

    explicit argument → typed Workspace → configuration → notebook context

The part worth testing is the *precedence*, because every step of it is a value
that would otherwise be plausible. A configured Weaver Lakehouse and an attached
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
from weaver.errors import BuildError, CommandError
from weaver.workspaces import FabricWorkspace, LocalWorkspace


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

    # Two seams rather than three: the emulator and a Fabric notebook are both
    # "this process is already where the data is", and the Session is what makes
    # them one path.
    monkeypatch.setattr(weaver.operations, "_build_in_process", capture("in_process"))
    monkeypatch.setattr(weaver.operations, "_build_desktop_fabric", capture("desktop"))
    return seen


@pytest.fixture
def repository(tmp_path):
    from test_item_repository import _schema, _table, _write

    root = tmp_path / "Estate"
    _write(root, "Lakehouse/Sales/schemas/DWG.yml", _schema("DWG"))
    _write(root, "Lakehouse/Sales/DWG__Customer.py", _table("DWG.Customer"))
    return root


def _build(repository, **kwargs):
    return weaver.build(str(repository), bind="Lakehouse/Sales_LH=Lakehouse/Sales", **kwargs)


# --- notebook inference -------------------------------------------------------


def test_a_notebook_infers_the_current_workspace(in_notebook, captured, repository):
    """The Weaver Lakehouse is given so that only the workspace is in question."""

    with pytest.raises(Halt):
        _build(repository, weaver_lakehouse="Weaver")

    assert captured["workspace"].workspace == "Analytics"


def test_a_notebook_infers_the_attached_lakehouse_as_the_weaver_lakehouse(
    in_notebook, captured, repository, monkeypatch
):
    monkeypatch.setattr(
        weaver.operations, "_active_spark", lambda: object()
    )

    with pytest.raises(Halt):
        _build(repository)

    assert captured["workspace"].weaver_lakehouse == "AttachedWeaver"


def test_the_inferred_lakehouse_is_the_control_plane_and_not_an_authored_target(
    in_notebook, captured, repository, monkeypatch
):
    """The attachment names where the catalogue lives, not what to build into."""

    monkeypatch.setattr(weaver.operations, "_active_spark", lambda: object())

    with pytest.raises(Halt):
        _build(repository)

    bound = {str(binding.item) for binding in captured["bindings"].entries}
    targets = {
        binding.target.lakehouse.name
        for binding in captured["bindings"].entries
        if hasattr(binding.target, "lakehouse")
    }
    assert bound == {"Lakehouse/Sales", "Lakehouse/_weaver"}
    assert "AttachedWeaver" in targets, "only as the control binding"
    assert "Lakehouse/AttachedWeaver" not in bound


# --- explicit values win ------------------------------------------------------


def test_an_explicit_weaver_lakehouse_overrides_the_attached_default(
    in_notebook, captured, repository, monkeypatch
):
    monkeypatch.setattr(weaver.operations, "_active_spark", lambda: object())

    with pytest.raises(Halt):
        _build(repository, weaver_lakehouse="ChosenWeaver")

    assert captured["workspace"].weaver_lakehouse == "ChosenWeaver"


def test_an_explicit_weaver_lakehouse_overrides_a_typed_workspace(
    captured, repository, tmp_path
):
    """A typed Workspace is already resolved; an argument still outranks it."""

    workspace = LocalWorkspace(workspace=tmp_path / "ws", weaver_lakehouse="Configured")

    with pytest.raises(Halt):
        _build(repository, workspace=workspace, weaver_lakehouse="Chosen")

    assert captured["workspace"].weaver_lakehouse == "Chosen"


def test_a_typed_workspace_supplies_the_weaver_lakehouse_when_no_argument_does(
    captured, repository, tmp_path
):
    workspace = LocalWorkspace(workspace=tmp_path / "ws", weaver_lakehouse="Configured")

    with pytest.raises(Halt):
        _build(repository, workspace=workspace)

    assert captured["workspace"].weaver_lakehouse == "Configured"


def test_a_desktop_caller_needs_no_workspace_object(captured, repository, tmp_path):
    """`workspace=` and `weaver_lakehouse=` alone are a complete desktop context."""

    with pytest.raises(Halt):
        _build(repository, workspace="Analytics", weaver_lakehouse="Weaver")

    assert captured["mode"] == "desktop"
    assert captured["workspace"].workspace == "Analytics"
    assert captured["workspace"].weaver_lakehouse == "Weaver"


# --- and missing context is a sentence ----------------------------------------


def test_no_context_outside_fabric_names_what_to_supply(repository, monkeypatch):
    monkeypatch.delitem(sys.modules, "notebookutils", raising=False)
    monkeypatch.setattr(
        weaver.operations,
        "_current_fabric_workspace",
        lambda: (_ for _ in ()).throw(
            CommandError("give workspace or workspace_config outside a Fabric notebook")
        ),
    )

    with pytest.raises(CommandError, match="outside a Fabric notebook"):
        _build(repository)


def test_a_workspace_without_a_weaver_lakehouse_says_all_three_ways_to_give_one(
    repository, tmp_path
):
    workspace = LocalWorkspace(workspace=tmp_path / "ws")

    with pytest.raises(CommandError) as raised:
        _build(repository, workspace=workspace)

    message = str(raised.value)
    assert "weaver_lakehouse=" in message
    assert "workspace configuration" in message
    assert "default Lakehouse" in message


def test_configuration_cannot_be_layered_over_an_already_resolved_workspace(
    repository, tmp_path
):
    workspace = LocalWorkspace(workspace=tmp_path / "ws", weaver_lakehouse="Weaver")

    with pytest.raises(CommandError, match="already resolved Workspace"):
        _build(repository, workspace=workspace, workspace_config=tmp_path / "ws.yml")


# --- the Livy session is never reached by a build that cannot succeed ----------


def test_a_failed_preflight_does_not_create_a_livy_session(
    repository, monkeypatch, tmp_path
):
    """The whole point of preflight, stated as the call that must not happen."""

    from weaver.fabric import preflight as preflight_module

    def refuse(*args, **kwargs):
        raise preflight_module.PreflightError(
            "Fabric build preflight failed in workspace 'Analytics':\n"
            "- Weaver Lakehouse 'Weaver' was not found"
        )

    monkeypatch.setattr(preflight_module, "preflight_fabric_targets", refuse)

    import weaver.fabric as fabric

    def explode(*args, **kwargs):
        raise AssertionError("a Livy session was created after preflight failed")

    monkeypatch.setattr(fabric.LivySession, "for_workspace", explode)

    workspace = FabricWorkspace(
        workspace="Analytics", weaver_lakehouse="Weaver", environment="WeaverEnv"
    )

    with pytest.raises(preflight_module.PreflightError, match="was not found"):
        _build(repository, workspace=workspace)


def test_a_desktop_build_without_an_environment_fails_before_preflight(
    repository, monkeypatch
):
    from weaver.fabric import preflight as preflight_module

    def explode(*args, **kwargs):
        raise AssertionError("preflight ran without an Environment to check")

    monkeypatch.setattr(preflight_module, "preflight_fabric_targets", explode)

    workspace = FabricWorkspace(workspace="Analytics", weaver_lakehouse="Weaver")

    with pytest.raises(CommandError, match="requires an Environment"):
        _build(repository, workspace=workspace)


def test_a_repository_error_is_reported_before_any_fabric_call(
    tmp_path, monkeypatch
):
    """Repository errors come first: they need no workspace to be true."""

    from weaver.fabric import preflight as preflight_module

    def explode(*args, **kwargs):
        raise AssertionError("Fabric was contacted before the repository parsed")

    monkeypatch.setattr(preflight_module, "preflight_fabric_targets", explode)

    empty = tmp_path / "Empty"
    empty.mkdir()
    workspace = FabricWorkspace(
        workspace="Analytics", weaver_lakehouse="Weaver", environment="WeaverEnv"
    )

    with pytest.raises(BuildError):
        weaver.build(
            str(empty),
            bind="Lakehouse/Sales_LH=Lakehouse/Sales",
            workspace=workspace,
        )
