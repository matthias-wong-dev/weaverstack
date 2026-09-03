"""The two meanings that were previously both called ``install``."""

from __future__ import annotations

from importlib import import_module

import pytest
from support.weaver_test import weaver_test

from weaver_cli.main import (
    build_parser,
    command_requirements,
    handle_environment_publish,
)


@weaver_test()
def test_install_requires_a_bundle_and_accepts_workspace_configuration():
    parsed = build_parser().parse_args(["install", "handover", "--workspace", "Sales"])

    assert parsed.bundle == "handover"
    assert parsed.workspace == "Sales"
    assert not hasattr(parsed, "environment")
    assert not hasattr(parsed, "catalogue")
    assert command_requirements(parsed)


@weaver_test()
@pytest.mark.parametrize("option", ["--environment", "--catalogue"])
def test_bundle_install_does_not_accept_deployment_configuration(option, capsys):
    with pytest.raises(SystemExit):
        build_parser().parse_args(["install", "handover", option, "Runtime"])
    assert f"unrecognized arguments: {option} Runtime" in capsys.readouterr().err


@weaver_test()
def test_environment_publish_has_its_own_fabric_surface():
    parsed = build_parser().parse_args(
        ["fabric", "environment", "publish", "Runtime", "--workspace", "Sales"]
    )

    assert parsed.environment_ref == "Runtime"
    assert parsed.workspace == "Sales"


@weaver_test()
def test_environment_publish_accepts_a_qualified_reference_without_workspace():
    parsed = build_parser().parse_args(
        ["fabric", "environment", "publish", "Platform/Runtime"]
    )
    assert parsed.environment_ref == "Platform/Runtime"
    assert parsed.workspace is None


@weaver_test()
def test_bundle_install_passes_the_resolved_workspace_to_core(monkeypatch):
    cli = import_module("weaver_cli.main")
    import weaver.operations.install as install_operation
    from weaver.workspaces import Workspace

    seen = {}
    workspace = Workspace(workspace="Sales")
    monkeypatch.setattr(cli, "_resolve_workspace", lambda args: workspace)
    monkeypatch.setattr(
        install_operation,
        "install",
        lambda bundle, **kwargs: seen.update(bundle=bundle, **kwargs) or _Report(),
    )

    args = build_parser().parse_args(["install", "handover", "--workspace", "Sales"])
    assert cli.handle_install(args) == 0
    assert seen == {"bundle": "handover", "workspace": "Sales", "session": None}


class _Report:
    status = "succeeded"
    bundle_id = "bundle"
    succeeded = True

    def to_mapping(self):
        return {"status": self.status, "bundle_id": self.bundle_id}


class _Result:
    workspace_name = "Sales"

    def as_dict(self):
        return {"environment_name": "Runtime", "published": True, "timings": {}}


class _RecordingSession:
    closed = False

    def __init__(self):
        self.opened = []

    def _frame(self, kind, name):
        session = self

        class _Frame:
            def __enter__(self):
                session.opened.append((kind, name))
                return self

            def __exit__(self, *exc):
                return False

        return _Frame()

    def task(self, name, detail=None):
        return self._frame("task", name)

    def step(self, name, detail=None):
        return self._frame("step", name)


@weaver_test()
def test_environment_publish_prints_its_result(monkeypatch, capsys):
    cli = import_module("weaver_cli.main")
    from weaver.workspaces import Workspace

    workspace = Workspace(workspace="Sales", environment="Runtime")
    monkeypatch.setattr(cli, "_resolve_workspace", lambda args: workspace)
    monkeypatch.setattr(cli, "_prefer_desktop_credential", lambda: None)
    monkeypatch.setattr(cli, "_session", lambda args: _RecordingSession())

    import weaver.fabric as fabric

    monkeypatch.setattr(fabric, "publish_environment", lambda *a, **k: _Result())

    args = build_parser().parse_args(
        ["fabric", "environment", "publish", "Runtime", "--workspace", "Sales"]
    )
    assert handle_environment_publish(args) == 0
    assert '"published": true' in capsys.readouterr().out


@weaver_test()
def test_environment_publish_rejects_a_conflicting_workspace(monkeypatch):
    cli = import_module("weaver_cli.main")
    from weaver.errors import CommandError
    from weaver.workspaces import Workspace

    monkeypatch.setattr(
        cli, "_resolve_workspace", lambda args: Workspace(workspace="Sales")
    )
    args = build_parser().parse_args(
        [
            "fabric",
            "environment",
            "publish",
            "Platform/Runtime",
            "--workspace",
            "Sales",
        ]
    )
    with pytest.raises(CommandError, match="conflicts"):
        handle_environment_publish(args)


# --- the two switches ----------------------------------------------------------


@weaver_test()
def test_a_path_and_a_named_environment_are_not_both_given(monkeypatch):
    """``--path`` names the Environment through its directory."""

    from weaver.errors import CommandError
    from weaver_cli.main import build_parser, handle_environment_publish

    args = build_parser().parse_args(
        ["fabric", "environment", "publish", "Runtime", "--path", "x/R.Environment"]
    )

    with pytest.raises(CommandError, match="is not given as well"):
        handle_environment_publish(args)


@weaver_test()
def test_publishing_needs_a_name_or_a_path():
    from weaver.errors import CommandError
    from weaver_cli.main import build_parser, handle_environment_publish

    args = build_parser().parse_args(["fabric", "environment", "publish"])

    with pytest.raises(CommandError, match="or pass --path"):
        handle_environment_publish(args)


@weaver_test()
def test_a_path_reaches_the_operation_with_the_mode(monkeypatch, tmp_path, capsys):
    """The directory names the Environment, and workspace configuration does not.

    The workspace here is configured for ``Runtime``; the definition publishes
    ``Sales``, and the two are not required to agree.
    """

    cli = import_module("weaver_cli.main")
    from weaver.workspaces import Workspace
    from weaver_cli.main import build_parser, handle_environment_publish

    definition = tmp_path / "Sales.Environment"
    definition.mkdir()
    seen = {}

    workspace = Workspace(workspace="Analytics", environment="Runtime")
    monkeypatch.setattr(cli, "_resolve_workspace", lambda args: workspace)
    monkeypatch.setattr(cli, "_prefer_desktop_credential", lambda: None)
    monkeypatch.setattr(cli, "_session", lambda args: _RecordingSession())

    import weaver.fabric as fabric

    def publish(workspace_name, environment=None, **keywords):
        seen.update(keywords, workspace_name=workspace_name, environment=environment)
        return _Result()

    monkeypatch.setattr(fabric, "publish_environment", publish)

    args = build_parser().parse_args(
        ["fabric", "environment", "publish", "--path", str(definition), "--dev"]
    )

    assert handle_environment_publish(args) == 0
    assert seen["path"] == str(definition)
    assert seen["dev"] is True
    assert seen["environment"] is None
    assert seen["workspace_name"] == "Analytics"
