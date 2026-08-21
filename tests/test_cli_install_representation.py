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
    assert command_requirements(parsed)


@weaver_test()
def test_old_environment_install_surface_is_not_accepted(capsys):
    with pytest.raises(SystemExit):
        build_parser().parse_args(["install", "--environment", "Runtime"])
    assert "BUNDLE" in capsys.readouterr().err


@weaver_test()
def test_environment_publish_has_its_own_fabric_surface():
    parsed = build_parser().parse_args(
        ["fabric", "environment", "publish", "Runtime", "--workspace", "Sales"]
    )

    assert parsed.environment == "Runtime"
    assert parsed.workspace == "Sales"


@weaver_test()
def test_bundle_install_passes_the_resolved_workspace_to_core(monkeypatch):
    cli = import_module("weaver_cli.main")
    from weaver.workspaces import Workspace

    workspace = Workspace(workspace="Sales")
    monkeypatch.setattr(cli, "_resolve_workspace", lambda args: workspace)
    monkeypatch.setattr(cli.weaver, "install", lambda *args, **kwargs: _Report())

    args = build_parser().parse_args(["install", "handover", "--workspace", "Sales"])
    assert cli.handle_install(args) == 0


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
