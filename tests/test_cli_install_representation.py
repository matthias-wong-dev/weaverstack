"""What ``weaver install`` offers, and what it always does.

Three properties, and each one exists because the alternative confused somebody.

The command **always publishes**, so there is no flag to stop half way. It
**always prints its result**, because a result is what a command produced rather
than something you opt into. And it offers **no local workspace**, because it
refuses one anyway — a choice presented in order to be rejected is worse than no
choice at all.
"""

from __future__ import annotations

import json
from importlib import import_module

import pytest

from weaver_cli.main import build_parser, handle_install


def _parse(*words):
    return build_parser().parse_args(["install", *words])


# --- the surface --------------------------------------------------------------


@pytest.mark.parametrize("flag", ["--json", "--no-publish", "--workspace-type"])
def test_install_offers_no_flag_for_a_decision_it_has_already_made(flag, capsys):
    """Each of these was a way to ask for a half-finished or silent install."""

    with pytest.raises(SystemExit):
        _parse(flag, "local")
    assert "unrecognized arguments" in capsys.readouterr().err


def test_install_still_takes_the_workspace_and_environment_it_needs():
    parsed = _parse("--workspace", "Sales", "--environment", "weaver")
    assert parsed.workspace == "Sales"
    assert parsed.environment == "weaver"


def test_install_never_advertises_a_local_workspace():
    help_text = build_parser().parse_args(["install"]).__dict__
    assert "workspace_type" not in help_text


# --- what it does -------------------------------------------------------------


class _Result:
    workspace_name = "Sales"

    def as_dict(self):
        return {"environment_name": "weaver", "published": True, "timings": {}}


def test_install_prints_its_result_without_being_asked(monkeypatch, capsys):
    """No flag, and the payload is on stdout so a pipe gets only the result."""

    cli = import_module("weaver_cli.main")
    from weaver.workspaces import FabricWorkspace

    workspace = FabricWorkspace(workspace="Sales", environment="weaver")
    monkeypatch.setattr(cli, "_resolve_workspace", lambda args: workspace)
    monkeypatch.setattr(cli, "_prefer_desktop_credential", lambda: None)
    monkeypatch.setattr(cli, "_session", lambda args: _RecordingSession())

    import weaver.fabric as fabric

    monkeypatch.setattr(fabric, "install", lambda *a, **k: _Result())

    assert handle_install(_parse()) == 0
    printed = json.loads(capsys.readouterr().out)
    assert printed["environment_name"] == "weaver"
    assert "total" in printed["timings"]


def test_install_frames_its_work_so_a_long_publish_is_visible(monkeypatch, capsys):
    """A five-minute publish that prints nothing is indistinguishable from a hang.

    The frames are the existing Task/Step ones, which is what makes the live
    ticking line appear — this asserts install opens them, not that a second
    progress mechanism exists.
    """

    cli = import_module("weaver_cli.main")
    from weaver.workspaces import FabricWorkspace

    session = _RecordingSession()
    workspace = FabricWorkspace(workspace="Sales", environment="weaver")
    monkeypatch.setattr(cli, "_resolve_workspace", lambda args: workspace)
    monkeypatch.setattr(cli, "_prefer_desktop_credential", lambda: None)
    monkeypatch.setattr(cli, "_session", lambda args: session)

    import weaver.fabric as fabric
    from weaver.fabric import environment as env_mod

    def _install(workspace_name, environment_name, *, session=None, **kwargs):
        step = env_mod._reporter(session)
        with step("Publish"):
            pass
        return _Result()

    monkeypatch.setattr(fabric, "install", _install)

    handle_install(_parse())
    assert session.opened == [("task", "Install"), ("step", "Publish")]


def test_installing_without_a_session_still_works():
    """A pytest fixture installing Weaver wants the work, not the reporting."""

    from weaver.fabric import environment as env_mod

    step = env_mod._reporter(None)
    with step("Publish", "some detail"):
        pass


class _RecordingSession:
    """Enough Session to record which frames were opened."""

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
