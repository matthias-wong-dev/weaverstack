"""``weaver session`` — one console session, many commands, one set of resources.

The claim is about what survives between commands, so these tests drive the
shell with a scripted stdin and watch what the handlers receive. No workspace is
resolved and nothing physical is acquired: the Session's job here is to *be* the
same object each time, and to still be usable after a command has failed.
"""

from __future__ import annotations

import argparse
import io

import pytest
from support.workspaces import given_workspace

from weaver.errors import BuildError
from weaver.sessions import ConsoleSession
from weaver_cli.main import _resolve_workspace, _with_command_overrides
from weaver_cli.shell import run_shell


def _workspace(name: str = "Demo", **kwargs):
    return given_workspace(workspace=name, **kwargs)


@pytest.fixture
def recorded(monkeypatch):
    """A parser whose one command records the session it was handed."""

    seen = []

    def handler(args):
        seen.append(args)
        if args.fail:
            raise BuildError("the build failed for an ordinary reason")
        return 0

    def parser_factory():
        parser = argparse.ArgumentParser(prog="weaver")
        commands = parser.add_subparsers(dest="command")
        one = commands.add_parser("build")
        one.add_argument("repository", nargs="?")
        one.add_argument("--fail", action="store_true")
        one.set_defaults(handler=handler)
        return parser

    return seen, parser_factory


def _run(script: str, factory, workspace=None, environment="weaver") -> int:
    args = argparse.Namespace(
        workspace=workspace,
        workspace_config=None,
        environment=environment,
        catalogue=None,
    )
    return run_shell(args, parser_factory=factory, stdin=io.StringIO(script))


# --- one session across commands ---------------------------------------------


def test_every_command_runs_in_the_same_session(recorded):
    seen, factory = recorded

    _run("build .\nbuild .\nexit\n", factory)

    assert len(seen) == 2
    assert seen[0].session is seen[1].session
    assert isinstance(seen[0].session, ConsoleSession)


def test_the_session_closes_when_the_shell_leaves(recorded):
    seen, factory = recorded

    _run("build .\nexit\n", factory)

    assert seen[0].session.closed


def test_end_of_input_leaves_as_cleanly_as_exit(recorded):
    seen, factory = recorded

    assert _run("build .\n", factory) == 0
    assert seen[0].session.closed


# --- an ordinary failure is not the end of the session -----------------------


def test_a_command_that_fails_does_not_discard_the_session(recorded, capsys):
    seen, factory = recorded

    _run("build . --fail\nbuild .\nexit\n", factory)

    assert len(seen) == 2, "the second command ran after the first failed"
    assert seen[0].session is seen[1].session, "and in the same session"
    assert "the build failed for an ordinary reason" in capsys.readouterr().err


def test_a_usage_error_does_not_discard_the_session(recorded, capsys):
    seen, factory = recorded

    _run("nonsense --wat\nbuild .\nexit\n", factory)

    assert len(seen) == 1, "the good command still ran"


def test_an_unparseable_line_is_reported_and_survived(recorded, capsys):
    seen, factory = recorded

    _run("build 'unterminated\nbuild .\nexit\n", factory)

    assert len(seen) == 1
    assert "error:" in capsys.readouterr().err


def test_a_session_cannot_be_started_inside_a_session(recorded, capsys):
    seen, factory = recorded

    _run("session\nexit\n", factory)

    assert seen == []
    assert "already in a session" in capsys.readouterr().err


def test_blank_lines_and_comments_are_not_commands(recorded):
    seen, factory = recorded

    _run("\n   \n# a note\nbuild .\nexit\n", factory)

    assert len(seen) == 1


# --- what the banner claims --------------------------------------------------


def _banner_for(workspace, recorded, capsys, environment="weaver") -> str:
    _, factory = recorded
    _run("exit\n", factory, workspace=workspace, environment=environment)
    return capsys.readouterr().out


def test_the_banner_names_what_is_actually_starting(recorded, capsys):
    printed = _banner_for("A_Workspace", recorded, capsys)

    assert "Fabric credential" in printed
    assert "Spark session (Livy)" in printed


def test_a_workspace_with_no_environment_is_told_why_livy_is_not_starting(
    recorded, capsys
):
    """The rule is right; announcing work it then declines is not.

    Livy genuinely cannot start against a workspace that names no Environment,
    and warming it would replace the first command's clear message with a stale
    warm-up failure. What the prompt owes the reader is the reason.
    """

    printed = _banner_for("A_Workspace", recorded, capsys, environment=None)

    assert "Not started: Spark session (Livy)" in printed
    assert "--environment" in printed


def test_a_session_with_no_workspace_claims_to_start_nothing(recorded, capsys):
    printed = _banner_for(None, recorded, capsys)

    assert "No default workspace" in printed
    assert "Starting:" not in printed


# --- the prompt is a prompt --------------------------------------------------


def test_the_prompt_has_line_editing_and_history(monkeypatch, tmp_path, recorded):
    """``input()`` is line-edited only if readline has been imported.

    The import *is* the mechanism, and nothing else in Weaver imports it — which
    is why the up arrow answered with an escape sequence instead of the last
    command.
    """

    import sys

    from weaver_cli import shell

    monkeypatch.setenv(shell.HISTORY_ENV, str(tmp_path / "history"))
    _, factory = recorded

    _run("build .\nexit\n", factory)

    assert "readline" in sys.modules


def test_history_is_kept_where_the_environment_says(monkeypatch, tmp_path, recorded):
    from weaver_cli import shell

    wanted = tmp_path / "elsewhere" / "history"
    monkeypatch.setenv(shell.HISTORY_ENV, str(wanted))
    _, factory = recorded

    _run("build .\nexit\n", factory)

    assert wanted.exists(), "the session wrote its history where it was told"


def test_a_platform_without_readline_still_gets_a_session(monkeypatch, recorded):
    """Every part of line editing is best-effort; none of it gates a session."""

    import builtins

    real_import = builtins.__import__

    def refuse(name, *args, **kwargs):
        if name == "readline":
            raise ImportError("no readline here")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", refuse)
    seen, factory = recorded

    assert _run("build .\nexit\n", factory) == 0
    assert len(seen) == 1


def test_an_unwritable_history_location_does_not_fail_the_session(
    monkeypatch, tmp_path, recorded
):
    from weaver_cli import shell

    blocked = tmp_path / "a-file"
    blocked.write_text("not a directory")
    monkeypatch.setenv(shell.HISTORY_ENV, str(blocked / "history"))
    _, factory = recorded

    assert _run("build .\nexit\n", factory) == 0


# --- workspace inheritance ---------------------------------------------------


def _args(session=None, **overrides):
    values = dict(
        workspace=None,
        workspace_config=None,
        environment=None,
        catalogue=None,
        session=session,
    )
    values.update(overrides)
    return argparse.Namespace(**values)


def test_a_command_naming_nothing_inherits_the_session_workspace():
    with ConsoleSession(workspace=_workspace(catalogue="Lakehouse/Weaver")) as session:
        assert _resolve_workspace(_args(session)).workspace == "Demo"


def test_a_command_may_override_the_control_lakehouse_it_inherits():
    with ConsoleSession(workspace=_workspace(catalogue="Lakehouse/Weaver")) as session:
        resolved = _resolve_workspace(_args(session, catalogue="Lakehouse/Other"))

        assert resolved.catalogue == "Lakehouse/Other"
        assert resolved.workspace == "Demo"
        assert session.workspace.catalogue == "Lakehouse/Weaver", (
            "the session is unchanged"
        )


def test_a_command_naming_its_own_workspace_does_not_inherit():
    with ConsoleSession(workspace=_workspace("First_Workspace")) as session:
        resolved = _resolve_workspace(_args(session, workspace="Second_Workspace"))

        assert resolved.workspace == "Second_Workspace"


def test_without_a_session_nothing_is_inherited():
    from weaver.errors import ConfigError

    with pytest.raises(ConfigError, match="--workspace"):
        _resolve_workspace(_args(None))


def test_a_session_started_without_a_workspace_inherits_nothing():
    with ConsoleSession() as session:
        from weaver.errors import ConfigError

        with pytest.raises(ConfigError, match="--workspace"):
            _resolve_workspace(_args(session))


def test_overrides_do_not_mutate_the_workspace_they_are_applied_to():
    original = _workspace(catalogue="Lakehouse/Weaver")
    overridden = _with_command_overrides(
        original, _args(catalogue="Lakehouse/Other", environment="dev")
    )

    assert original.catalogue == "Lakehouse/Weaver"
    assert overridden.catalogue == "Lakehouse/Other"
    assert overridden.environment == "dev"
