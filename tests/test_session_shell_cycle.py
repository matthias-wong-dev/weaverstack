"""``weaver session`` — one console session, many commands, one set of resources.

The claim is about what survives between commands, so these tests drive the
shell with a scripted stdin and watch what the handlers receive. No workspace is
resolved and nothing physical is acquired: the Session's job here is to *be* the
same object each time, and to still be usable after a command has failed.
"""

from __future__ import annotations

import argparse
import io
from pathlib import Path

import pytest

from weaver.errors import BuildError, CommandError
from weaver.session import ConsoleSession
from weaver.workspaces import LocalWorkspace
from weaver_cli.main import _resolve_workspace, _with_command_overrides
from weaver_cli.shell import run_shell


def _local(root="./emulator", **kwargs) -> LocalWorkspace:
    return LocalWorkspace(workspace=Path(root), **kwargs)


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


def _run(script: str, factory, workspace=None) -> int:
    args = argparse.Namespace(
        workspace=workspace,
        workspace_config=None,
        workspace_type=None,
        environment=None,
        weaver_lakehouse=None,
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


# --- workspace inheritance ---------------------------------------------------


def _args(session=None, **overrides):
    values = dict(
        workspace=None,
        workspace_config=None,
        workspace_type=None,
        environment=None,
        weaver_lakehouse=None,
        session=session,
    )
    values.update(overrides)
    return argparse.Namespace(**values)


def test_a_command_naming_nothing_inherits_the_session_workspace():
    with ConsoleSession(workspace=_local(weaver_lakehouse="Weaver")) as session:
        assert _resolve_workspace(_args(session)).workspace == Path("emulator")


def test_a_command_may_override_the_control_lakehouse_it_inherits():
    with ConsoleSession(workspace=_local(weaver_lakehouse="Weaver")) as session:
        resolved = _resolve_workspace(_args(session, weaver_lakehouse="Other"))

        assert resolved.weaver_lakehouse == "Other"
        assert resolved.workspace == Path("emulator")
        assert session.workspace.weaver_lakehouse == "Weaver", "the session is unchanged"


def test_a_command_naming_its_own_workspace_does_not_inherit():
    with ConsoleSession(workspace=_local("./one")) as session:
        resolved = _resolve_workspace(
            _args(session, workspace="./two", workspace_type="local")
        )

        assert resolved.workspace == Path("two")


def test_a_workspace_type_that_disagrees_needs_its_own_workspace():
    with ConsoleSession(workspace=_local()) as session:
        with pytest.raises(CommandError, match="name a --workspace"):
            _resolve_workspace(_args(session, workspace_type="fabric"))


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
    original = _local(weaver_lakehouse="Weaver")
    overridden = _with_command_overrides(
        original, _args(weaver_lakehouse="Other", environment="dev")
    )

    assert original.weaver_lakehouse == "Weaver"
    assert overridden.weaver_lakehouse == "Other"
    assert overridden.environment == "dev"
