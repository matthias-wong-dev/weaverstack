"""``weaver session``: one console session, many commands, one set of resources.

The claim is about what survives between commands, so these tests drive the
shell with a scripted stdin and watch what the handlers receive. No workspace is
resolved and nothing physical is acquired: the Session's job here is to be the
same object each time, and to still be usable after a command has failed.

Commands are written as they are everywhere else, ``weaver build .``, because
the session runs the ordinary CLI, not a dialect of it. The terminal behaviour
of the prompt itself is proved in ``test_session_terminal_boundary``.
"""

from __future__ import annotations

import argparse
import io

import pytest
from support.weaver_test import weaver_test
from support.workspaces import given_workspace

from weaver.errors import BuildError
from weaver.sessions import ConsoleSession
from weaver_cli.main import _resolve_workspace, _with_command_overrides, handle_compose
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


@weaver_test()
def test_every_command_runs_in_the_same_session(recorded):
    seen, factory = recorded

    _run("weaver build .\nweaver build .\nexit\n", factory)

    assert len(seen) == 2
    assert seen[0].session is seen[1].session
    assert isinstance(seen[0].session, ConsoleSession)


@weaver_test()
def test_the_session_closes_when_the_shell_leaves(recorded):
    seen, factory = recorded

    _run("weaver build .\nexit\n", factory)

    assert seen[0].session.closed


@weaver_test()
def test_end_of_input_leaves_as_cleanly_as_exit(recorded):
    seen, factory = recorded

    assert _run("weaver build .\n", factory) == 0
    assert seen[0].session.closed


# --- the command language is the CLI's -----------------------------------------


@pytest.fixture
def every_command(monkeypatch):
    """The real parser, with every handler replaced by one that records."""

    from weaver_cli.main import build_parser

    calls: list = []
    parser = build_parser()

    def record(parsed):
        calls.append(parsed)
        return 0

    for action in parser._subparsers._group_actions[0].choices.values():
        action.set_defaults(handler=record)
    return calls, (lambda: parser)


@pytest.mark.parametrize(
    "line",
    [
        "weaver build . --item Lakehouse/Sales=Lakehouse/Sales_LH",
        "weaver load --item Lakehouse/Sales --item Warehouse/Curated",
        "weaver test --item Warehouse/Curated",
        "weaver wipe Lakehouse/Sales_LH --yes",
    ],
)
@weaver_test()
def test_an_ordinary_weaver_command_line_runs_unchanged(line, every_command):
    """The acceptance criterion: a line copied from a terminal is the line here."""

    calls, factory = every_command

    _run(f"{line}\nexit\n", factory)

    assert len(calls) == 1, "the pasted command line ran"
    assert calls[0].command == line.split()[1]


@weaver_test()
def test_the_leading_program_name_is_optional(recorded, capsys):
    """`build .` and `weaver build .` are the same command at the prompt."""

    seen, factory = recorded

    _run("build .\nweaver build .\nexit\n", factory)

    assert capsys.readouterr().err == ""
    assert len(seen) == 2, "both spellings ran"


@weaver_test()
def test_a_word_that_is_not_a_command_is_rejected_by_the_parser(recorded, capsys):
    """The prompt does not re-derive argparse's answer; it lets argparse give it."""

    seen, factory = recorded

    _run("weaver frobnicate\nweaver build .\nexit\n", factory)

    reported = capsys.readouterr().err
    assert "invalid choice" in reported, "argparse said what was wrong"
    assert len(seen) == 1, "and the good command still ran"


@pytest.mark.parametrize(
    "line, answer",
    [("weaver --help", "usage:"), ("weaver --version", "weaverstack")],
)
@weaver_test()
def test_a_top_level_option_behaves_as_it_does_in_a_terminal(
    line, answer, every_command, capsys
):
    """`weaver --help` and `weaver --version` are ordinary CLI invocations."""

    calls, factory = every_command

    assert _run(f"{line}\nexit\n", factory) == 0

    assert answer in capsys.readouterr().out
    assert calls == [], "the option was the whole command"


@weaver_test()
def test_quoted_arguments_survive_the_prompt(every_command):
    calls, factory = every_command

    _run('weaver build . --workspace "35 South Data"\nexit\n', factory)

    assert calls[0].workspace == "35 South Data"


@pytest.mark.parametrize(
    "line, repository",
    [
        (r"weaver build C:\Users\Matthias\repo", r"C:\Users\Matthias\repo"),
        (
            r'weaver build "C:\Users\Matthias Wong\repo"',
            r"C:\Users\Matthias Wong\repo",
        ),
    ],
)
@weaver_test()
def test_a_windows_path_reaches_the_handler_intact(line, repository, every_command):
    """The line copied out of PowerShell is the line that runs."""

    calls, factory = every_command

    _run(f"{line}\nexit\n", factory)

    assert calls[0].repository == repository


@weaver_test()
def test_a_quoted_shell_character_is_part_of_a_workspace_name(every_command):
    """`Research & Development` is a workspace name, not a shell operator."""

    calls, factory = every_command

    _run('weaver build . --workspace "Research & Development"\nexit\n', factory)

    assert calls[0].workspace == "Research & Development"


@weaver_test()
def test_the_available_commands_come_from_the_parser(recorded, capsys):
    """No hand-written list: what the session offers is what the parser has."""

    from weaver_cli.main import build_parser
    from weaver_cli.shell import (
        NOT_IN_A_SESSION,
        SECONDARY_SESSION_COMMANDS,
        _available,
    )

    expected = sorted(
        set(build_parser()._subparsers._group_actions[0].choices)
        - set(NOT_IN_A_SESSION)
        - SECONDARY_SESSION_COMMANDS
    )

    assert _available(build_parser()) == ", ".join(expected)
    assert expected == [
        "build",
        "compose",
        "health",
        "load",
        "test",
        "wipe",
    ]
    assert SECONDARY_SESSION_COMMANDS.isdisjoint(NOT_IN_A_SESSION)


@weaver_test()
def test_secondary_commands_remain_accepted_in_a_session():
    from weaver_cli.commandline import command_words
    from weaver_cli.shell import NOT_IN_A_SESSION

    assert command_words("weaver check .", excluded=NOT_IN_A_SESSION) == ["check", "."]
    assert command_words("weaver install bundle", excluded=NOT_IN_A_SESSION) == [
        "install",
        "bundle",
    ]


# --- a composition run from the prompt ---------------------------------------


COMPOSITION = """\
compose:
  dev:
    - weaver build ./repository --item Lakehouse/Sales=Lakehouse/Sales_LH
    - weaver load --item Warehouse/Curated
"""


@weaver_test()
def test_a_composition_runs_from_the_prompt_in_the_session_already_open(
    tmp_path, every_command, monkeypatch
):
    """`weaver compose` is an ordinary command, and joins the open Session."""

    from importlib import import_module

    from weaver.sessions import host

    # `weaver_cli.main` the module, not the `main` function the package exports.
    cli = import_module("weaver_cli.main")
    calls, factory = every_command
    path = tmp_path / "compose.yml"
    path.write_text(COMPOSITION, encoding="utf-8")

    opened = []
    monkeypatch.setattr(
        host, "session_for", lambda workspace, **kwargs: opened.append(workspace)
    )

    # `compose` keeps its own handler; the commands it names are recorded.
    parser = factory()
    parser._subparsers._group_actions[0].choices["compose"].set_defaults(
        handler=handle_compose
    )
    monkeypatch.setattr(cli, "build_parser", lambda: parser)
    _run(
        f'weaver compose dev --file "{path.as_posix()}" --yes\nexit\n',
        lambda: parser,
    )

    assert [parsed.command for parsed in calls] == ["build", "load"]
    assert len({id(parsed.session) for parsed in calls}) == 1
    assert isinstance(calls[0].session, ConsoleSession)
    assert opened == [], "the composition opened no second Session"


# --- an ordinary failure is not the end of the session -----------------------


@weaver_test()
def test_a_command_that_fails_does_not_discard_the_session(recorded, capsys):
    seen, factory = recorded

    _run("weaver build . --fail\nweaver build .\nexit\n", factory)

    assert len(seen) == 2, "the second command ran after the first failed"
    assert seen[0].session is seen[1].session, "and in the same session"
    assert "the build failed for an ordinary reason" in capsys.readouterr().err


@weaver_test()
def test_a_usage_error_does_not_discard_the_session(recorded, capsys):
    seen, factory = recorded

    _run("weaver build --wat\nweaver build .\nexit\n", factory)

    assert len(seen) == 1, "the good command still ran"


@weaver_test()
def test_an_unparseable_line_is_reported_and_survived(recorded, capsys):
    seen, factory = recorded

    _run("weaver build 'unterminated\nweaver build .\nexit\n", factory)

    assert len(seen) == 1
    assert "error:" in capsys.readouterr().err


@weaver_test()
def test_an_unexpected_defect_does_not_discard_the_session(capsys):
    """A command that raises something nobody planned for still leaves a prompt."""

    seen = []

    def handler(args):
        seen.append(args)
        if len(seen) == 1:
            raise ZeroDivisionError("a defect, not a Weaver error")
        return 0

    def factory():
        parser = argparse.ArgumentParser(prog="weaver")
        commands = parser.add_subparsers(dest="command")
        one = commands.add_parser("build")
        one.add_argument("repository", nargs="?")
        one.set_defaults(handler=handler)
        return parser

    assert _run("weaver build .\nweaver build .\nexit\n", factory) == 0
    assert len(seen) == 2
    assert "ZeroDivisionError" in capsys.readouterr().err


@weaver_test()
def test_an_interrupted_command_leaves_the_session_usable(capsys):
    seen = []

    def handler(args):
        seen.append(args)
        if len(seen) == 1:
            raise KeyboardInterrupt
        return 0

    def factory():
        parser = argparse.ArgumentParser(prog="weaver")
        commands = parser.add_subparsers(dest="command")
        one = commands.add_parser("build")
        one.add_argument("repository", nargs="?")
        one.set_defaults(handler=handler)
        return parser

    assert _run("weaver build .\nweaver build .\nexit\n", factory) == 0
    assert len(seen) == 2
    assert "interrupted" in capsys.readouterr().err


@weaver_test()
def test_a_session_cannot_be_started_inside_a_session(recorded, capsys):
    seen, factory = recorded

    _run("weaver session\nexit\n", factory)

    assert seen == []
    assert "already in a session" in capsys.readouterr().err


@pytest.mark.parametrize("command", ["fabric"])
@weaver_test()
def test_the_commands_a_session_does_not_offer(command, recorded, capsys):
    """Fabric estate management remains shell work."""

    seen, factory = recorded

    _run(f"{command}\nexit\n", factory)

    assert seen == []
    assert "not a session" in capsys.readouterr().err


@weaver_test()
def test_blank_lines_and_comments_are_not_commands(recorded):
    seen, factory = recorded

    _run("\n   \n# a note\nweaver build .\nexit\n", factory)

    assert len(seen) == 1


# --- what the banner claims --------------------------------------------------


def _banner_for(workspace, recorded, capsys, environment="weaver") -> str:
    _, factory = recorded
    _run("exit\n", factory, workspace=workspace, environment=environment)
    return capsys.readouterr().out


@weaver_test()
def test_the_banner_names_what_is_actually_starting(recorded, capsys):
    """Opening a session warms the credential, and nothing a command must ask for.

    A Livy session costs a minute and a capacity's only slot, and Fabric attaches
    one to a Lakehouse. Before a command is typed there is no Lakehouse to attach
    to and no way to know Spark is wanted at all, so it waits.
    """

    printed = _banner_for("A_Workspace", recorded, capsys)

    assert "Fabric credential" in printed
    assert "Spark session (Livy)" not in printed


@weaver_test()
def test_the_banner_lists_the_parser_s_commands(recorded, capsys):
    """The recorded parser has one command, so the banner has one command."""

    printed = _banner_for("A_Workspace", recorded, capsys)

    assert "Available: build." in printed
    assert "the leading `weaver` is optional" in printed


class _Preparing:
    """A Session that records what a command asked it to get ready."""

    def __init__(self, skipped=()):
        self.offered = None
        self.required = None
        self._skipped = tuple(skipped)

    def offer_spark_home(self, lakehouses, *, workspace=None):
        self.offered = tuple(lakehouses)

    def prepare(self, required, *, workspace=None):
        from weaver.sessions.console import WarmUp

        self.required = set(required)
        return WarmUp(started=("Fabric credential",), skipped=self._skipped)


def _prepared(session, *words) -> None:
    """One real command line, through the real parser and the shell's own hook."""

    from weaver_cli.main import build_parser
    from weaver_cli.shell import _prepare_for

    _prepare_for(session, build_parser().parse_args(list(words)))


@weaver_test()
def test_a_build_target_naming_its_lakehouse_offers_it(recorded):
    """Fabric attaches a Spark session to a Lakehouse, and the command has one.

    The physical half of a build target, not workspace configuration: this
    workspace configures no Lakehouses at all and the build still has somewhere
    to attach.
    """

    from weaver.sessions.requirements import LIVY

    session = _Preparing()
    _prepared(
        session,
        "build",
        ".",
        "--item",
        "Lakehouse/Sales=Lakehouse/Sales_LH",
        "--workspace",
        "A_Workspace",
    )

    assert session.offered == ("Sales_LH",)
    assert LIVY in session.required


@weaver_test()
def test_a_logical_load_offers_nothing_to_the_prompt(recorded):
    """The prompt resolves no logical item, so it offers no Lakehouse.

    ``load --item Lakehouse/Sales`` names an item that may be installed in
    ``Sales_Dev``. Which one is the catalogue's answer, and the operation reads
    it and offers it. Livy is still declared, because a Lakehouse item usually
    holds Python primitives, and declaring is not acquiring.
    """

    from weaver.sessions.requirements import LIVY

    session = _Preparing()
    _prepared(
        session,
        "load",
        "--item",
        "Lakehouse/Sales",
        "--workspace",
        "A_Workspace",
    )

    assert session.offered == ()
    assert LIVY in session.required


@weaver_test()
def test_a_warehouse_only_command_offers_no_lakehouse_and_wants_no_spark(recorded):
    """A Warehouse-only build writes T-SQL, so it neither needs nor asks for Spark."""

    from weaver.sessions.requirements import LIVY

    session = _Preparing()
    _prepared(
        session,
        "build",
        ".",
        "--item",
        "Warehouse/Curated=Warehouse/Analysis",
        "--workspace",
        "A_Workspace",
    )

    assert session.offered == ()
    assert LIVY not in session.required


@weaver_test()
def test_a_command_wanting_spark_is_told_why_livy_is_not_starting(recorded, capsys):
    """The rule is right; announcing work it then declines is not.

    Livy cannot start against a workspace that names no Environment.
    The reason is owed to the reader at the point something needs Spark, which is
    the first command that names a Lakehouse rather than the banner.
    """

    session = _Preparing(
        skipped=(
            (
                "Spark session (Livy)",
                "this workspace names no Environment - pass --environment",
            ),
        )
    )
    _prepared(
        session,
        "build",
        ".",
        "--item",
        "Lakehouse/Sales=Lakehouse/Sales_LH",
        "--workspace",
        "A_Workspace",
    )

    printed = capsys.readouterr().out

    assert "Not started: Spark session (Livy)" in printed
    assert "--environment" in printed


@weaver_test()
def test_a_session_with_no_workspace_claims_to_start_nothing(recorded, capsys):
    printed = _banner_for(None, recorded, capsys)

    assert "No default workspace" in printed
    assert "Starting:" not in printed


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


@weaver_test()
def test_a_command_naming_nothing_inherits_the_session_workspace():
    with ConsoleSession(workspace=_workspace(catalogue="Warehouse/Weaver")) as session:
        assert _resolve_workspace(_args(session)).workspace == "Demo"


@weaver_test()
def test_a_command_may_override_the_control_lakehouse_it_inherits():
    with ConsoleSession(workspace=_workspace(catalogue="Warehouse/Weaver")) as session:
        resolved = _resolve_workspace(_args(session, catalogue="Warehouse/Other"))

        assert resolved.catalogue == "Warehouse/Other"
        assert resolved.workspace == "Demo"
        assert session.workspace.catalogue == "Warehouse/Weaver", (
            "the session is unchanged"
        )


@weaver_test()
def test_a_command_naming_the_sessions_own_workspace_is_accepted():
    """Saying what is already true, so the session's workspace is the base."""

    with ConsoleSession(workspace=_workspace(catalogue="Warehouse/Weaver")) as session:
        resolved = _resolve_workspace(
            _args(session, workspace="Demo", catalogue="Warehouse/Other")
        )

        assert resolved.workspace == "Demo"
        assert resolved.catalogue == "Warehouse/Other"


@weaver_test()
def test_a_command_naming_another_workspace_is_refused():
    """One Session is one Fabric workspace, and it stays the one it opened on.

    Refused rather than resolved and then ignored, which is what a command that
    addressed another workspace through a borrowed Session would be.
    """

    from weaver.errors import CommandError

    with ConsoleSession(workspace=_workspace("First_Workspace")) as session:
        with pytest.raises(CommandError, match="Second_Workspace"):
            _resolve_workspace(_args(session, workspace="Second_Workspace"))

        assert session.workspace.workspace == "First_Workspace"


@weaver_test()
def test_without_a_session_nothing_is_inherited():
    from weaver.errors import ConfigError

    with pytest.raises(ConfigError, match="--workspace"):
        _resolve_workspace(_args(None))


@weaver_test()
def test_a_session_started_without_a_workspace_inherits_nothing():
    with ConsoleSession() as session:
        from weaver.errors import ConfigError

        with pytest.raises(ConfigError, match="--workspace"):
            _resolve_workspace(_args(session))


@weaver_test()
def test_overrides_do_not_mutate_the_workspace_they_are_applied_to():
    original = _workspace(catalogue="Warehouse/Weaver")
    overridden = _with_command_overrides(
        original, _args(catalogue="Warehouse/Other", environment="dev")
    )

    assert original.catalogue == "Warehouse/Weaver"
    assert overridden.catalogue == "Warehouse/Other"
    from weaver.workspaces import EnvironmentRef

    assert overridden.environment == EnvironmentRef(None, "dev")
