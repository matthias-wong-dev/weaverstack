"""``--non-interactive`` is the whole invocation's policy, and ``--yes`` is not it.

Two options, two meanings. ``--non-interactive`` says the invocation never
waits for a person. ``--yes`` grants permission for a destructive action.
Neither implies the other, so an unattended destructive command carries both.
"""

from __future__ import annotations

import argparse
import importlib
import io

import pytest
from support.weaver_test import weaver_test

from weaver_cli.interaction import (
    add_non_interactive,
    authorised,
    can_prompt,
    confirm,
    non_interactive,
)
from weaver_cli.main import build_parser

#: Every executable command, in the spelling §13 of the contract names.
EXECUTABLE_COMMANDS = (
    ["check"],
    ["build"],
    ["load"],
    ["test"],
    ["health"],
    ["wipe"],
    ["mirror"],
    ["initialise"],
    ["initialize"],
    ["install", "bundle"],
    ["doctor", "--workspace", "Analytics"],
    ["workflow", "full"],
    ["fabric", "environment", "publish", "Runtime"],
    ["fabric", "notebook", "push", "notebook.py"],
    ["fabric", "notebook", "run", "Notebook"],
    ["fabric", "capacity", "status", "--resource-group", "g", "--capacity-name", "c"],
)


def _module(name: str):
    """One CLI module, not the callable of the same dotted name.

    ``weaver_cli.main`` is both a module and the function the package
    re-exports, and attribute access finds the function.
    """

    import importlib
    import sys

    importlib.import_module(name)
    return sys.modules[name]


class _Terminal(io.StringIO):
    """A stream that reports itself as a terminal."""

    def isatty(self) -> bool:
        return True


# --- the option --------------------------------------------------------------


@pytest.mark.parametrize("words", EXECUTABLE_COMMANDS, ids=lambda w: " ".join(w))
@weaver_test()
def test_every_executable_command_accepts_the_policy(words):
    assert build_parser().parse_args([*words, "--non-interactive"]).non_interactive


@pytest.mark.parametrize("words", EXECUTABLE_COMMANDS, ids=lambda w: " ".join(w))
@weaver_test()
def test_the_policy_is_absent_unless_it_is_asked_for(words):
    assert build_parser().parse_args(words).non_interactive is False


@weaver_test()
def test_one_spelling_and_one_help_text():
    """A shared helper adds it, so no command acquires wording of its own."""

    import inspect

    source = inspect.getsource(_module("weaver_cli.main"))

    assert '"--non-interactive"' not in source
    assert source.count("add_non_interactive(") == len(EXECUTABLE_COMMANDS) - 1


@weaver_test()
def test_the_retired_no_input_spelling_is_an_argparse_error():
    """Deleted rather than aliased. One spelling for one policy."""

    with pytest.raises(SystemExit):
        build_parser().parse_args(["initialise", "--no-input"])


@weaver_test()
def test_a_session_is_the_interactive_command_and_takes_no_policy():
    with pytest.raises(SystemExit):
        build_parser().parse_args(["session", "--non-interactive"])


# --- what the policy decides --------------------------------------------------


@weaver_test()
def test_a_terminal_makes_an_invocation_interactive():
    args = argparse.Namespace(non_interactive=False)

    assert can_prompt(args, _Terminal()) is True


@weaver_test()
def test_a_pseudo_terminal_does_not_override_an_explicit_policy():
    args = argparse.Namespace(non_interactive=True)

    assert can_prompt(args, _Terminal()) is False


@weaver_test()
def test_a_pipe_is_not_somebody_to_ask():
    args = argparse.Namespace(non_interactive=False)

    assert can_prompt(args, io.StringIO("y\n")) is False


@weaver_test()
def test_the_policy_never_grants_authorisation():
    args = argparse.Namespace(non_interactive=True, yes=False)

    assert non_interactive(args) is True
    assert authorised(args) is False


@weaver_test()
def test_yes_authorises_and_says_nothing_about_interaction():
    args = argparse.Namespace(non_interactive=False, yes=True)

    assert authorised(args) is True
    assert non_interactive(args) is False


@weaver_test()
def test_a_confirmed_workflow_authorises_each_command():
    assert authorised(argparse.Namespace(authorised=True)) is True


@weaver_test()
def test_a_question_nobody_can_answer_reads_as_no(capsys):
    args = argparse.Namespace(non_interactive=True)
    stream = _Terminal("y\n")

    assert confirm(args, "Remove? [y/N] ", stream=stream) is False
    assert capsys.readouterr().out == ""


@weaver_test()
def test_a_question_at_a_terminal_reads_the_answer(capsys):
    args = argparse.Namespace(non_interactive=False)

    assert confirm(args, "Remove? [y/N] ", stream=_Terminal("y\n")) is True
    assert confirm(args, "Remove? [y/N] ", stream=_Terminal("n\n")) is False
    assert "Remove? [y/N] " in capsys.readouterr().out


@weaver_test()
def test_the_option_carries_one_help_text():
    parser = argparse.ArgumentParser()
    add_non_interactive(parser)

    assert "Never ask" in parser.format_help()


# --- no command decides interaction for itself --------------------------------


@weaver_test()
def test_no_handler_reads_a_terminal_or_a_line_of_its_own():
    """Interaction lives in one module. The rest ask it."""

    import inspect

    for name in ("weaver_cli.main", "weaver_cli.workflow", "weaver_cli.initialise"):
        source = inspect.getsource(_module(name))
        assert "sys.stdin.isatty()" not in source, name
        assert "input(" not in source, name
    # The shell's own reader chooses a prompt or a scripted stream, which is
    # what a REPL is, and it asks no policy question.
    assert "read_key" not in inspect.getsource(_module("weaver_cli.shell"))


# --- retry --------------------------------------------------------------------


@weaver_test()
def test_a_non_interactive_retryable_command_makes_one_attempt(monkeypatch):
    cli = importlib.import_module("weaver_cli.main")
    interaction = importlib.import_module("weaver_cli.interaction")
    monkeypatch.setattr(interaction, "read_key", lambda: pytest.fail("a key was read"))
    monkeypatch.setattr(
        cli, "_resolve_workspace", lambda _args: pytest.fail("a workspace was resolved")
    )
    calls = []
    args = argparse.Namespace(session=None, non_interactive=True)

    assert cli._until_fixed(args, lambda: calls.append(1) or 1) == 1
    assert calls == [1]


@weaver_test()
def test_a_non_interactive_retry_prompt_prints_nothing(monkeypatch, capsys):
    from weaver_cli.interaction import retry_wanted

    assert retry_wanted(argparse.Namespace(non_interactive=True)) is False
    assert capsys.readouterr().err == ""


# --- authentication -----------------------------------------------------------


@weaver_test()
def test_a_non_interactive_command_signs_in_without_a_browser(monkeypatch):
    """Browser sign-in waits for a person, so the chain leaves it out."""

    from weaver.fabric import auth

    cli = importlib.import_module("weaver_cli.main")
    installed = []
    monkeypatch.setattr(auth, "use_credential", installed.append)
    monkeypatch.setattr(
        auth, "desktop_credential", lambda: pytest.fail("a browser chain was built")
    )
    monkeypatch.setattr(auth, "unattended_credential", lambda: "unattended")

    cli._prefer_desktop_credential(argparse.Namespace(non_interactive=True))

    assert installed == ["unattended"]


@weaver_test()
def test_an_ordinary_command_still_reaches_browser_sign_in(monkeypatch):
    from weaver.fabric import auth

    cli = importlib.import_module("weaver_cli.main")
    installed = []
    monkeypatch.setattr(auth, "use_credential", installed.append)
    monkeypatch.setattr(auth, "desktop_credential", lambda: "desktop")

    cli._prefer_desktop_credential(argparse.Namespace(non_interactive=False))

    assert installed == ["desktop"]


@weaver_test()
def test_the_unattended_chain_holds_the_principal_and_the_azure_cli(monkeypatch):
    from weaver.fabric import auth

    monkeypatch.setattr(auth, "_unattended_chain", None)
    chain = auth.unattended_credential()

    assert [each.name for each in chain.credentials] == [
        "Service principal",
        "Azure CLI",
    ]
    assert auth.unattended_credential() is chain


# --- workflow -----------------------------------------------------------------


@weaver_test()
def test_a_workflow_propagates_the_policy_into_every_nested_command(
    tmp_path, monkeypatch, capsys
):
    from weaver_cli.workflow import run_workflow

    path = tmp_path / "workflow.yml"
    path.write_text(
        "workflows:\n  full:\n    - build .\n    - load Lakehouse/Sales\n",
        encoding="utf-8",
    )
    seen = []
    parser = build_parser()
    for command in ("build", "load"):
        parser._subparsers._group_actions[0].choices[command].set_defaults(
            handler=lambda parsed: seen.append(parsed) or 0, requires=None
        )

    status = run_workflow(
        argparse.Namespace(
            name="full",
            file=str(path),
            yes=True,
            non_interactive=True,
            session=None,
            timings=False,
            workspace=None,
            workspace_config=None,
            catalogue=None,
            environment=None,
        ),
        parser_factory=lambda: parser,
    )

    assert status == 0
    assert [parsed.non_interactive for parsed in seen] == [True, True]
    capsys.readouterr()


@weaver_test()
def test_a_non_interactive_workflow_without_yes_refuses_before_it_runs(
    tmp_path, capsys
):
    from weaver_cli.workflow import run_workflow

    path = tmp_path / "workflow.yml"
    path.write_text("workflows:\n  full:\n    - build .\n", encoding="utf-8")
    parser = build_parser()
    parser._subparsers._group_actions[0].choices["build"].set_defaults(
        handler=lambda _parsed: pytest.fail("a command ran without authorisation"),
        requires=None,
    )

    status = run_workflow(
        argparse.Namespace(
            name="full",
            file=str(path),
            yes=False,
            non_interactive=True,
            session=None,
            timings=False,
            workspace=None,
            workspace_config=None,
            catalogue=None,
            environment=None,
        ),
        parser_factory=lambda: parser,
        stdin=_Terminal("y\n"),
    )

    assert status == 1
    assert "Pass --yes" in capsys.readouterr().err
