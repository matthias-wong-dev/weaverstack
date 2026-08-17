"""The prompt at the terminal: pasted batches, history, and a clean redraw.

These drive the real input layer over a pipe and render the real output through
a VT100 screen, because the behaviour under test is terminal state. A prompt
that lands on top of a progress line is a screen defect, and a captured string
would show both texts present and call it correct.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager

import pytest
from support.terminal import Terminal, screen
from support.weaver_test import weaver_test

from weaver.errors import BuildError
from weaver.sessions import ConsoleSession
from weaver_cli.shell import Prompt, _loop

#: What a terminal sends when a block of text is pasted into it.
PASTE_START = "\x1b[200~"
PASTE_END = "\x1b[201~"

ENTER = "\r"
CTRL_C = "\x03"


def pasted(*lines: str) -> str:
    """The keystrokes a terminal sends for one pasted block, then Enter."""

    return PASTE_START + "".join(f"{line}\n" for line in lines) + PASTE_END + ENTER


@contextmanager
def driven(keys: str, *, session=None, parser=None, history=None, terminal=None):
    """Run the shell's own loop over a piped terminal, and yield the screen."""

    from prompt_toolkit.data_structures import Size
    from prompt_toolkit.input import create_pipe_input
    from prompt_toolkit.output.vt100 import Vt100_Output

    screen_buffer = terminal if terminal is not None else Terminal()
    output = Vt100_Output(
        screen_buffer, lambda: Size(rows=24, columns=100), term="xterm"
    )
    with create_pipe_input() as pipe:
        pipe.send_text(keys)
        prompt = Prompt(input=pipe, output=output, history_path=history)
        _loop(session, parser, prompt)
    yield screen_buffer


def recording_parser(handler):
    parser = argparse.ArgumentParser(prog="weaver")
    commands = parser.add_subparsers(dest="command")
    build = commands.add_parser("build")
    build.add_argument("repository", nargs="?")
    build.set_defaults(handler=handler)
    load = commands.add_parser("load")
    load.add_argument("targets", nargs="+")
    load.set_defaults(handler=handler)
    validate = commands.add_parser("test")
    validate.add_argument("targets", nargs="+")
    validate.set_defaults(handler=handler)
    return parser


@pytest.fixture
def recorded(tmp_path):
    """A parser recording every command, and the history file it writes to."""

    calls: list = []

    def handler(parsed):
        calls.append(parsed)
        return getattr(handler, "status", lambda parsed: 0)(parsed)

    return calls, recording_parser(handler), handler, tmp_path / "history"


# --- pasting several complete commands ----------------------------------------


@weaver_test()
def test_a_pasted_block_runs_every_command_in_order(recorded):
    calls, parser, _, history = recorded
    keys = (
        pasted(
            "weaver build ./repository",
            "weaver load Lakehouse/Landing Warehouse/Curated",
            "weaver test Lakehouse/Landing",
        )
        + f"exit{ENTER}"
    )

    with ConsoleSession() as session:
        with driven(keys, session=session, parser=parser, history=history):
            pass

    assert [parsed.command for parsed in calls] == ["build", "load", "test"]
    assert calls[1].targets == ["Lakehouse/Landing", "Warehouse/Curated"]


@weaver_test()
def test_every_pasted_command_runs_in_the_same_session(recorded):
    calls, parser, _, history = recorded
    keys = pasted("weaver build .", "weaver load Lakehouse/Landing") + f"exit{ENTER}"

    with ConsoleSession() as session:
        with driven(keys, session=session, parser=parser, history=history):
            pass

    assert {id(parsed.session) for parsed in calls} == {id(session)}


@weaver_test()
def test_blank_lines_and_comments_in_a_pasted_block_are_ignored(recorded):
    calls, parser, _, history = recorded
    keys = (
        pasted(
            "# rebuild the landing zone",
            "",
            "weaver build .",
            "   ",
            "# then load it",
            "weaver load Lakehouse/Landing",
        )
        + f"exit{ENTER}"
    )

    with ConsoleSession() as session:
        with driven(keys, session=session, parser=parser, history=history):
            pass

    assert [parsed.command for parsed in calls] == ["build", "load"]


@weaver_test()
def test_quoted_paths_survive_a_pasted_block(recorded):
    calls, parser, _, history = recorded
    keys = pasted('weaver build "my repository"') + f"exit{ENTER}"

    with ConsoleSession() as session:
        with driven(keys, session=session, parser=parser, history=history):
            pass

    assert calls[0].repository == "my repository"


@weaver_test()
def test_a_failure_stops_the_rest_of_the_pasted_block(recorded):
    """The third line was written expecting the second to have succeeded."""

    calls, parser, handler, history = recorded
    handler.status = lambda parsed: 1 if parsed.command == "load" else 0
    keys = (
        pasted(
            "weaver build .",
            "weaver load Lakehouse/Landing",
            "weaver test Lakehouse/Landing",
        )
        + f"exit{ENTER}"
    )

    with ConsoleSession() as session:
        with driven(keys, session=session, parser=parser, history=history):
            pass

    assert [parsed.command for parsed in calls] == ["build", "load"]


@weaver_test()
def test_a_raised_error_stops_the_rest_of_the_pasted_block(recorded, capsys):
    calls, parser, handler, history = recorded

    def fail_the_load(parsed):
        if parsed.command == "load":
            raise BuildError("Lakehouse/Landing was not found in workspace 'Demo'.")
        return 0

    handler.status = fail_the_load
    keys = (
        pasted(
            "weaver build .",
            "weaver load Lakehouse/Landing",
            "weaver test Lakehouse/Landing",
        )
        + f"exit{ENTER}"
    )

    with ConsoleSession() as session:
        with driven(keys, session=session, parser=parser, history=history):
            pass

    assert [parsed.command for parsed in calls] == ["build", "load"]
    assert "was not found" in capsys.readouterr().err


@weaver_test()
def test_a_bare_command_in_a_pasted_block_stops_it(recorded, capsys):
    calls, parser, _, history = recorded
    keys = pasted("build .", "weaver load Lakehouse/Landing") + f"exit{ENTER}"

    with ConsoleSession() as session:
        with driven(keys, session=session, parser=parser, history=history):
            pass

    assert calls == []
    assert "weaver build ." in capsys.readouterr().err


@weaver_test()
def test_a_help_line_in_a_pasted_block_does_not_stop_it(recorded, capsys):
    """`--help` is an answer rather than a failure, so the block carries on."""

    calls, parser, _, history = recorded
    keys = (
        pasted("weaver build --help", "weaver load Lakehouse/Landing") + f"exit{ENTER}"
    )

    with ConsoleSession() as session:
        with driven(keys, session=session, parser=parser, history=history):
            pass

    assert [parsed.command for parsed in calls] == ["load"]
    assert "usage: weaver build" in capsys.readouterr().out


@weaver_test()
def test_the_prompt_survives_a_failed_block_and_takes_the_next_one(recorded):
    calls, parser, handler, history = recorded
    handler.status = lambda parsed: 1 if len(calls) == 1 else 0
    keys = (
        pasted("weaver build .", "weaver load Lakehouse/Landing")
        + f"weaver test Lakehouse/Landing{ENTER}"
        + f"exit{ENTER}"
    )

    with ConsoleSession() as session:
        with driven(keys, session=session, parser=parser, history=history):
            pass

    assert [parsed.command for parsed in calls] == ["build", "test"]


# --- interruption --------------------------------------------------------------


@weaver_test()
def test_ctrl_c_at_the_prompt_returns_to_a_clean_prompt(recorded):
    """Nothing runs, and the session is still there to take the next command."""

    calls, parser, _, history = recorded
    keys = f"weaver build .{CTRL_C}weaver load Lakehouse/Landing{ENTER}exit{ENTER}"

    with ConsoleSession() as session:
        with driven(keys, session=session, parser=parser, history=history):
            pass

        assert [parsed.command for parsed in calls] == ["load"]
        assert not session.closed, "the interrupt did not take the session with it"


# --- history -------------------------------------------------------------------


@weaver_test()
def test_the_prompt_keeps_history_where_the_environment_says(recorded):
    calls, parser, _, history = recorded

    with ConsoleSession() as session:
        with driven(
            f"weaver build .{ENTER}exit{ENTER}",
            session=session,
            parser=parser,
            history=history,
        ):
            pass

    assert "weaver build ." in history.read_text(encoding="utf-8")


@weaver_test()
def test_an_unwritable_history_location_does_not_fail_the_session(tmp_path, recorded):
    calls, parser, _, _ = recorded
    blocked = tmp_path / "a-file"
    blocked.write_text("not a directory", encoding="utf-8")

    with ConsoleSession() as session:
        with driven(
            f"weaver build .{ENTER}exit{ENTER}",
            session=session,
            parser=parser,
            history=blocked / "history",
        ):
            pass

    assert len(calls) == 1


# --- the prompt never lands on top of what a command drew ----------------------


@weaver_test()
def test_the_next_prompt_starts_on_a_fresh_line_after_progress(tmp_path):
    """The invariant the shell owns: settle the renderer, then draw the prompt.

    The handler returns with a Step still open, so the live progress line is on
    screen and the cursor is part-way along it — the state that used to put
    ``weaver>`` in the middle of a progress line.
    """

    terminal = Terminal()
    calls: list = []

    def handler(parsed):
        calls.append(parsed)
        parsed.session.step_started("Read physical state")
        return 0

    with ConsoleSession(progress=terminal) as session:
        with driven(
            f"weaver build .{ENTER}exit{ENTER}",
            session=session,
            parser=recording_parser(handler),
            history=tmp_path / "history",
            terminal=terminal,
        ):
            pass

    lines = screen(terminal.getvalue())
    prompts = [line for line in lines if "weaver>" in line]

    assert len(calls) == 1
    assert len(prompts) == 2, "one prompt for the command, one for the exit"
    assert all(line.startswith("weaver>") for line in prompts), (
        f"a prompt was drawn part-way along a line: {lines}"
    )
    assert not any("⋯" in line for line in lines), (
        f"the live progress line was left on screen: {lines}"
    )


@weaver_test()
def test_the_next_prompt_starts_on_a_fresh_line_after_unterminated_output(tmp_path):
    """A command whose last write has no newline still gets a clean prompt."""

    terminal = Terminal()

    def handler(parsed):
        print("build ok, no newline here", end="", file=terminal)
        return 0

    with ConsoleSession(progress=terminal) as session:
        with driven(
            f"weaver build .{ENTER}exit{ENTER}",
            session=session,
            parser=recording_parser(handler),
            history=tmp_path / "history",
            terminal=terminal,
        ):
            pass

    lines = screen(terminal.getvalue())
    prompts = [line for line in lines if "weaver>" in line]

    assert all(line.startswith("weaver>") for line in prompts), (
        f"a prompt was drawn part-way along a line: {lines}"
    )
    assert any(line.startswith("build ok, no newline here") for line in lines)
