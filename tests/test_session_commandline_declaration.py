"""One way to read a written Weaver command line.

``weaver session`` and ``weaver compose`` both accept command lines a person
typed or pasted. They read them with the same function, so an option, a quoted
path and the leading ``weaver`` mean the same thing in both.
"""

from __future__ import annotations

import argparse

import pytest
from support.weaver_test import weaver_test

from weaver.errors import CommandError
from weaver_cli.commandline import command_names, command_words, weaver_commands


@weaver_test()
def test_the_program_name_is_stripped():
    assert command_words("weaver load Lakehouse/Sales") == ["load", "Lakehouse/Sales"]


@weaver_test()
def test_quoting_survives():
    words = command_words('weaver build . --workspace "35 South Data"')

    assert words == ["build", ".", "--workspace", "35 South Data"]


@weaver_test()
def test_a_session_requires_the_program_name_and_shows_the_line_to_write():
    with pytest.raises(CommandError) as raised:
        command_words("load Lakehouse/Sales", require_program=True)

    assert "weaver load Lakehouse/Sales" in str(raised.value)


@weaver_test()
def test_a_composition_does_not_require_the_program_name():
    assert command_words("load Lakehouse/Sales") == ["load", "Lakehouse/Sales"]


@weaver_test()
def test_an_unknown_word_is_reported_as_an_unknown_word():
    """A bare word that is not a command is not a missing ``weaver``."""

    with pytest.raises(CommandError) as raised:
        command_words("frobnicate", require_program=True)

    assert "not a Weaver command" in str(raised.value)
    assert "build" in str(raised.value)


@weaver_test()
def test_invalid_shell_quoting_is_refused():
    with pytest.raises(CommandError):
        command_words("weaver build 'unterminated")


@pytest.mark.parametrize(
    "line",
    [
        "weaver load Lakehouse/Sales | tee log",
        "weaver load Lakehouse/Sales && weaver test Lakehouse/Sales",
        "weaver load Lakehouse/Sales > out.txt",
        "weaver load $TARGET",
        "weaver load `echo Lakehouse/Sales`",
        "weaver load Lakehouse/Sales; weaver test Lakehouse/Sales",
    ],
)
@weaver_test()
def test_nothing_shell_shaped_is_accepted(line):
    """A Weaver command batch is not a shell, in either place that reads one."""

    with pytest.raises(CommandError):
        command_words(line)


@weaver_test()
def test_a_context_may_exclude_a_command_and_say_why():
    with pytest.raises(CommandError) as raised:
        command_words("weaver session", excluded={"session": "already in a session"})

    assert "already in a session" in str(raised.value)


@weaver_test()
def test_the_accepted_commands_come_from_the_parser():
    parser = argparse.ArgumentParser(prog="weaver")
    commands = parser.add_subparsers(dest="command")
    commands.add_parser("build")

    assert command_names(parser) == {"build"}
    assert command_words("weaver build", known=command_names(parser)) == ["build"]
    with pytest.raises(CommandError):
        command_words("weaver load Lakehouse/Sales", known=command_names(parser))


@weaver_test()
def test_the_cli_s_own_commands_are_the_default():
    from weaver_cli.main import build_parser

    assert weaver_commands() == command_names(build_parser())
    assert {"build", "load", "test", "wipe", "compose"} <= weaver_commands()


@weaver_test()
def test_the_session_and_a_composition_read_a_line_with_the_same_function():
    """One parser, not two sets of rules that agree until they do not."""

    from weaver_cli import compose, shell

    assert shell.command_words is command_words
    assert compose.command_words is command_words
