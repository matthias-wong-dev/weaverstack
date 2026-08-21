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
from weaver_cli.commandline import command_names, command_words


@weaver_test()
def test_the_program_name_is_stripped():
    assert command_words("weaver load Lakehouse/Sales") == ["load", "Lakehouse/Sales"]


@weaver_test()
def test_quoting_survives():
    words = command_words('weaver build . --workspace "35 South Data"')

    assert words == ["build", ".", "--workspace", "35 South Data"]


@weaver_test()
def test_the_program_name_is_optional():
    """The prompt and a composition both accept a line without it."""

    assert command_words("load Lakehouse/Sales") == ["load", "Lakehouse/Sales"]


@weaver_test()
def test_invalid_shell_quoting_is_refused():
    with pytest.raises(CommandError):
        command_words("weaver build 'unterminated")


# --- backslashes ---------------------------------------------------------------


@weaver_test()
def test_an_unquoted_windows_path_survives():
    """A line copied from PowerShell reaches the parser with its path intact."""

    assert command_words(r"weaver build C:\Users\Matthias\repo") == [
        "build",
        r"C:\Users\Matthias\repo",
    ]


@weaver_test()
def test_a_quoted_windows_path_survives():
    assert command_words(r'weaver build "C:\Users\Matthias Wong\repo"') == [
        "build",
        r"C:\Users\Matthias Wong\repo",
    ]


@weaver_test()
def test_a_single_quoted_windows_path_survives():
    assert command_words(r"weaver build 'C:\Users\Matthias Wong\repo'") == [
        "build",
        r"C:\Users\Matthias Wong\repo",
    ]


@weaver_test()
def test_a_unc_path_keeps_both_leading_separators():
    assert command_words(r"weaver build \\server\share\repo") == [
        "build",
        r"\\server\share\repo",
    ]


@weaver_test()
def test_a_posix_path_is_unaffected():
    assert command_words("weaver build /srv/weaver/repo") == [
        "build",
        "/srv/weaver/repo",
    ]


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


@pytest.mark.parametrize(
    "line, expected",
    [
        (
            'weaver build . --workspace "Research & Development"',
            "Research & Development",
        ),
        ("weaver build . --workspace 'Sales; Marketing'", "Sales; Marketing"),
        ('weaver build . --workspace "Cost $5"', "Cost $5"),
        ('weaver build . --workspace "A > B"', "A > B"),
        ("weaver build . --workspace 'A < B'", "A < B"),
        ('weaver build . --workspace "back`tick"', "back`tick"),
        ("weaver build . --workspace 'a | b'", "a | b"),
    ],
)
@weaver_test()
def test_a_quoted_shell_character_is_an_argument(line, expected):
    """A quoted character is part of a value, not an operator.

    A workspace named "Research & Development" is an ordinary workspace name.
    """

    assert command_words(line)[-1] == expected


@pytest.mark.parametrize(
    "line",
    [
        "weaver load Lakehouse/Sales | tee log",
        "weaver load Lakehouse/Sales; weaver test Lakehouse/Sales",
        "weaver load $TARGET",
    ],
)
@weaver_test()
def test_the_same_character_unquoted_is_still_refused(line):
    with pytest.raises(CommandError):
        command_words(line)


@weaver_test()
def test_a_refused_operator_is_named_with_what_to_do_about_it():
    with pytest.raises(CommandError) as raised:
        command_words("weaver build . --workspace Research & Development")

    assert "&" in str(raised.value)
    assert "quote it" in str(raised.value)


@weaver_test()
def test_a_command_line_is_one_line():
    with pytest.raises(CommandError):
        command_words("weaver build .\nweaver load Lakehouse/Sales")


@weaver_test()
def test_a_context_may_exclude_a_command_and_say_why():
    with pytest.raises(CommandError) as raised:
        command_words("weaver session", excluded={"session": "already in a session"})

    assert "already in a session" in str(raised.value)


# --- the parser decides what a line means -------------------------------------


@pytest.mark.parametrize("line", ["weaver --help", "weaver --version"])
@weaver_test()
def test_a_top_level_option_reaches_the_parser(line):
    """``weaver --help`` is a normal invocation, so it stays one here."""

    assert command_words(line) == [line.split()[1]]


@weaver_test()
def test_an_unknown_command_is_passed_on_for_the_parser_to_reject():
    """Whether a word is a command is argparse's answer, so it is not re-derived."""

    assert command_words("weaver frobnicate --wat") == ["frobnicate", "--wat"]


@weaver_test()
def test_a_line_that_is_only_the_program_name_names_no_command():
    with pytest.raises(CommandError) as raised:
        command_words("weaver")

    assert "names no command" in str(raised.value)


@weaver_test()
def test_command_names_reads_the_parser_s_own_choices():
    parser = argparse.ArgumentParser(prog="weaver")
    commands = parser.add_subparsers(dest="command")
    commands.add_parser("build")

    assert command_names(parser) == {"build"}


@weaver_test()
def test_the_session_and_a_composition_read_a_line_with_the_same_function():
    """One parser, not two sets of rules that agree until they do not."""

    from weaver_cli import compose, shell

    assert shell.command_words is command_words
    assert compose.command_words is command_words
