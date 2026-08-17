"""Reading a Weaver command line that was written down as text.

``weaver session`` and ``weaver compose`` both take command lines a person
typed or pasted, so they read them the same way: ``shlex`` quoting, an optional
or required leading ``weaver``, and the few commands the context cannot run.
A line copied from a terminal, a composition file or the documentation means
the same thing in all three places.

What a line *means* is argparse's answer, not this module's. Nothing here
decides whether a command exists or whether its options are valid; the words go
to the CLI's own parser, so ``weaver --help`` and ``weaver --version`` behave at
a prompt as they do in a terminal.
"""

from __future__ import annotations

import argparse
import shlex
from typing import Mapping

from weaver.errors import CommandError

#: The program name a command line may carry.
PROGRAM = "weaver"

#: Shell operators a Weaver command line does not carry. Weaver commands are
#: run by the CLI's own handlers, so there is no shell to interpret these —
#: but only outside quoting, where they would be operators. Quoted, they are
#: ordinary characters in an argument such as "Research & Development".
SHELL_OPERATORS = ("|", ">", "<", "&", ";", "$", "`")


def command_names(parser: argparse.ArgumentParser) -> frozenset[str]:
    """Every command name ``parser`` accepts at the top level."""

    for action in parser._subparsers._group_actions:
        if action.choices:
            return frozenset(action.choices)
    return frozenset()


def command_words(
    line: str,
    *,
    require_program: bool = False,
    excluded: Mapping[str, str] | None = None,
) -> list[str]:
    """The arguments one written Weaver command line means.

    ``require_program`` demands the leading ``weaver``; without it the program
    name is optional. ``excluded`` maps a command name to the reason it cannot
    run in this context.
    """

    text = line.strip()
    if "\n" in text:
        raise CommandError("a Weaver command line is one line")
    operator = _unquoted_operator(text)
    if operator is not None:
        raise CommandError(
            f"{text!r} is not a Weaver command line. Shell syntax such as "
            f"{operator} is not accepted; quote it to pass it as an argument."
        )
    try:
        words = shlex.split(text)
    except ValueError as exc:
        raise CommandError(f"{text!r}: {exc}") from exc
    if not words:
        raise CommandError("a Weaver command line cannot be empty")

    if words[0] == PROGRAM:
        rest = words[1:]
    elif require_program:
        raise CommandError(f"Commands start with `{PROGRAM}`. Write: {PROGRAM} {text}")
    else:
        rest = words
    if not rest:
        raise CommandError(f"{text!r} names no command")

    refusal = (excluded or {}).get(rest[0])
    if refusal is not None:
        raise CommandError(f"{rest[0]}: {refusal}")
    return rest


def _unquoted_operator(text: str) -> str | None:
    """The first shell operator standing outside quoting, where it would be one.

    Quoting follows :mod:`shlex` in POSIX mode, which is what splits the line
    afterwards: either quote character opens a quoted run, and a backslash
    escapes the next character outside single quotes.
    """

    quote = ""
    index = 0
    while index < len(text):
        character = text[index]
        if character == "\\" and quote != "'":
            index += 2
            continue
        if quote:
            if character == quote:
                quote = ""
        elif character in "\"'":
            quote = character
        elif character in SHELL_OPERATORS:
            return character
        index += 1
    return None


__all__ = [
    "PROGRAM",
    "SHELL_OPERATORS",
    "command_names",
    "command_words",
]
