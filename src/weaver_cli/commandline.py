"""Parse session and workflow commands with the ordinary CLI parser."""

from __future__ import annotations

import argparse
import shlex
from typing import Mapping

from weaver.errors import CommandError

PROGRAM = "weaver"

#: Shell operators are refused outside quotes because no shell interprets them.
SHELL_OPERATORS = ("|", ">", "<", "&", ";", "$", "`")


def command_names(parser: argparse.ArgumentParser) -> frozenset[str]:
    for action in parser._subparsers._group_actions:
        if action.choices:
            return frozenset(action.choices)
    return frozenset()


def command_words(
    line: str,
    *,
    excluded: Mapping[str, str] | None = None,
) -> list[str]:
    """Tokenise a command with optional leading ``weaver``.

    The CLI parser validates the resulting command names and options.
    ``excluded`` maps commands to context-specific refusal reasons.
    """

    text = line.strip()
    if "\n" in text:
        raise CommandError("A Weaver command line must be one line.")
    operator = _unquoted_operator(text)
    if operator is not None:
        raise CommandError(
            f"Shell syntax {operator!r} is not accepted in {text!r}. "
            "Quote it to pass it as an argument."
        )
    try:
        words = _split(text)
    except ValueError as exc:
        raise CommandError(f"{text!r}: {exc}") from exc
    if not words:
        raise CommandError("A Weaver command line cannot be empty.")

    rest = words[1:] if words[0] == PROGRAM else words
    if not rest:
        raise CommandError(f"{text!r} names no command.")

    refusal = (excluded or {}).get(rest[0])
    if refusal is not None:
        raise CommandError(f"{rest[0]}: {refusal}")
    return rest


def _split(text: str) -> list[str]:
    """Split arguments without treating backslashes as escapes.

    Standard :func:`shlex.split` would corrupt unquoted Windows paths.
    """

    lexer = shlex.shlex(text, posix=True)
    lexer.whitespace_split = True
    lexer.commenters = ""
    lexer.escape = ""
    return list(lexer)


def _unquoted_operator(text: str) -> str | None:
    quote = ""
    for character in text:
        if quote:
            if character == quote:
                quote = ""
        elif character in "\"'":
            quote = character
        elif character in SHELL_OPERATORS:
            return character
    return None


__all__ = [
    "PROGRAM",
    "SHELL_OPERATORS",
    "command_names",
    "command_words",
]
