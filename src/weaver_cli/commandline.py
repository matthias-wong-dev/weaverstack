"""Reading a Weaver command line that was written down as text.

``weaver session`` and ``weaver compose`` both take command lines a person
typed or pasted, so they read them the same way: ``shlex`` quoting, an optional
or required leading ``weaver``, and the command names the top-level parser
actually accepts. A line copied from a terminal, a composition file or the
documentation means the same thing in all three places.
"""

from __future__ import annotations

import argparse
import shlex
from typing import Iterable, Mapping

from weaver.errors import CommandError

#: The program name a command line may carry.
PROGRAM = "weaver"

#: Shell syntax a Weaver command line does not carry. Weaver commands are run
#: by the CLI's own handlers, so there is no shell to interpret these.
SHELL_CHARACTERS = ("|", ">", "<", "&", ";", "$", "`", "\n")


def command_names(parser: argparse.ArgumentParser) -> frozenset[str]:
    """Every command name ``parser`` accepts at the top level."""

    for action in parser._subparsers._group_actions:
        if action.choices:
            return frozenset(action.choices)
    return frozenset()


def weaver_commands() -> frozenset[str]:
    """Every command name the Weaver CLI accepts at the top level."""

    from .main import build_parser

    return command_names(build_parser())


def command_words(
    line: str,
    *,
    known: Iterable[str] | None = None,
    require_program: bool = False,
    excluded: Mapping[str, str] | None = None,
) -> list[str]:
    """The arguments one written Weaver command line means.

    ``known`` is the set of command names to accept, defaulting to the CLI's
    own. ``require_program`` demands the leading ``weaver``; without it the
    program name is optional. ``excluded`` maps a command name to the reason it
    cannot run in this context.
    """

    text = line.strip()
    for character in SHELL_CHARACTERS:
        if character in text:
            raise CommandError(
                f"{text!r} is not a Weaver command line. Shell syntax such as "
                "|, >, && and $ is not accepted."
            )
    try:
        words = shlex.split(text)
    except ValueError as exc:
        raise CommandError(f"{text!r}: {exc}") from exc
    if not words:
        raise CommandError("a Weaver command line cannot be empty")

    accepted = frozenset(known) if known is not None else weaver_commands()
    if words[0] == PROGRAM:
        rest = words[1:]
        if not rest:
            raise CommandError(f"{text!r} names no command")
    elif require_program:
        if words[0] in accepted:
            raise CommandError(
                f"Commands start with `{PROGRAM}`. Write: {PROGRAM} {text}"
            )
        raise CommandError(_not_a_command(words[0], accepted))
    else:
        rest = words

    refusal = (excluded or {}).get(rest[0])
    if refusal is not None:
        raise CommandError(f"{rest[0]}: {refusal}")
    if rest[0] not in accepted:
        raise CommandError(_not_a_command(rest[0], accepted))
    return rest


def _not_a_command(word: str, accepted: frozenset[str]) -> str:
    return f"{word!r} is not a Weaver command. Available: " + ", ".join(
        sorted(accepted)
    )


__all__ = [
    "PROGRAM",
    "SHELL_CHARACTERS",
    "command_names",
    "command_words",
    "weaver_commands",
]
