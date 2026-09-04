"""Asking for the names a project needs, and showing what was set up.

Two jobs, both rendering. The questions collect values and nothing else, and the
same core operation runs whether they were typed at a prompt or given as
options. Nothing here decides what initialise does.

Prompts appear when a terminal is there to answer them and a required value is
missing. ``--interactive`` asks regardless, ``--no-input`` never asks and names
the options a run is short of.
"""

from __future__ import annotations

import argparse
import sys

from weaver.errors import CommandError

#: The status a run reports, as a person reads it.
DISPLAY = {
    "created": "created",
    "existing": "already exists",
    "published": "published",
    "unchanged": "unchanged",
    "written": "written",
    "planned": "create",
}

#: Column widths for the item table. The role fits "Environment" and the name
#: is given room for an ordinary Fabric display name.
ROLE_WIDTH = 14
NAME_WIDTH = 19

INTRODUCTION = (
    "Set up a Weaver project.\n"
    "\n"
    "You'll use your Fabric workspace and choose the items you want for this "
    "project.\n"
    "Any missing items will be created automatically.\n"
)


def collect(args: argparse.Namespace, *, ask: bool = True, stdin=None) -> bool:
    """Fill in the values this run is short of, and say whether it asked.

    Values already given are left alone, so a half-written command line is
    finished at the prompt and a complete one never asks. ``ask`` false is
    ``--no-input``: a missing value is then an error naming the option.
    """

    stream = stdin if stdin is not None else sys.stdin
    asked = ask and (args.interactive or bool(_missing(args)) and _can_ask(stream))
    if asked:
        _ask(args, stream)
    missing = _missing(args)
    if missing:
        raise CommandError(_short_of(missing))
    return asked


def _missing(args: argparse.Namespace) -> tuple[str, ...]:
    """The options a run cannot proceed without, in the order they are asked."""

    short = []
    if not args.workspace:
        short.append("--workspace")
    if not args.lakehouse and not args.warehouse:
        short.append("--lakehouse or --warehouse")
    return tuple(short)


def _short_of(missing) -> str:
    listed = " and ".join(missing)
    return (
        f"This needs {listed}.\n"
        "\n"
        "Provide them on the command line, or run `weaver initialise` on its\n"
        "own to be asked."
    )


def _can_ask(stream) -> bool:
    try:
        return bool(stream.isatty())
    except (AttributeError, ValueError):
        return False


def _ask(args: argparse.Namespace, stream) -> None:
    """Ask for what is missing, keeping every value already supplied."""

    from weaver.initialise import DEFAULT_CATALOGUE, DEFAULT_ENVIRONMENT

    print(INTRODUCTION)
    if not args.workspace:
        args.workspace = _answer(stream, "Fabric workspace")
    if not args.catalogue:
        args.catalogue = _answer(stream, "Catalogue", default=DEFAULT_CATALOGUE)
    if not args.environment:
        args.environment = _answer(stream, "Environment", default=DEFAULT_ENVIRONMENT)
    if not args.lakehouse:
        args.lakehouse = _answer(stream, "Lakehouse", skippable=True)
    if not args.warehouse:
        args.warehouse = _answer(stream, "Warehouse", skippable=True)
    if args.example is None:
        args.example = _yes(
            stream, "Would you like to create and run a small Sales example?"
        )
    print()


def _answer(stream, label: str, *, default: str | None = None, skippable=False):
    """One typed answer, or the default an empty line accepts."""

    shown = default if default is not None else ("skip" if skippable else None)
    prompt = f"{label} [{shown}]: " if shown else f"{label}: "
    while True:
        print(prompt, end="", flush=True)
        typed = (stream.readline() or "").strip()
        if typed:
            return typed
        if default is not None:
            return default
        if skippable:
            return None
        print(f"{label} is needed to continue.")


def _yes(stream, question: str) -> bool:
    """A yes or no answer, defaulting to yes on an empty line."""

    while True:
        print(f"{question} [Y/n]: ", end="", flush=True)
        typed = (stream.readline() or "").strip().lower()
        if typed in ("", "y", "yes"):
            return True
        if typed in ("n", "no"):
            return False
        print("Answer y or n.")


def equivalent_command(args: argparse.Namespace) -> str:
    """The command line that runs this again without any questions."""

    parts = ["weaver initialise"]
    if args.repository:
        parts.append(_quoted(args.repository))
    for option, value in (
        ("--workspace", args.workspace),
        ("--catalogue", args.catalogue),
        ("--environment", args.environment),
        ("--lakehouse", args.lakehouse),
        ("--warehouse", args.warehouse),
    ):
        if value:
            parts.append(f"{option} {_quoted(value)}")
    if args.example:
        parts.append("--example")
    return " ".join(parts)


def _quoted(value: str) -> str:
    text = str(value)
    return f'"{text}"' if " " in text else text


def render(report) -> None:
    """Show what a run set up, and what to run next."""

    print()
    print("Everything is ready." if report.succeeded else "Something did not finish.")
    print()
    _table(report)
    print(f"Your Weaver project is in {report.repository}.")
    print()
    print("You can now run:")
    print()
    for command in report.next_commands:
        print(f"  {command}")


def render_dry_run(report) -> None:
    """Show what a run would set up, having changed nothing."""

    print("Here's what will be set up:")
    print()
    _table(report)
    print(f"Project files will be created in {report.repository}.")
    if report.example.generated:
        print("A small Sales example will also be added.")
    print()
    print("No changes were made.")


def _table(report) -> None:
    for outcome in report.resources:
        role = outcome.role.ljust(ROLE_WIDTH)
        name = outcome.name.ljust(NAME_WIDTH)
        print(f"  {role}{name}{DISPLAY.get(outcome.status, outcome.status)}")
    if report.example.ran:
        stages = ", ".join(
            stage
            for stage, status in (
                ("built", report.example.build),
                ("loaded", report.example.load),
                ("tested", report.example.test),
            )
            if status is not None
        )
        print(f"  {'Sales example'.ljust(ROLE_WIDTH + NAME_WIDTH)}{stages}")
    print()
