"""Asking for the names a project needs, and showing what was set up.

Two jobs, both rendering. The questions collect values and nothing else, and the
same core operation runs whether they were typed at a prompt or given as
options. Nothing here decides what initialise does.

Prompts appear when a terminal is there to answer them and a required value is
missing. ``--interactive`` asks for the optional ones too, ``--no-input`` never
asks and names the options a run is short of.

The Environment is the one question with a Fabric answer behind it: which
Environments a workspace has, and whether Weaver is installed in the chosen one.
Both are asked of core and answered here, so this module still decides nothing.
"""

from __future__ import annotations

import argparse
import sys

from weaver.errors import CommandError

#: The status a run reports, as a person reads it.
DISPLAY = {
    "created": "created",
    "existing": "already exists",
    "ready": "ready",
    "planned": "create",
    "install": "install Weaver",
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

#: How long Fabric takes to resolve the libraries an Environment declares. Said
#: before the question, because it is the one wait in the run worth warning about.
INSTALL_WARNING = "Installing Weaver in Fabric can take about 5 minutes."


def collect(
    args: argparse.Namespace,
    *,
    ask: bool = True,
    stdin=None,
    environments=None,
    state_of=None,
) -> bool:
    """Fill in the values this run is short of, and say whether it asked.

    Values already given are left alone, so a half-written command line is
    finished at the prompt and a complete one never asks. ``ask`` false is
    ``--no-input``: a missing value is then an error naming the option.

    ``environments`` lists the workspace's Environments and ``state_of`` says
    whether Weaver is installed in one. Both are supplied by the caller that
    holds a Session, and neither is called until a workspace is known.
    """

    stream = stdin if stdin is not None else sys.stdin
    asked = ask and (args.interactive or bool(_missing(args)) and _can_ask(stream))
    if asked:
        _ask(args, stream, environments=environments)
    missing = _missing(args)
    if missing:
        raise CommandError(_short_of(missing))
    # Whether Weaver has to be installed is a Fabric answer, so it is asked last
    # and asked whenever there is somebody to answer, including after a command
    # line that needed nothing else. A dry run changes nothing, so it asks
    # nothing and reports the installation as part of what a run would do.
    if (
        ask
        and state_of is not None
        and not getattr(args, "dry_run", False)
        and (args.interactive or _can_ask(stream))
    ):
        asked = _consent_if_needed(args, stream, state_of) or asked
    return asked


def collect_workspace(
    args: argparse.Namespace, *, ask: bool = True, stdin=None
) -> bool:
    """Ask for the workspace alone, which every later question needs.

    Which Environments there are, and whether Weaver is in one, are questions
    about a workspace. So this one comes first, before a Session is opened to
    ask them.
    """

    stream = stdin if stdin is not None else sys.stdin
    if args.workspace or not ask:
        return False
    if not args.interactive and not _can_ask(stream):
        return False
    print(INTRODUCTION)
    args.workspace = _answer(stream, "Fabric workspace")
    return True


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


def _ask(args: argparse.Namespace, stream, *, environments) -> None:
    """Ask for what is missing, keeping every value already supplied."""

    from weaver.initialise import DEFAULT_CATALOGUE

    if not args.workspace:
        print(INTRODUCTION)
        args.workspace = _answer(stream, "Fabric workspace")
    if not args.catalogue:
        args.catalogue = _answer(stream, "Catalogue", default=DEFAULT_CATALOGUE)
    _ask_which_environment(args, stream, environments=environments)
    if not args.lakehouse:
        args.lakehouse = _answer(stream, "Lakehouse", skippable=True)
    if not args.warehouse:
        args.warehouse = _answer(stream, "Warehouse", skippable=True)
    if args.example is None:
        args.example = _yes(
            stream, "Would you like to create and run a small Sales example?"
        )
    print()


# --- the Environment -----------------------------------------------------------


def _ask_which_environment(args, stream, *, environments) -> None:
    """Which Environment this project runs against.

    A name on its own does not say whether it is one the workspace has or one to
    make, so the choice comes first and the list is offered for the first.
    """

    from weaver.initialise import DEFAULT_ENVIRONMENT

    if args.environment:
        return
    if _use_an_existing_environment(stream, environments):
        args.environment = _choose_an_environment(stream, environments)
    else:
        args.environment = _answer(
            stream, "Environment name", default=DEFAULT_ENVIRONMENT
        )


def _consent_if_needed(args, stream, state_of) -> bool:
    """Ask about installing Weaver, where the chosen Environment needs it."""

    from weaver.initialise import DEFAULT_ENVIRONMENT, READY

    name = args.environment or DEFAULT_ENVIRONMENT
    state = state_of(name)
    if state == READY:
        return False
    args.install_weaver = _consent_to_install(stream, name, state)
    return True


def _use_an_existing_environment(stream, environments) -> bool:
    """Whether to name one the workspace has, or make a new one."""

    available = tuple(environments()) if environments is not None else ()
    print("Fabric Environment:")
    print("  1. Use an existing Environment")
    print("  2. Create a new Environment")
    print()
    if not available:
        print("This workspace has no Environments yet, so a new one is needed.")
        print()
        return False
    return _numbered(stream, "Choose", 2) == 1


def _choose_an_environment(stream, environments) -> str:
    """One of the workspace's Environments, chosen from a numbered list."""

    available = tuple(environments())
    print("Available Environments:")
    print()
    for number, name in enumerate(available, start=1):
        print(f"  {number}. {name}")
    print()
    return available[_numbered(stream, "Choose an Environment", len(available)) - 1]


def _consent_to_install(stream, name: str, state: str) -> bool:
    """One question, for the one slow thing a run does.

    Creating the Environment and installing Weaver in it are one product action,
    so they are one question. A run told no stops before anything changes.
    """

    from weaver.initialise import MISSING

    print()
    if state == MISSING:
        print(
            f"Environment '{name}' will be created and Weaver will be installed in it."
        )
    else:
        print(
            f"Weaver needs to be installed in the Fabric Environment '{name}'\n"
            "before this project can run."
        )
    print()
    print(INSTALL_WARNING)
    print()
    return _yes(stream, "Would you like to continue?")


# --- reading an answer ---------------------------------------------------------


def _read(stream, prompt: str) -> str:
    """One line, or a failure when there is nothing left to read.

    An empty line is an answer and end of input is not. A stream that has run
    out stops the run, so a question is never asked twice for want of one.
    """

    print(prompt, end="", flush=True)
    line = stream.readline()
    if line == "":
        raise CommandError(
            "The answers ran out before the questions did.\n"
            "\n"
            "Run `weaver initialise` at a terminal, or give every name on the\n"
            "command line and pass --no-input."
        )
    return line.strip()


def _answer(stream, label: str, *, default: str | None = None, skippable=False):
    """One typed answer, or the default an empty line accepts."""

    shown = default if default is not None else ("skip" if skippable else None)
    prompt = f"{label} [{shown}]: " if shown else f"{label}: "
    while True:
        typed = _read(stream, prompt)
        if typed:
            return typed
        if default is not None:
            return default
        if skippable:
            return None
        print(f"{label} is needed to continue.")


def _numbered(stream, label: str, count: int) -> int:
    """One choice from a numbered list, asked until it is one of them.

    A list of one is not a choice. It is taken, and the line says which.
    """

    if count == 1:
        print(f"{label}: 1")
        return 1
    options = "/".join(str(number) for number in range(1, count + 1))
    while True:
        typed = _read(stream, f"{label} [{options}]: ")
        if typed.isdigit() and 1 <= int(typed) <= count:
            return int(typed)
        print(f"Answer with a number from 1 to {count}.")


def _yes(stream, question: str) -> bool:
    """A yes or no answer, defaulting to yes on an empty line."""

    while True:
        typed = _read(stream, f"{question} [Y/n]: ").lower()
        if typed in ("", "y", "yes"):
            return True
        if typed in ("n", "no"):
            return False
        print("Answer y or n.")


# --- showing what happened -----------------------------------------------------


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
