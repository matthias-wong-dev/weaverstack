"""``weaver compose`` — one named sequence of ordinary Weaver commands.

The development loop is four commands and it is always the same four:

.. code-block:: text

    wipe → build → load → test

Typing them costs little; typing them *correctly* costs more, because each
carries the bindings, targets and workspace flags the last one had. So the
sequence is written down once:

.. code-block:: yaml

    compose:
      dev:
        - weaver wipe Lakehouse/Sales Warehouse/Reporting
        - weaver build ./repository --bind Lakehouse/Sales=Lakehouse/Sales
        - weaver load Warehouse/Reporting
        - weaver test Warehouse/Reporting

**Entries are ordinary Weaver command lines, and nothing else.** They are
parsed by the CLI's own parser and run by the CLI's own handlers, so there is
no second grammar to learn or to keep correct, and a command's options mean
here exactly what they mean at a prompt. Nothing shell-like is accepted: no
pipes, no redirection, no ``&&``, no environment assignments, no other
executables. A file that could run arbitrary programs would be a different and
much larger thing to hand somebody, and this one is meant to be readable at a
glance.

**One Session runs the whole sequence**, which is the point of composing at
all: authentication, item resolution and the Livy session are paid for once
across four commands rather than four times. Run inside ``weaver session``, the
composition joins the Session already open.

Deliberately not a workflow engine. No conditionals, no parallelism, no
retries, no matrices, no variables, no includes, no project-root discovery. It
runs the listed commands in order and stops at the first failure. When that
stops being enough the answer is a task runner, not a bigger ``compose``.
"""

from __future__ import annotations

import argparse
import shlex
import sys
from pathlib import Path

from weaver.errors import CommandError, WeaverError
from weaver.session.requirements import union

#: Where a composition lives when nobody says otherwise. Read from the working
#: directory and nowhere else — a file that is found by searching upwards is a
#: file whose meaning depends on where you happened to be standing.
DEFAULT_FILE = "compose.yml"

#: The top-level key. Named, rather than the document being a bare mapping of
#: sequences, so the file has somewhere to grow a sibling key without every
#: existing file becoming ambiguous.
COMPOSE_KEY = "compose"

#: Commands a composition may not contain, and why. ``session`` would hold a
#: prompt open in the middle of a sequence nobody is watching; ``compose``
#: would let a file include itself.
NOT_IN_A_COMPOSITION = {
    "session": "a composition cannot open an interactive prompt",
    "compose": "a composition cannot run another composition",
    "doctor": "run it from a shell; it reports on this machine, not a workspace",
}

#: What tells a command line from a shell line. Anything here means the entry
#: wanted a shell, and a shell is what this deliberately is not.
SHELL_CHARACTERS = ("|", ">", "<", "&", ";", "$", "`", "\n")


def load_composition(name: str, *, file: str | None = None) -> tuple[list[str], Path]:
    """The commands ``name`` resolves to, and the file they came from."""

    import yaml

    path = Path(file or DEFAULT_FILE).expanduser()
    if not path.is_file():
        raise CommandError(
            f"no composition file at {path}"
            + ("" if file else " — write one, or name one with --file")
        )
    try:
        document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        raise CommandError(f"{path}: {exc}") from exc

    if not isinstance(document, dict):
        raise CommandError(f"{path}: expected a mapping at the top level")
    compositions = document.get(COMPOSE_KEY)
    if compositions is None:
        raise CommandError(f"{path}: no {COMPOSE_KEY!r} key")
    if not isinstance(compositions, dict):
        raise CommandError(f"{path}: {COMPOSE_KEY!r} must be a mapping of names")
    if name not in compositions:
        known = ", ".join(sorted(compositions)) or "none"
        raise CommandError(f"{path}: no composition named {name!r} — found: {known}")

    entries = compositions[name]
    if not isinstance(entries, list) or not entries:
        raise CommandError(f"{path}: composition {name!r} must be a non-empty list")
    return [_one_entry(entry, path=path, name=name) for entry in entries], path


def _one_entry(entry, *, path: Path, name: str) -> str:
    if not isinstance(entry, str):
        raise CommandError(
            f"{path}: composition {name!r} contains a {type(entry).__name__}; "
            "every entry is one Weaver command line"
        )
    text = entry.strip()
    if not text:
        raise CommandError(f"{path}: composition {name!r} contains an empty entry")
    return text


def command_words(entry: str) -> list[str]:
    """One entry as argv, or a refusal saying what a composition accepts.

    The leading ``weaver`` is how the file reads as the commands somebody would
    type, so it is expected and then dropped. What it must not become is an
    invitation to write any other program's name there.
    """

    for character in SHELL_CHARACTERS:
        if character in entry:
            raise CommandError(
                f"{entry!r} is not a Weaver command: a composition runs Weaver "
                "commands, not shell lines"
            )
    try:
        words = shlex.split(entry)
    except ValueError as exc:
        raise CommandError(f"{entry!r}: {exc}") from exc
    if not words:
        raise CommandError("a composition entry cannot be empty")
    if words[0] != "weaver":
        raise CommandError(
            f"{entry!r} is not a Weaver command: every entry begins with 'weaver'"
        )
    rest = words[1:]
    if not rest:
        raise CommandError(f"{entry!r} names no command")
    refusal = NOT_IN_A_COMPOSITION.get(rest[0])
    if refusal is not None:
        raise CommandError(f"{entry!r}: {refusal}")
    return rest


def run_composition(args: argparse.Namespace, *, parser_factory=None, stdin=None) -> int:
    """Show the sequence, ask once, then run it in one Session."""

    from weaver.session.host import use_or_create_session

    if parser_factory is None:
        from .main import build_parser

        parser_factory = build_parser
    parser = parser_factory()

    entries, path = load_composition(args.name, file=args.file)
    # Parsed before anything is shown, so a typo in the last entry is reported
    # before the first one has changed a workspace. A sequence that is displayed
    # and confirmed should be a sequence that can run.
    parsed_commands = [_parse(parser, entry) for entry in entries]

    _show(args.name, path, entries)
    stream = stdin or sys.stdin
    if not _interactive(stream):
        # A refusal, not a decision — so it is worth a non-zero status. Silence
        # is the one answer that must never be read as yes: the first entry of a
        # development composition is usually a wipe.
        print(
            "Refusing to run a composition without confirmation: "
            "no interactive input stream.",
            file=sys.stderr,
        )
        return 1
    if not _confirmed(stream):
        # Somebody was asked and said no. Nothing went wrong.
        print("Nothing was run.")
        return 0

    from .shell import _default_workspace

    workspace = _default_workspace(args)
    with use_or_create_session(getattr(args, "session", None), workspace=workspace) as session:
        _warm_for(session, parsed_commands, workspace=workspace)
        try:
            return _execute(entries, parsed_commands, session=session)
        finally:
            if getattr(args, "timings", False):
                from .shell import _report_spending

                _report_spending(session)


def _warm_for(session, parsed_commands, *, workspace) -> None:
    """Start everything the whole sequence will want, before the first command.

    The union rather than each command's own: a sequence that ends in a load
    should not wait for a Spark session at the end of the build in front of it,
    and the resources are shared, so warming the maximum set once is warming it
    correctly.

    Speculative throughout. A composition of nothing but Warehouse work asks for
    no Spark and gets none; one that asks for it and turns out not to need it
    has paid for a head start nobody used, which is the right way round.
    """

    from .main import command_requirements

    if workspace is None:
        return
    required = union(*(command_requirements(parsed) for parsed in parsed_commands))
    if not required:
        return
    warm = session.prepare(required, workspace=workspace)
    if warm.started:
        print(f"Starting in the background: {', '.join(warm.started)}\n")


def _parse(parser: argparse.ArgumentParser, entry: str) -> argparse.Namespace:
    """One entry, through the CLI's own parser."""

    words = command_words(entry)
    try:
        parsed = parser.parse_args(words)
    except SystemExit as exc:
        # argparse has already said what was wrong with the line; what it cannot
        # say is which entry of which composition it was reading.
        raise CommandError(f"{entry!r} is not a valid Weaver command") from exc
    if getattr(parsed, "handler", None) is None:
        raise CommandError(f"{entry!r} names no command")
    return parsed


def _show(name: str, path: Path, entries: list[str]) -> None:
    print(f"Compose: {name}  ({path})\n")
    for number, entry in enumerate(entries, start=1):
        print(f"{number}. {entry}")
    print()


def _interactive(stdin) -> bool:
    """Whether there is somebody there to answer.

    A terminal, and nothing else. A pipe or a file is not somebody: the first
    entry of a development composition is usually a wipe, and silence must never
    be read as yes.
    """

    try:
        return bool(stdin.isatty())
    except (AttributeError, ValueError):
        return False


def _confirmed(stdin) -> bool:
    """Explicit ``y``/``yes`` and nothing else, defaulting to no.

    Reads the stream it was given rather than calling ``input()``, which always
    reads ``sys.stdin``. Passing a stream in and then asking a different one is
    how a test proves a confirmation it never actually gave — and for the one
    prompt that authorises a wipe, the stream that is checked for a terminal has
    to be the stream that is read.
    """

    print("Execute this sequence? [y/N] ", end="", flush=True)
    answer = stdin.readline()
    return answer.strip().lower() in {"y", "yes"}


def _execute(entries, parsed_commands, *, session) -> int:
    """In order, in one Session, stopping at the first failure."""

    for number, (entry, parsed) in enumerate(zip(entries, parsed_commands), start=1):
        print(f"\n[{number}/{len(entries)}] {entry}\n")
        parsed.session = session
        # The sequence was authorised as a whole, so a command that would
        # otherwise stop to confirm does not ask a second time for what has
        # already been shown and agreed to.
        parsed.authorised = True
        try:
            status = parsed.handler(parsed)
        except WeaverError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return _stopped(number, entry)
        if status:
            return _stopped(number, entry)
    print(f"\n✓ {len(entries)} command(s)")
    return 0


def _stopped(number: int, entry: str) -> int:
    print(f"\nstopped at [{number}] {entry}", file=sys.stderr)
    return 1


__all__ = ["command_words", "load_composition", "run_composition"]
