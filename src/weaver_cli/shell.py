"""``weaver session`` — one console session, many commands.

The cost of a Fabric command is mostly not the command. It is acquiring a
credential, asking the workspace what a handful of names mean, and waiting for a
Spark session that a small capacity will only give you one of. A developer
running ``wipe``, ``build``, ``load`` and ``test`` pays all of that four times:

.. code-block:: text

    weaver wipe  ...   auth, resolve, Livy, work, close
    weaver build ...   auth, resolve, Livy, work, close
    weaver load  ...   auth, resolve, Livy, work, close
    weaver test  ...   auth, resolve, Livy, work, close

The shell pays once:

.. code-block:: text

    weaver session     auth, Livy starting in the background
    weaver> wipe  ...  work
    weaver> build ...  work
    weaver> load  ...  work
    weaver> test  ...  work
    weaver> exit       close

**Same grammar, same handlers.** The shell parses each line with the CLI's own
parser and calls the same handler function, with one difference: the Session is
attached to the parsed arguments, so every operation reuses it instead of
creating one. A second command grammar would be a second thing to keep correct,
and would drift.

**No workspace is required to start.** A workspace arrives with each command,
exactly as it does for a one-shot invocation, and the Session caches resources
per workspace context. A ``--workspace`` given here is a default context for
commands that name none — never the Session's identity.

**An ordinary failure does not end the session.** A build that fails, a typo, a
Spark error: the command reports and the prompt returns, with the Livy session
still up. Only a genuinely dead resource is reacquired, and only within its
bounded allowance.
"""

from __future__ import annotations

import argparse
import shlex
import sys

from weaver.errors import WeaverError

PROMPT = "weaver> "

#: Commands the shell does not run, and why. ``session`` would nest a console
#: inside a console; the rest are one-shot machine-level operations whose whole
#: point is a fresh process.
NOT_IN_A_SESSION = {
    "session": "already in a session",
    "doctor": "run it from a shell, not a session",
}

EXITS = {"exit", "quit"}
HELP = {"help", "?"}


def run_shell(args: argparse.Namespace, *, parser_factory=None, stdin=None) -> int:
    """Hold one :class:`~weaver.session.console.ConsoleSession` open for a REPL."""

    from weaver.session import ConsoleSession

    if parser_factory is None:
        from .main import build_parser

        parser_factory = build_parser
    parser = parser_factory()

    workspace = _default_workspace(args)
    with ConsoleSession(workspace=workspace) as session:
        _banner(workspace)
        if workspace is not None:
            # Proactive, and deliberately not awaited: the prompt is available
            # while the credential and the Spark session are still being
            # acquired, and the first command that needs either waits on the
            # acquisition already running rather than starting a second one.
            session.warm()
        return _loop(session, parser, stdin=stdin or sys.stdin)


def _loop(session, parser, *, stdin) -> int:
    while True:
        try:
            line = _read(stdin)
        except KeyboardInterrupt:
            print()
            continue
        except EOFError:
            print()
            return 0

        if line is None:
            return 0
        words = _words(line)
        if words is None:
            continue
        if not words:
            continue
        if words[0] in EXITS:
            return 0
        if words[0] in HELP:
            parser.print_help()
            continue
        refusal = NOT_IN_A_SESSION.get(words[0])
        if refusal is not None:
            print(f"error: {words[0]}: {refusal}", file=sys.stderr)
            continue

        _run_one(session, parser, words)


def _run_one(session, parser, words: list[str]) -> None:
    """One command, whose failure the session outlives."""

    try:
        parsed = parser.parse_args(words)
    except SystemExit:
        # argparse has already printed what was wrong with the line. A usage
        # error is not a reason to throw away a Livy session.
        return

    handler = getattr(parsed, "handler", None)
    if handler is None:
        parser.print_help()
        return

    parsed.session = session
    try:
        handler(parsed)
    except WeaverError as exc:
        print(f"error: {exc}", file=sys.stderr)
    except KeyboardInterrupt:
        print("\ninterrupted", file=sys.stderr)
    except Exception as exc:  # noqa: BLE001 - the prompt outlives a defect too
        print(f"error: {type(exc).__name__}: {exc}", file=sys.stderr)


def _read(stdin) -> str | None:
    if stdin is sys.stdin and stdin.isatty():
        return input(PROMPT)
    line = stdin.readline()
    if not line:
        return None
    print(f"{PROMPT}{line.rstrip()}")
    return line


def _words(line: str) -> list[str] | None:
    text = line.strip()
    if not text or text.startswith("#"):
        return []
    try:
        return shlex.split(text)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return None


def _default_workspace(args: argparse.Namespace):
    """The default context, where the invocation gave enough to resolve one.

    Leniently: a session that cannot resolve a default is still a useful
    session, because every command may name its own workspace. What would be
    unhelpful is refusing to start.
    """

    from .main import _resolve_workspace

    try:
        return _resolve_workspace(args)
    except WeaverError:
        return None


def _banner(workspace) -> None:
    print(f"Weaver · {workspace.workspace if workspace else 'no default workspace'}")
    if workspace is not None:
        print("Starting resources in the background...")
    print("Commands are the ordinary CLI commands. `exit` to leave.\n")


__all__ = ["run_shell"]
