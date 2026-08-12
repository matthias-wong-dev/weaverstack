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

#: Where the prompt remembers what was typed. Overridable, because a shared or
#: read-only home is somebody's real setup and a session that refused to start
#: over its history file would be worse than one that forgets.
HISTORY_ENV = "WEAVER_SESSION_HISTORY"
HISTORY_LIMIT = 1000

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
    history = _enable_line_editing()
    with ConsoleSession(workspace=workspace) as session:
        _banner(workspace)
        if workspace is not None:
            # Proactive, and deliberately not awaited: the prompt is available
            # while the credential and the Spark session are still being
            # acquired, and the first command that needs either waits on the
            # acquisition already running rather than starting a second one.
            _report_warm_up(session.warm())
        try:
            return _loop(session, parser, stdin=stdin or sys.stdin)
        finally:
            _save_history(history)
            if getattr(args, "timings", False):
                _report_spending(session)


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
    _prepare_for(session, parsed)
    try:
        handler(parsed)
    except WeaverError as exc:
        print(f"error: {exc}", file=sys.stderr)
    except KeyboardInterrupt:
        print("\ninterrupted", file=sys.stderr)
    except Exception as exc:  # noqa: BLE001 - the prompt outlives a defect too
        print(f"error: {type(exc).__name__}: {exc}", file=sys.stderr)


def _prepare_for(session, parsed) -> None:
    """Start resources declared by a command before it runs."""

    from .main import _resolve_workspace, command_requirements

    required = command_requirements(parsed)
    if not required:
        return
    try:
        session.prepare(required, workspace=_resolve_workspace(parsed))
    except WeaverError:
        # Let the command report its own workspace error.
        pass


def _enable_line_editing():
    """Give the prompt arrow keys, editing and history, and return where to save.

    ``input()`` is line-edited only if :mod:`readline` has been imported — the
    import is the whole mechanism, which is why a prompt without it answers the
    up arrow with ``^[[A`` instead of the last command. Nothing else in Weaver
    imports it, so nothing else was enabling it.

    Every part of this is best-effort. A platform without readline, a home
    directory that is read-only, a corrupt history file: none of them is a
    reason to refuse to start a session.
    """

    try:
        import readline
    except ImportError:  # a platform without it still gets a working prompt
        return None

    path = _history_path()
    if path is not None:
        try:
            readline.read_history_file(str(path))
        except (OSError, ValueError):
            pass  # no history yet, or none that can be read
    readline.set_history_length(HISTORY_LIMIT)
    return path


def _history_path():
    import os
    from pathlib import Path

    override = os.environ.get(HISTORY_ENV)
    if override:
        return Path(override).expanduser()
    try:
        return Path.home() / ".weaver" / "session_history"
    except (OSError, RuntimeError):  # a home that cannot be resolved
        return None


def _save_history(path) -> None:
    if path is None:
        return
    try:
        import readline

        path.parent.mkdir(parents=True, exist_ok=True)
        readline.write_history_file(str(path))
    except (ImportError, OSError, ValueError):
        pass  # the session is over; failing to remember it is not a failure


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
    """Return the default workspace when the invocation defines one."""

    from .main import _resolve_workspace

    try:
        return _resolve_workspace(args)
    except WeaverError:
        return None


def _banner(workspace) -> None:
    if workspace is None:
        print("Weaver · No default workspace")
        print("Use --workspace on each command.")
        print("\nCommands: wipe, build, load, test. Type `exit` to leave.\n")
        return
    print(f"Weaver · {workspace.workspace}")


def _report_spending(session) -> None:
    """Print session time grouped by transport."""

    print("\n" + session.telemetry.report(), file=sys.stderr)


def _report_warm_up(warm) -> None:
    """Report session resources that are starting or unavailable."""

    if warm.started:
        print(f"Starting: {', '.join(warm.started)}")
    for resource, reason in warm.skipped:
        print(f"Not started: {resource} — {reason}")
    print("\nCommands: wipe, build, load, test. Type `exit` to leave.\n")


__all__ = ["run_shell"]
