"""Interactive Weaver session: ordinary CLI commands, one ConsoleSession.

Commands are written as they are in a terminal, in ``compose.yml`` and in the
documentation, ``weaver build .``, and are parsed by the top-level CLI parser
and run by its handlers. The leading ``weaver`` is optional at the prompt, so
``build .`` and ``weaver build .`` are the same command. What the session adds
is the Session underneath them, held open so a credential, item resolution and
Livy are paid for once.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass

from weaver.errors import CommandError, WeaverError

from .commandline import PROGRAM, command_names, command_words

PROMPT = "weaver> "

#: Optional history-file override.
HISTORY_ENV = "WEAVER_SESSION_HISTORY"

#: Commands unavailable from an interactive session. A session holds one
#: workspace open for repository and bundle work; Fabric estate management is
#: done from a shell.
NOT_IN_A_SESSION = {
    "session": "already in a session",
    "fabric": "run it from a shell, not a session",
    # A session is open on a workspace already. Setting a project up is what
    # comes before one, and it creates Fabric items, which a session does not.
    "initialise": "run it from a shell, not a session",
    "initialize": "run it from a shell, not a session",
}

# Source checking, connectivity checking and bundle installation remain usable at
# the prompt, but the session banner introduces the core lifecycle rather than
# every available seam.
SECONDARY_SESSION_COMMANDS = {"check", "doctor", "install"}

EXITS = {"exit", "quit"}
HELP = {"help", "?"}


@dataclass(frozen=True)
class _Outcome:
    """What one prompt entry did: whether it ran, and whether it said to leave."""

    ran: bool = False
    leave: bool = False


def run_shell(
    args: argparse.Namespace,
    *,
    parser_factory=None,
    stdin=None,
    console=None,
) -> int:
    """Hold one :class:`~weaver.sessions.console.ConsoleSession` open for a REPL."""

    from weaver.sessions import ConsoleSession

    if parser_factory is None:
        from .main import build_parser

        parser_factory = build_parser
    parser = parser_factory()

    workspace = _default_workspace(args)
    with ConsoleSession(workspace=workspace) as session:
        _banner(workspace, parser)
        if workspace is not None:
            # Start reusable resources while the prompt remains available.
            _report_warm_up(session.warm(), parser)
        reader = console if console is not None else _console(stdin or sys.stdin)
        try:
            return _loop(session, parser, reader)
        finally:
            reader.close()
            if getattr(args, "timings", False):
                _report_spending(session)


def _loop(session, parser, console) -> int:
    """Read an entry, run it, settle the terminal, ask again."""

    while True:
        try:
            entry = console.read()
        except KeyboardInterrupt:
            # Ctrl-C abandons what was being typed and asks again.
            continue
        except EOFError:
            print()
            return 0

        if entry is None:
            return 0
        outcome = _run_entry(session, parser, entry)
        # The shell owns the transition from output back to the prompt: the
        # renderer's transient line is taken down here rather than by whichever
        # command drew it.
        session.stop_presenting()
        if outcome.ran:
            console.settle()
        if outcome.leave:
            return 0


def _run_entry(session, parser, entry: str) -> _Outcome:
    """Run every command line in one prompt entry, in order.

    A pasted block is several complete Weaver commands, one per line, and a
    failure stops the rest of it: the commands after a failed build were
    written expecting it to have succeeded.
    """

    ran = False
    for line in entry.splitlines():
        text = line.strip()
        if not text or text.startswith("#"):
            continue
        if text in EXITS:
            return _Outcome(ran=ran, leave=True)
        if text in HELP:
            parser.print_help()
            ran = True
            continue
        try:
            words = command_words(text, excluded=NOT_IN_A_SESSION)
        except CommandError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return _Outcome(ran=True)
        ran = True
        if not _run_one(session, parser, words):
            return _Outcome(ran=True)
    return _Outcome(ran=ran)


def _run_one(session, parser, words: list[str]) -> bool:
    """One command, whose failure the session outlives. True when it succeeded."""

    try:
        parsed = parser.parse_args(words)
    except SystemExit as leaving:
        # argparse has answered the line itself: `--help` and `--version` print
        # and exit zero, a usage error prints and exits non-zero. Either way it
        # has said what it needed to, and neither is a reason to throw away a
        # Livy session.
        return not leaving.code

    handler = getattr(parsed, "handler", None)
    if handler is None:
        parser.print_help()
        return True

    parsed.session = session
    _prepare_for(session, parsed)
    try:
        return not handler(parsed)
    except WeaverError as exc:
        print(f"error: {exc}", file=sys.stderr)
    except KeyboardInterrupt:
        # Interrupting a command leaves the session and its resources up.
        print("\ninterrupted", file=sys.stderr)
    except Exception as exc:  # noqa: BLE001 - the prompt outlives a defect too
        print(f"error: {type(exc).__name__}: {exc}", file=sys.stderr)
    return False


def _prepare_for(session, parsed) -> None:
    """Start resources declared by a command before it runs.

    The Lakehouses the command names go in first, because Fabric attaches a
    Spark session to one and a warm-up that had none to attach to would be
    skipped.
    """

    from .main import _resolve_workspace, command_lakehouses, command_requirements

    required = command_requirements(parsed)
    if not required:
        return
    try:
        workspace = _resolve_workspace(parsed)
        session.offer_spark_home(command_lakehouses(parsed), workspace=workspace)
        # A resource the command needs and this workspace cannot start is
        # reported here, where the reader can still act on it before the command
        # fails for the same reason further in.
        _report_skipped(session.prepare(required, workspace=workspace))
    except WeaverError:
        # Let the command report its own workspace error.
        pass


# --- where commands are read from --------------------------------------------


def _console(stream):
    """The reader for this input: a terminal prompt, or a scripted stream."""

    if stream is sys.stdin and _isatty(stream):
        return Prompt()
    return ScriptedInput(stream)


def _isatty(stream) -> bool:
    try:
        return bool(stream.isatty())
    except (AttributeError, ValueError):
        return False


class Prompt:
    """A terminal prompt with editing, history and bracketed paste.

    ``prompt_toolkit`` owns the line editor, so a pasted block arrives as one
    entry with its newlines intact, and the prompt is redrawn by a renderer
    that knows where the cursor is.
    """

    def __init__(self, *, input=None, output=None, history_path=None) -> None:
        from prompt_toolkit.history import FileHistory, InMemoryHistory
        from prompt_toolkit.shortcuts import PromptSession

        path = history_path if history_path is not None else _history_path()
        history = InMemoryHistory()
        if path is not None:
            try:
                path.parent.mkdir(parents=True, exist_ok=True)
                history = FileHistory(str(path))
            except OSError:
                pass  # a read-only home is not a reason to refuse a session
        self._session = PromptSession(history=history, input=input, output=output)
        self._stream = getattr(output, "stdout", None) or sys.stdout

    def read(self) -> str | None:
        return self._session.prompt(PROMPT)

    def settle(self) -> None:
        """Leave the cursor at the start of a blank line before the next prompt."""

        print(file=self._stream, flush=True)

    def close(self) -> None:
        pass


class ScriptedInput:
    """Commands from a stream that is not a terminal, one line per entry."""

    def __init__(self, stream) -> None:
        self._stream = stream

    def read(self) -> str | None:
        line = self._stream.readline()
        if not line:
            return None
        print(f"{PROMPT}{line.rstrip()}")
        return line

    def settle(self) -> None:
        pass

    def close(self) -> None:
        pass


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


# --- what the session says about itself --------------------------------------


def _default_workspace(args: argparse.Namespace):
    """Return the default workspace when the invocation defines one.

    An invocation names one on the command line, or by being run from a project
    directory: `workspace-config.yml` beside it is what a composition of bare
    entries runs in, and what a session with no arguments opens on. An
    invocation with neither has none, and that is a state.

    A configuration file that cannot be read raises the ``ConfigError`` it
    carries, naming the field that is wrong.
    """

    from weaver.config import discovered_workspace_config

    from .main import _resolve_workspace, workspace_supplied

    if not workspace_supplied(args) and discovered_workspace_config() is None:
        return None
    return _resolve_workspace(args)


def _available(parser) -> str:
    """The commands this session accepts, from the parser rather than a list."""

    return ", ".join(
        sorted(
            command_names(parser) - set(NOT_IN_A_SESSION) - SECONDARY_SESSION_COMMANDS
        )
    )


def _usage(parser) -> str:
    return (
        f"Available: {_available(parser)}.\n"
        "Commands are written as they are in a terminal; the leading "
        f"`{PROGRAM}` is optional. `help` for options, `exit` to leave.\n"
    )


def _banner(workspace, parser) -> None:
    if workspace is None:
        print("Weaver · No default workspace")
        print("Use --workspace on each command.")
        print(f"\n{_usage(parser)}")
        return
    print(f"Weaver · {workspace.workspace}")


def _report_spending(session) -> None:
    """Print session time grouped by transport."""

    print("\n" + session.telemetry.report(), file=sys.stderr)


def _report_warm_up(warm, parser) -> None:
    """Report session resources that are starting or unavailable."""

    if warm.started:
        print(f"Starting: {', '.join(warm.started)}")
    _report_skipped(warm)
    print(f"\n{_usage(parser)}")


def _report_skipped(warm) -> None:
    """Name any resource that could not start, and why."""

    for resource, reason in warm.skipped:
        print(f"Not started: {resource} - {reason}")


__all__ = ["Prompt", "ScriptedInput", "run_shell"]
