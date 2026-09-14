"""Run ordinary CLI commands in one interactive ConsoleSession."""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass

from weaver.errors import CommandError, WeaverError

from .commandline import PROGRAM, command_names, command_words

PROMPT = "weaver> "

HISTORY_ENV = "WEAVER_SESSION_HISTORY"

#: A session is bound to one workspace; setup and Fabric estate management run
#: from a shell.
NOT_IN_A_SESSION = {
    "session": "already in a session",
    "fabric": "run it from a shell, not a session",
    "initialise": "run it from a shell, not a session",
    "initialize": "run it from a shell, not a session",
}

# Accepted at the prompt but omitted from the lifecycle banner.
SECONDARY_SESSION_COMMANDS = {"check", "doctor", "install"}

#: Lifecycle order used by the banner.
SESSION_COMMAND_ORDER = (
    "mirror",
    "build",
    "load",
    "test",
    "wipe",
    "workflow",
    "health",
)

EXITS = {"exit", "quit"}
HELP = {"help", "?"}


@dataclass(frozen=True)
class _Outcome:
    ran: bool = False
    leave: bool = False


def run_shell(
    args: argparse.Namespace,
    *,
    parser_factory=None,
    stdin=None,
    console=None,
) -> int:
    """Run a REPL in one :class:`~weaver.sessions.console.ConsoleSession`."""

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
    """Run command lines in one entry in order, stopping after a failure."""

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
    """Run one command without closing the Session on failure."""

    try:
        parsed = parser.parse_args(words)
    except SystemExit as leaving:
        # Argparse exits for help, version and usage errors; the session survives.
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
    """Offer an attachment Lakehouse before starting declared resources."""

    from .main import _resolve_workspace, command_lakehouses, command_requirements

    required = command_requirements(parsed)
    if not required:
        return
    try:
        workspace = _resolve_workspace(parsed)
        session.offer_spark_home(command_lakehouses(parsed), workspace=workspace)
        _report_skipped(session.prepare(required, workspace=workspace))
    except WeaverError:
        # Let the command report its own workspace error.
        pass


# --- where commands are read from --------------------------------------------


def _console(stream):
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
    """Resolve an explicit Workspace or one discovered in the current directory.

    Return ``None`` when neither the invocation nor ``workspace-config.yml``
    supplies one.
    """

    from weaver.config import discovered_workspace_config

    from .main import _resolve_workspace, workspace_supplied

    if not workspace_supplied(args) and discovered_workspace_config() is None:
        return None
    return _resolve_workspace(args)


def _available(parser) -> str:
    accepted = (
        command_names(parser) - set(NOT_IN_A_SESSION) - SECONDARY_SESSION_COMMANDS
    )
    ordered = [name for name in SESSION_COMMAND_ORDER if name in accepted]
    # New parser commands remain visible until assigned a lifecycle position.
    ordered += sorted(accepted - set(SESSION_COMMAND_ORDER))
    return ", ".join(ordered)


def _usage(parser) -> str:
    return (
        f"Available: {_available(parser)}.\n"
        f"Use normal CLI syntax; the leading `{PROGRAM}` is optional. "
        "Enter `help` for options or `exit` to leave.\n"
    )


def _banner(workspace, parser) -> None:
    if workspace is None:
        print("Weaver · No default workspace. Pass --workspace with each command.")
        print(f"\n{_usage(parser)}")
        return
    print(f"Weaver · {workspace.workspace}")


def _report_spending(session) -> None:
    print("\n" + session.telemetry.report(), file=sys.stderr)


def _report_warm_up(warm, parser) -> None:
    if warm.started:
        print(f"Starting: {', '.join(warm.started)}")
    _report_skipped(warm)
    print(f"\n{_usage(parser)}")


def _report_skipped(warm) -> None:
    for resource, reason in warm.skipped:
        print(f"Not started: {resource} - {reason}")


__all__ = ["SESSION_COMMAND_ORDER", "Prompt", "ScriptedInput", "run_shell"]
