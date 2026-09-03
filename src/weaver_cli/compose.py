"""Named sequences of Weaver commands run in one Session."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from weaver.errors import CommandError, WeaverError
from weaver.sessions.requirements import union

from .commandline import command_words

#: Default composition file in the current directory.
DEFAULT_FILE = "compose.yml"

#: Top-level key containing named compositions.
COMPOSE_KEY = "compose"

#: Commands that cannot run inside a composition.
NOT_IN_A_COMPOSITION = {
    "session": "a composition cannot open an interactive prompt",
    "compose": "a composition cannot run another composition",
}


def load_composition(name: str, *, file: str | None = None) -> tuple[list[str], Path]:
    """The commands ``name`` resolves to, and the file they came from."""

    import yaml

    path = Path(file or DEFAULT_FILE).expanduser()
    if not path.is_file():
        raise CommandError(
            f"no composition file at {path}"
            + ("" if file else ". Write one, or name one with --file")
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
        raise CommandError(f"{path}: no composition named {name!r}. Found: {known}")

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


def composition_words(entry: str) -> list[str]:
    """The arguments one composition entry means.

    The leading ``weaver`` is optional here, because a composition holds
    nothing else and a line written for the file need not repeat it.
    """

    return command_words(entry, excluded=NOT_IN_A_COMPOSITION)


def run_composition(
    args: argparse.Namespace, *, parser_factory=None, stdin=None
) -> int:
    """Show the sequence, ask once, then run it in one Session."""

    from weaver.sessions.host import use_or_create_session

    if parser_factory is None:
        from .main import build_parser

        parser_factory = build_parser
    parser = parser_factory()

    entries, path = load_composition(args.name, file=args.file)
    # Parse every entry, and resolve the one Workspace, before displaying a
    # sequence for confirmation. A malformed workspace configuration is a fact
    # about the invocation, and it is reported here.
    parsed_commands = [_parse(parser, entry) for entry in entries]
    workspace = _composition_workspace(args, parsed_commands)

    _show(args.name, path, entries)
    if not getattr(args, "yes", False):
        stream = stdin or sys.stdin
        if not _interactive(stream):
            print(
                "A composition asks before it runs. Pass --yes to run it unattended.",
                file=sys.stderr,
            )
            return 1
        if not _confirmed(stream):
            print("Composition cancelled.")
            return 0

    with use_or_create_session(
        getattr(args, "session", None), workspace=workspace
    ) as session:
        _warm_for(session, parsed_commands, workspace=workspace)
        try:
            from weaver.run import new_workflow_id

            with session.workflow(new_workflow_id()):
                return _execute(entries, parsed_commands, session=session)
        finally:
            if getattr(args, "timings", False):
                from .shell import _report_spending

                _report_spending(session)


def _composition_workspace(args, parsed_commands):
    """The one Workspace named by the composition or its commands."""

    from .main import _resolve_workspace, workspace_supplied
    from .shell import _default_workspace

    workspaces = []
    outer = _default_workspace(args)
    if outer is not None:
        workspaces.append(outer)
    for parsed in parsed_commands:
        # An entry naming no workspace takes the composition's. One that names
        # a configuration file raises the error that file carries.
        if not workspace_supplied(parsed):
            continue
        workspace = _resolve_workspace(parsed)
        if workspace not in workspaces:
            workspaces.append(workspace)
    if not workspaces:
        return None
    if len(workspaces) > 1:
        raise CommandError(
            "a composition runs in one Workspace; its commands name different "
            "workspace configurations"
        )
    return workspaces[0]


def _warm_for(session, parsed_commands, *, workspace) -> None:
    """Start resources required by a composition before its first command.

    The whole sequence is known, so the Lakehouses go in as one set: Fabric
    attaches a Spark session to a Lakehouse, and a sequence whose Spark work is
    at the end should not wait for a session to start there.
    """

    from .main import command_lakehouses, command_requirements

    if workspace is None:
        return
    required = union(*(command_requirements(parsed) for parsed in parsed_commands))
    if not required:
        return
    lakehouses = []
    for parsed in parsed_commands:
        for name in command_lakehouses(parsed):
            if name not in lakehouses:
                lakehouses.append(name)
    session.offer_spark_home(lakehouses, workspace=workspace)
    warm = session.prepare(required, workspace=workspace)
    if warm.started:
        print(f"Starting: {', '.join(warm.started)}\n")


def _parse(parser: argparse.ArgumentParser, entry: str) -> argparse.Namespace:
    """One entry, through the CLI's own parser."""

    words = composition_words(entry)
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
    """Return whether the input stream supports confirmation."""

    try:
        return bool(stdin.isatty())
    except (AttributeError, ValueError):
        return False


def _confirmed(stdin) -> bool:
    """Read an explicit yes confirmation from the given stream."""

    print("Execute this sequence? [y/N] ", end="", flush=True)
    answer = stdin.readline()
    return answer.strip().lower() in {"y", "yes"}


def _execute(entries, parsed_commands, *, session) -> int:
    """In order, in one Session, stopping at the first failure."""

    for number, (entry, parsed) in enumerate(zip(entries, parsed_commands), start=1):
        print(f"\n[{number}/{len(entries)}] {entry}\n")
        parsed.session = session
        # The sequence confirmation authorises each command.
        parsed.authorised = True
        try:
            status = parsed.handler(parsed)
        except WeaverError as exc:
            from .main import _render_error

            _render_error(exc)
            return _stopped(number, entry)
        if status:
            return _stopped(number, entry)
    print(f"\n✓ {len(entries)} command(s)")
    return 0


def _stopped(number: int, entry: str) -> int:
    print(f"\nComposition stopped at [{number}] {entry}", file=sys.stderr)
    return 1


__all__ = ["composition_words", "load_composition", "run_composition"]
