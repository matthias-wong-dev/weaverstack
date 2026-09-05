"""Collect and review project choices before initialisation."""

from __future__ import annotations

import argparse
import sys

from weaver.errors import CommandError
from weaver.initialise import DEFAULT_CATALOGUE, DEFAULT_ENVIRONMENT
from weaver.onboarding.project import ProjectRequest, validate_fabric_name

INTRODUCTION = "Set up a Weaver project."
INSTALL_WARNING = "Publishing a Fabric Environment can take about 5 minutes."
DISPLAY = {"existing": "already exists", "planned": "create"}
ROLE_WIDTH = 14
NAME_WIDTH = 19
GAP = 2


def _can_ask(stream):
    return stream.isatty()


def collect_workspace(args, *, ask=True, stdin=None):
    """Collect the workspace needed for item discovery."""

    stream = stdin if stdin is not None else sys.stdin
    if args.workspace or not ask or not (args.interactive or _can_ask(stream)):
        return False
    print(INTRODUCTION)
    print(f"Project folder: {args.repository}")
    args.workspace = _answer(stream, "Fabric workspace")
    return True


def collect(
    args: argparse.Namespace,
    *,
    ask=True,
    stdin=None,
    environments=None,
    items=None,
    introduced=False,
) -> bool:
    """Collect names, allow revisions, and record publication preference."""

    stream = stdin if stdin is not None else sys.stdin
    interactive = ask and (args.interactive or _can_ask(stream))
    if not interactive:
        _validate(args)
        return False
    if not introduced:
        print(INTRODUCTION)
        print(f"Project folder: {args.repository}")
    if not args.workspace:
        args.workspace = _answer(stream, "Fabric workspace")
    if not args.catalogue:
        print(
            "\nCatalogue\nThe Catalogue is a Warehouse where Weaver keeps its build, load and test state."
        )
        args.catalogue = _item_answer(
            stream, "Catalogue name", "Warehouse", default=DEFAULT_CATALOGUE
        )
    _collect_workspace_items(args, stream, environments=environments, items=items)
    if args.example is None:
        args.example = _yes(stream, "Add the Sales example to this project?")
    fields = (
        ("repository", "Folder"),
        ("workspace", "Workspace"),
        ("catalogue", "Catalogue"),
        ("environment", "Environment"),
        ("lakehouse", "Lakehouse"),
        ("warehouse", "Warehouse"),
        ("example", "Sales example"),
    )
    while True:
        print("\nProject\n")
        for field, label in fields:
            value = getattr(args, field)
            if field == "example":
                value = "Yes" if value else "No"
            print(f"  {label:14}{value or 'None'}")
        print("\n1. Continue\n2. Change an answer\n3. Cancel")
        choice = _numbered(stream, "Choose", 3)
        if choice == 3:
            args.cancelled = True
            return True
        if choice == 1:
            try:
                _validate(args)
            except CommandError as exc:
                print(str(exc))
                continue
            break
        for number, (_, label) in enumerate(fields, 1):
            print(f"{number}. {label}")
        field, label = fields[_numbered(stream, "Change", len(fields)) - 1]
        if field == "example":
            value = _yes(stream, "Add the Sales example to this project?")
        elif field in ("catalogue", "environment", "lakehouse", "warehouse"):
            kind = "Warehouse" if field == "catalogue" else label
            value = _item_answer(
                stream, label, kind, skippable=field in ("lakehouse", "warehouse")
            )
        else:
            value = _answer(stream, label)
        changed_workspace = field == "workspace" and value != args.workspace
        setattr(args, field, value)
        if changed_workspace:
            args.environment = args.lakehouse = args.warehouse = None
            _collect_workspace_items(
                args, stream, environments=environments, items=items
            )
    if not args.dry_run and not getattr(args, "publish_environment", False):
        print(INSTALL_WARNING)
        args.publish_environment = _yes(stream, "Publish the Environment now?")
    return True


def _collect_workspace_items(args, stream, *, environments, items):
    """Collect Environment and target choices from the selected workspace."""

    if not args.environment:
        print(
            "\nEnvironment\nWeaver uses a Fabric Environment for Python work. Its definition is kept in this project so you can add packages later."
        )
        available = tuple(environments(args.workspace)) if environments else ()
        print("1. Use an existing Environment\n2. Create a new Environment")
        if available and _numbered(stream, "Choose", 2) == 1:
            for number, name in enumerate(available, 1):
                print(f"{number}. {name}")
            args.environment = available[
                _numbered(stream, "Choose an Environment", len(available)) - 1
            ]
        else:
            if not available:
                print("This workspace has no Environments yet.")
            args.environment = _item_answer(
                stream, "Environment name", "Environment", default=DEFAULT_ENVIRONMENT
            )
    for kind, purpose, examples in (
        ("Lakehouse", "files and Python/Delta tables", "Landing, Bronze"),
        ("Warehouse", "SQL tables and views", "Curated, Reporting"),
    ):
        if not getattr(args, kind.lower()):
            print(f"\n{kind} (optional)\nUse a {kind} for {purpose}.")
            print("Press Enter if this project does not need one.")
            available = tuple(items(args.workspace, kind)) if items else ()
            if available:
                print(f"Existing: {', '.join(available)}")
            print(f"Examples: {examples}")
            setattr(
                args, kind.lower(), _item_answer(stream, kind, kind, skippable=True)
            )


def _validate(args):
    if not args.workspace:
        raise CommandError("Provide --workspace.")
    ProjectRequest(
        workspace=args.workspace,
        catalogue=args.catalogue or DEFAULT_CATALOGUE,
        environment=args.environment or DEFAULT_ENVIRONMENT,
        lakehouse=args.lakehouse,
        warehouse=args.warehouse,
        example=bool(args.example),
    )


def _read(stream, prompt):
    print(prompt, end="", flush=True)
    line = stream.readline()
    if line == "":
        raise CommandError(
            "The answers ran out before the questions did. Provide the names and --no-input, or run at a terminal."
        )
    return line.strip()


def _answer(stream, label, *, default=None, skippable=False):
    while True:
        typed = _read(stream, f"{label} [{default}]: " if default else f"{label}: ")
        if typed or default is not None:
            return typed or default
        if skippable:
            return None
        print(f"{label} is needed to continue.")


def _item_answer(stream, label, kind, **kwargs):
    while True:
        value = _answer(stream, label, **kwargs)
        try:
            return validate_fabric_name(value, kind) if value else None
        except CommandError as exc:
            print(str(exc))


def _numbered(stream, label, count):
    while True:
        typed = _read(stream, f"{label} [{'/'.join(map(str, range(1, count + 1)))}]: ")
        if typed.isdigit() and 1 <= int(typed) <= count:
            return int(typed)
        print(f"Answer with a number from 1 to {count}.")


def _yes(stream, question):
    while True:
        typed = _read(stream, f"{question} [y/N]: ").lower()
        if typed in ("", "n", "no"):
            return False
        if typed in ("y", "yes"):
            return True
        print("Answer y or n.")


def equivalent_command(args):
    import shlex

    parts = ["weaver", "initialise", str(args.repository)]
    for field in ("workspace", "catalogue", "environment", "lakehouse", "warehouse"):
        value = getattr(args, field)
        if value:
            parts.extend(["--" + field, value])
    if args.example:
        parts.append("--example")
    if getattr(args, "publish_environment", False):
        parts.append("--publish-environment")
    parts.append("--no-input")
    return shlex.join(parts)


def _table(report):
    for outcome in report.resources:
        print(
            f"  {outcome.role:14}{outcome.name:22}{DISPLAY.get(outcome.status, outcome.status)}"
        )
    print(f"  Environment publication: {report.environment_publication}")


def render(report):
    """Report completed setup and the commands to run from the project."""

    import shlex

    from weaver.onboarding.environment import environment_directory

    _table(report)
    print(f"\nYour Weaver project is ready in {report.repository}.")
    print(f"\nNext:\n\n  cd {shlex.quote(report.repository)}")
    if report.environment_publication == "deferred":
        name = next(
            item.name for item in report.resources if item.role == "Environment"
        )
        print("\nEnvironment publication was deferred.")
        print("Publish before your first load that needs Python execution.\n")
        print(
            f"  weaver fabric environment publish --path {shlex.quote(environment_directory(name))}"
        )
    for command in report.next_commands:
        print(f"  {command}")


def render_dry_run(report):
    _table(report)
    print(f"Project files will be created in {report.repository}.")
    if report.example.generated:
        print("Sales example source will be added.")
    print("No changes were made.")
