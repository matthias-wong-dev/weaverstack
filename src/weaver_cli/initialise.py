"""Collect and review project choices before initialisation."""

from __future__ import annotations

import argparse
import sys

from weaver.errors import CommandError
from weaver.initialise import DEFAULT_CATALOGUE, DEFAULT_ENVIRONMENT
from weaver.onboarding.project import ProjectRequest, validate_fabric_name

from .interaction import can_prompt, non_interactive

INTRODUCTION = "Set up a Weaver project."
INSTALL_WARNING = "Publishing a Fabric Environment can take about 5 minutes."
DISPLAY = {"existing": "already exists", "planned": "create"}
ROLE_WIDTH = 14
NAME_WIDTH = 19
GAP = 2


def _asking(args, stream) -> bool:
    """Apply ``--non-interactive`` before forced or terminal interaction.

    ``--interactive`` also permits questions over a scripted input stream.
    """

    if non_interactive(args):
        return False
    return bool(args.interactive) or can_prompt(args, stream)


def collect_workspace(args, *, ask=True, stdin=None):
    stream = stdin if stdin is not None else sys.stdin
    if args.workspace or not ask or not _asking(args, stream):
        return False
    print(INTRODUCTION)
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
    interactive = ask and _asking(args, stream)
    if not interactive:
        _validate(args)
        return False
    if not introduced:
        print(INTRODUCTION)
    if not args.workspace:
        args.workspace = _answer(stream, "Fabric workspace")
    suggested_folder = False
    if not args.project_folder:
        args.project_folder, suggested_folder = _project_folder(stream, args.workspace)
    if not args.catalogue:
        print(
            "\nCatalogue\nThe Weaver catalogue is a Warehouse that stores build, load and test state."
        )
        args.catalogue = _item_answer(
            stream, "Catalogue name", "Warehouse", default=DEFAULT_CATALOGUE
        )
    _collect_workspace_items(args, stream, environments=environments, items=items)
    if args.example is None:
        args.example = _yes(stream, "Add the Sales example to this project?")
    fields = (
        ("project_folder", "Project folder"),
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
            print(f"  {label:16}{value or 'None'}")
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
        if field == "project_folder":
            suggested_folder = False
        changed_workspace = field == "workspace" and value != args.workspace
        setattr(args, field, value)
        if changed_workspace:
            if suggested_folder:
                args.project_folder, suggested_folder = _project_folder(
                    stream, args.workspace
                )
            args.environment = args.lakehouse = args.warehouse = None
            _collect_workspace_items(
                args, stream, environments=environments, items=items
            )
    if not args.dry_run and not getattr(args, "publish_environment", False):
        print(INSTALL_WARNING)
        args.publish_environment = _yes(stream, "Publish the Environment now?")
    return True


def _collect_workspace_items(args, stream, *, environments, items):
    if not args.environment:
        print(
            "\nEnvironment\nWeaver uses a Fabric Environment for Python work. Add packages to its definition in this project."
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
                print("This workspace has no Environments.")
            args.environment = _item_answer(
                stream, "Environment name", "Environment", default=DEFAULT_ENVIRONMENT
            )
    for kind, purpose, examples in (
        ("Lakehouse", "files and Python/Delta tables", "Landing, Bronze"),
        ("Warehouse", "SQL tables and views", "Curated, Silver"),
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


def _project_folder(stream, workspace):
    print(
        "\nProject folder\nA project folder contains the local files that define Fabric items, data logic, tests and the Environment."
    )
    typed = _read(stream, f"Project folder [{workspace}]: ")
    return typed or workspace, not bool(typed)


def _validate(args):
    if not args.workspace:
        raise CommandError("Pass --workspace.")
    if not args.project_folder:
        raise CommandError("Pass --project-folder for non-interactive setup.")
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
            "Input ended before setup was complete. Pass all required options with "
            "--non-interactive, or run in a terminal."
        )
    return line.strip()


def _answer(stream, label, *, default=None, skippable=False):
    while True:
        typed = _read(stream, f"{label} [{default}]: " if default else f"{label}: ")
        if typed or default is not None:
            return typed or default
        if skippable:
            return None
        print(f"{label} is required.")


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

    parts = [
        "weaver",
        "initialise",
        "--workspace",
        args.workspace,
        "--project-folder",
        str(args.project_folder),
    ]
    for field in ("catalogue", "environment", "lakehouse", "warehouse"):
        value = getattr(args, field)
        if value:
            parts.extend(["--" + field, value])
    if args.example:
        parts.append("--example")
    if getattr(args, "publish_environment", False):
        parts.append("--publish-environment")
    parts.append("--non-interactive")
    return shlex.join(parts)


def _table(report):
    for outcome in report.resources:
        print(
            f"  {outcome.role:14}{outcome.name:22}{DISPLAY.get(outcome.status, outcome.status)}"
        )
    print(f"  Environment publication: {report.environment_publication}")


def render(report):
    import shlex

    from weaver.onboarding.environment import environment_directory

    _table(report)
    print(f"\nWeaver project ready in {report.project_folder}.")
    print(f"\nNext:\n\n  cd {shlex.quote(report.project_folder)}")
    if report.environment_publication == "deferred":
        name = next(
            item.name for item in report.resources if item.role == "Environment"
        )
        print("\nEnvironment publication deferred.")
        print("Publish it before the first load that runs Python.\n")
        print(
            f"  weaver fabric environment publish --path {shlex.quote(environment_directory(name))}"
        )
    print(f"  {report.next_commands[0]}")
    print("\nOr run individually:")
    for command in report.next_commands[1:]:
        print(f"  {command}")


def render_dry_run(report):
    _table(report)
    print(f"Project files will be created in {report.project_folder}.")
    if report.example.generated:
        print("Sales example source will be added.")
    print("No changes were made.")
