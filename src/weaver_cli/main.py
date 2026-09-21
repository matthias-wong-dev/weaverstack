"""CLI command parsing and rendering."""

from __future__ import annotations

import argparse
import sys
import time

import weaver
from weaver.errors import (
    CommandError,
    DiscoveryError,
    GraphError,
    IdentityError,
    MetadataError,
    WeaverError,
)

from .interaction import (
    add_non_interactive,
    authorised,
    can_prompt,
    confirm,
    non_interactive,
    retry_wanted,
)
from .status import DIM as _DIM
from .status import RED as _RED
from .status import YELLOW as _AMBER
from .status import configure_stdio as _configure_stdio
from .status import semantic_colour as _status_colour
from .status import status_symbol as _status_symbol
from .status import style as _style

# Keep parser construction independent of Fabric transports.
CAPACITY_ACTIONS = ("status", "resume", "suspend")

# Keep parser construction independent of workflow imports.
WORKFLOW_DEFAULT_FILE = "workflow.yml"


def _count_style(text: str, status: str, count: int) -> str:
    return _style(text, _DIM if count == 0 else _status_colour(status))


def _json_value(value):
    """Project provider values into stable JSON scalars."""

    isoformat = getattr(value, "isoformat", None)
    if callable(isoformat):
        return isoformat()
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).hex()
    return str(value)


def _json_document(value) -> str:
    import json

    return json.dumps(value, indent=2, default=_json_value)


INITIALISE_DESCRIPTION = """\
Set up a Weaver project and its Fabric items.

Choose a catalogue Warehouse, Environment, Lakehouse and/or Warehouse.
Missing items are created.

Optionally add Sales example source files.\
"""

WIPE_DESCRIPTION = """\
Empty physical Fabric items and the catalogue that records them.

Naming targets selects exactly those physical items. Naming none selects the
estate recorded in the catalogue.

A resolved catalogue is emptied last. Pass --unbind to keep it and remove its
claims for the emptied targets.\
"""

DOCTOR_DESCRIPTION = """\
Check Microsoft Fabric connectivity.

Name a workspace to probe its items: TDS for a Warehouse; OneLake and Livy for
a Lakehouse. Project configuration is not read.

Checking a Lakehouse starts a Fabric Spark session and can take a minute.\
"""


# --- what each command will want ----------------------------------------------
#
# Commands declare coarse requirements from parsed arguments; the Session
# prepares them without interpreting the operation.
#
# Arguments cannot reveal all project or catalogue contents, so declarations
# may be supersets. The BuildBundle or RunGraph decides exact routing later.


def _kind_and_name(value) -> tuple[str, str]:
    """Split a ``Kind/Name`` token without pre-empting command validation."""

    kind, _, name = str(value).partition("/")
    return kind.strip().lower(), name.strip()


def _kind_requirements(values) -> set[str]:
    from weaver.sessions.requirements import LIVY, ONELAKE, TDS

    wanted: set[str] = set()
    for value in values or ():
        kind, _name = _kind_and_name(value)
        if kind.startswith("warehouse"):
            wanted.add(TDS)
        else:
            # A Lakehouse is files and Spark until something says otherwise.
            wanted |= {ONELAKE, LIVY}
    return wanted


def run_items(parsed) -> tuple[str, ...]:
    """Combine positional items with legacy ``--item`` values, in that order."""

    return tuple(getattr(parsed, "items", None) or ()) + tuple(
        getattr(parsed, "extra_items", None) or ()
    )


def _requires_run(args) -> frozenset[str]:
    """Always include catalogue TDS; unscoped runs need the coarse superset."""

    from weaver.sessions.requirements import (
        AUTH,
        LIVY,
        ONELAKE,
        RESOLVER,
        TDS,
        requirements,
    )

    items = run_items(args)
    if not items:
        return requirements(AUTH, RESOLVER, ONELAKE, LIVY, TDS)
    return requirements(AUTH, RESOLVER, TDS, *_kind_requirements(items))


def _requires_build(args) -> frozenset[str]:
    """Avoid Spark for Warehouse-only builds.

    An unscoped build needs the superset because arguments do not reveal source
    or configured items.
    """

    from weaver.sessions.requirements import (
        AUTH,
        LIVY,
        ONELAKE,
        RESOLVER,
        TDS,
        requirements,
    )

    items = getattr(args, "items", None)
    if not items:
        return requirements(AUTH, RESOLVER, ONELAKE, LIVY, TDS)
    # `LOGICAL[=PHYSICAL]`. Both halves agree on kind, so the left one answers.
    logical = [str(value).split("=", 1)[0] for value in items]
    # The catalogue is a Warehouse, so a build always reaches TDS.
    return requirements(AUTH, RESOLVER, TDS, *_kind_requirements(logical))


def _requires_wipe(args) -> frozenset[str]:
    """Always include catalogue TDS; Lakehouse wiping uses storage, not Livy."""

    from weaver.sessions.requirements import (
        AUTH,
        ONELAKE,
        RESOLVER,
        TDS,
        requirements,
    )

    targets = getattr(args, "targets", ()) or ()
    if not targets:
        return requirements(AUTH, RESOLVER, ONELAKE, TDS)
    return requirements(AUTH, RESOLVER, TDS, *_kind_requirements(targets))


def _requires_mirror(args) -> frozenset[str]:
    """Always include catalogue TDS; Lakehouses add OneLake and Livy."""

    from weaver.sessions.requirements import (
        AUTH,
        LIVY,
        ONELAKE,
        RESOLVER,
        TDS,
        requirements,
    )

    if getattr(args, "no_item", False):
        return requirements(AUTH, RESOLVER, TDS)
    items = getattr(args, "items", None)
    if not items:
        return requirements(AUTH, RESOLVER, ONELAKE, LIVY, TDS)
    logical = [str(value).split("=", 1)[0] for value in items]
    return requirements(AUTH, RESOLVER, TDS, *_kind_requirements(logical))


def _requires_health(args) -> frozenset[str]:
    """Use TDS and, where needed, OneLake; health never needs Livy."""

    from weaver.sessions.requirements import AUTH, ONELAKE, RESOLVER, TDS, requirements

    items = getattr(args, "items", ()) or ()
    wanted = {AUTH, RESOLVER, TDS}
    if not items or any(
        _kind_and_name(value)[0].startswith("lakehouse") for value in items
    ):
        wanted.add(ONELAKE)
    return requirements(*wanted)


def _requires_rest(args) -> frozenset[str]:
    from weaver.sessions.requirements import AUTH, RESOLVER, requirements

    return requirements(AUTH, RESOLVER)


def _requires_doctor(args) -> frozenset[str]:
    from weaver.sessions.requirements import (
        AUTH,
        LIVY,
        ONELAKE,
        RESOLVER,
        TDS,
        requirements,
    )

    return requirements(AUTH, RESOLVER, ONELAKE, LIVY, TDS)


def _requires_initialise(args) -> frozenset[str]:
    from weaver.sessions.requirements import AUTH, RESOLVER, requirements

    return requirements(AUTH, RESOLVER)


def _requires_install(args) -> frozenset[str]:
    """Install declares nothing to warm.

    What an installation needs is in its bundle, and the bundle has not been
    read when a shell or workflow warms resources. Warming from the ambient
    workspace could start a Spark session the bundle does not want and attach it
    to the wrong Lakehouse.
    """

    return frozenset()


def command_requirements(parsed) -> frozenset[str]:
    declares = getattr(parsed, "requires", None)
    return frozenset(declares(parsed)) if declares is not None else frozenset()


def _target_lakehouses(targets) -> tuple[str, ...]:
    names = []
    for value in targets or ():
        kind, name = _kind_and_name(value)
        if kind.startswith("lakehouse") and name:
            names.append(name)
    return tuple(names)


def _physical_target_lakehouses(args) -> tuple[str, ...]:
    return _target_lakehouses(getattr(args, "targets", None) or ())


def _build_item_lakehouses(args) -> tuple[str, ...]:
    named = []
    for value in getattr(args, "items", None) or ():
        _logical, separator, physical = str(value).partition("=")
        if separator:
            named.append(physical)
    return _target_lakehouses(named)


def command_lakehouses(parsed) -> tuple[str, ...]:
    """Return physical Lakehouses named before an operation resolves its scope.

    Spark warming needs a physical Lakehouse. Load and test name logical items,
    so their operations provide that Lakehouse after reading the catalogue.
    """

    declares = getattr(parsed, "lakehouses", None)
    return tuple(declares(parsed)) if declares is not None else ()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="weaver",
        description="Build, load, and test Fabric Lakehouse and Warehouse objects.",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"weaverstack {weaver.__version__}",
    )
    subcommands = parser.add_subparsers(dest="command", metavar="command")

    shell = subcommands.add_parser(
        "session",
        help="Run multiple commands in one persistent session.",
    )
    _add_workspace_args(shell)
    shell.add_argument(
        "--timings",
        action="store_true",
        help="Report Fabric access timings when the session ends.",
    )
    shell.set_defaults(handler=handle_session)

    workflow = subcommands.add_parser(
        "workflow",
        help="Run a named workflow in one session.",
    )
    workflow.add_argument("name", help="Workflow name in workflow.yml.")
    workflow.add_argument(
        "--file",
        metavar="PATH",
        help=f"Workflow file. Defaults to ./{WORKFLOW_DEFAULT_FILE}.",
    )
    workflow.add_argument(
        "--timings",
        action="store_true",
        help="Report Fabric access timings after the workflow.",
    )
    workflow.add_argument(
        "--yes",
        action="store_true",
        help="Authorise the workflow and all its commands.",
    )
    add_non_interactive(workflow)
    _add_workspace_args(workflow)
    workflow.set_defaults(handler=handle_workflow)

    for name in ("initialise", "initialize"):
        # One command, spelled both ways. The command list carries the first,
        # and the second is registered without help text, which is what keeps it
        # out of that list: argparse renders whatever `help` holds, including
        # SUPPRESS.
        listed = (
            {"help": "Set up a Weaver project and its Fabric items."}
            if name == "initialise"
            else {}
        )
        initialise = subcommands.add_parser(
            name,
            description=INITIALISE_DESCRIPTION,
            formatter_class=argparse.RawDescriptionHelpFormatter,
            **listed,
        )
        _add_initialise_args(initialise)
        initialise.set_defaults(
            handler=handle_initialise, requires=_requires_initialise
        )

    doctor = subcommands.add_parser(
        "doctor",
        help="Check Microsoft Fabric connectivity.",
        description=DOCTOR_DESCRIPTION,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    doctor.add_argument("--json", action="store_true", help="Emit the result as JSON.")
    doctor.add_argument("--workspace", required=True, help="Fabric workspace to check.")
    add_non_interactive(doctor)
    doctor.set_defaults(handler=handle_doctor, requires=_requires_doctor)

    check = subcommands.add_parser(
        "check", help="Check a project folder without contacting Fabric."
    )
    check.add_argument(
        "project_folder",
        metavar="PROJECT_FOLDER",
        nargs="?",
        help="Project folder. Defaults to the current directory.",
    )
    check.add_argument("--json", action="store_true", help="Emit the result as JSON.")
    add_non_interactive(check)
    check.set_defaults(handler=handle_check)

    build = subcommands.add_parser(
        "build", help="Build a project's objects into named items."
    )
    build.add_argument(
        "source",
        metavar="SOURCE",
        nargs="?",
        help=(
            "Project folder, or an abfss location inside a Fabric session. "
            "Defaults to the current directory or Notebook Resources."
        ),
    )
    build.add_argument(
        "--item",
        dest="items",
        action="append",
        metavar="ITEM[=TARGET]",
        help=(
            "Weaver item to build. Its physical target comes from workspace "
            "configuration; use ITEM=TARGET to supply or override it. Repeat to "
            "select multiple items. Naming none builds every configured item."
        ),
    )
    build.add_argument(
        "--bind",
        dest="retired_bind",
        action="append",
        help=argparse.SUPPRESS,
    )
    build.add_argument(
        "--target",
        dest="retired_target",
        action="append",
        help=argparse.SUPPRESS,
    )
    build.add_argument(
        "--bundle-only",
        action="store_true",
        help="Create a deployment bundle without installing it.",
    )
    build.add_argument(
        "--bundle-path",
        metavar="PATH",
        help="Directory to write a bundle created with --bundle-only.",
    )
    build.add_argument("--json", action="store_true", help="Emit the result as JSON.")
    add_non_interactive(build)
    _add_workspace_args(build)
    build.set_defaults(
        handler=handle_build,
        requires=_requires_build,
        lakehouses=_build_item_lakehouses,
    )

    load = subcommands.add_parser(
        "load", help="Load objects installed for the selected items."
    )
    load.add_argument(
        "items",
        nargs="*",
        metavar="ITEM",
        help=(
            "Weaver items to load, as Lakehouse/Name or Warehouse/Name. "
            "Naming none loads every installed item."
        ),
    )
    load.add_argument(
        "--item",
        dest="extra_items",
        action="append",
        metavar="ITEM",
        help="Legacy spelling for a positional Weaver item.",
    )
    load.add_argument(
        "--target",
        dest="retired_target",
        action="append",
        help=argparse.SUPPRESS,
    )
    load.add_argument(
        "--name",
        dest="names",
        action="append",
        metavar="NAME",
        help=(
            "Load one installed object, as Tables/Schema.Object or "
            "Files/Schema.Object in a Lakehouse and Schema.Object in a "
            "Warehouse. A bare Schema.Object is accepted where it names one "
            "object. Repeat to select more than one."
        ),
    )
    load.add_argument(
        "--fault-tolerant",
        action="store_true",
        help="Continue independent branches after a failure.",
    )
    load.add_argument(
        "--reload",
        action="store_true",
        help=(
            "Rebuild each selected table: reset its bookmark, empty it, then "
            "load. Does not affect unselected tables."
        ),
    )
    load.add_argument(
        "--stale",
        action="store_true",
        help="Load only the objects whose load health is not green.",
    )
    load.add_argument(
        "--as-of",
        metavar="DATETIME",
        help=(
            "ISO-8601 instant with a zone, used as the health freshness "
            "threshold when selecting objects. Defaults to 24 hours ago. Only "
            "valid with --stale."
        ),
    )
    load.add_argument(
        "--dry-run",
        action="store_true",
        help="Show the load plan without running it.",
    )
    load.add_argument("--json", action="store_true", help="Emit the report as JSON.")
    add_non_interactive(load)
    _add_workspace_args(load)
    load.set_defaults(handler=handle_load, requires=_requires_run)

    validate = subcommands.add_parser(
        "test", help="Run Tests and Assumptions installed for the selected items."
    )
    validate.add_argument(
        "items",
        nargs="*",
        metavar="ITEM",
        help=(
            "Weaver items to validate, as Lakehouse/Name or Warehouse/Name. "
            "Naming none validates every installed item."
        ),
    )
    validate.add_argument(
        "--item",
        dest="extra_items",
        action="append",
        metavar="ITEM",
        help="Legacy spelling for a positional Weaver item.",
    )
    validate.add_argument(
        "--target",
        dest="retired_target",
        action="append",
        help=argparse.SUPPRESS,
    )
    selection = validate.add_mutually_exclusive_group()
    selection.add_argument(
        "--name",
        metavar="Schema.Object",
        help="Run one installed validation and return diagnostic rows.",
    )
    selection.add_argument(
        "--file",
        metavar="PATH",
        help="Compile and run a source file without installing it.",
    )
    validate.add_argument(
        "--dry-run",
        action="store_true",
        help="Show the test plan without running it.",
    )
    validate.add_argument(
        "--json", action="store_true", help="Emit the report as JSON."
    )
    add_non_interactive(validate)
    _add_workspace_args(validate)
    validate.set_defaults(handler=handle_test, requires=_requires_run)

    report = subcommands.add_parser(
        "health",
        help="Report the installed estate's load, test and build health.",
    )
    report.add_argument(
        "--item",
        dest="items",
        action="append",
        metavar="ITEM",
        help=(
            "Weaver item to report on, as Lakehouse/Name or Warehouse/Name. "
            "Repeat to select more than one. Naming none reports on the whole "
            "installed estate."
        ),
    )
    report.add_argument(
        "--as-of",
        metavar="DATETIME",
        help=(
            "ISO-8601 instant with a zone. A load settled before it reads as "
            "stale. Defaults to 24 hours ago."
        ),
    )
    report.add_argument(
        "--no-inventory",
        action="store_true",
        help="Skip the physical inventory check for certified objects.",
    )
    report.add_argument("--json", action="store_true", help="Emit the report as JSON.")
    add_non_interactive(report)
    _add_workspace_args(report, include_environment=False)
    # Health reads Lakehouses through storage, not Spark.
    report.set_defaults(handler=handle_health, requires=_requires_health)

    wipe = subcommands.add_parser(
        "wipe",
        help="Empty a physical Lakehouse or Warehouse, and its catalogue.",
        description=WIPE_DESCRIPTION,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    wipe.add_argument(
        "targets",
        nargs="*",
        metavar="TARGET",
        help=(
            "Physical items to empty, as Lakehouse/Name or Warehouse/Name. "
            "Naming none empties the estate the catalogue records."
        ),
    )
    _add_workspace_args(wipe)
    wipe.add_argument(
        "--unbind",
        action="store_true",
        help=(
            "Keep the catalogue and remove its claims for the named targets. "
            "Requires a catalogue and at least one target."
        ),
    )
    wipe.add_argument(
        "--dry-run", action="store_true", help="Show the estate this would empty."
    )
    wipe.add_argument(
        "--yes",
        action="store_true",
        help="Authorise the removal without asking.",
    )
    wipe.add_argument("--json", action="store_true", help="Emit the result as JSON.")
    add_non_interactive(wipe)
    wipe.set_defaults(
        handler=handle_wipe,
        requires=_requires_wipe,
        lakehouses=_physical_target_lakehouses,
    )

    mirror = subcommands.add_parser(
        "mirror",
        help=(
            "Fork another catalogue's installed estate into this workspace's catalogue."
        ),
    )
    mirror.add_argument(
        "--item",
        action="append",
        dest="items",
        metavar="ITEM[=TARGET]",
        help=(
            "A logical item to rebind, such as Warehouse/Model or "
            "Warehouse/Model=Warehouse/Model_Dev. Repeat to select multiple "
            "items. Naming none selects every configured target."
        ),
    )
    mirror.add_argument(
        "--no-item",
        action="store_true",
        help="Fork the catalogue and rebind no physical item.",
    )
    mirror.add_argument(
        "--mirror",
        metavar="CATALOGUE",
        dest="mirror_source",
        help=(
            "Catalogue to fork, for example Warehouse/Weaver. Overrides mirror: "
            "in workspace configuration."
        ),
    )
    _add_workspace_args(mirror)
    mirror.add_argument(
        "--yes",
        action="store_true",
        help="Authorise emptying the destinations without asking.",
    )
    mirror.add_argument("--json", action="store_true", help="Emit the result as JSON.")
    add_non_interactive(mirror)
    mirror.set_defaults(handler=handle_mirror, requires=_requires_mirror)

    install = subcommands.add_parser(
        "install",
        help="Install a previously built deployment bundle.",
    )
    install.add_argument(
        "bundle", metavar="BUNDLE", help="Bundle directory or .weaver.zip archive."
    )
    install.add_argument("--json", action="store_true", help="Emit the report as JSON.")
    add_non_interactive(install)
    # No workspace options: the bundle names where it installs.
    install.set_defaults(handler=handle_install, requires=_requires_install)

    # Fabric commands do not read or write the Weaver catalogue.
    fabric = subcommands.add_parser(
        "fabric", help="Manage Fabric items, Environments, and capacity."
    )
    fabric_commands = fabric.add_subparsers(dest="fabric_command", metavar="command")
    fabric.set_defaults(handler=_group_help(fabric))

    environment = fabric_commands.add_parser(
        "environment", help="Manage Fabric Environments."
    )
    environment_commands = environment.add_subparsers(
        dest="environment_command", metavar="command"
    )
    environment.set_defaults(handler=_group_help(environment))
    environment_publish = environment_commands.add_parser(
        "publish", help="Publish Weaver into a Fabric Environment."
    )
    environment_publish.add_argument(
        "environment_ref",
        metavar="ENVIRONMENT",
        nargs="?",
        help="Fabric Environment name or Workspace/Environment reference.",
    )
    environment_publish.add_argument(
        "--path",
        metavar="DIRECTORY",
        help=(
            "Local <Name>.Environment definition directory. It names the "
            "Environment and supplies the complete definition."
        ),
    )
    environment_publish.add_argument(
        "--dev",
        action="store_true",
        help="Supply Weaver as a wheel built from this checkout.",
    )
    add_non_interactive(environment_publish)
    _add_workspace_args(
        environment_publish, include_catalogue=False, include_environment=False
    )
    environment_publish.set_defaults(
        handler=handle_environment_publish, requires=_requires_rest
    )

    notebook = fabric_commands.add_parser(
        "notebook", help="Deploy or run a Fabric notebook."
    )
    notebook_commands = notebook.add_subparsers(
        dest="notebook_command", metavar="command"
    )
    notebook.set_defaults(handler=_group_help(notebook))

    notebook_push = notebook_commands.add_parser(
        "push", help="Create or update a notebook definition."
    )
    notebook_push.add_argument("source", help="Local .py or .ipynb notebook source.")
    notebook_push.add_argument(
        "--name", help="Fabric display name. Defaults to the filename."
    )
    notebook_push.add_argument("--description")
    notebook_push.add_argument("--json", action="store_true")
    add_non_interactive(notebook_push)
    _add_workspace_args(notebook_push, include_catalogue=False)
    notebook_push.set_defaults(handler=handle_notebook_push)

    notebook_run = notebook_commands.add_parser(
        "run", help="Run a deployed notebook in Fabric."
    )
    notebook_run.add_argument("name", help="Fabric Notebook display name.")
    notebook_run.add_argument(
        "--lakehouse",
        help="Default Lakehouse for the notebook session.",
    )
    notebook_run.add_argument("--no-wait", action="store_true")
    notebook_run.add_argument("--timeout", type=float, default=7200.0)
    notebook_run.add_argument("--poll-interval", type=float, default=10.0)
    notebook_run.add_argument("--json", action="store_true")
    add_non_interactive(notebook_run)
    _add_workspace_args(notebook_run)
    notebook_run.set_defaults(handler=handle_notebook_run)

    capacity = fabric_commands.add_parser(
        "capacity", help="Resume, suspend, or report a Fabric capacity."
    )
    capacity.add_argument("action", choices=CAPACITY_ACTIONS)
    capacity.add_argument("--resource-group", required=True)
    capacity.add_argument("--capacity-name", required=True)
    capacity.add_argument(
        "--subscription-id",
        help="Azure subscription ID when more than one subscription is available.",
    )
    add_non_interactive(capacity)
    capacity.set_defaults(handler=handle_capacity)

    return parser


def _fabric_cli_workspace(args: argparse.Namespace):
    return _resolve_workspace(args)


def handle_notebook_push(args: argparse.Namespace) -> int:
    import json

    from weaver.fabric.notebooks import push_notebook

    workspace = _fabric_cli_workspace(args)
    result = push_notebook(
        args.source,
        workspace=workspace.workspace,
        name=args.name,
        description=args.description,
    )
    if args.json:
        print(json.dumps(result.to_mapping(), indent=2))
    else:
        print(f"{result.action} notebook {result.notebook!r} in {result.workspace!r}")
        print(f"  id:     {result.notebook_id}")
        print(f"  source: {result.source}")
    return 0


def handle_notebook_run(args: argparse.Namespace) -> int:
    import json

    from weaver.errors import CommandError
    from weaver.fabric.notebooks import run_notebook

    workspace = _fabric_cli_workspace(args)
    configured_lakehouses = workspace.configured_lakehouses
    lakehouse = args.lakehouse
    if lakehouse is None and len(configured_lakehouses) == 1:
        lakehouse = configured_lakehouses[0]
    if not lakehouse:
        raise CommandError(
            "A Lakehouse is required to run this notebook. "
            "Use --lakehouse or configure exactly one Lakehouse for this workspace."
        )
    if not workspace.environment:
        raise CommandError(
            "A Fabric Environment is required to run this notebook. "
            "Use --environment or configure one for this workspace."
        )
    result = run_notebook(
        args.name,
        workspace=workspace.workspace,
        lakehouse=lakehouse,
        environment=workspace.environment,
        wait=not args.no_wait,
        timeout=args.timeout,
        poll_interval=args.poll_interval,
    )
    if args.json:
        print(json.dumps(result.to_mapping(), indent=2))
    else:
        print(f"notebook {result.notebook!r}: {result.status}")
        print(f"  job: {result.job_url}")
        if result.exit_value is not None:
            print(f"  result: {result.exit_value}")
    return 0 if result.succeeded or args.no_wait else 1


def handle_environment_publish(args: argparse.Namespace) -> int:
    """Publish Weaver to an Environment for notebooks and Livy sessions.

    Output is always JSON on stdout; progress goes to stderr.
    """

    import json
    from dataclasses import replace

    from weaver.fabric.environment import resolve_environment_owner
    from weaver.fabric.environment_definition import environment_name_from_path
    from weaver.sessions.host import use_or_create_session
    from weaver.workspaces import EnvironmentRef, Workspace

    if args.path is not None and args.environment_ref is not None:
        raise CommandError(
            "--path and ENVIRONMENT cannot be used together; the directory names "
            "the Environment."
        )
    if args.path is None and args.environment_ref is None:
        raise CommandError(
            "Name a Fabric Environment, or pass --path to publish a local "
            "<Name>.Environment definition."
        )

    if args.path is not None:
        # The directory names the Environment. Workspace configuration may name
        # a different one, and it is not consulted here.
        workspace = _resolve_workspace(args)
        label = environment_name_from_path(args.path)
        environment = None
    else:
        environment = EnvironmentRef.parse(args.environment_ref)
        if (
            environment.workspace is not None
            and args.workspace is None
            and args.workspace_config is None
        ):
            workspace = Workspace(
                workspace=environment.workspace, environment=environment
            )
        else:
            workspace = _resolve_workspace(args)
            resolve_environment_owner(workspace.workspace, environment)
            workspace = replace(workspace, environment=environment)
        label = str(environment)
    _prefer_desktop_credential(args)
    from weaver.fabric import publish_environment

    started = time.perf_counter()
    with use_or_create_session(_session(args), workspace=workspace) as session:
        with session.task("Publish Environment", label):
            result = publish_environment(
                workspace.workspace,
                environment,
                path=args.path,
                dev=args.dev,
                session=session,
            )
    total = time.perf_counter() - started

    payload = result.as_dict()
    payload["timings"]["total"] = round(total, 2)
    print(json.dumps(payload, indent=2))
    return 0


def handle_install(args: argparse.Namespace) -> int:
    """Install a bundle against the execution context it froze.

    No workspace is resolved here: doing so would let the caller's directory
    decide where a frozen bundle lands.
    """

    import json

    from weaver.operations.install import install

    _prefer_desktop_credential(args)
    # The Session opens with no workspace: the bundle binds it to one.
    with _running_session(args, None) as opened:
        report = install(args.bundle, session=opened)
    if args.json:
        print(json.dumps(report.to_mapping(), indent=2))
    else:
        _print_install(report)
    return 0 if report.succeeded else 1


def handle_capacity(args: argparse.Namespace) -> int:
    _prefer_desktop_credential(args)
    from weaver.fabric import run_capacity_action

    result = run_capacity_action(
        args.action,
        resource_group=args.resource_group,
        capacity_name=args.capacity_name,
        subscription_id=args.subscription_id,
    )
    print(result)
    if args.action == "resume" and not result.running:
        print("  The capacity is starting. Run `capacity status` to confirm its state.")
    return 0


def _add_initialise_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--workspace", help="Fabric workspace name. It must exist.")
    parser.add_argument(
        "--project-folder",
        dest="project_folder",
        metavar="PATH",
        help="Project folder to create or reuse. Required for unattended setup.",
    )
    parser.add_argument(
        "--catalogue",
        help="Catalogue Warehouse. Defaults to Catalogue.",
    )
    parser.add_argument(
        "--environment",
        help=(
            "Fabric Environment for the project. It may already exist. Defaults "
            "to Weaver."
        ),
    )
    parser.add_argument("--lakehouse", help="Lakehouse for Delta tables and files.")
    parser.add_argument("--warehouse", help="Warehouse for SQL tables and views.")
    parser.add_argument(
        "--example",
        dest="example",
        action="store_true",
        default=None,
        help="Add Sales example source files.",
    )
    parser.add_argument(
        "--no-example",
        dest="example",
        action="store_false",
        help="Do not add example source files.",
    )
    parser.add_argument(
        "--interactive",
        action="store_true",
        help="Ask setup questions even when input is not a terminal.",
    )
    add_non_interactive(parser)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview the setup without making changes.",
    )
    parser.add_argument("--json", action="store_true", help="Emit the result as JSON.")
    parser.add_argument(
        "--publish-environment",
        action="store_true",
        help="Publish the project Environment after setup.",
    )


def _add_workspace_args(
    parser: argparse.ArgumentParser,
    *,
    include_catalogue: bool = True,
    include_environment: bool = True,
) -> None:
    """Add the explicit values that a Workspace configuration can abbreviate."""

    parser.add_argument("--workspace", help="Fabric Workspace name.")
    parser.add_argument("--workspace-config", help="Workspace configuration file.")
    if include_environment:
        parser.add_argument(
            "--environment",
            help="Fabric Environment name or Workspace/Environment reference.",
        )
    if include_catalogue:
        parser.add_argument(
            "--catalogue",
            help="Where the Weaver catalogue lives, for example Warehouse/Weaver.",
        )


def _prefer_desktop_credential(args: argparse.Namespace | None = None) -> None:
    """Install the CLI credential policy when Fabric support is available.

    Interactive commands fall back from Azure CLI to browser sign-in;
    ``--non-interactive`` omits the browser credential.
    """

    try:
        from weaver.fabric.auth import (
            desktop_credential,
            unattended_credential,
            use_credential,
        )
    except ImportError:
        return
    chain = unattended_credential if non_interactive(args) else desktop_credential
    use_credential(chain())


def _desktop_store(workspace):
    from weaver.fabric import OneLakeDfsClient

    return OneLakeDfsClient()


def workspace_supplied(args: argparse.Namespace) -> bool:
    """Distinguish an absent workspace from one that fails resolution."""

    return bool(
        getattr(args, "workspace", None)
        or getattr(args, "workspace_config", None)
        or getattr(getattr(args, "session", None), "workspace", None)
    )


def _resolve_workspace(args: argparse.Namespace):
    """Resolve against a Session's fixed workspace when one is inherited.

    Commands may override its catalogue and Environment. Each starts from the
    Session's original configuration.
    """

    from weaver.config import resolve_workspace

    inherited = getattr(getattr(args, "session", None), "workspace", None)
    if inherited is not None:
        _refuse_another_workspace(args, inherited)
        workspace = _with_command_overrides(inherited, args)
    else:
        workspace = resolve_workspace(
            workspace=args.workspace,
            environment=getattr(args, "environment", None),
            catalogue=getattr(args, "catalogue", None),
            workspace_config=args.workspace_config,
        )

    _prefer_desktop_credential(args)
    return workspace


def _refuse_another_workspace(args: argparse.Namespace, inherited) -> None:
    """Keep every command in a Session on its fixed Fabric workspace."""

    from weaver.config import resolve_workspace
    from weaver.errors import CommandError

    if args.workspace is None and args.workspace_config is None:
        return
    named = resolve_workspace(
        workspace=args.workspace, workspace_config=args.workspace_config
    ).workspace
    if named == inherited.workspace:
        return
    raise CommandError(
        f"Session workspace is '{inherited.workspace}'; cannot use '{named}'. "
        f"Open a session on '{named}' to run there."
    )


def _with_command_overrides(workspace, args: argparse.Namespace):
    """Apply command configuration without changing the Session workspace."""

    from dataclasses import replace

    overrides = {}
    if getattr(args, "environment", None) is not None:
        overrides["environment"] = args.environment
    if getattr(args, "catalogue", None) is not None:
        overrides["catalogue"] = str(args.catalogue)
    return replace(workspace, **overrides) if overrides else workspace


def _command_context(workspace, *, environment: bool = True) -> dict:
    """Pass command-level catalogue and Environment names to an operation."""

    context = {"catalogue": workspace.catalogue or None}
    if environment:
        context["environment"] = (
            str(workspace.environment) if workspace.environment else None
        )
    return context


def _session(args: argparse.Namespace):
    return getattr(args, "session", None)


def _running_session(args: argparse.Namespace, workspace):
    from contextlib import contextmanager

    from weaver.sessions.host import use_or_create_session

    @contextmanager
    def running():
        with use_or_create_session(_session(args), workspace=workspace) as opened:
            machine_output = hasattr(opened, "machine_output")
            previous = getattr(opened, "machine_output", False)
            if machine_output:
                opened.machine_output = bool(getattr(args, "json", False))
            try:
                yield opened
            finally:
                if machine_output:
                    opened.machine_output = previous

    return running()


def handle_session(args: argparse.Namespace) -> int:
    from .shell import run_shell

    return run_shell(args)


def handle_workflow(args: argparse.Namespace) -> int:
    from .workflow import run_workflow

    return run_workflow(args)


# Convert source errors into failed attempts so a retry rereads the project.
SOURCE_ERRORS = (DiscoveryError, GraphError, IdentityError, MetadataError)


def _retry_until_fixed(args: argparse.Namespace, attempt) -> int:
    if bool(getattr(args, "json", False)) or not can_prompt(args):
        return attempt()
    while True:
        status = attempt()
        if not status:
            return status
        if not retry_wanted(args):
            return status


def _until_fixed(args: argparse.Namespace, attempt) -> int:
    """Retry with fresh inputs while keeping the Session open."""

    if bool(getattr(args, "json", False)) or not can_prompt(args):
        return attempt()

    from weaver.sessions.host import use_or_create_session

    with use_or_create_session(
        _session(args), workspace=_resolve_workspace(args)
    ) as session:
        args.session = session
        return _retry_until_fixed(args, attempt)


def _refuse_retired_target(args: argparse.Namespace) -> None:
    if getattr(args, "retired_target", None):
        raise CommandError(
            "--target is replaced by --item on build, load and test. These "
            "commands select Weaver items; physical targets come from workspace "
            "configuration or the Weaver catalogue.\n"
            "Use: --item Lakehouse/Landing"
        )


def handle_load(args: argparse.Namespace) -> int:
    _refuse_retired_target(args)
    return _until_fixed(args, lambda: _load_once(args))


def _load_once(args: argparse.Namespace) -> int:
    import json

    from weaver.errors import LoadError

    workspace = _resolve_workspace(args)
    try:
        with _running_session(args, workspace) as opened:
            report = _run_load(
                workspace,
                items=run_items(args) or None,
                names=args.names,
                fault_tolerant=args.fault_tolerant,
                dry_run=args.dry_run,
                reload=args.reload,
                stale=args.stale,
                as_of=args.as_of,
                session=opened,
            )
    except LoadError as exc:
        # Preserve the partial report carried by an intolerant failure.
        if not args.json and getattr(exc, "report", None) is not None:
            _print_load(exc.report)
        _render_error(exc, args=args, report=getattr(exc, "report", None))
        if not args.json and getattr(exc, "workflow_id", None):
            print(f"  Workflow: {exc.workflow_id}", file=sys.stderr)
        return 1

    if args.json:
        print(json.dumps(report.to_mapping(), indent=2))
    else:
        _print_load(report)
    return 0 if report.succeeded else 1


def _run_load(
    workspace,
    *,
    items,
    names=None,
    fault_tolerant: bool,
    dry_run: bool,
    reload: bool = False,
    stale: bool = False,
    as_of=None,
    session=None,
):

    from weaver.sessions.host import use_or_create_session

    with use_or_create_session(session, workspace=workspace) as opened:
        return weaver.load(
            items,
            names=names,
            fault_tolerant=fault_tolerant,
            dry_run=dry_run,
            reload=reload,
            stale=stale,
            as_of=as_of,
            session=opened,
            **_command_context(workspace),
        )


def _print_load(report) -> None:
    mode = "plan" if report.dry_run else "load"
    reload = " (reload)" if getattr(report, "reload", False) else ""
    report_status = _style(report.status, _status_colour(report.status))
    requested = _public_requested(report.requested)
    print(f"{mode}{reload} {report_status}: {', '.join(requested)}\n")
    for node in report.nodes:
        mark = _status_symbol(node.status)
        colour = _status_colour(node.status)
        counts = ""
        # Failures before row movement have no row-count fields.
        if node.result is not None and hasattr(node.result, "rows_read"):
            counts = (
                f"  (read {node.result.rows_read}, "
                f"+{node.result.rows_inserted} "
                f"~{node.result.rows_updated} "
                f"-{node.result.rows_deleted} "
                f"!{node.result.rows_rejected})"
            )
        status = f"{node.status:<24}"
        print(
            f"  {_style(mark, colour)} {_style(status, colour)} {node.node_id}{counts}"
        )
        for message in node.messages:
            if message.severity != "info":
                message_colour = _RED if message.severity == "error" else _AMBER
                prefix = _style(f"{message.severity}:", message_colour)
                print(f"      {prefix} {message.message}")
    _print_load_summary(report)
    if report.workflow_id:
        print(f"\n  Workflow: {_style(report.workflow_id, _DIM)}")


def _print_load_summary(report) -> None:
    from weaver.load_report import (
        BLOCKED,
        FAILED,
        PENDING,
        SKIPPED,
        SUCCEEDED,
        SUCCEEDED_WITH_REJECTS,
    )

    if report.dry_run:
        print(f"\nPlan\n  {len(report.nodes):>3} selected")
        return

    from weaver.load_plan import ENDPOINT_REFRESH, ONELAKE_PUBLICATION

    loaders = [
        node
        for node in report.nodes
        if node.primitive_kind not in (ENDPOINT_REFRESH, ONELAKE_PUBLICATION)
    ]
    from weaver.operations.load import status_counts

    counts = status_counts(report)["load"]
    print("\nLoad summary")
    print(
        _count_style(
            f"  {counts[SUCCEEDED]:>3} succeeded", SUCCEEDED, counts[SUCCEEDED]
        )
    )
    if counts[SUCCEEDED_WITH_REJECTS]:
        print(
            _style(
                f"  {counts[SUCCEEDED_WITH_REJECTS]:>3} succeeded with rejects",
                _AMBER,
            )
        )
    print(_count_style(f"  {counts[FAILED]:>3} failed", FAILED, counts[FAILED]))
    print(_count_style(f"  {counts[BLOCKED]:>3} blocked", BLOCKED, counts[BLOCKED]))
    if counts[PENDING]:
        print(_style(f"  {counts[PENDING]:>3} pending", _AMBER))
    if counts[SKIPPED]:
        print(f"  {counts[SKIPPED]:>3} skipped")

    executed_loaders = [node for node in loaders if node.executed]
    if not executed_loaders:
        return
    print("  Rows")
    for label, field in (
        ("read", "rows_read"),
        ("inserted", "rows_inserted"),
        ("updated", "rows_updated"),
        ("deleted", "rows_deleted"),
        ("rejected", "rows_rejected"),
    ):
        values = [
            None if node.result is None else getattr(node.result, field, None)
            for node in executed_loaders
        ]
        rendered = (
            "unknown"
            if any(value is None for value in values)
            else f"{sum(int(value) for value in values if value is not None):,}"
        )
        print(f"    {label:<10}{rendered:>14}")


def _public_requested(values) -> tuple[str, ...]:
    internal = "Warehouse/_weaver"
    visible = tuple(str(value) for value in values if str(value) != internal)
    return visible or ("Catalogue",)


def handle_test(args: argparse.Namespace) -> int:
    _refuse_retired_target(args)
    return _test_once(args)


def _test_once(args: argparse.Namespace) -> int:
    """Render a completed validation report before returning its process status."""

    from weaver.errors import ValidationError

    workspace = _resolve_workspace(args)
    try:
        with _running_session(args, workspace) as opened:
            report = _run_test(
                workspace,
                items=run_items(args) or None,
                name=args.name,
                file=args.file,
                dry_run=args.dry_run,
                strict=True,
                session=opened,
            )
    except ValidationError as exc:
        if exc.report is None:
            _render_error(exc, args=args)
        elif args.json:
            print(
                _json_document(
                    _test_mapping(
                        exc.report,
                        targeted=args.name is not None or args.file is not None,
                    )
                )
            )
        else:
            _print_test(exc.report)
        return 1

    if args.json:
        print(
            _json_document(
                _test_mapping(
                    report, targeted=args.name is not None or args.file is not None
                )
            )
        )
    else:
        _print_test(report)
    return 0


def _run_test(
    workspace, *, items, name, file, dry_run: bool, strict: bool, session=None
):
    """Dispatch Warehouse validations over TDS and Lakehouse modules in-session."""

    from weaver.sessions.host import use_or_create_session

    with use_or_create_session(session, workspace=workspace) as opened:
        return weaver.test(
            items,
            name=name,
            file=file,
            dry_run=dry_run,
            strict=strict,
            session=opened,
            **_command_context(workspace),
        )


def _print_test(report) -> None:
    status = _style(report.status, _status_colour(report.status))
    print(f"test {status}\n")
    for node in report.nodes:
        result = node.result
        found = ""
        if (
            result is not None
            and getattr(result, "error_message", None) is None
            and hasattr(result, "violation_count")
        ):
            found = f"  ({result.violation_count} violation(s))"
        elif (
            result is not None
            and getattr(result, "error_message", None) is None
            and hasattr(result, "missing_count")
        ):
            found = (
                f"  ({result.missing_count} missing, "
                f"{result.unexpected_count} unexpected)"
            )
        status = f"{node.status:<10}"
        print(
            f"  {_style(status, _status_colour(node.status))} "
            f"{node.kind:<11} {node.logical_id}{found}"
        )
        for message in node.messages:
            print(f"      {message}")
        error_message = (
            None if result is None else getattr(result, "error_message", None)
        )
        if error_message and not node.messages:
            print(f"      {error_message}")

    totals = report.totals()
    print("\nTest summary")
    print(_count_style(f"  {totals['passed']:>3} passed", "passed", totals["passed"]))
    print(_count_style(f"  {totals['failed']:>3} failed", "failed", totals["failed"]))
    if totals["invalid"]:
        print(_style(f"  {totals['invalid']:>3} could not run", _AMBER))
    if report.workflow_id:
        print(f"  Workflow: {_style(report.workflow_id, _DIM)}")

    for node in report.nodes:
        if not node.diagnostics:
            continue
        print(f"\n  {node.logical_id}:")
        for row in node.diagnostics:
            print(f"    {row}")


def _test_mapping(report, *, targeted: bool) -> dict:
    mapping = report.to_mapping()
    if not targeted:
        return mapping
    for rendered, node in zip(mapping["nodes"], report.nodes, strict=True):
        if node.diagnostics is not None:
            rendered["diagnostics"] = node.diagnostics
    return mapping


def handle_health(args: argparse.Namespace) -> int:
    """Return zero for Green health and one for any worse verdict."""

    import json

    workspace = _resolve_workspace(args)
    with _running_session(args, workspace) as opened:
        report = weaver.health(
            args.items,
            as_of=args.as_of,
            inventories=not args.no_inventory,
            session=opened,
            **_command_context(workspace, environment=False),
        )
    if args.json:
        print(json.dumps(report.to_mapping(), indent=2))
    else:
        print(render_health(report))
    return 0 if report.is_healthy else 1


def render_health(report) -> str:
    """Render health with the CLI's shared semantic status treatment."""

    from weaver.health import AREAS

    lines = [f"Weaver Health  {_semantic_status(report.status)}", ""]
    for area, section in zip(AREAS, report.sections):
        lines.append(f"{area.title():<8}{_semantic_status(section.status)}")
        lines.extend(_health_section(area, section, report))
        lines.append("")
    lines.extend(_health_activity(report))
    return "\n".join(lines).rstrip() + "\n"


def _health_section(area: str, section, report) -> list[str]:
    from weaver.health import BUILD, LOAD

    lines = []
    if area == LOAD and report.current_load is not None:
        current = report.current_load
        lines.append(
            f"  Last load activity   {_ago(current.completed_at, report.generated_at)}"
        )
    counts = " · ".join(
        _count_style(f"{count} {word}", word, count)
        for word, count in sorted(section.counts.items())
    )
    if counts:
        lines.append(f"  {counts}")
    if area == BUILD and not section.findings:
        lines.append(f"  Installed estate consistent ({section.subjects} objects)")
    for finding in section.findings:
        where = _health_subject(finding)
        lines.append(f"  {_semantic_status(finding.severity, width=7)}{where}")
        lines.append(f"          {finding.message}")
    return lines


# Minimum object-id column width across report sections.
_ID_WIDTH = 42


def _health_activity(report) -> list[str]:
    lines = []
    slowest = report.slowest()
    if slowest:
        lines.append("Slowest loads")
        lines.extend(
            _health_row(each.object_id, f"{each.duration_ms / 1000:.1f}s", slowest)
            for each in slowest
        )
        lines.append("")
    moved = report.moved()
    if moved:
        lines.append("Recent activity")
        lines.extend(
            _health_row(
                each.object_id,
                f"read {each.rows_read:,}  "
                f"+{each.rows_inserted} ~{each.rows_updated} "
                f"-{each.rows_deleted} !{each.rows_rejected}",
                moved,
            )
            for each in moved
        )
    return lines


def _health_row(object_id: str, value: str, among) -> str:
    """Keep a separator after ids that exceed the block's minimum width."""

    width = max(_ID_WIDTH, *(len(str(each.object_id)) for each in among))
    return f"  {object_id:<{width}}  {value}"


def _health_subject(finding) -> str:
    object_id = str(finding.object_id or "")
    catalogue_item = "Warehouse/_weaver"
    if object_id == catalogue_item or object_id.startswith(f"{catalogue_item}/"):
        subject = f"Catalogue {finding.target}" if finding.target else "Catalogue"
        suffix = object_id.removeprefix(catalogue_item).lstrip("/")
        if suffix:
            subject = f"{subject} / {suffix.replace('/', '.', 1)}"
        return subject
    return object_id or finding.target or ""


def _titled(word: str) -> str:
    return str(word).title()


def _semantic_status(word: str, *, width: int = 0) -> str:
    text = _titled(word)
    if width:
        text = f"{text:<{width}}"
    return _style(text, _status_colour(word))


def _ago(at, now) -> str:
    if at is None:
        return "never"
    seconds = max(int((now - at).total_seconds()), 0)
    hours, remainder = divmod(seconds, 3600)
    return f"{hours}h {remainder // 60}m ago"


def handle_wipe(args: argparse.Namespace) -> int:
    """Show and authorise the exact plan passed to the destructive operation.

    Human output shows the plan even with ``--yes``. ``--json`` keeps it inside
    the single result document.
    """

    import json

    workspace = _resolve_workspace(args)

    with _running_session(args, workspace) as opened:
        plan = weaver.plan_wipe(
            args.targets,
            unbind=args.unbind,
            session=opened,
            **_command_context(workspace),
        )

        if args.dry_run:
            if args.json:
                print(json.dumps(plan.to_mapping(), indent=2))
            else:
                print(plan.describe())
                print("\nNothing was changed.")
            return 0

        if not args.json:
            print(plan.describe())
            print()

        if not authorised(args):
            emptied = len(plan.targets)
            if args.json or not can_prompt(args):
                _render_error(
                    CommandError(
                        f"Confirmation required to empty {emptied} item(s). "
                        "Pass --yes to proceed, or --dry-run to preview."
                    ),
                    args=args,
                )
                return 1
            if not confirm(
                args,
                f"Empty {emptied} item(s)? This cannot be undone. [y/N] ",
            ):
                _render_error(CommandError("Cancelled."), args=args)
                return 1

        result = weaver.wipe(plan=plan, session=opened)

    if args.json:
        print(json.dumps(result.to_mapping(), indent=2))
    else:
        print("Wipe complete\n")
        for item in result.items:
            print(f"  {item.describe()}")
    return 0


def handle_mirror(args: argparse.Namespace) -> int:
    """Resolve and check the complete scope before authorising destructive work.

    Human output shows the targets even with ``--yes``. ``--json`` keeps them in
    the single result document.
    """

    import json

    if args.items and args.no_item:
        raise CommandError("--item and --no-item cannot be used together.")

    # Resolve the source and destination together from the original arguments.
    plan = weaver.plan_mirror(
        args.items,
        no_item=args.no_item,
        workspace=args.workspace,
        catalogue=args.catalogue,
        mirror=args.mirror_source,
        environment=getattr(args, "environment", None),
        workspace_config=args.workspace_config,
        session=_session(args),
    )

    with _running_session(args, plan.workspace) as opened:
        resolved = weaver.check_mirror(plan, session=opened)

        if not args.json:
            print(f"Mirror\n\n{resolved.describe()}\n")

        if not authorised(args):
            emptied = ", ".join(resolved.wiped)
            if args.json or not can_prompt(args):
                _render_error(
                    CommandError(
                        f"Confirmation required to empty {emptied}. Pass --yes."
                    ),
                    args=args,
                )
                return 1
            if not confirm(
                args,
                "Empty these targets? This cannot be undone. [y/N] ",
            ):
                _render_error(CommandError("Cancelled."), args=args)
                return 1

        result = weaver.mirror(plan=resolved, session=opened)

    if args.json:
        print(json.dumps(result.to_mapping(), indent=2))
        return 0
    _print_mirror(result)
    return 0


def _print_mirror(result) -> None:
    print("Mirror complete")
    print(f"  Catalogue  {result.source_catalogue} → {result.destination_catalogue}")
    if result.items:
        noun = "item" if len(result.items) == 1 else "items"
        print(f"  {len(result.items)} {noun} mirrored")
    noun = "destination" if len(result.wiped) == 1 else "destinations"
    print(f"  {len(result.wiped)} {noun} emptied")


def handle_build(args: argparse.Namespace) -> int:
    if getattr(args, "retired_bind", None):
        raise CommandError(
            "--bind is replaced by --item; put the Weaver item first.\n"
            "Old: --bind Lakehouse/Landing_Dev=Landing\n"
            "New: --item Lakehouse/Landing=Lakehouse/Landing_Dev"
        )
    _refuse_retired_target(args)
    if args.bundle_path and not args.bundle_only:
        raise CommandError("--bundle-path requires --bundle-only")
    return _until_fixed(args, lambda: _build_once(args))


def _build_once(args: argparse.Namespace) -> int:
    import json

    workspace = _resolve_workspace(args)
    try:
        with _running_session(args, workspace) as opened:
            result = weaver.build(
                args.source,
                items=args.items,
                bundle_only=args.bundle_only,
                bundle_path=args.bundle_path,
                session=opened,
                **_command_context(workspace),
            )
    except SOURCE_ERRORS as exc:
        # Return a failed attempt so an interactive retry rereads the project.
        _render_error(exc, args=args)
        return 1
    payload = result.to_mapping()
    if args.json:
        print(json.dumps(payload, indent=2))
    else:
        _print_build(result)
        for error in result.errors:
            # Show the operation and source before lower-level diagnostics.
            print()
            print(_indented(error.describe()), file=sys.stderr)
    return 0 if result.succeeded else 1


def _action_counts(report) -> dict[str, int]:
    statuses = ("succeeded", "failed", "skipped")
    actions = tuple(report.action_results())
    return {
        status: sum(action.status == status for action in actions)
        for status in statuses
    }


def _print_action_counts(report, *, indent: str = "  ") -> None:
    counts = _action_counts(report)
    for status in ("succeeded", "failed", "skipped"):
        print(
            _count_style(
                f"{indent}{counts[status]:>3} {status}", status, counts[status]
            )
        )


def _print_build(result) -> None:
    if result.installation:
        print("Installation")
        _print_action_counts(result.installation_report)
    else:
        selection = result.selection
        print("Bundle prepared")
        print(f"  selected for build    {len(selection.selected_for_build):>4}")
        print(f"  selected for removal  {len(selection.selected_for_drop):>4}")
        if result.bundle_path:
            print(f"  Path  {result.bundle_path}")
    print(f"  Bundle  {_style(result.bundle_id, _DIM)}")


def _print_install(report) -> None:
    _print_action_counts(report, indent="")
    print(f"Bundle  {_style(report.bundle_id, _DIM)}")


def handle_initialise(args: argparse.Namespace) -> int:
    """Use one Session for Fabric-backed questions and project setup."""

    from .initialise import (
        collect,
        collect_workspace,
    )

    if args.interactive and non_interactive(args):
        raise CommandError(
            "--interactive and --non-interactive cannot be used together."
        )
    _prefer_desktop_credential(args)

    asked = collect_workspace(args, ask=not args.json)
    if non_interactive(args) or args.json:
        collect(args, ask=False)
        from weaver.config import resolve_workspace

        workspace = resolve_workspace(workspace=args.workspace)
        with _running_session(args, workspace) as opened:
            report = _initialise_once(args, session=opened)
        return _report(args, report, asked=False)

    from weaver.config import resolve_workspace

    if not args.workspace:
        collect(args, ask=False)
    with _running_session(args, resolve_workspace(workspace=args.workspace)) as opened:
        from weaver.initialise import available_environments, available_items

        def client_for(workspace):
            return opened.resolver(resolve_workspace(workspace=workspace)).client

        asked = (
            collect(
                args,
                introduced=asked,
                environments=lambda workspace: available_environments(
                    workspace, client=client_for(workspace)
                ),
                items=lambda workspace, kind: available_items(
                    workspace, kind, client=client_for(workspace)
                ),
            )
            or asked
        )
        if getattr(args, "cancelled", False):
            print("Project setup cancelled.")
            return 0
        report = _initialise_once(args, session=opened)
    return _report(args, report, asked=asked)


def _report(args: argparse.Namespace, report, *, asked: bool) -> int:
    import json

    from .initialise import equivalent_command, render, render_dry_run

    if args.json:
        print(json.dumps(report.to_mapping(), indent=2))
    elif args.dry_run:
        render_dry_run(report)
    else:
        render(report)
        if asked:
            print()
            print("You can set this up again with:")
            print()
            print(f"  {equivalent_command(args)}")
    return 0 if report.succeeded else 1


def _initialise_once(args: argparse.Namespace, *, session):
    """Leave unspecified catalogue and Environment defaults to the operation."""

    named = {
        keyword: value
        for keyword, value in (
            ("catalogue", args.catalogue),
            ("environment", args.environment),
        )
        if value
    }
    return weaver.initialise(
        args.project_folder,
        workspace=args.workspace,
        lakehouse=args.lakehouse,
        warehouse=args.warehouse,
        example=bool(args.example),
        publish_environment=args.publish_environment,
        dry_run=args.dry_run,
        session=session,
        **named,
    )


def handle_doctor(args: argparse.Namespace) -> int:
    import json

    from weaver.operations.doctor import doctor

    from .doctor import render

    _prefer_desktop_credential(args)
    report = doctor(
        workspace=args.workspace,
        session=_session(args),
    )
    if args.json:
        print(json.dumps(report.to_mapping(), indent=2))
    else:
        render(report)
    return 0 if report.succeeded else 1


def handle_check(args: argparse.Namespace) -> int:
    return _retry_until_fixed(args, lambda: _check_once(args))


def _check_once(args: argparse.Namespace) -> int:
    import json

    from weaver.operations.check import check

    try:
        result = check(args.project_folder)
    except WeaverError as exc:
        _render_error(exc, args=args)
        return 1
    if args.json:
        print(
            json.dumps(
                {"status": "succeeded", "project_folder": result.project_folder},
                indent=2,
            )
        )
    else:
        print("Project valid.")
    return 0


def _render_error(exc: BaseException, *, args=None, report=None) -> None:
    import json

    from weaver.errors import reported_message

    message = reported_message(exc) or str(exc)
    executor = _reported_executor(exc)
    error = {"message": message}
    if executor is not None:
        error = {"executor": executor, **error}
    if bool(getattr(args, "json", False)):
        payload = {"status": "failed", "error": error}
        if report is not None:
            payload["report"] = report.to_mapping()
        print(json.dumps(payload, indent=2))
        return
    source = f"{executor} reported: " if executor is not None else ""
    print(
        f"{_style('error:', _RED, stream=sys.stderr)} {source}{message}",
        file=sys.stderr,
    )


def _reported_executor(exc: BaseException) -> str | None:
    from weaver.errors import reported_executor

    return reported_executor(exc)


def _indented(text: str, prefix: str = "  ") -> str:
    return "\n".join(prefix + line if line else line for line in text.splitlines())


def _group_help(group: argparse.ArgumentParser):
    def show(args: argparse.Namespace) -> int:
        group.print_help()
        return 0

    return show


def main(argv: list[str] | None = None) -> int:
    _configure_stdio()
    parser = build_parser()
    words = sys.argv[1:] if argv is None else argv
    if words and words[0] in {"initalise", "initailise"}:
        print("Did you mean 'initialise'?", file=sys.stderr)
    args = parser.parse_args(words)

    handler = getattr(args, "handler", None)
    if handler is None:
        parser.print_help()
        return 0

    try:
        return int(handler(args))
    except WeaverError as exc:
        _render_error(exc, args=args)
        return 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
