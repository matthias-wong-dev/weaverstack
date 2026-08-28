"""CLI command parsing and rendering."""

from __future__ import annotations

import argparse
import sys
import time

import weaver
from weaver.errors import CommandError, WeaverError

#: The capacity verbs, kept here so building the parser imports nothing from
#: `weaver.fabric`, because `weaver --help` should not pay for a transport.
CAPACITY_ACTIONS = ("status", "resume", "suspend")

#: Named in help text. Spelled out here rather than imported at module scope so
#: building the parser stays free of everything ``compose`` pulls in.
COMPOSE_DEFAULT_FILE = "compose.yml"


# --- what each command will want ----------------------------------------------
#
# Declared here, from parsed arguments alone, and coarse. A Session
# cannot work these out. It holds no notion of what a build is, and one that did
# would be a second place deciding what an operation does. So commands declare
# and the Session prepares.
#
# These are a superset, because arguments cannot know what a repository or a
# catalogue turns out to contain. `load Lakehouse/Sales` says Livy may be needed
# because a Lakehouse usually holds Python primitives, not because this estate
# does. Exact routing comes later, from the BuildBundle or the RunGraph, and
# nothing below treats a declaration as permission to acquire.


def _target_kind_and_name(value) -> tuple[str, str]:
    """One ``Kind/Name`` target token, split without validating either half.

    Deliberately tolerant. This reads arguments before the command runs, so a
    malformed target has to reach the command that reports it properly rather
    than failing here.
    """

    kind, _, name = str(value).partition("/")
    return kind.strip().lower(), name.strip()


def _target_requirements(targets) -> set[str]:
    """What the named targets imply, by their type alone.

    Logical or physical: a logical Lakehouse item is installed in a Lakehouse, so
    the type on the left of the slash answers either way.
    """

    from weaver.sessions.requirements import LIVY, ONELAKE, TDS

    wanted: set[str] = set()
    for value in targets or ():
        kind, _name = _target_kind_and_name(value)
        if kind.startswith("warehouse"):
            wanted.add(TDS)
        else:
            # A Lakehouse is files and Spark until something says otherwise.
            wanted |= {ONELAKE, LIVY}
    return wanted


def _requires_targets(args) -> frozenset[str]:
    """What a load or test will want, from the logical items it names.

    A logical item's type is its physical target's type, so the coarse routing a
    command declares is knowable from the request without resolving anything.
    The catalogue is a Warehouse, so a run always reaches TDS.
    """

    from weaver.sessions.requirements import AUTH, RESOLVER, TDS, requirements

    return requirements(
        AUTH, RESOLVER, TDS, *_target_requirements(getattr(args, "targets", ()))
    )


def _requires_build(args) -> frozenset[str]:
    """What a build will want, from the logical items it was told to build.

    A build that names only Warehouses needs no Spark: its objects are T-SQL and
    the catalogue it writes is a Warehouse too. Declaring Livy anyway would have
    the console start a Spark session, costing a minute and a capacity's only
    slot, for a build that never submits one.

    A build that names nothing has not said, so it gets the superset: targets
    can come from workspace configuration, and what a repository turns out to
    hold is not knowable from arguments.
    """

    from weaver.sessions.requirements import (
        AUTH,
        LIVY,
        ONELAKE,
        RESOLVER,
        TDS,
        requirements,
    )

    targets = getattr(args, "targets", None)
    if not targets:
        return requirements(AUTH, RESOLVER, ONELAKE, LIVY, TDS)
    # `LOGICAL[=PHYSICAL]`, and the logical half names the kind. Both halves
    # agree, so the left one answers without resolving the right.
    logical = [str(value).split("=", 1)[0] for value in targets]
    # The catalogue is a Warehouse, so a build always reaches TDS.
    return requirements(AUTH, RESOLVER, TDS, *_target_requirements(logical))


def _requires_wipe(args) -> frozenset[str]:
    """What emptying named physical targets will want.

    The coarse set. A wipe addresses physical resources whether or not an
    installation exists, so its targets stay physical and say their own kind.
    """

    from weaver.sessions.requirements import AUTH, RESOLVER, TDS, requirements

    return requirements(
        AUTH, RESOLVER, TDS, *_target_requirements(getattr(args, "targets", ()))
    )


def _requires_health(args) -> frozenset[str]:
    """What a health report will want.

    TDS always, because the catalogue is a Warehouse. OneLake where a Lakehouse
    was named or where no target was, since discovering the estate may find one.
    Never Livy: health runs no authored code and reads a Lakehouse over storage.
    """

    from weaver.sessions.requirements import AUTH, ONELAKE, RESOLVER, TDS, requirements

    targets = getattr(args, "targets", ()) or ()
    wanted = {AUTH, RESOLVER, TDS}
    if not targets or any(
        _target_kind_and_name(value)[0].startswith("lakehouse") for value in targets
    ):
        wanted.add(ONELAKE)
    return requirements(*wanted)


def _requires_rest(args) -> frozenset[str]:
    """Fabric control-plane work: a credential and the resolver, nothing more."""

    from weaver.sessions.requirements import AUTH, RESOLVER, requirements

    return requirements(AUTH, RESOLVER)


def _requires_install(args) -> frozenset[str]:
    """A frozen bundle may contain any target kind, so declare the coarse set."""

    from weaver.sessions.requirements import (
        AUTH,
        LIVY,
        ONELAKE,
        RESOLVER,
        TDS,
        requirements,
    )

    return requirements(AUTH, RESOLVER, ONELAKE, LIVY, TDS)


def command_requirements(parsed) -> frozenset[str]:
    """What one parsed command says it will want. Empty when it says nothing."""

    declares = getattr(parsed, "requires", None)
    return frozenset(declares(parsed)) if declares is not None else frozenset()


def _target_lakehouses(targets) -> tuple[str, ...]:
    """The Lakehouse names among some target tokens, in the order given."""

    names = []
    for value in targets or ():
        kind, name = _target_kind_and_name(value)
        if kind.startswith("lakehouse") and name:
            names.append(name)
    return tuple(names)


def _physical_target_lakehouses(args) -> tuple[str, ...]:
    """The Lakehouses a command whose targets are physical names outright."""

    return _target_lakehouses(getattr(args, "targets", None) or ())


def _build_target_lakehouses(args) -> tuple[str, ...]:
    """The Lakehouses a build target names on its physical side.

    ``--target Lakehouse/Landing=Lakehouse/Landing_Dev`` says the physical item,
    so warming Spark against it needs no resolution. The bare
    ``--target Lakehouse/Landing`` says only the logical one, and the build
    resolves it through workspace configuration and offers it itself.
    """

    named = []
    for value in getattr(args, "targets", None) or ():
        _logical, separator, physical = str(value).partition("=")
        if separator:
            named.append(physical)
    return _target_lakehouses(named)


def command_lakehouses(parsed) -> tuple[str, ...]:
    """The physical Lakehouses one parsed command names. Empty when it names none.

    Fabric creates a Livy session against a Lakehouse, so warming Spark for a
    command needs the id of one, and only a physical name will do. Declared per
    command, as its requirements are.

    ``load --target Lakehouse/Landing`` declares none. It names a logical item
    whose installation may be ``Landing_Dev``, and offering ``Landing`` would
    attach a Spark session to a Lakehouse the estate does not use, or to none at
    all. Which physical target a logical item is installed in is the catalogue's
    answer, so the operation reads it and offers it: see
    :func:`weaver.operations.load.run_load`. Teaching the CLI to read
    ``_.Installation`` would make one-shot, shell, compose and notebook paths
    four places that resolve it.
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
        help="Run multiple Weaver commands in one persistent session.",
    )
    _add_workspace_args(shell)
    shell.add_argument(
        "--timings",
        action="store_true",
        help="Report time spent by transport when the session ends.",
    )
    shell.set_defaults(handler=handle_session)

    compose = subcommands.add_parser(
        "compose",
        help="Run a named composition in one session.",
    )
    compose.add_argument("name", help="Composition name in compose.yml.")
    compose.add_argument(
        "--file",
        metavar="PATH",
        help=f"Composition file. Defaults to ./{COMPOSE_DEFAULT_FILE}.",
    )
    compose.add_argument(
        "--timings",
        action="store_true",
        help="Report time spent by transport after the composition finishes.",
    )
    compose.add_argument(
        "--yes",
        action="store_true",
        help="Run without asking. Also authorises each command in the sequence.",
    )
    _add_workspace_args(compose)
    compose.set_defaults(handler=handle_compose)

    check = subcommands.add_parser(
        "check", help="Check repository source without contacting Fabric."
    )
    check.add_argument(
        "repository",
        nargs="?",
        help="Repository folder. Defaults to the current directory.",
    )
    check.set_defaults(handler=handle_check)

    build = subcommands.add_parser(
        "build", help="Build repository objects into their logical items."
    )
    build.add_argument(
        "repository",
        nargs="?",
        help="Repository folder. Defaults to the current directory or Notebook Resources.",
    )
    build.add_argument(
        "--target",
        dest="targets",
        action="append",
        metavar="LOGICAL[=PHYSICAL]",
        help=(
            "Logical Weaver item to build. Its physical item comes from "
            "workspace configuration, or write LOGICAL=PHYSICAL to supply it."
        ),
    )
    build.add_argument(
        "--bind",
        dest="retired_bind",
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
    build.add_argument("--json", action="store_true", help="emit the result as JSON")
    _add_workspace_args(build)
    build.set_defaults(
        handler=handle_build,
        requires=_requires_build,
        lakehouses=_build_target_lakehouses,
    )

    load = subcommands.add_parser(
        "load", help="Load installed objects in named logical items."
    )
    load.add_argument(
        "--target",
        dest="targets",
        action="append",
        metavar="LOGICAL",
        help=(
            "Logical Weaver item to load, as Lakehouse/Name or Warehouse/Name. "
            "Repeat to select more than one."
        ),
    )
    load.add_argument(
        "--name",
        dest="names",
        action="append",
        metavar="SCHEMA.OBJECT",
        help="Load one installed object. Repeat to select more than one.",
    )
    load.add_argument(
        "--fault-tolerant",
        action="store_true",
        help="Continue independent branches after a failure.",
    )
    load.add_argument(
        "--dry-run",
        action="store_true",
        help="Show the load plan without running it.",
    )
    load.add_argument("--json", action="store_true", help="emit the report as JSON")
    _add_workspace_args(load)
    load.set_defaults(handler=handle_load, requires=_requires_targets)

    validate = subcommands.add_parser(
        "test", help="Run installed Tests and Assumptions in named logical items."
    )
    validate.add_argument(
        "--target",
        dest="targets",
        action="append",
        metavar="LOGICAL",
        help=(
            "Logical Weaver item to validate, as Lakehouse/Name or "
            "Warehouse/Name. Repeat to select more than one."
        ),
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
    validate.add_argument("--json", action="store_true", help="emit the report as JSON")
    _add_workspace_args(validate)
    validate.set_defaults(handler=handle_test, requires=_requires_targets)

    report = subcommands.add_parser(
        "health",
        help="Report the installed estate's load, test and build health.",
    )
    report.add_argument(
        "targets",
        nargs="*",
        metavar="TARGET",
        help="Lakehouse/Name or Warehouse/Name. Defaults to the whole estate.",
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
        help="Skip the physical read that proves certified objects are there.",
    )
    report.add_argument("--json", action="store_true", help="emit the report as JSON")
    _add_workspace_args(report, include_environment=False)
    report.set_defaults(
        handler=handle_health,
        requires=_requires_health,
        lakehouses=_physical_target_lakehouses,
    )

    wipe = subcommands.add_parser(
        "wipe", help="Clear a physical Lakehouse or Warehouse."
    )
    wipe.add_argument(
        "targets",
        nargs="+",
        metavar="TARGET",
        help="Lakehouse/Name[/Files|/Tables] or Warehouse/Name",
    )
    _add_workspace_args(wipe)
    wipe.add_argument(
        "--unbind-from",
        metavar="LAKEHOUSE",
        help="Remove claims for wiped targets from this Weaver catalogue.",
    )
    wipe.add_argument(
        "--dry-run", action="store_true", help="Show what would be removed."
    )
    wipe.add_argument(
        "--yes", action="store_true", help="Skip the confirmation prompt."
    )
    wipe.add_argument("--json", action="store_true", help="emit the result as JSON")
    wipe.set_defaults(
        handler=handle_wipe,
        requires=_requires_wipe,
        lakehouses=_physical_target_lakehouses,
    )

    install = subcommands.add_parser(
        "install",
        help="Install a previously built deployment bundle.",
    )
    install.add_argument(
        "bundle", metavar="BUNDLE", help="Bundle directory or .weaver.zip archive."
    )
    install.add_argument("--json", action="store_true", help="emit the report as JSON")
    _add_workspace_args(install, include_catalogue=False, include_environment=False)
    install.set_defaults(handler=handle_install, requires=_requires_install)

    # Fabric estate management rather than a Weaver lifecycle verb: these act on
    # workspace items and on the capacity underneath them, and nothing they do
    # reads or writes the catalogue.
    fabric = subcommands.add_parser(
        "fabric", help="Manage the Fabric estate Weaver runs on."
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
        help="Fabric Environment name or Workspace/Environment reference.",
    )
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
    _add_workspace_args(notebook_run)
    notebook_run.set_defaults(handler=handle_notebook_run)

    capacity = fabric_commands.add_parser(
        "capacity", help="Start, stop, or report the state of a Fabric capacity."
    )
    capacity.add_argument("action", choices=CAPACITY_ACTIONS)
    capacity.add_argument("--resource-group", required=True)
    capacity.add_argument("--capacity-name", required=True)
    capacity.add_argument(
        "--subscription-id",
        help="Azure subscription ID when more than one subscription is available.",
    )
    capacity.set_defaults(handler=handle_capacity)

    return parser


def _fabric_cli_workspace(args: argparse.Namespace):
    """Resolve the workspace the notebook utilities act in."""

    return _resolve_workspace(args)


def handle_notebook_push(args: argparse.Namespace) -> int:
    """Deploy a notebook definition without adding notebook APIs to core."""

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
    """Run a notebook with explicit session attachments."""

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
    """Build Weaver from this checkout and publish it to a Fabric Environment.

    The authoritative deployment path. Afterwards a notebook, Livy session or
    Fabric pytest run attached to the Environment can ``import weaver`` with no
    source shipped into a Lakehouse.

    The result is always printed, and it is the JSON: a command's result is what
    it produced, not an option. Progress goes to stderr and the result to
    stdout, so ``weaver fabric environment publish Runtime | jq`` works while a person still watches the
    publish tick over.
    """

    import json
    from dataclasses import replace

    from weaver.fabric.environment import resolve_environment_owner
    from weaver.sessions.host import use_or_create_session
    from weaver.workspaces import EnvironmentRef, Workspace

    environment = EnvironmentRef.parse(args.environment_ref)
    if (
        environment.workspace is not None
        and args.workspace is None
        and args.workspace_config is None
    ):
        workspace = Workspace(workspace=environment.workspace, environment=environment)
    else:
        workspace = _resolve_workspace(args)
        resolve_environment_owner(workspace.workspace, environment)
        workspace = replace(workspace, environment=environment)
    _prefer_desktop_credential()
    from weaver.fabric import publish_environment

    started = time.perf_counter()
    with use_or_create_session(_session(args), workspace=workspace) as session:
        with session.task("Publish Environment", str(environment)):
            result = publish_environment(
                workspace.workspace,
                environment,
                session=session,
            )
    total = time.perf_counter() - started

    payload = result.as_dict()
    payload["timings"]["total"] = round(total, 2)
    print(json.dumps(payload, indent=2))
    return 0


def handle_install(args: argparse.Namespace) -> int:
    """Install a frozen bundle into the selected workspace."""

    import json

    from weaver.operations.install import install

    workspace = _resolve_workspace(args)
    report = install(
        args.bundle,
        workspace=workspace.workspace,
        session=_session(args),
    )
    if args.json:
        print(json.dumps(report.to_mapping(), indent=2))
    else:
        print(f"install {report.status}")
        print(f"  bundle: {report.bundle_id}")
    return 0 if report.succeeded else 1


def handle_capacity(args: argparse.Namespace) -> int:
    """Report or change a capacity's state.

    Capacity is billed while it runs, so this is the first and last thing a
    Fabric session touches.
    """

    _prefer_desktop_credential()
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


def _prefer_desktop_credential() -> None:
    """Pin the Azure CLI credential for desktop commands.

    Credential choice is the CLI's policy, not the core's. Best-effort, because
    if the Fabric extra is not installed there is nothing to pin, and a local
    never needs it.
    """

    try:
        from weaver.fabric.auth import prefer_cli_credential
    except ImportError:
        return
    prefer_cli_credential()


def _desktop_store(workspace):
    """The store a desktop command uses to reach a workspace.

    Reaching into Fabric is a crossing, so the CLI constructs the
    OneLakeDfsClient here. Core never turns a Workspace into a DFS client.
    """

    from weaver.fabric import OneLakeDfsClient

    return OneLakeDfsClient()


def _resolve_workspace(args: argparse.Namespace):
    """The workspace this command line means.

    Inside ``weaver session`` the Fabric workspace is the session's, and it stays
    the session's. A command line is still an ordinary Weaver command line and
    gives its own configuration within that workspace, so ``--catalogue`` and
    ``--environment`` apply on top of it:

    .. code-block:: text

        weaver session --workspace "Weaver Example" --environment weaver
        weaver> weaver load Lakehouse/Sales --catalogue Warehouse/Reporting

    Naming the workspace the session is already open on says what is already
    true. Naming another one is refused: one Session is one Fabric workspace.

    Inheritance is only ever from the session's starting workspace. A default
    accumulated from whichever command ran last would mean the next command
    silently borrowing another workspace's Environment.
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

    _prefer_desktop_credential()
    return workspace


def _refuse_another_workspace(args: argparse.Namespace, inherited) -> None:
    """Refuse a command addressing a workspace other than the Session's own.

    A Session holds one Fabric workspace for its whole life, so a command naming
    a different one has nowhere to run. Refused here rather than resolved and
    then ignored.
    """

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
        f"This session is open on workspace '{inherited.workspace}', so "
        f"'{named}' cannot be reached from it. Open a session on '{named}' "
        "to run there."
    )


def _with_command_overrides(workspace, args: argparse.Namespace):
    """The session's workspace, with whatever this command line said on top.

    The workspace itself is never one of them. What a command may choose is
    configuration within the workspace the session holds.
    """

    from dataclasses import replace

    overrides = {}
    if getattr(args, "environment", None) is not None:
        overrides["environment"] = args.environment
    if getattr(args, "catalogue", None) is not None:
        overrides["catalogue"] = str(args.catalogue)
    return replace(workspace, **overrides) if overrides else workspace


def _command_context(workspace, *, environment: bool = True) -> dict:
    """What this command line settled on, for the operation to apply, as names.

    Operations take names and a Session, and a borrowed Session resolves its own
    workspace as the base. This is how a command's ``--catalogue`` and
    ``--environment`` reach the operation, applied to the Session's workspace
    there and changing nothing about the Session. Where this command opened the
    Session, they are already what it carries.

    ``environment`` is false for an operation that takes none. Health runs no
    authored code, so it has no Environment argument to pass one to.
    """

    context = {"catalogue": workspace.catalogue or None}
    if environment:
        context["environment"] = (
            str(workspace.environment) if workspace.environment else None
        )
    return context


def _session(args: argparse.Namespace):
    """The Session this command inherits, where there is one.

    ``weaver session`` attaches one to every line it parses. A one-shot
    invocation has none.
    """

    return getattr(args, "session", None)


def _running_session(args: argparse.Namespace, workspace):
    """The Session this command runs in, borrowed or opened for it.

    Operations take names and a Session, never a resolved Workspace, so the CLI,
    which resolves one for its own inheritance and override rules, is what
    turns it into a Session. Borrowed from ``weaver session`` where there is
    one, and closed here only when this opened it.
    """

    from weaver.sessions.host import use_or_create_session

    return use_or_create_session(_session(args), workspace=workspace)


def handle_session(args: argparse.Namespace) -> int:
    """Hold one console session open and run commands in it."""

    from .shell import run_shell

    return run_shell(args)


def handle_compose(args: argparse.Namespace) -> int:
    """Show a named sequence, ask once, then run it in one Session."""

    from .compose import run_composition

    return run_composition(args)


#: Retry controls for an interactive task failure.
RETRY_PROMPT = "Enter to retry, Esc to exit."

ESC = "\x1b"
INTERRUPT = "\x03"
END_OF_FILE = "\x04"
ENTER = ("\r", "\n")


def _retry_until_fixed(attempt) -> int:
    """Run an attempt and repeat it after an interactive failure."""

    if not _can_ask():
        return attempt()
    while True:
        status = attempt()
        if not status:
            return status
        if not _retry_wanted():
            return status


def _until_fixed(args: argparse.Namespace, attempt) -> int:
    """Run a task and offer another attempt after an interactive failure.

    Each retry reads fresh inputs but keeps the existing Session open.
    """

    if not _can_ask():
        return attempt()

    from weaver.sessions.host import use_or_create_session

    with use_or_create_session(
        _session(args), workspace=_resolve_workspace(args)
    ) as session:
        args.session = session
        return _retry_until_fixed(attempt)


def _retry_wanted() -> bool:
    """Read one retry decision. Enter retries; Esc leaves."""

    print(f"\n{RETRY_PROMPT} ", end="", file=sys.stderr, flush=True)
    try:
        while True:
            key = _read_key()
            if key in ENTER:
                print(file=sys.stderr)
                return True
            if key in (ESC, INTERRUPT, END_OF_FILE, ""):
                # Ctrl-C and Ctrl-D decline the retry without creating another error.
                print(file=sys.stderr)
                return False
    except (EOFError, KeyboardInterrupt):
        print(file=sys.stderr)
        return False


def _read_key() -> str:
    """Read one keypress without waiting for a line.

    Preserve complete escape sequences so arrow keys are not treated as Esc.
    """

    try:
        import termios
        import tty
    except ImportError:
        return _read_key_windows()

    descriptor = sys.stdin.fileno()
    try:
        saved = termios.tcgetattr(descriptor)
    except termios.error:  # not a terminal after all
        return sys.stdin.readline()[:1]

    import os
    import select

    try:
        tty.setcbreak(descriptor)
        # Text buffering would hide the remaining bytes of an escape sequence.
        key = os.read(descriptor, 1).decode(errors="replace")
        if key == ESC:
            while select.select([descriptor], [], [], 0.05)[0]:
                key += os.read(descriptor, 1).decode(errors="replace")
        return key
    finally:
        termios.tcsetattr(descriptor, termios.TCSADRAIN, saved)


def _read_key_windows() -> str:
    """One keypress on a console without POSIX terminal control.

    ``msvcrt`` reads a key as it is pressed, so Esc declines a retry here as it
    does elsewhere. Reading a line instead would wait for Enter, which is the
    other answer.

    A function or arrow key arrives as a prefix and then its code. Both are
    returned together, so it matches neither answer and the caller asks again
    rather than reading the code as the next keypress.
    """

    try:
        import msvcrt
    except ImportError:  # neither POSIX nor Windows: read a line and take one key
        return sys.stdin.readline()[:1]

    key = msvcrt.getwch()
    if key in ("\x00", "\xe0"):
        return key + msvcrt.getwch()
    return key


def _can_ask() -> bool:
    """Whether there is somebody at a terminal to answer."""

    return sys.stdin.isatty()


def _authorised(args: argparse.Namespace) -> bool:
    """Return whether a command has already received confirmation."""

    return bool(getattr(args, "yes", False) or getattr(args, "authorised", False))


def handle_load(args: argparse.Namespace) -> int:
    return _until_fixed(args, lambda: _load_once(args))


def _load_once(args: argparse.Namespace) -> int:
    """Adapt command-line values to :func:`weaver.load`, wherever it has to run.

    The CLI owns exactly one thing the API does not: the host boundary. A load
    runs where the data is, so a desktop asking for a Fabric workspace has to
    reach into a session to get one, and that crossing is the CLI's, as it is for
    ``build`` and ``unbind``. Resolving the workspace, validating targets,
    planning, orchestrating and reporting all happen once, inside
    :func:`weaver.load`, whichever side of the boundary it runs on.
    """

    import json

    from weaver.errors import LoadError

    workspace = _resolve_workspace(args)
    try:
        report = _run_load(
            workspace,
            targets=args.targets,
            names=args.names,
            fault_tolerant=args.fault_tolerant,
            dry_run=args.dry_run,
            session=_session(args),
        )
    except LoadError as exc:
        # An intolerant failure. The report is the useful half of the answer and
        # the message is the other, so both are shown before the non-zero exit.
        if getattr(exc, "report", None) is not None:
            _print_load(exc.report)
        print(f"error: {exc}", file=sys.stderr)
        if getattr(exc, "workflow_id", None):
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
    targets,
    names=None,
    fault_tolerant: bool,
    dry_run: bool,
    session=None,
):
    """Run one load through the selected Session."""

    from weaver.sessions.host import use_or_create_session

    with use_or_create_session(session, workspace=workspace) as opened:
        return weaver.load(
            list(targets),
            names=names,
            fault_tolerant=fault_tolerant,
            dry_run=dry_run,
            session=opened,
            **_command_context(workspace),
        )


def _print_load(report) -> None:
    """One renderer, for a report produced here or one that crossed Livy."""

    mode = "plan" if report.dry_run else "load"
    print(f"{mode} {report.status}: {', '.join(report.requested)}\n")
    for node in report.nodes:
        counts = ""
        # A node that failed before it moved any rows carries a failure rather
        # than a count, and asking one for rows read is how a rendered report
        # turns a clear error into an AttributeError.
        if node.result is not None and hasattr(node.result, "rows_read"):
            counts = (
                f"  (read {node.result.rows_read}, "
                f"+{node.result.rows_inserted} "
                f"~{node.result.rows_updated} "
                f"-{node.result.rows_deleted} "
                f"!{node.result.rows_rejected})"
            )
        print(f"  {node.status:<24} {node.node_id}{counts}")
        for message in node.messages:
            if message.severity != "info":
                print(f"      {message.severity}: {message.message}")
    if report.workflow_id:
        print(f"\n  Workflow: {report.workflow_id}")


def handle_test(args: argparse.Namespace) -> int:
    return _until_fixed(args, lambda: _test_once(args))


def _test_once(args: argparse.Namespace) -> int:
    """Adapt command-line values to :func:`weaver.test`, wherever it has to run.

    The same host boundary ``load`` crosses, and for the same reason: a
    validation reads the data, so it runs where the data is.

    A failing validation exits non-zero. That is what makes ``weaver test``
    usable in a pipeline: the report is the evidence and the exit code is the
    verdict. It is why the API returns a report where this returns a status.
    """

    import json

    from weaver.errors import ValidationError

    workspace = _resolve_workspace(args)
    try:
        report = _run_test(
            workspace,
            targets=args.targets,
            name=args.name,
            file=args.file,
            dry_run=args.dry_run,
            session=_session(args),
        )
    except ValidationError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(json.dumps(report.to_mapping(), indent=2))
    else:
        _print_test(report)
    return 0 if report.succeeded else 1


def _run_test(workspace, *, targets, name, file, dry_run: bool, session=None):
    """One validation run, decided here and dispatched where each check lives.

    The crossing is ``load``'s, for the reason it is ``load``'s: a Warehouse
    validation is a stored procedure TDS reaches from anywhere, and a Lakehouse
    one is a deployed module that belongs where the imports happen.
    """

    from weaver.sessions.host import use_or_create_session

    with use_or_create_session(session, workspace=workspace) as opened:
        return weaver.test(
            list(targets),
            name=name,
            file=file,
            dry_run=dry_run,
            session=opened,
            **_command_context(workspace),
        )


def _print_test(report) -> None:
    """One renderer, for a report produced here or one that crossed Livy."""

    print(f"test {report.status}\n")
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
        print(f"  {node.status:<10} {node.kind:<11} {node.logical_id}{found}")
        for message in node.messages:
            print(f"      {message}")
        error_message = (
            None if result is None else getattr(result, "error_message", None)
        )
        if error_message and not node.messages:
            print(f"      {error_message}")

    totals = report.totals()
    print(
        f"\n  {totals['passed']} passed, {totals['failed']} failed, "
        f"{totals['invalid']} could not run"
    )
    if report.workflow_id:
        print(f"  Workflow: {report.workflow_id}")

    # Print requested diagnostic rows after the summary.
    for node in report.nodes:
        if not node.diagnostics:
            continue
        print(f"\n  {node.logical_id}:")
        for row in node.diagnostics:
            print(f"    {row}")


def handle_health(args: argparse.Namespace) -> int:
    """Adapt command-line values to :func:`weaver.health` and render the report.

    Exit 0 for Green and 1 for anything worse, so a scheduled check is a
    pipeline step. Configuration and transport failures take the ordinary
    command error path.
    """

    import json

    workspace = _resolve_workspace(args)
    with _running_session(args, workspace) as opened:
        report = weaver.health(
            list(args.targets),
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
    """One health report as plain text.

    The status words carry the meaning, so the output reads the same redirected
    to a file as it does on a terminal.
    """

    from weaver.health import AREAS

    lines = [f"Weaver Health  {_titled(report.status)}", ""]
    for area, section in zip(AREAS, report.sections):
        lines.append(f"{area.title():<8}{_titled(section.status)}")
        lines.extend(_health_section(area, section, report))
        lines.append("")
    lines.extend(_health_activity(report))
    return "\n".join(lines).rstrip() + "\n"


def _health_section(area: str, section, report) -> list[str]:
    """One section's counts, then the objects behind them."""

    from weaver.health import BUILD, LOAD

    lines = []
    if area == LOAD and report.latest_load is not None:
        latest = report.latest_load
        lines.append(
            f"  Last load activity   {_ago(latest.completed_at, report.generated_at)}"
        )
    counts = " · ".join(
        f"{count} {word}" for word, count in sorted(section.counts.items())
    )
    if counts:
        lines.append(f"  {counts}")
    if area == BUILD and not section.findings:
        lines.append(f"  Installed estate consistent ({section.subjects} objects)")
    for finding in section.findings:
        where = finding.object_id or finding.target or ""
        lines.append(f"  {_titled(finding.severity):<7}{where}")
        lines.append(f"          {finding.message}")
    return lines


#: The narrowest an object-id column gets, so short ids in one report still line
#: up with a longer one in the next.
_ID_WIDTH = 42


def _health_activity(report) -> list[str]:
    """The slowest loads and the rows that moved, from the bounded window."""

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
    """One activity line, with the id column wide enough for the block it is in.

    Each block is measured on its own, and the separator is written rather than
    left to the padding: an id longer than the column would otherwise run
    straight into the value beside it.
    """

    width = max(_ID_WIDTH, *(len(str(each.object_id)) for each in among))
    return f"  {object_id:<{width}}  {value}"


def _titled(word: str) -> str:
    return str(word).title()


def _ago(at, now) -> str:
    """How long ago an instant was, in hours and minutes."""

    if at is None:
        return "never"
    seconds = max(int((now - at).total_seconds()), 0)
    hours, remainder = divmod(seconds, 3600)
    return f"{hours}h {remainder // 60}m ago"


def handle_wipe(args: argparse.Namespace) -> int:
    """Preview, confirm, then invoke the same public wipe operation."""

    import json

    workspace = _resolve_workspace(args)

    # A preview is needed only when the command needs confirmation.
    previewing = args.dry_run or not _authorised(args)
    if previewing:
        with _running_session(args, workspace) as opened:
            planned = weaver.wipe(
                args.targets,
                unbind_from=args.unbind_from,
                dry_run=True,
                session=opened,
                **_command_context(workspace),
            )
        print(f"wipe on {workspace.workspace}\n")
        for report in planned.reports:
            print(f"  {report.target}")
            print(f"    {report.location}")
            for name in report.removed:
                print(f"      - {name}")
            if not report.removed:
                print("      (already empty)")
        total = planned.count
        print()

        if args.dry_run:
            if args.json:
                print(json.dumps(planned.to_mapping(), indent=2))
            else:
                print(f"{total} item(s) would be removed. Nothing was changed.")
            return 0

        if total:
            if not sys.stdin.isatty():
                print(
                    f"Refusing to remove {total} item(s) without confirmation. "
                    "Pass --yes, or --dry-run to preview.",
                    file=sys.stderr,
                )
                return 1
            answer = input(f"Remove {total} item(s)? This cannot be undone [y/N] ")
            if answer.strip().lower() not in {"y", "yes"}:
                print("Cancelled.")
                return 1

    with _running_session(args, workspace) as opened:
        result = weaver.wipe(
            args.targets,
            unbind_from=args.unbind_from,
            session=opened,
            **_command_context(workspace),
        )
    if args.json:
        print(json.dumps(result.to_mapping(), indent=2))
    elif result.count:
        for report in result.reports:
            print(f"  {report.target}: removed {report.count}")
    else:
        print("Nothing to remove.")
    return 0


def handle_build(args: argparse.Namespace) -> int:
    if getattr(args, "retired_bind", None):
        raise CommandError(
            "--bind is replaced by --target, and the two halves have swapped.\n"
            "Old: --bind Lakehouse/Landing_Dev=Landing\n"
            "New: --target Lakehouse/Landing=Lakehouse/Landing_Dev"
        )
    if args.bundle_path and not args.bundle_only:
        raise CommandError("--bundle-path requires --bundle-only")
    return _until_fixed(args, lambda: _build_once(args))


def _build_once(args: argparse.Namespace) -> int:
    """Adapt command-line values to :func:`weaver.build`."""

    import json

    workspace = _resolve_workspace(args)
    with _running_session(args, workspace) as opened:
        result = weaver.build(
            args.repository,
            targets=args.targets,
            bundle_only=args.bundle_only,
            bundle_path=args.bundle_path,
            session=opened,
            **_command_context(workspace),
        )
    payload = result.to_mapping()
    if args.json:
        print(json.dumps(payload, indent=2))
    else:
        print(f"build {result.status}: workspace declaration")
        print(f"  bundle: {result.bundle_id}")
        if result.bundle_path:
            print(f"  path:   {result.bundle_path}")
        print(f"  items:  {', '.join(result.items)}")
        for error in result.errors:
            # Show the operation and source before lower-level diagnostics.
            print()
            print(_indented(error.describe()), file=sys.stderr)
    return 0 if result.succeeded else 1


def handle_check(args: argparse.Namespace) -> int:
    return _retry_until_fixed(lambda: _check_once(args))


def _check_once(args: argparse.Namespace) -> int:
    from weaver.operations.check import check

    try:
        check(args.repository)
    except WeaverError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print("Repository valid.")
    return 0


def _indented(text: str, prefix: str = "  ") -> str:
    return "\n".join(prefix + line if line else line for line in text.splitlines())


def _group_help(group: argparse.ArgumentParser):
    """A group named without a subcommand lists its own, not the whole CLI."""

    def show(args: argparse.Namespace) -> int:
        group.print_help()
        return 0

    return show


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    handler = getattr(args, "handler", None)
    if handler is None:
        parser.print_help()
        return 0

    try:
        return int(handler(args))
    except WeaverError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
