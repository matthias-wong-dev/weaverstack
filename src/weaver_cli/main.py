"""Top-level CLI routing.

Commands are registered here as the core APIs they wrap become available. The
handler contract is the one convention worth keeping from the old repository:
a command function returns a plain serialisable structure and the CLI prints
it.
"""

from __future__ import annotations

import argparse
import sys
import time

import weaver
from weaver.errors import WeaverError

#: The capacity verbs, kept here so the parser needs no Fabric import — a
#: CLI-only install without the [fabric] extra must still build its parser.
CAPACITY_ACTIONS = ("status", "resume", "suspend")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="weaver",
        description="Weaver — build and load Fabric Lakehouse and Warehouse objects.",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"weaverstack {weaver.__version__}",
    )
    subcommands = parser.add_subparsers(dest="command", metavar="command")

    doctor = subcommands.add_parser(
        "doctor", help="check whether this machine can run local Spark and Delta"
    )
    doctor.add_argument("--json", action="store_true", help="emit the report as JSON")
    doctor.set_defaults(handler=handle_doctor)

    build = subcommands.add_parser(
        "build", help="build bound logical items from an explicit repository"
    )
    build.add_argument(
        "repository",
        nargs="?",
        help="authored repository folder; defaults to the current directory/Notebook Resources",
    )
    build.add_argument(
        "--bind",
        dest="item_bindings",
        action="append",
        metavar="PHYSICAL[=LOGICAL]",
        help="typed physical target with configured default or logical override",
    )
    build.add_argument(
        "--bundle",
        nargs="?",
        const="",
        metavar="NAME",
        help=(
            "optionally retain a .weaver.zip build record; omit NAME for a UTC "
            "timestamp"
        ),
    )
    build.add_argument("--json", action="store_true", help="emit the result as JSON")
    _add_workspace_args(build)
    build.set_defaults(handler=handle_build)

    push = subcommands.add_parser(
        "push", help="compatibility utility: validate and upload an authored repository"
    )
    push.add_argument("repository", help="local authored repository folder")
    push.add_argument("--json", action="store_true", help="emit the result as JSON")
    _add_workspace_args(push)
    push.set_defaults(handler=handle_push)

    load = subcommands.add_parser(
        "load", help="load every installed object in named physical targets"
    )
    load.add_argument(
        "--targets",
        nargs="+",
        required=True,
        metavar="TARGET",
        help="Lakehouse/Name or Warehouse/Name",
    )
    load.add_argument(
        "--fault-tolerant",
        action="store_true",
        help="continue independent branches after a node fails, and report",
    )
    load.add_argument(
        "--dry-run",
        action="store_true",
        help="resolve and render the plan without dispatching anything",
    )
    load.add_argument("--json", action="store_true", help="emit the report as JSON")
    _add_workspace_args(load)
    load.set_defaults(handler=handle_load)

    validate = subcommands.add_parser(
        "test", help="run the installed Tests and Assumptions in named targets"
    )
    validate.add_argument(
        "targets",
        nargs="+",
        metavar="TARGET",
        help="Lakehouse/Name or Warehouse/Name",
    )
    selection = validate.add_mutually_exclusive_group()
    selection.add_argument(
        "--name",
        metavar="Schema.Object",
        help="run one installed validation and return its diagnostic rows",
    )
    selection.add_argument(
        "--file",
        metavar="PATH",
        help="compile and run a source file without installing it",
    )
    validate.add_argument(
        "--dry-run",
        action="store_true",
        help="report what would run without dispatching anything",
    )
    validate.add_argument("--json", action="store_true", help="emit the report as JSON")
    _add_workspace_args(validate)
    validate.set_defaults(handler=handle_test)

    unbind = subcommands.add_parser(
        "unbind", help="remove catalogue state for named physical targets"
    )
    unbind.add_argument(
        "targets",
        nargs="+",
        metavar="TARGET",
        help="Lakehouse/Name or Warehouse/Name",
    )
    unbind.add_argument("--json", action="store_true", help="emit the result as JSON")
    _add_workspace_args(unbind)
    unbind.set_defaults(handler=handle_unbind)

    wipe = subcommands.add_parser(
        "wipe", help="clear a physical Lakehouse or Warehouse"
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
        help="immediately remove wiped target claims from this Weaver catalogue",
    )
    wipe.add_argument("--dry-run", action="store_true", help="report without removing")
    wipe.add_argument("--yes", action="store_true", help="do not ask for confirmation")
    wipe.add_argument("--json", action="store_true", help="emit the result as JSON")
    wipe.set_defaults(handler=handle_wipe)

    install = subcommands.add_parser(
        "install",
        help="build Weaver and install it into a Fabric Environment",
    )
    _add_workspace_args(install, include_weaver_lakehouse=False)
    install.add_argument(
        "--no-publish",
        action="store_true",
        help="stage the wheel and dependencies but do not publish (development only)",
    )
    install.add_argument("--json", action="store_true", help="emit the result as JSON")
    install.set_defaults(handler=handle_install)

    notebook = subcommands.add_parser(
        "notebook", help="deploy or execute a Fabric notebook"
    )
    notebook_commands = notebook.add_subparsers(
        dest="notebook_command", metavar="command"
    )

    notebook_push = notebook_commands.add_parser(
        "push", help="create or update a notebook definition"
    )
    notebook_push.add_argument("source", help="local .py or .ipynb notebook source")
    notebook_push.add_argument("--name", help="Fabric display name; defaults to filename")
    notebook_push.add_argument("--description")
    notebook_push.add_argument("--json", action="store_true")
    _add_workspace_args(notebook_push, include_weaver_lakehouse=False)
    notebook_push.set_defaults(handler=handle_notebook_push)

    notebook_run = notebook_commands.add_parser(
        "run", help="execute a deployed notebook in Fabric"
    )
    notebook_run.add_argument("name", help="Fabric Notebook display name")
    notebook_run.add_argument(
        "--lakehouse",
        help="default Lakehouse attached to the notebook session",
    )
    notebook_run.add_argument("--no-wait", action="store_true")
    notebook_run.add_argument("--timeout", type=float, default=7200.0)
    notebook_run.add_argument("--poll-interval", type=float, default=10.0)
    notebook_run.add_argument("--json", action="store_true")
    _add_workspace_args(notebook_run)
    notebook_run.set_defaults(handler=handle_notebook_run)

    capacity = subcommands.add_parser(
        "capacity", help="turn a Fabric capacity on or off, or report its state"
    )
    capacity.add_argument("action", choices=CAPACITY_ACTIONS)
    capacity.add_argument("--resource-group", required=True)
    capacity.add_argument("--capacity-name", required=True)
    capacity.add_argument(
        "--subscription-id",
        help="only needed when az has more than one subscription",
    )
    capacity.set_defaults(handler=handle_capacity)

    return parser


def _fabric_cli_workspace(args: argparse.Namespace):
    """Resolve the Fabric-only values shared by notebook CLI utilities."""

    from weaver.errors import CommandError
    from weaver.workspaces import FabricWorkspace

    workspace = _resolve_workspace(args)
    if not isinstance(workspace, FabricWorkspace):
        raise CommandError("weaver notebook requires a Fabric Workspace")
    return workspace


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
    lakehouse = args.lakehouse or workspace.weaver_lakehouse
    if not lakehouse:
        raise CommandError(
            "notebook run requires --lakehouse or a configured Weaver Lakehouse"
        )
    if not workspace.environment:
        raise CommandError(
            "notebook run requires --environment or a configured Environment"
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


def handle_install(args: argparse.Namespace) -> int:
    """Build Weaver from this checkout and install it into a Fabric Environment.

    The authoritative deployment path. Afterwards a notebook, Livy session or
    Fabric pytest run attached to the Environment can ``import weaver`` with no
    source shipped into a Lakehouse.
    """

    from weaver.workspaces import FabricWorkspace
    from weaver.errors import CommandError

    workspace = _resolve_workspace(args)
    if not isinstance(workspace, FabricWorkspace):
        raise CommandError("weaver install requires a Fabric Workspace")
    if not workspace.environment:
        raise CommandError("weaver install requires --environment or a configured environment")
    _prefer_desktop_credential()
    from weaver.fabric import install as run_install

    started = time.perf_counter()
    result = run_install(
        workspace.workspace,
        workspace.environment,
        publish=not args.no_publish,
    )
    total = time.perf_counter() - started

    if args.json:
        import json

        payload = result.as_dict()
        payload["timings"]["total"] = round(total, 2)
        print(json.dumps(payload, indent=2))
        return 0

    _print_install(result, total)
    return 0


def _print_install(result, total: float) -> None:
    print("Installed Weaver into Microsoft Fabric\n")
    print("Workspace")
    print(f"  Name: {result.workspace_name}")
    print(f"  ID:   {result.workspace_id}\n")
    print("Environment")
    print(f"  Name: {result.environment_name}")
    print(f"  ID:   {result.environment_id}\n")
    print("Package")
    print(f"  Distribution: {result.package_name}")
    print(f"  Version:      {result.package_version}")
    print(f"  Wheel:        {result.wheel_filename}\n")
    print("Changes")
    print(f"  Environment created:  {'yes' if result.created_environment else 'no'}")
    print(f"  Dependencies changed: {'yes' if result.dependencies_changed else 'no'}")
    print(f"  Wheel changed:        {'yes' if result.wheel_changed else 'no'}")
    print(f"  Published:            {result.publish_status}\n")
    parts = ", ".join(f"{name} {secs:.1f}s" for name, secs in result.timings.items())
    print(f"Timing  {parts + ', ' if parts else ''}total {total:.1f}s\n")
    print("Notebook use")
    print(f'  1. Attach the "{result.environment_name}" Environment.')
    print("  2. Start a new session.")
    print("  3. Run: import weaver")


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
        print("  (resuming takes a moment; run `capacity status` to confirm)")
    return 0


def _add_workspace_args(
    parser: argparse.ArgumentParser, *, include_weaver_lakehouse: bool = True
) -> None:
    """Add the explicit values that a Workspace configuration can abbreviate."""

    parser.add_argument("--workspace", help="Fabric Workspace name or local folder path")
    parser.add_argument("--workspace-config", help="one Workspace configuration file")
    parser.add_argument(
        "--workspace-type",
        choices=("fabric", "local"),
        help="resource environment; defaults to fabric",
    )
    parser.add_argument("--environment", help="Fabric Environment name")
    if include_weaver_lakehouse:
        parser.add_argument("--weaver-lakehouse", help="control Lakehouse name")


def _prefer_desktop_credential() -> None:
    """Pin the Azure CLI credential for desktop commands.

    Credential choice is the CLI's policy, not the core's. Best-effort — if the
    Fabric extra is not installed there is nothing to pin, and a local command
    never needs it.
    """

    try:
        from weaver.fabric.auth import prefer_cli_credential
    except ImportError:
        return
    prefer_cli_credential()


def _desktop_store(workspace):
    """The store a desktop command uses to reach a workspace.

    Local is within-workspace; Fabric is cross-boundary, so the CLI constructs the
    OneLakeDfsClient here — core never turns a FabricWorkspace into a DFS client.
    """

    from weaver.workspaces import LocalWorkspace
    from weaver.store import FilesystemStore

    if isinstance(workspace, LocalWorkspace):
        return FilesystemStore()
    from weaver.fabric import OneLakeDfsClient

    return OneLakeDfsClient()


def _resolve_workspace(args: argparse.Namespace):
    from weaver.config import resolve_workspace
    from weaver.workspaces import LocalWorkspace

    workspace = resolve_workspace(
        workspace=args.workspace,
        workspace_type=args.workspace_type,
        environment=args.environment,
        weaver_lakehouse=getattr(args, "weaver_lakehouse", None),
        workspace_config=args.workspace_config,
    )

    if not isinstance(workspace, LocalWorkspace):
        _prefer_desktop_credential()
    return workspace


def handle_push(args: argparse.Namespace) -> int:
    """Validate locally, then replace ``Files/weaver_items`` as one unit."""

    import json

    from weaver.locations import Location
    from weaver.push import push_item_repository
    from weaver.errors import CommandError
    from weaver.resolution import resolver_for

    workspace = _resolve_workspace(args)
    if not workspace.weaver_lakehouse:
        raise CommandError("push requires --weaver-lakehouse or a configured value")
    resolver = resolver_for(workspace)
    result = push_item_repository(
        Location(args.repository),
        resolver.weaver_items_root,
        destination_store=_desktop_store(workspace),
    )
    payload = result.to_mapping()
    if args.json:
        print(json.dumps(payload, indent=2))
    else:
        print(f"pushed {len(result.files)} file(s)")
        print(f"  from: {result.source}")
        print(f"  to:   {result.destination}")
        print(f"  signature: {result.repository_signature}")
    return 0


def handle_unbind(args: argparse.Namespace) -> int:
    import json

    from weaver.operations import _unbind_target_names
    from weaver.errors import CommandError

    lakehouses, warehouses = _unbind_target_names(args.targets)
    workspace = _resolve_workspace(args)
    if not workspace.weaver_lakehouse:
        raise CommandError("unbind requires a configured Weaver Lakehouse")
    result = _run_unbind(
        workspace, lakehouses=lakehouses, warehouses=warehouses
    )
    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print(f"unbound {len(result['logical_items'])} logical installation(s)")
        for target in result["targets"]:
            print(f"  {target}")
    return 0


def _run_unbind(workspace, *, lakehouses, warehouses) -> dict:
    from weaver.workspaces import LocalWorkspace

    if isinstance(workspace, LocalWorkspace):
        from weaver.targets import ItemRef
        from weaver.unbind import unbind_targets
        from weaver.resolution import resolver_for
        from weaver.spark import SparkCatalogue, local_delta_session

        resolver = resolver_for(workspace)
        with local_delta_session(workspace) as session:
            catalogue = SparkCatalogue(
                session,
                resolver.spark_destination(ItemRef(workspace.weaver_lakehouse)),
            )
            return unbind_targets(
                catalogue, lakehouses=lakehouses, warehouses=warehouses
            ).to_mapping()

    from weaver.fabric import LivySession

    body = (
        "from weaver.workspaces import FabricWorkspace\n"
        "from weaver.targets import ItemRef\n"
        "from weaver.unbind import unbind_targets\n"
        "from weaver.resolution import resolver_for\n"
        "from weaver.spark import SparkCatalogue\n"
        f"workspace = FabricWorkspace(workspace={workspace.workspace!r}, "
        f"environment={workspace.environment!r}, "
        f"weaver_lakehouse={workspace.weaver_lakehouse!r})\n"
        "resolver = resolver_for(workspace)\n"
        "catalogue = SparkCatalogue(spark, resolver.spark_destination("
        "ItemRef(workspace.weaver_lakehouse)))\n"
        f"result = unbind_targets(catalogue, lakehouses={tuple(lakehouses)!r}, "
        f"warehouses={tuple(warehouses)!r})\n"
        "emit(result.to_mapping())\n"
    )
    with LivySession.for_workspace(workspace) as session:
        return session.run(body).payload


def handle_load(args: argparse.Namespace) -> int:
    """Adapt command-line values to :func:`weaver.load`, wherever it has to run.

    The CLI owns exactly one thing the API does not: the *host boundary*. A load
    runs where the data is, so a desktop asking for a Fabric workspace has to
    reach into a session to get one — and that crossing is the CLI's, the same
    way it is for ``build`` and ``unbind``. Everything else — resolving the
    workspace, validating targets, planning, orchestrating, reporting — happens
    once, inside :func:`weaver.load`, whichever side of the boundary it runs on.
    """

    import json

    from weaver.errors import LoadError

    workspace = _resolve_workspace(args)
    try:
        report = _run_load(
            workspace,
            targets=args.targets,
            fault_tolerant=args.fault_tolerant,
            dry_run=args.dry_run,
        )
    except LoadError as exc:
        # An intolerant failure. The report is the useful half of the answer and
        # the message is the other, so both are shown before the non-zero exit.
        if getattr(exc, "report", None) is not None:
            _print_load(exc.report)
        print(f"error: {exc}", file=sys.stderr)
        if getattr(exc, "task_log", None):
            print(f"  evidence: {exc.task_log}", file=sys.stderr)
        return 1

    if args.json:
        print(json.dumps(report.to_mapping(), indent=2))
    else:
        _print_load(report)
    return 0 if report.succeeded else 1


def _run_load(workspace, *, targets, fault_tolerant: bool, dry_run: bool):
    """One load, run where the data is.

    Local and in-session are the same call; a desktop reaching into Fabric is
    the same call submitted through Livy. What comes back is a real
    :class:`~weaver.load_report.LoadRunReport` either way, so the renderer below
    cannot tell which happened.
    """

    from weaver.operations import _inside_fabric_session
    from weaver.workspaces import LocalWorkspace

    if isinstance(workspace, LocalWorkspace) or _inside_fabric_session(workspace):
        return weaver.load(
            list(targets),
            workspace=workspace,
            fault_tolerant=fault_tolerant,
            dry_run=dry_run,
        )
    _refuse_absent_targets(workspace, targets)
    return _run_load_over_livy(
        workspace, targets=targets, fault_tolerant=fault_tolerant, dry_run=dry_run
    )


def _refuse_absent_targets(workspace, targets) -> None:
    """Check the requested items exist, over REST, before a session is opened.

    A guard bought cheaply and spent well. Starting a Livy session costs tens of
    seconds and a capacity's only session slot; resolving a name over REST costs
    one call. So a request that can already be rejected — a mistyped
    ``Lakehouse/Rwa``, a Warehouse somebody deleted — is rejected before any of
    that is spent.

    Deliberately *only* that. The catalogue is not read, no graph is built, no
    upstream target is discovered and no inventory is fetched: those need the
    estate, the estate is inside Fabric, and asking about them here would be
    doing the remote run's work on the wrong side of the boundary. What is
    checked is exactly what the user typed.

    A genuine not-found is the missing-target error. Anything else — an expired
    credential, an unreachable tenant, a name that matches two items — keeps its
    own diagnosis, because "your Lakehouse is gone" is a bad answer to "your
    token expired".
    """

    from weaver.errors import CommandError
    from weaver.fabric import FabricResolver, ItemNotFoundError
    from weaver.fabric.resources import LAKEHOUSE, WAREHOUSE
    from weaver.targets import DeltaTarget, parse_physical_target, physical_item

    resolver = FabricResolver(workspace)
    absent = []
    for value in targets:
        target = parse_physical_target(value, what="load target", error=CommandError)
        item_type = LAKEHOUSE if isinstance(target, DeltaTarget) else WAREHOUSE
        try:
            resolver.resolve(physical_item(target), item_type=item_type)
        except ItemNotFoundError:
            # What the user typed, not a re-spelling of it: this message exists
            # to help them see a typo, and showing them a normalised form is
            # showing them something they did not write.
            absent.append(value)
    if absent:
        raise CommandError(
            "no such item in "
            + f"{workspace.workspace!r}: "
            + ", ".join(absent)
            + " — check the name, or build into it first"
        )


#: What the submitted program sends back. A failure is *data* on the way across:
#: an exception raised inside a Fabric session cannot be re-raised on a desktop
#: that has never heard of its class, so what travels is the diagnosis rather
#: than the exception, and the desktop raises its own.
_LOAD_BODY = """\
from weaver.workspaces import FabricWorkspace
import weaver

workspace = FabricWorkspace(workspace={workspace!r}, environment={environment!r},
                            weaver_lakehouse={weaver_lakehouse!r})
try:
    report = weaver.load(
        {targets!r},
        workspace=workspace,
        fault_tolerant={fault_tolerant!r},
        dry_run={dry_run!r},
    )
except Exception as exc:
    carried = getattr(exc, "report", None)
    result = getattr(exc, "result", None)
    emit({{
        "failed": True,
        "error_type": type(exc).__name__,
        "message": str(exc),
        "result": None if result is None else result.as_row(),
        "report": None if carried is None else carried.to_mapping(),
        "task_log": getattr(exc, "task_log", None),
    }})
else:
    emit({{"failed": False, "report": report.to_mapping()}})
"""


def _run_load_over_livy(workspace, *, targets, fault_tolerant: bool, dry_run: bool):
    from weaver.errors import CommandError, LoadError
    from weaver.fabric import LivySession
    from weaver.load_report import LoadRunReport
    from weaver.runtime.load_result import LoadResult

    body = _LOAD_BODY.format(
        workspace=workspace.workspace,
        environment=workspace.environment,
        weaver_lakehouse=workspace.weaver_lakehouse,
        targets=list(targets),
        fault_tolerant=fault_tolerant,
        dry_run=dry_run,
    )
    with LivySession.for_workspace(workspace) as session:
        payload = session.run(body).payload
    if payload is None:
        raise CommandError(
            "the load ran in Fabric but returned nothing; see the Livy session output"
        )
    if not payload.get("failed"):
        return LoadRunReport.from_mapping(payload["report"])

    carried = payload.get("report")
    counts = payload.get("result")
    raise LoadError(
        f"{payload['error_type']}: {payload['message']}",
        result=None if counts is None else LoadResult.from_row(counts),
        report=None if carried is None else LoadRunReport.from_mapping(carried),
        task_log=payload.get("task_log"),
    )


def _print_load(report) -> None:
    """One renderer, for a report produced here or one that crossed Livy."""

    mode = "plan" if report.dry_run else "load"
    print(f"{mode} {report.status}: {', '.join(report.requested)}\n")
    for node in report.nodes:
        counts = ""
        if node.result is not None:
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
    if report.task_log:
        print(f"\n  evidence: {report.task_log}")


def handle_test(args: argparse.Namespace) -> int:
    """Adapt command-line values to :func:`weaver.test`, wherever it has to run.

    The same host boundary ``load`` crosses, and for the same reason: a
    validation reads the data, so it runs where the data is.

    A failing validation exits non-zero. That is what makes ``weaver test``
    usable in a pipeline — the report is the evidence and the exit code is the
    verdict — and it is why the API returns a report where this returns a
    status.
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
        )
    except ValidationError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(json.dumps(report.to_mapping(), indent=2))
    else:
        _print_test(report)
    return 0 if report.succeeded else 1


def _run_test(workspace, *, targets, name, file, dry_run: bool):
    """One validation run, run where the data is."""

    from weaver.operations import _inside_fabric_session
    from weaver.workspaces import LocalWorkspace

    if isinstance(workspace, LocalWorkspace) or _inside_fabric_session(workspace):
        return weaver.test(
            list(targets),
            workspace=workspace,
            name=name,
            file=file,
            dry_run=dry_run,
        )
    _refuse_absent_targets(workspace, targets)
    return _run_test_over_livy(
        workspace, targets=targets, name=name, file=file, dry_run=dry_run
    )


_TEST_BODY = """\
from weaver.workspaces import FabricWorkspace
import weaver

workspace = FabricWorkspace(workspace={workspace!r}, environment={environment!r},
                            weaver_lakehouse={weaver_lakehouse!r})
source = {source!r}
path = None
if source is not None:
    import pathlib, tempfile

    path = pathlib.Path(tempfile.mkdtemp()) / {filename!r}
    path.write_text(source, encoding="utf-8")

def _portable(value):
    # Diagnostic rows carry whatever the validation selected — dates, decimals,
    # binary — and this crosses as JSON. Anything JSON cannot hold is rendered,
    # because these rows are read by a person and never compared or persisted.
    if value is None or isinstance(value, (str, bool, int, float)):
        return value
    return str(value)


try:
    report = weaver.test(
        {targets!r},
        workspace=workspace,
        name={name!r},
        file=None if path is None else str(path),
        dry_run={dry_run!r},
    )
except Exception as exc:
    emit({{
        "failed": True,
        "error_type": type(exc).__name__,
        "message": str(exc),
    }})
else:
    # Beside the mapping, never inside it. `to_mapping` excludes diagnostics
    # deliberately — it is what a task log and JSON output are built from — so
    # interactive evidence travels as its own field, keyed by node, and is
    # reattached on the other side.
    emit({{
        "failed": False,
        "report": report.to_mapping(),
        "diagnostics": {{
            node.logical_id: [
                {{key: _portable(value) for key, value in row.items()}}
                for row in (node.diagnostics or ())
            ]
            for node in report.nodes
            if node.diagnostics
        }},
    }})
"""


def _run_test_over_livy(workspace, *, targets, name, file, dry_run: bool):
    """Submit the run into Fabric, carrying a ``--file``'s *content*.

    The path is the developer's, and the session has never heard of it — so what
    crosses is the source, written down on the far side under the same filename.
    The filename matters: a validation's ID and its filename must agree, and the
    reader on the other side checks that exactly as a build does.
    """

    from pathlib import Path

    from weaver.errors import CommandError, ValidationError
    from weaver.fabric import LivySession
    from weaver.test_report import ValidationRunReport

    source = None
    filename = ""
    if file is not None:
        local = Path(file)
        if not local.exists():
            raise CommandError(f"no validation source at {local}")
        source = local.read_text(encoding="utf-8")
        filename = local.name

    body = _TEST_BODY.format(
        workspace=workspace.workspace,
        environment=workspace.environment,
        weaver_lakehouse=workspace.weaver_lakehouse,
        targets=list(targets),
        name=name,
        source=source,
        filename=filename,
        dry_run=dry_run,
    )
    with LivySession.for_workspace(workspace) as session:
        payload = session.run(body).payload
    if payload is None:
        raise CommandError(
            "the run happened in Fabric but returned nothing; see the Livy session output"
        )
    if payload.get("failed"):
        raise ValidationError(f"{payload['error_type']}: {payload['message']}")

    report = ValidationRunReport.from_mapping(payload["report"])
    return _with_diagnostics(report, payload.get("diagnostics") or {})


def _with_diagnostics(report, diagnostics: dict):
    """Reattach the evidence a targeted run produced on the other side.

    A report that crossed a boundary would otherwise carry counts alone, so
    ``--name`` would print rows when run inside Fabric and print none when run
    from a desktop against the same estate — the same command answering two
    different ways depending on where it happened to be typed.
    """

    from dataclasses import replace

    if not diagnostics:
        return report
    return replace(
        report,
        nodes=tuple(
            replace(node, diagnostics=tuple(diagnostics[node.logical_id]))
            if node.logical_id in diagnostics
            else node
            for node in report.nodes
        ),
    )


def _print_test(report) -> None:
    """One renderer, for a report produced here or one that crossed Livy."""

    print(f"test {report.status}\n")
    for node in report.nodes:
        result = node.result
        found = ""
        if result is not None and hasattr(result, "violation_count"):
            found = f"  ({result.violation_count} violation(s))"
        elif result is not None:
            found = (
                f"  ({result.missing_count} missing, "
                f"{result.unexpected_count} unexpected)"
            )
        print(f"  {node.status:<10} {node.kind:<11} {node.logical_id}{found}")
        for message in node.messages:
            print(f"      {message}")

    totals = report.totals()
    print(
        f"\n  {totals['passed']} passed, {totals['failed']} failed, "
        f"{totals['invalid']} could not run"
    )
    if report.task_log:
        print(f"  evidence: {report.task_log}")

    # The rows a targeted run asked for. Printed last, because the verdict is
    # what a reader wants first and the evidence is what they want next.
    for node in report.nodes:
        if not node.diagnostics:
            continue
        print(f"\n  {node.logical_id}:")
        for row in node.diagnostics:
            print(f"    {row}")


def handle_wipe(args: argparse.Namespace) -> int:
    """Preview, confirm, then invoke the same public wipe operation."""

    import json

    workspace = _resolve_workspace(args)
    planned = weaver.wipe(
        args.targets,
        workspace=workspace,
        unbind_from=args.unbind_from,
        dry_run=True,
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

    if total and not args.yes:
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

    result = weaver.wipe(
        args.targets,
        workspace=workspace,
        unbind_from=args.unbind_from,
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
    """Adapt command-line values to :func:`weaver.build`."""

    import json

    workspace = _resolve_workspace(args)
    result = weaver.build(
        args.repository,
        bind=args.item_bindings,
        workspace=workspace,
        bundle=args.bundle,
    )
    payload = result.to_mapping()
    if args.json:
        print(json.dumps(payload, indent=2))
    else:
        print(f"build {result.status}: workspace declaration")
        print(f"  bundle: {result.bundle_id}")
        if result.archive:
            print(f"  record: {result.archive}")
        print(f"  items:  {', '.join(result.items)}")
        if result.errors:
            for error in payload["errors"]:
                print(f"  failed: {error['id']}: {error['type']}: {error['message']}")
    return 0 if result.succeeded else 1


def handle_doctor(args: argparse.Namespace) -> int:
    """Report what a local build and load needs, and what is missing.

    None of it is required to use Weaver on Fabric. It matters for local
    development, where a missing JDK otherwise surfaces as a Java stack trace.
    """

    from weaver.diagnostics import check_local_spark, platform_summary

    report = check_local_spark()

    if args.json:
        import json

        print(json.dumps(report.as_dict(), indent=2))
        return 0 if report.ok else 1

    print(f"local Spark and Delta on {platform_summary()}\n")
    for check in report.checks:
        print(f"  {check}")
    if report.ok:
        print("\nReady. Run the local tests with:  pytest -m spark")
        return 0
    print()
    for hint in report.hints:
        print(f"  → {hint}")
    # A non-zero status keeps this usable as a gate in a script, but on its own
    # it reads as "your installation is broken" to someone who never wanted
    # local Spark. Say plainly that it is optional.
    print(
        "\nThis reports local Spark only. Weaver on Fabric needs none of it —\n"
        "wipe, install and capacity work without a JVM."
    )
    return 1


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
