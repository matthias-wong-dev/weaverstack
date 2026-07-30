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
        "build", help="build bound logical items from the workspace declaration"
    )
    build.add_argument(
        "--bind",
        dest="item_bindings",
        action="append",
        required=True,
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
        "push", help="validate and upload a complete authored repository"
    )
    push.add_argument("repository", help="local authored repository folder")
    push.add_argument("--json", action="store_true", help="emit the result as JSON")
    _add_workspace_args(push)
    push.set_defaults(handler=handle_push)

    initialise = subcommands.add_parser(
        "initialise", help="create or prepare the Weaver Lakehouse"
    )
    initialise.add_argument("--exists-ok", action="store_true")
    initialise.add_argument("--json", action="store_true", help="emit the result as JSON")
    _add_workspace_args(initialise)
    initialise.set_defaults(handler=handle_initialise)

    unbind = subcommands.add_parser(
        "unbind", help="remove catalogue state for named physical targets"
    )
    unbind.add_argument("--lakehouse", action="append", default=[])
    unbind.add_argument("--warehouse", action="append", default=[])
    unbind.add_argument("--json", action="store_true", help="emit the result as JSON")
    _add_workspace_args(unbind)
    unbind.set_defaults(handler=handle_unbind)

    wipe = subcommands.add_parser(
        "wipe", help="clear a physical Lakehouse or Warehouse"
    )
    wipe.add_argument(
        "--lakehouse",
        dest="lakehouses",
        action="append",
        default=[],
        metavar="NAME",
        help="a Lakehouse to clear completely; repeat for several",
    )
    wipe.add_argument(
        "--warehouse",
        dest="warehouses",
        action="append",
        default=[],
        metavar="NAME",
        help="a Fabric Warehouse to clear completely; repeat for several",
    )
    _add_workspace_args(wipe)
    wipe.add_argument("--dry-run", action="store_true", help="report without removing")
    wipe.add_argument("--yes", action="store_true", help="do not ask for confirmation")
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


def handle_install(args: argparse.Namespace) -> int:
    """Build Weaver from this checkout and install it into a Fabric Environment.

    The authoritative deployment path. Afterwards a notebook, Livy session or
    Fabric pytest run attached to the Environment can ``import weaver`` with no
    source shipped into a Lakehouse.
    """

    from weaver import FabricWorkspace
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

    from weaver import LocalWorkspace
    from weaver.store import LocalStore

    if isinstance(workspace, LocalWorkspace):
        return LocalStore()
    from weaver.fabric import OneLakeDfsClient

    return OneLakeDfsClient()


def _resolve_workspace(args: argparse.Namespace):
    from weaver import LocalWorkspace, resolve_workspace

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

    from weaver import Location, push_item_repository
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


def handle_initialise(args: argparse.Namespace) -> int:
    """Prepare the control Lakehouse and build its package-owned catalogue."""

    import json

    from weaver import FabricWorkspace, LocalWorkspace, prepare_weaver_lakehouse
    from weaver.errors import CommandError

    workspace = _resolve_workspace(args)
    if not workspace.weaver_lakehouse:
        raise CommandError(
            "initialise requires --weaver-lakehouse or a configured value"
        )
    if isinstance(workspace, FabricWorkspace) and not workspace.environment:
        raise CommandError(
            "Fabric initialise requires --environment or a configured environment"
        )
    prepared = prepare_weaver_lakehouse(workspace, exists_ok=args.exists_ok)
    if isinstance(workspace, LocalWorkspace):
        result = _run_local_initialise(workspace)
    elif isinstance(workspace, FabricWorkspace):
        result = _run_fabric_initialise(workspace)
    else:  # pragma: no cover - resolve_workspace returns the closed pair
        raise CommandError(f"unsupported Workspace type: {type(workspace).__name__}")
    payload = {
        "workspace": prepared.workspace,
        "weaver_lakehouse": prepared.weaver_lakehouse,
        "created": prepared.created,
        **result,
    }
    if args.json:
        print(json.dumps(payload, indent=2))
    else:
        state = "created" if prepared.created else "prepared"
        print(f"{state} Weaver Lakehouse {prepared.weaver_lakehouse}")
        print(f"  catalogue: {result['status']}")
        print(f"  bundle:    {result['bundle_id']}")
    return 0 if result["status"] == "succeeded" else 1


def _run_local_initialise(workspace) -> dict:
    from weaver import ItemRef, LocalStore, initialise_weaver_lakehouse
    from weaver.spark import local_delta_session

    with local_delta_session() as session:
        result = initialise_weaver_lakehouse(
            weaver_lakehouse=ItemRef(workspace.weaver_lakehouse),
            workspace=workspace,
            store=LocalStore(),
            spark=session,
        )
    return {"status": result.report.status, "bundle_id": result.bundle.plan.bundle_id}


def _run_fabric_initialise(workspace) -> dict:
    from weaver.fabric import LivySession

    body = (
        "from weaver import FabricWorkspace, ItemRef, initialise_weaver_lakehouse\n"
        "from weaver.resolution import resolver_for, store_for\n"
        f"workspace = FabricWorkspace(workspace={workspace.workspace!r}, "
        f"environment={workspace.environment!r}, "
        f"weaver_lakehouse={workspace.weaver_lakehouse!r})\n"
        "result = initialise_weaver_lakehouse(\n"
        "    weaver_lakehouse=ItemRef(workspace.weaver_lakehouse),\n"
        "    workspace=workspace, store=store_for(workspace), spark=spark)\n"
        "emit({'status': result.report.status, "
        "'bundle_id': result.bundle.plan.bundle_id})\n"
    )
    with LivySession.for_workspace(workspace) as session:
        return session.run(body).payload


def handle_unbind(args: argparse.Namespace) -> int:
    import json

    from weaver.errors import CommandError

    if not args.lakehouse and not args.warehouse:
        raise CommandError("give at least one --lakehouse or --warehouse to unbind")
    workspace = _resolve_workspace(args)
    if not workspace.weaver_lakehouse:
        raise CommandError("unbind requires a configured Weaver Lakehouse")
    result = _run_unbind(
        workspace, lakehouses=args.lakehouse, warehouses=args.warehouse
    )
    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print(f"unbound {len(result['logical_items'])} logical installation(s)")
        for target in result["targets"]:
            print(f"  {target}")
    return 0


def _run_unbind(workspace, *, lakehouses, warehouses) -> dict:
    from weaver import LocalWorkspace

    if isinstance(workspace, LocalWorkspace):
        from weaver import ItemRef, unbind_targets
        from weaver.resolution import resolver_for
        from weaver.spark import SparkCatalogue, local_delta_session

        resolver = resolver_for(workspace)
        with local_delta_session() as session:
            catalogue = SparkCatalogue(
                session,
                resolver.spark_destination(ItemRef(workspace.weaver_lakehouse)),
            )
            return unbind_targets(
                catalogue, lakehouses=lakehouses, warehouses=warehouses
            ).to_mapping()

    from weaver.fabric import LivySession

    body = (
        "from weaver import FabricWorkspace, ItemRef, unbind_targets\n"
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


def handle_wipe(args: argparse.Namespace) -> int:
    """Clear the named targets.

    A wipe removes everything in a target, not only what Weaver manages, so it
    asks before doing it unless told not to.
    """

    from weaver import (
        FabricWorkspace,
        ItemRef,
        WarehouseTarget,
        wipe_lakehouse,
        wipe_sql_target,
    )
    from weaver.errors import CommandError

    if not any((args.lakehouses, args.warehouses)):
        raise CommandError(
            "give at least one --lakehouse or --warehouse to wipe"
        )

    workspace = _resolve_workspace(args)
    if not workspace.weaver_lakehouse:
        raise CommandError("wipe requires a configured Weaver Lakehouse")
    lakehouses = tuple(ItemRef.parse(name) for name in args.lakehouses)
    warehouses = tuple(
        WarehouseTarget.parse(name) for name in args.warehouses
    )
    if warehouses and not isinstance(workspace, FabricWorkspace):
        raise CommandError(
            "Warehouse targets require a Fabric Workspace; the local emulator has no SQL"
        )

    store = _desktop_store(workspace) if lakehouses else None
    planned = []
    for lakehouse in lakehouses:
        planned.extend(
            wipe_lakehouse(lakehouse, workspace, store=store, dry_run=True)
        )

    print(f"wipe on {workspace.workspace}\n")
    for report in planned:
        print(f"  {report.target}")
        print(f"    {report.location}")
        for name in report.removed:
            print(f"      - {name}")
        if not report.removed:
            print("      (already empty)")
    for warehouse in warehouses:
        print(f"  warehouse:{warehouse}")
        print("    all user-created SQL objects")
    total = sum(report.count for report in planned) + len(warehouses)
    print()

    if args.dry_run:
        print(f"{total} item(s) would be removed. Nothing was changed.")
        return 0
    if total == 0:
        _run_unbind(
            workspace,
            lakehouses=[item.name for item in lakehouses],
            warehouses=[item.warehouse.name for item in warehouses],
        )
        print("Nothing to remove.")
        return 0

    if not args.yes:
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

    for lakehouse in lakehouses:
        for report in wipe_lakehouse(lakehouse, workspace, store=store):
            print(f"  {report}")
    if warehouses:
        from weaver.fabric import desktop_sql_executor

        for warehouse in warehouses:
            with desktop_sql_executor(warehouse, workspace) as sql:
                wipe_sql_target(warehouse, workspace, sql=sql)
            print(f"  warehouse:{warehouse}: wiped")
    _run_unbind(
        workspace,
        lakehouses=[item.name for item in lakehouses],
        warehouses=[item.warehouse.name for item in warehouses],
    )
    return 0


def handle_build(args: argparse.Namespace) -> int:
    """Adapt CLI strings and transport to the item-oriented core build."""

    import json

    from weaver import (
        FabricWorkspace,
        ItemBindings,
        effective_item_bindings,
        parse_item_binding,
    )
    from weaver.errors import CommandError

    workspace = _resolve_workspace(args)
    selected_bindings = ItemBindings(
        tuple(
            parse_item_binding(text, workspace=workspace)
            for text in args.item_bindings
        )
    )
    if not workspace.weaver_lakehouse:
        raise CommandError("build requires --weaver-lakehouse or a configured value")
    bindings = effective_item_bindings(
        selected_bindings, weaver_lakehouse=workspace.weaver_lakehouse
    )
    if isinstance(workspace, FabricWorkspace):
        if not workspace.environment:
            raise CommandError(
                "Fabric build requires --environment or a configured environment"
            )
        result = _run_fabric_item_build(
            workspace,
            bindings=bindings,
            bundle_name=args.bundle,
        )
    else:
        result = _run_local_item_build(
            workspace,
            bindings=bindings,
            bundle_name=args.bundle,
        )
    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print(f"build {result['status']}: workspace declaration")
        print(f"  bundle: {result['bundle_id']}")
        if result.get("archive"):
            print(f"  record: {result['archive']}")
        print(f"  items:  {', '.join(result['items'])}")
        if result["errors"]:
            for error in result["errors"]:
                print(f"  failed: {error['id']}: {error['type']}: {error['message']}")
    return 0 if result["status"] == "succeeded" else 1


def _run_local_item_build(workspace, *, bindings, bundle_name: str | None) -> dict:
    """Run generation and installation in-process and always close the session."""

    from weaver import ItemRef, LocalStore
    from weaver.build_bundle import (
        InstallationEnvironment,
        LakehouseBinding,
        build_uploaded_item_repository,
        timestamped_archive_name,
    )
    from weaver.errors import CommandError
    from weaver.resolution import resolver_for
    from weaver.spark import local_delta_session

    warehouse_items = [
        str(binding.item)
        for binding in bindings.entries
        if binding.item.item_type == "Warehouse"
    ]
    if warehouse_items:
        raise CommandError(
            "local Workspace builds cannot target Warehouses: "
            + ", ".join(warehouse_items)
        )

    store = LocalStore()
    resolver = resolver_for(workspace)
    record_name = bundle_name
    if record_name is not None:
        record_name = record_name or timestamped_archive_name()
        if not record_name.endswith(".weaver.zip"):
            record_name += ".weaver.zip"
    archive = resolver.build_bundle(record_name) if record_name else None
    control = LakehouseBinding(ItemRef(workspace.weaver_lakehouse))
    with local_delta_session() as session:
        result = build_uploaded_item_repository(
            resolver.weaver_items_root,
            bindings=bindings,
            environment=InstallationEnvironment(
                store=store, resolver=resolver, spark=session, workspace=workspace
            ),
            control_lakehouse=control,
            archive=archive,
        )
    report = result.report
    return {
        "source": "weaver_items",
        "items": [str(binding.item) for binding in bindings.entries],
        "bundle_id": result.bundle_id,
        "archive": result.archive.value if result.archive else None,
        "status": report.status,
        "errors": [
            {
                "id": action.action_id,
                "type": action.error_type,
                "message": action.error_message,
            }
            for action in report.action_results()
            if action.status == "failed"
        ],
    }


def _run_fabric_item_build(
    workspace,
    *,
    bindings,
    bundle_name: str | None,
) -> dict:
    """Run both build phases inside the workspace's Environment-backed session."""

    from weaver.errors import CommandError
    from weaver.fabric import LivySession, list_workspace_livy_sessions

    if not workspace.weaver_lakehouse:
        raise CommandError("a Fabric build workspace must name its weaver_lakehouse")
    try:
        active_sessions = list_workspace_livy_sessions(workspace, active_only=True)
    except WeaverError as exc:
        print(f"warning: could not inspect Fabric Spark sessions: {exc}", file=sys.stderr)
    else:
        _print_livy_preflight(active_sessions)
    binding_texts = []
    for binding in bindings.entries:
        target = binding.target
        physical = (
            target.lakehouse.name if hasattr(target, "lakehouse") else target.warehouse.name
        )
        physical_type = "Lakehouses" if hasattr(target, "lakehouse") else "Warehouses"
        binding_texts.append(f"{physical_type}/{physical}={binding.item}")

    workspace_literal = (
        f"FabricWorkspace(workspace={workspace.workspace!r}, "
        f"weaver_lakehouse={workspace.weaver_lakehouse!r}, "
        f"environment={workspace.environment!r})"
    )
    body = (
        "from weaver import (FabricWorkspace, ItemRef, "
        "build_uploaded_item_repository, timestamped_archive_name)\n"
        "from weaver.build_bundle import (InstallationEnvironment, ItemBindings, "
        "LakehouseBinding, parse_item_binding)\n"
        "from weaver.resolution import resolver_for, store_for\n"
        f"workspace = {workspace_literal}\n"
        "store = store_for(workspace)\n"
        "resolver = resolver_for(workspace)\n"
        f"bindings = ItemBindings(tuple(parse_item_binding(text) for text in {binding_texts!r}))\n"
        "control = LakehouseBinding(ItemRef(workspace.weaver_lakehouse))\n"
        f"record_name = {bundle_name!r}\n"
        "if record_name is not None:\n"
        "    record_name = record_name or timestamped_archive_name()\n"
        "    if not record_name.endswith('.weaver.zip'):\n"
        "        record_name += '.weaver.zip'\n"
        "archive = resolver.build_bundle(record_name) if record_name else None\n"
        "environment = InstallationEnvironment(\n"
        "    store=store, resolver=resolver, spark=spark, workspace=workspace)\n"
        "result = build_uploaded_item_repository(\n"
        "    resolver.weaver_items_root,\n"
        "    bindings=bindings, environment=environment,\n"
        "    control_lakehouse=control,\n"
        "    archive=archive)\n"
        "report = result.report\n"
        "emit({\n"
        "    'source': 'weaver_items',\n"
        "    'items': [str(binding.item) for binding in bindings.entries],\n"
        "    'bundle_id': result.bundle_id,\n"
        "    'archive': result.archive.value if result.archive else None,\n"
        "    'status': report.status,\n"
        "    'errors': [\n"
        "        {'id': action.action_id, 'type': action.error_type, "
        "         'message': action.error_message}\n"
        "        for action in report.action_results() if action.status == 'failed'],\n"
        "})\n"
    )
    with LivySession.for_workspace(workspace) as session:
        return session.run(body).payload


def _print_livy_preflight(active_sessions) -> None:
    if not active_sessions:
        print("Fabric Spark preflight: no active or queued sessions.", file=sys.stderr)
        return
    print("Fabric Spark preflight: active or queued sessions:", file=sys.stderr)
    for entry in active_sessions:
        session = entry.session
        states = "/".join(
            state or "-"
            for state in (session.scheduler_state, session.plugin_state, session.livy_state)
        )
        print(
            f"  {entry.lakehouse_name}: session {session.id or '?'} "
            f"({states})"
            + (f"; submitted by {session.submitter_name}" if session.submitter_name else ""),
            file=sys.stderr,
        )


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
