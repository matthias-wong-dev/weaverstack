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

    wipe = subcommands.add_parser(
        "wipe", help="clear a Lakehouse, a Warehouse, or one folder root"
    )
    wipe.add_argument(
        "--lakehouse-target",
        "--lakehouse_target",
        dest="lakehouse_targets",
        action="append",
        default=[],
        metavar="NAME",
        help="a Lakehouse to clear completely; repeat for several",
    )
    wipe.add_argument(
        "--warehouse-target",
        "--warehouse_target",
        dest="warehouse_targets",
        action="append",
        default=[],
        metavar="NAME",
        help="a Fabric Warehouse to clear completely; repeat for several",
    )
    wipe.add_argument(
        "--folder-target",
        "--folder_target",
        dest="folder_targets",
        action="append",
        default=[],
        metavar="PATH",
        help="a Lakehouse Files root to clear; repeat for several",
    )
    _add_host_args(wipe)
    wipe.add_argument("--dry-run", action="store_true", help="report without removing")
    wipe.add_argument("--yes", action="store_true", help="do not ask for confirmation")
    wipe.set_defaults(handler=handle_wipe)

    install = subcommands.add_parser(
        "install",
        help="build Weaver and install it into a Fabric Environment",
    )
    install.add_argument("--workspace", required=True, help="the Fabric workspace display name")
    install.add_argument(
        "--environment",
        required=True,
        dest="environment_name",
        metavar="NAME",
        help="the Environment to create or reuse, e.g. weaver",
    )
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

    _prefer_desktop_credential()
    from weaver.fabric import install as run_install

    started = time.perf_counter()
    result = run_install(
        args.workspace,
        args.environment_name,
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


def _add_host_args(parser: argparse.ArgumentParser) -> None:
    """Where the work happens. A host is named in a config, or given directly.

    The config is a convenience, so `--root` exists to build a local host
    without one — nothing should require a file to be expressible.
    """

    parser.add_argument("--host", help="a host named in the config file")
    parser.add_argument(
        "--hosts", dest="hosts_file", help="a hosts file, e.g. env.yml"
    )
    parser.add_argument("--root", help="a local host root, instead of --host/--hosts")


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


def _desktop_store(host):
    """The store a desktop command uses to reach a host.

    Local is within-host; Fabric is cross-boundary, so the CLI constructs the
    OneLakeDfsClient here — core never turns a FabricHost into a DFS client.
    """

    from weaver import LocalHost
    from weaver.store import LocalStore

    if isinstance(host, LocalHost):
        return LocalStore()
    from weaver.fabric import OneLakeDfsClient

    return OneLakeDfsClient()


def _resolve_host(args: argparse.Namespace):
    from weaver import LocalHost, load_hosts
    from weaver.errors import CommandError

    if args.root:
        if args.host or args.hosts_file:
            raise CommandError("--root builds a host directly; drop --host and --hosts")
        return LocalHost(root=args.root)

    if not args.hosts_file:
        raise CommandError("give --hosts with --host, or --root for a local host")
    if not args.host:
        raise CommandError("give --host to say which host in the config to use")

    hosts = load_hosts(args.hosts_file)
    if args.host not in hosts:
        known = ", ".join(sorted(hosts)) or "none"
        raise CommandError(f"no host {args.host!r} in {args.hosts_file} — found: {known}")

    host = hosts[args.host]
    from weaver import LocalHost as _LocalHost

    if not isinstance(host, _LocalHost):
        _prefer_desktop_credential()
    return host


def handle_wipe(args: argparse.Namespace) -> int:
    """Clear the named targets.

    A wipe removes everything in a target, not only what Weaver manages, so it
    asks before doing it unless told not to.
    """

    from weaver import (
        FabricHost,
        FolderTarget,
        ItemRef,
        WarehouseTarget,
        wipe_folder_target,
        wipe_lakehouse,
        wipe_sql_target,
    )
    from weaver.errors import CommandError

    if not any(
        (args.lakehouse_targets, args.warehouse_targets, args.folder_targets)
    ):
        raise CommandError(
            "give at least one --lakehouse-target, --warehouse-target, "
            "or --folder-target to wipe"
        )

    host = _resolve_host(args)
    lakehouses = tuple(ItemRef.parse(name) for name in args.lakehouse_targets)
    warehouses = tuple(
        WarehouseTarget.parse(name) for name in args.warehouse_targets
    )
    folders = tuple(FolderTarget.parse(path) for path in args.folder_targets)

    if warehouses and not isinstance(host, FabricHost):
        raise CommandError(
            "Warehouse targets require a Fabric host; a local root has no SQL"
        )

    store = _desktop_store(host) if lakehouses or folders else None
    planned = []
    for lakehouse in lakehouses:
        planned.extend(
            wipe_lakehouse(lakehouse, host, store=store, dry_run=True)
        )
    for folder in folders:
        planned.append(
            wipe_folder_target(folder, host, store=store, dry_run=True)
        )

    print(f"wipe on {host.alias or host.__class__.__name__}\n")
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
        for report in wipe_lakehouse(lakehouse, host, store=store):
            print(f"  {report}")
    for folder in folders:
        print(f"  {wipe_folder_target(folder, host, store=store)}")
    if warehouses:
        from weaver.fabric import desktop_sql_executor

        for warehouse in warehouses:
            with desktop_sql_executor(warehouse, host) as sql:
                wipe_sql_target(warehouse, host, sql=sql)
            print(f"  warehouse:{warehouse}: wiped")
    return 0


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
