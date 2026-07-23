"""Top-level CLI routing.

Commands are registered here as the core APIs they wrap become available. The
handler contract is the one convention worth keeping from the old repository:
a command function returns a plain serialisable structure and the CLI prints
it.
"""

from __future__ import annotations

import argparse
import sys

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
        "--target",
        action="append",
        default=[],
        metavar="NAME",
        help=(
            "an item (Sales_LH) or a folder root (Sales_LH/Files/Extracts). "
            "Repeat the flag for several: --target A --target B"
        ),
    )
    _add_host_args(wipe)
    wipe.add_argument("--dry-run", action="store_true", help="report without removing")
    wipe.add_argument("--yes", action="store_true", help="do not ask for confirmation")
    wipe.set_defaults(handler=handle_wipe)

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

    from weaver import wipe_selection

    from weaver.errors import CommandError

    if not args.target:
        raise CommandError("give at least one --target to wipe")

    host = _resolve_host(args)
    store = _desktop_store(host)
    planned = wipe_selection(args.target, host, store=store, dry_run=True)

    print(f"wipe on {host.alias or host.__class__.__name__}\n")
    for report in planned:
        print(f"  {report.target}")
        print(f"    {report.location}")
        for name in report.removed:
            print(f"      - {name}")
        if not report.removed:
            print("      (already empty)")
    total = sum(report.count for report in planned)
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

    for report in wipe_selection(args.target, host, store=store):
        print(f"  {report}")
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
