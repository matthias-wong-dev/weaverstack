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
        "target",
        nargs="+",
        help="an item (Sales_LH) or a folder root (Sales_LH/Files/Extracts)",
    )
    _add_host_args(wipe)
    wipe.add_argument("--dry-run", action="store_true", help="report without removing")
    wipe.add_argument("--yes", action="store_true", help="do not ask for confirmation")
    wipe.set_defaults(handler=handle_wipe)

    return parser


def _add_host_args(parser: argparse.ArgumentParser) -> None:
    """Where the work happens. A host is named in a config, or given directly.

    The config is a convenience, so `--root` exists to build a local host
    without one — nothing should require a file to be expressible.
    """

    parser.add_argument("--host", help="a host named in the config file")
    parser.add_argument("--config", help="a hosts file, e.g. env.yml")
    parser.add_argument("--root", help="a local host root, instead of --host/--config")


def _resolve_host(args: argparse.Namespace):
    from weaver import LocalHost, load_hosts
    from weaver.errors import CommandError

    if args.root:
        if args.host or args.config:
            raise CommandError("--root builds a host directly; drop --host and --config")
        return LocalHost(root=args.root)

    if not args.config:
        raise CommandError("give --config with --host, or --root for a local host")
    if not args.host:
        raise CommandError("give --host to say which host in the config to use")

    hosts = load_hosts(args.config)
    if args.host not in hosts:
        known = ", ".join(sorted(hosts)) or "none"
        raise CommandError(f"no host {args.host!r} in {args.config} — found: {known}")
    return hosts[args.host]


def handle_wipe(args: argparse.Namespace) -> int:
    """Clear the named targets.

    A wipe removes everything in a target, not only what Weaver manages, so it
    asks before doing it unless told not to.
    """

    from weaver import wipe_selection

    host = _resolve_host(args)
    planned = wipe_selection(args.target, host, dry_run=True)

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

    for report in wipe_selection(args.target, host):
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
