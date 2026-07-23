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
    # Subcommands are added at their checkpoints. Not required yet, so that
    # a bare `weaver` prints help rather than failing.
    parser.add_subparsers(dest="command", metavar="command")
    return parser


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
