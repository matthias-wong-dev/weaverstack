"""The whole desktop lifecycle, driven from Python instead of the CLI.

The same four operations `weaver compose dev` runs, in the same order, against
the same estate — written as a script because the Python API and the CLI are
the same surface. The CLI parses arguments and prints; everything it then does
is what this file does directly.

.. code-block:: bash

    python examples/lifecycle.py --workspace "Weaver Example" \\
        --catalogue Weaver --environment weaver \\
        --lakehouse Sales --warehouse Reporting

One Session is opened and passed to each operation, which is the point worth
demonstrating: a Session holds the credential, the resolved items, the Livy
session and the Warehouse connections, so four operations pay for them once. A
script that called each operation without one would work and would open four.

``ConsoleSession`` is what a desktop opens today. The public ``weaver.session()``
callable that replaces this import is a later milestone of the Fabric-only
refactor; nothing else in this file changes when it lands.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import weaver
from weaver.config import resolve_workspace
from weaver.fabric.auth import prefer_cli_credential
from weaver.session import ConsoleSession

#: The repository this example builds, relative to the repository root.
ESTATE = (
    Path(__file__).resolve().parent
    / "workspaces"
    / "Weaver Example"
    / "Sales-Estate.Notebook"
    / "Resources"
    / "builtin"
    / "repository"
)


def arguments(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--workspace", required=True, help="Fabric workspace name.")
    parser.add_argument(
        "--catalogue",
        required=True,
        help="The Weaver Lakehouse holding the catalogue.",
    )
    parser.add_argument(
        "--environment", required=True, help="The Fabric Environment for Spark work."
    )
    parser.add_argument(
        "--lakehouse", required=True, help="Physical Lakehouse for Lakehouse/Sales."
    )
    parser.add_argument(
        "--warehouse",
        required=True,
        help="Physical Warehouse for Warehouse/Reporting.",
    )
    parser.add_argument(
        "--keep",
        action="store_true",
        help="Skip the opening wipe and build onto what is already there.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    options = arguments(argv)
    lakehouse = f"Lakehouse/{options.lakehouse}"
    warehouse = f"Warehouse/{options.warehouse}"

    # Credential choice is a caller's policy, not the core's: a desktop script
    # prefers the Azure CLI sign-in, exactly as `weaver` itself does.
    prefer_cli_credential()
    workspace = resolve_workspace(
        workspace=options.workspace,
        weaver_lakehouse=options.catalogue,
        environment=options.environment,
    )

    with ConsoleSession(workspace=workspace) as session:
        # Everything from empty, unless the caller asked to keep what is there.
        # The control Lakehouse goes too: wipe skips the catalogue unbind
        # entirely when the catalogue itself is going, because deleting rows
        # from tables that are about to be removed is work nobody needs.
        if not options.keep:
            wiped = weaver.wipe(
                [lakehouse, warehouse, f"Lakehouse/{options.catalogue}"],
                session=session,
            )
            print(f"wiped {wiped.count} object(s)")

        built = weaver.build(
            str(ESTATE),
            bind=[
                f"{lakehouse}=Lakehouse/Sales",
                f"{warehouse}=Warehouse/Reporting",
            ],
            session=session,
        )
        print(f"build {built.status}: bundle {built.bundle_id}")
        if built.status != "succeeded":
            for failure in built.errors:
                print(f"  {failure.action_id}: {failure.error_message}")
            return 1

        loaded = weaver.load([lakehouse, warehouse], session=session)
        print(f"load {'succeeded' if loaded.succeeded else 'failed'}")

        tested = weaver.test([lakehouse, warehouse], session=session)
        totals = tested.totals()
        print(
            f"test {tested.status}: {totals['passed']} passed, "
            f"{totals['failed']} failed, {totals['invalid']} could not run"
        )

    return 0 if loaded.succeeded and tested.status != "failed" else 1


if __name__ == "__main__":
    sys.exit(main())
