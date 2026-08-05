"""Provision the permanent Weaver Fabric pytest estate.

Creates any missing Lakehouses and Warehouses in PYTEST_WORKSPACE.
Existing items are reused. Nothing is deleted.

Usage:
    python scripts/provision_pytest_estate.py

Optional environment variables:
    WEAVER_FABRIC_WORKSPACE
    WEAVER_PYTEST_WEAVER
    WEAVER_PYTEST_TARGET
    WEAVER_PYTEST_PRODUCER
    WEAVER_PYTEST_CONSUMER
    WEAVER_PYTEST_WAREHOUSE_PRODUCER
    WEAVER_PYTEST_WAREHOUSE
"""

from __future__ import annotations

import os
import sys
from collections.abc import Callable
from typing import Any

from weaver.fabric import (
    FabricClient,
    LAKEHOUSE,
    WAREHOUSE,
    create_lakehouse,
    create_warehouse,
    find_item,
    find_workspace,
)
from weaver.fabric.auth import prefer_cli_credential


DEFAULT_WORKSPACE = "PYTEST_WORKSPACE"

LAKEHOUSE_ROLES = {
    "weaver": "PYTEST_WEAVER",
    "target": "PYTEST_LH_1",
    "producer": "PYTEST_LH_2",
    "consumer": "PYTEST_LH_3",
    "warehouse_producer": "PYTEST_HOUSE",
}

WAREHOUSE_ROLES = {
    "warehouse": "PYTEST_WH_1",
}


def configured_name(role: str, default: str) -> str:
    variable = f"WEAVER_PYTEST_{role.upper()}"
    return os.environ.get(variable, default)


def find_or_create(
    *,
    workspace: Any,
    client: FabricClient,
    name: str,
    item_type: str,
    create: Callable[..., Any],
) -> tuple[Any, bool]:
    try:
        item = find_item(
            workspace,
            name,
            item_type=item_type,
            client=client,
        )
        return item, False
    except Exception:
        item = create(
            workspace,
            name,
            client=client,
        )
        return item, True


def main() -> int:
    prefer_cli_credential()

    workspace_name = os.environ.get(
        "WEAVER_FABRIC_WORKSPACE",
        DEFAULT_WORKSPACE,
    )

    client = FabricClient()

    print(f"Resolving workspace: {workspace_name}")
    workspace = find_workspace(workspace_name)

    print(f"Workspace resolved: {workspace.name} ({workspace.id})")
    print()

    failures: list[str] = []

    for role, default_name in LAKEHOUSE_ROLES.items():
        name = configured_name(role, default_name)

        try:
            item, created = find_or_create(
                workspace=workspace,
                client=client,
                name=name,
                item_type=LAKEHOUSE,
                create=create_lakehouse,
            )
        except Exception as exc:
            failures.append(
                f"Lakehouse {name}: {type(exc).__name__}: {exc}"
            )
            print(f"FAILED  Lakehouse  {name}: {exc}")
            continue

        action = "CREATED" if created else "EXISTS "
        print(f"{action}  Lakehouse  {item.name} ({item.id})")

    for role, default_name in WAREHOUSE_ROLES.items():
        name = configured_name(role, default_name)

        try:
            item, created = find_or_create(
                workspace=workspace,
                client=client,
                name=name,
                item_type=WAREHOUSE,
                create=create_warehouse,
            )
        except Exception as exc:
            failures.append(
                f"Warehouse {name}: {type(exc).__name__}: {exc}"
            )
            print(f"FAILED  Warehouse  {name}: {exc}")
            continue

        action = "CREATED" if created else "EXISTS "
        print(f"{action}  Warehouse  {item.name} ({item.id})")

    print()

    if failures:
        print("Provisioning completed with failures:", file=sys.stderr)
        for failure in failures:
            print(f"  - {failure}", file=sys.stderr)
        return 1

    print("Pytest Fabric estate is provisioned.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())