"""Provision the persistent Fabric estate used by the Weaver test suite.

Creates missing items and reuses existing ones. It never deletes, empties,
updates, or otherwise modifies an existing item.

Run from the repository root:

    python tests/fabric/provision_estate.py
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from typing import Any, Callable

from weaver.fabric import (
    LAKEHOUSE,
    WAREHOUSE,
    FabricClient,
    create_lakehouse,
    create_warehouse,
    find_item,
    find_workspace,
)
from weaver.fabric.auth import prefer_cli_credential

DEFAULT_WORKSPACE = "PYTEST_WORKSPACE"


@dataclass(frozen=True)
class RequiredItem:
    role: str
    default_name: str
    item_type: str
    create: Callable[..., Any]


REQUIRED_ITEMS = (
    RequiredItem(
        role="weaver",
        default_name="PYTEST_WEAVER",
        item_type=LAKEHOUSE,
        create=create_lakehouse,
    ),
    RequiredItem(
        role="target",
        default_name="PYTEST_LH_1",
        item_type=LAKEHOUSE,
        create=create_lakehouse,
    ),
    RequiredItem(
        role="producer",
        default_name="PYTEST_LH_2",
        item_type=LAKEHOUSE,
        create=create_lakehouse,
    ),
    RequiredItem(
        role="consumer",
        default_name="PYTEST_LH_3",
        item_type=LAKEHOUSE,
        create=create_lakehouse,
    ),
    RequiredItem(
        role="warehouse_producer",
        default_name="PYTEST_HOUSE",
        item_type=LAKEHOUSE,
        create=create_lakehouse,
    ),
    RequiredItem(
        role="warehouse",
        default_name="PYTEST_WH_1",
        item_type=WAREHOUSE,
        create=create_warehouse,
    ),
)


def configured_item_name(item: RequiredItem) -> str:
    variable = f"WEAVER_PYTEST_{item.role.upper()}"
    return os.environ.get(variable, item.default_name)


def find_existing(
    *,
    workspace: Any,
    client: FabricClient,
    name: str,
    item_type: str,
) -> Any | None:
    try:
        return find_item(
            workspace,
            name,
            item_type=item_type,
            client=client,
        )
    except Exception:
        return None


def provision_item(
    *,
    workspace: Any,
    client: FabricClient,
    specification: RequiredItem,
) -> tuple[Any, bool]:
    name = configured_item_name(specification)

    existing = find_existing(
        workspace=workspace,
        client=client,
        name=name,
        item_type=specification.item_type,
    )
    if existing is not None:
        return existing, False

    created = specification.create(
        workspace,
        name,
        client=client,
    )
    return created, True


def main() -> int:
    prefer_cli_credential()

    workspace_name = os.environ.get(
        "WEAVER_FABRIC_WORKSPACE",
        DEFAULT_WORKSPACE,
    )

    client = FabricClient()

    try:
        workspace = find_workspace(workspace_name)
    except Exception as exc:
        print(
            f"Could not resolve workspace {workspace_name!r}: "
            f"{type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return 1

    print(f"Workspace: {workspace.name} ({workspace.id})")

    failures: list[str] = []

    for specification in REQUIRED_ITEMS:
        name = configured_item_name(specification)

        try:
            item, created = provision_item(
                workspace=workspace,
                client=client,
                specification=specification,
            )
        except Exception as exc:
            message = f"{specification.item_type} {name}: {type(exc).__name__}: {exc}"
            failures.append(message)
            print(f"FAILED   {message}", file=sys.stderr)
            continue

        status = "CREATED" if created else "EXISTS "
        print(f"{status}  {specification.item_type:<10} {item.name} ({item.id})")

    if failures:
        print(
            f"\nProvisioning failed for {len(failures)} item(s).",
            file=sys.stderr,
        )
        return 1

    print("\nPytest Fabric estate is ready.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
