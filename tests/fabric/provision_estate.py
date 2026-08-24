"""Provision the permanent Weaver Fabric pytest estate.

Creates any missing Lakehouses and Warehouses in PYTEST_WORKSPACE, and the
external estate in PYTEST_WORKSPACE_EXT that shortcut tests point at. Existing
items are reused and existing external contents are left as they are. Nothing is
deleted.

The external workspace is not a Weaver target workspace. It holds a Lakehouse and
a Warehouse whose contents a test may reference, seeded here rather than by the
suite. Its `Reference` schema is never mutated; its `Source` schema is what the
acceptance journey mutates and restores.

Usage:
    python -m tests.fabric.provision_estate

Optional environment variables:
    WEAVER_FABRIC_WORKSPACE
    WEAVER_FABRIC_WORKSPACE_EXT
    WEAVER_PYTEST_WEAVER
    WEAVER_PYTEST_TARGET
    WEAVER_PYTEST_PRODUCER
    WEAVER_PYTEST_CONSUMER
    WEAVER_PYTEST_WAREHOUSE_PRODUCER
    WEAVER_PYTEST_WAREHOUSE
    WEAVER_PYTEST_EXTERNAL
    WEAVER_PYTEST_EXTERNAL_WAREHOUSE
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from collections.abc import Callable
from typing import Any

from support import external_estate

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

LAKEHOUSE_ROLES = {
    # Repositories and bundles need OneLake and the catalogue Warehouse has
    # none, so the suite stages them here and never builds into it.
    "staging": "PYTEST_STAGING",
    "target": "PYTEST_LH_1",
    "producer": "PYTEST_LH_2",
    "consumer": "PYTEST_LH_3",
    "warehouse_producer": "PYTEST_HOUSE",
}

WAREHOUSE_ROLES = {
    # Where the Weaver catalogue lives. A Warehouse: catalogue state is read and
    # written over TDS.
    "weaver": "PYTEST_WEAVER",
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


def external_tables_seeded(workspace_id: str, item_id: str) -> bool:
    """Whether the external estate's tables are already there.

    Checked rather than rewritten, so the rows a test asserts on stay the rows
    an earlier run put there.
    """

    from weaver.fabric import OneLakeDfsClient
    from weaver.fabric.onelake import onelake_url
    from weaver.locations import Location

    store = OneLakeDfsClient()
    paths = [external_estate.table_path(table) for table in external_estate.TABLES]
    paths += [
        external_estate.mutable_table_path(table)
        for table in external_estate.MUTABLE_TABLES
    ]
    return all(
        store.exists(Location(onelake_url(workspace_id, item_id, path)))
        for path in paths
    )


def seed_external_tables(workspace, item, *, host_workspace) -> None:
    """Write the external Delta tables, through Spark in the host workspace.

    Spark reaches another workspace by ``abfss://`` path, so the external
    workspace needs no Environment and no session of its own. The body imports
    nothing: making a Delta table needs a session, not the installed package.
    """

    from support import external_seed

    from weaver.fabric import LivySession
    from weaver.fabric.onelake import abfss_root
    from weaver.targets import ItemRef

    body = external_seed.lakehouse_seed_program(abfss_root(workspace.id, item.id))
    session = LivySession.for_workspace(
        host_workspace, lakehouse=ItemRef(configured_name("staging", "PYTEST_STAGING"))
    )
    with session:
        result = session.run(body)
    if result.payload is not True:
        raise RuntimeError(f"seeding the external tables returned {result.payload!r}")


def seed_external_events(workspace_id: str, item_id: str) -> int:
    """Put the baseline event files in place. Returns how many were written."""

    from weaver.fabric import OneLakeDfsClient
    from weaver.fabric.onelake import onelake_url
    from weaver.locations import Location

    store = OneLakeDfsClient()
    written = 0
    for name, content in external_estate.EVENT_FILES.items():
        location = Location(
            onelake_url(workspace_id, item_id, external_estate.events_path(name))
        )
        if store.exists(location) and store.read(location) == content:
            continue
        store.write(location, content)
        written += 1
    return written


def seed_external_warehouse(workspace_name: str, name: str) -> None:
    """Create the external Warehouse's tables and write their baseline rows."""

    from support import external_seed

    from weaver.fabric import desktop_sql_executor
    from weaver.targets import ItemRef, WarehouseTarget
    from weaver.workspaces import Workspace

    executor = desktop_sql_executor(
        WarehouseTarget(ItemRef(name)),
        Workspace(workspace=workspace_name, lakehouses={}),
    )
    try:
        executor.execute_script(external_seed.warehouse_ddl())
        executor.execute_script(external_seed.warehouse_baseline())
    finally:
        executor.close()


def seed_external_file(workspace_id: str, item_id: str) -> bool:
    """Put the sentinel file in place if it is not already there."""

    from weaver.fabric import OneLakeDfsClient
    from weaver.fabric.onelake import onelake_url
    from weaver.locations import Location

    store = OneLakeDfsClient()
    location = Location(
        onelake_url(
            workspace_id,
            item_id,
            f"Files/{external_estate.SCHEMA}/{external_estate.FILE}",
        )
    )
    if store.exists(location) and store.read(location) == external_estate.FILE_BYTES:
        return False
    store.write(location, external_estate.FILE_BYTES)
    return True


def provision_external(client: FabricClient, host_workspace) -> list[str]:
    """Create and seed the external workspace's estate. Returns any failures."""

    failures: list[str] = []
    workspace_name = os.environ.get(
        external_estate.WORKSPACE_ENV, external_estate.DEFAULT_WORKSPACE
    )
    print(f"Resolving external workspace: {workspace_name}")
    try:
        workspace = find_workspace(workspace_name)
    except Exception as exc:
        return [f"external workspace {workspace_name}: {type(exc).__name__}: {exc}"]
    print(f"External workspace resolved: {workspace.name} ({workspace.id})")

    for role, default_name in external_estate.ROLES.items():
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
            failures.append(f"Lakehouse {name}: {type(exc).__name__}: {exc}")
            print(f"FAILED  Lakehouse  {name}: {exc}")
            continue

        action = "CREATED" if created else "EXISTS "
        print(f"{action}  Lakehouse  {item.name} ({item.id})  [external]")

        try:
            if external_tables_seeded(workspace.id, item.id):
                print("EXISTS     tables     Reference.* and Source.*")
            else:
                seed_external_tables(workspace, item, host_workspace=host_workspace)
                print("SEEDED     tables     Reference.* and Source.*")
            if seed_external_file(workspace.id, item.id):
                print(f"SEEDED     file       {external_estate.file_path()}")
            else:
                print(f"EXISTS     file       {external_estate.file_path()}")
            written = seed_external_events(workspace.id, item.id)
            action = (
                f"SEEDED     events     {written} file(s)"
                if written
                else ("EXISTS     events     baseline")
            )
            print(action)
        except Exception as exc:
            failures.append(f"external contents: {type(exc).__name__}: {exc}")
            print(f"FAILED  contents: {exc}")

    for role, default_name in external_estate.WAREHOUSE_ROLES.items():
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
            failures.append(f"Warehouse {name}: {type(exc).__name__}: {exc}")
            print(f"FAILED  Warehouse  {name}: {exc}")
            continue

        action = "CREATED" if created else "EXISTS "
        print(f"{action}  Warehouse  {item.name} ({item.id})  [external]")

        try:
            seed_external_warehouse(workspace.name, item.name)
            print("SEEDED     tables     Reference.Region and Source.Transaction")
        except Exception as exc:
            failures.append(f"external Warehouse: {type(exc).__name__}: {exc}")
            print(f"FAILED  external Warehouse contents: {exc}")

    return failures


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
            failures.append(f"Lakehouse {name}: {type(exc).__name__}: {exc}")
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
            failures.append(f"Warehouse {name}: {type(exc).__name__}: {exc}")
            print(f"FAILED  Warehouse  {name}: {exc}")
            continue

        action = "CREATED" if created else "EXISTS "
        print(f"{action}  Warehouse  {item.name} ({item.id})")

    from weaver.workspaces import Workspace

    print()
    failures.extend(
        provision_external(
            client,
            Workspace(
                workspace=workspace.name,
                lakehouses={},
            ),
        )
    )

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
