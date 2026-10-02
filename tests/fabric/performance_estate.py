"""Fixed items the performance qualification builds, wipes and mirrors into.

Separate from the suite's items so a 1,000-object estate never meets a test's
fixture state. Each name is overridable by ``WEAVER_PYTEST_<ROLE>``.

Usage, to create whichever are missing and nothing else:
    python -m tests.fabric.performance_estate
"""

from __future__ import annotations

import os

LAKEHOUSE_ROLES = {
    "perf_lakehouse_0": "PYTEST_PERF_LH_0",
    "perf_lakehouse_1": "PYTEST_PERF_LH_1",
    "perf_lakehouse_mirror_0": "PYTEST_PERF_LH_MIRROR_0",
    "perf_lakehouse_mirror_1": "PYTEST_PERF_LH_MIRROR_1",
}

WAREHOUSE_ROLES = {
    "perf_weaver": "PYTEST_PERF_WEAVER",
    "perf_weaver_fork": "PYTEST_PERF_FORK",
    "perf_warehouse_0": "PYTEST_PERF_WH_0",
    "perf_warehouse_1": "PYTEST_PERF_WH_1",
    "perf_warehouse_mirror_0": "PYTEST_PERF_WH_MIRROR_0",
    "perf_warehouse_mirror_1": "PYTEST_PERF_WH_MIRROR_1",
}


def name(role: str) -> str:
    roles = {**LAKEHOUSE_ROLES, **WAREHOUSE_ROLES}
    return os.environ.get(f"WEAVER_PYTEST_{role.upper()}", roles[role])


def provision(workspace_name: str = "PYTEST_WORKSPACE") -> None:
    from weaver.fabric import (
        LAKEHOUSE,
        WAREHOUSE,
        FabricClient,
        create_lakehouse,
        create_warehouse,
        find_item,
        find_workspace,
    )
    from weaver.fabric.auth import desktop_credential, use_credential

    use_credential(desktop_credential())
    client = FabricClient()
    workspace = find_workspace(
        os.environ.get("WEAVER_FABRIC_WORKSPACE", workspace_name), client=client
    )
    for roles, item_type, create in (
        (LAKEHOUSE_ROLES, LAKEHOUSE, create_lakehouse),
        (WAREHOUSE_ROLES, WAREHOUSE, create_warehouse),
    ):
        for role in roles:
            wanted = name(role)
            try:
                item = find_item(workspace, wanted, item_type=item_type, client=client)
                print(f"EXISTS   {item_type:<10} {item.name}")
            except Exception:
                item = create(workspace, wanted, client=client)
                print(f"CREATED  {item_type:<10} {item.name}")


if __name__ == "__main__":
    provision()
