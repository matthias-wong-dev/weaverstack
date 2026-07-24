"""The complete desktop-populate → in-Fabric wipe → desktop-verify path."""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from sql_support import CatalogObject, populate_warehouse, system_schemas, user_objects

pytestmark = pytest.mark.fabric

FIXTURE = (
    Path(__file__).resolve().parents[1]
    / "fixtures"
    / "sql"
    / "warehouse_wipe_fixture.sql"
)

EXPECTED_OBJECTS = {
    CatalogObject("TestA", "Parent", "U"),
    CatalogObject("TestA", "Child", "U"),
    CatalogObject("TestA", "ParentView", "V"),
    CatalogObject("TestA", "RefreshParent", "P"),
    CatalogObject("TestB", "Independent", "U"),
    CatalogObject("TestB", "CrossSchemaView", "V"),
}


def test_installed_weaver_wipes_a_desktop_populated_warehouse(
    disposable_warehouse,
    fabric_client,
    fabric_workspace,
    livy_session,
):
    """Exercise installed Weaver, not a duplicate test-side SQL implementation."""

    from weaver.fabric import WAREHOUSE, find_item

    warehouse = disposable_warehouse

    started = time.monotonic()
    populate_warehouse(warehouse.executor, FIXTURE)
    warehouse.timings["fixture population"] = time.monotonic() - started
    before = user_objects(warehouse.executor)
    assert before == EXPECTED_OBJECTS
    print(
        f"Warehouse {warehouse.item.name} fixture population: "
        f"{warehouse.timings['fixture population']:.2f}s; "
        f"{len(before)} fixture objects present before wipe"
    )

    body = (
        "from weaver import FabricHost, WarehouseTarget, wipe_sql_target\n"
        f"host = FabricHost(workspace={warehouse.host.workspace!r}, "
        f"weaver_lakehouse={warehouse.host.weaver_lakehouse!r}, "
        f"fabric_environment={warehouse.host.fabric_environment!r})\n"
        f"target = WarehouseTarget.parse({warehouse.target.warehouse.name!r})\n"
        "result = wipe_sql_target(target, host)\n"
        "emit({'completed': result is None})\n"
    )
    started = time.monotonic()
    result = livy_session.run(body)
    warehouse.timings["Fabric wipe execution"] = time.monotonic() - started
    assert result.payload == {"completed": True}
    print(
        f"Warehouse {warehouse.item.name} Fabric wipe execution: "
        f"{warehouse.timings['Fabric wipe execution']:.2f}s"
    )

    after = user_objects(warehouse.executor)
    assert after == set()
    assert {"dbo", "guest", "information_schema", "sys"} <= system_schemas(
        warehouse.executor
    )
    print(f"Warehouse {warehouse.item.name}: 0 fixture objects remain after wipe")

    # Wipe preserves the physical item; the fixture owns its later deletion.
    still_there = find_item(
        fabric_workspace,
        warehouse.item.name,
        item_type=WAREHOUSE,
        client=fabric_client,
    )
    assert still_there.id == warehouse.item.id
