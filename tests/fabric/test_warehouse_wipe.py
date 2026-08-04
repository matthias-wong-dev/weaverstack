"""Wiping a real Warehouse: populate, wipe, verify — all over TDS.

A Warehouse is reached over TDS, and `wipe_sql_target` takes its SQL executor as
an argument. So none of this needs a Livy session: the production implementation
runs here, against the real Warehouse, on the connection this process already
holds.

It used to submit the wipe into a session purely because *that* is where the
installed package lived — which put a five-minute `weaver install` on the path to
finding out whether the generated DROP statements were right. That the installed
package can acquire a Warehouse connection from the session's own identity is a
separate claim, made once in `test_published_weaver.py`.
"""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from sql_support import CatalogObject, populate_warehouse, system_schemas, user_objects

pytestmark = [pytest.mark.fabric, pytest.mark.remote]

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


def test_weaver_wipes_a_populated_warehouse(
    clean_disposable_warehouse,
    fabric_client,
    fabric_workspace_item,
):
    """Exercise Weaver's own wipe, not a duplicate test-side SQL implementation."""

    from weaver.fabric import WAREHOUSE, find_item

    warehouse = clean_disposable_warehouse

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

    from weaver import wipe_sql_target

    started = time.monotonic()
    # The production implementation, given the SQL executor explicitly — which is
    # the only thing a session would have supplied differently.
    wipe_sql_target(
        warehouse.target, warehouse.workspace, sql=warehouse.executor
    )
    warehouse.timings["wipe execution"] = time.monotonic() - started
    print(
        f"Warehouse {warehouse.item.name} wipe execution: "
        f"{warehouse.timings['wipe execution']:.2f}s"
    )

    after = user_objects(warehouse.executor)
    assert after == set()
    assert {"dbo", "guest", "information_schema", "sys"} <= system_schemas(
        warehouse.executor
    )
    print(f"Warehouse {warehouse.item.name}: 0 fixture objects remain after wipe")

    # Wipe preserves the physical item; the fixture owns its later deletion.
    still_there = find_item(
        fabric_workspace_item,
        warehouse.item.name,
        item_type=WAREHOUSE,
        client=fabric_client,
    )
    assert still_there.id == warehouse.item.id
