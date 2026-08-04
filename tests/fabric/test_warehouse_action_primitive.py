"""Warehouse build actions executed over TDS against a real Fabric engine.

Each assertion here is about the action seam: the rendered action reached its
executor and created or removed the intended object. The declared physical
shape and inventory-reader fidelity are separate primitive and binding claims,
kept in adjacent modules over the same session-scoped estate.
"""

from __future__ import annotations

import pytest

from weaver.build_bundle.prune import read_warehouse_inventory

pytestmark = [pytest.mark.fabric, pytest.mark.remote]


def test_create_schema_action_creates_the_schema_in_the_warehouse(
    warehouse_primitive_estate,
):
    rows = warehouse_primitive_estate.warehouse.executor.query(
        "select name from sys.schemas where name = N'DWG'"
    )

    assert [str(row["name"]) for row in rows] == ["DWG"]


def test_build_table_action_is_accepted_by_fabric(warehouse_primitive_estate):
    rows = warehouse_primitive_estate.warehouse.executor.query(
        "select count(*) as n from [DWG].[Customer]"
    )

    assert rows[0]["n"] == 0


def test_build_view_action_creates_a_view_over_the_table_it_reads(
    warehouse_primitive_estate,
):
    rows = warehouse_primitive_estate.warehouse.executor.query(
        "select count(*) as n from [DWG].[ActiveCustomer]"
    )

    assert rows[0]["n"] == 0


def test_prune_table_action_removes_an_object_nothing_declares(
    warehouse_primitive_estate,
):
    """Execute destructive prune while proving the declared estate survives."""

    executor = warehouse_primitive_estate.warehouse.executor
    executor.execute_script("create schema Legacy;")
    executor.execute_script("create table [Legacy].[Thing] ([x] int not null);")
    executor.execute_script("create table [DWG].[OldTable] ([x] int not null);")

    installed = read_warehouse_inventory(
        warehouse_primitive_estate.target.bound, sql=executor
    )
    results = warehouse_primitive_estate.run(
        warehouse_primitive_estate.repository,
        inventory=installed,
        build=False,
    )

    failures = {r.action_id: r.error_message for r in results if r.status == "failed"}
    assert not failures, failures

    after = read_warehouse_inventory(
        warehouse_primitive_estate.target.bound, sql=executor
    )
    remaining = {name.casefold() for name in after.tables}
    assert "dwg.oldtable" not in remaining
    assert "legacy.thing" not in remaining
    assert "legacy" not in {name.casefold() for name in after.schemas}
    assert {"dwg.customer", "dwg.customerdim"} <= remaining
    assert "dwg.activecustomer" in {name.casefold() for name in after.views}


def test_the_warehouse_action_primitives_spend_no_livy():
    from support.livy_telemetry import LEDGER

    mine = [
        call
        for call in LEDGER.calls
        if "test_warehouse_action_primitive" in call.nodeid
    ]
    assert mine == []
