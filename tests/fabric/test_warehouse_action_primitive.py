"""Warehouse build actions executed over TDS against a real Fabric engine.

The shared `warehouse_primitive_estate` fixture fails if any action it runs
fails, and the adjacent modules read back what those actions created. What
stays here is prune: the action that removes an object nothing declares.
"""

from __future__ import annotations

from support.weaver_test import weaver_test

from weaver.build_bundle.prune import read_warehouse_inventory


@weaver_test(remote=True, resources={"tds"})
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
