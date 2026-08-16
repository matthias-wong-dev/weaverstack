"""Catalogue deletion executed over TDS against a real Fabric Warehouse."""

from __future__ import annotations

from dataclasses import replace
from uuid import uuid4

import pytest

from weaver.catalogue.render import (
    VALUES_ROWS,
    InstallationScope,
    render_delete_obsolete,
)
from weaver.catalogue.tables import REGISTRY

pytestmark = [pytest.mark.fabric, pytest.mark.remote]


def test_a_large_keep_relation_is_accepted_by_the_catalogue_warehouse(
    fabric_workspace,
):
    """More than one constructor remains one safe, executable delete.

    The random scope has no catalogue rows, so the statement is compiled and
    executed by Fabric without changing an installation another test owns.
    """

    from weaver.fabric import FabricResolver, desktop_sql_executor
    from weaver.targets import WarehouseTarget

    token = uuid4().hex
    scope = InstallationScope("Lakehouse", f"weavertest-delete-{token}")
    table = replace(REGISTRY, name=f"DeleteProbe{token}")
    rows = [
        {
            "item_type": scope.item_type,
            "item_name": scope.item_name,
            "schema_name": "Sales",
            "object_name": f"Object{index:05d}",
        }
        for index in range(VALUES_ROWS + 1)
    ]
    statement = render_delete_obsolete(table, rows, scope=scope)
    assert statement is not None

    target = WarehouseTarget(warehouse=fabric_workspace.catalogue_item)
    executor = desktop_sql_executor(
        target, fabric_workspace, resolver=FabricResolver(fabric_workspace)
    )
    created_schema = False
    created_table = False
    try:
        schema_existed = bool(
            executor.query("select 1 as present from sys.schemas where name = N'_'")
        )
        if not schema_existed:
            executor.execute_script("create schema [_];")
            created_schema = True
        executor.execute_script(
            f"create table [_].[{table.name}] ("
            "[Item type] varchar(128) not null, "
            "[Item name] varchar(128) not null, "
            "[Schema name] varchar(128) not null, "
            "[Object name] varchar(128) not null);"
        )
        created_table = True
        executor.execute_script(statement)
    finally:
        try:
            if created_table:
                executor.execute_script(f"drop table if exists [_].[{table.name}];")
            if created_schema:
                executor.execute_script("drop schema if exists [_];")
        finally:
            executor.close()


def test_the_catalogue_delete_primitive_spends_no_livy():
    from support.livy_telemetry import LEDGER

    mine = [
        call
        for call in LEDGER.calls
        if "test_catalogue_delete_primitive" in call.nodeid
    ]
    assert mine == []
