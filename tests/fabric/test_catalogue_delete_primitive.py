"""Catalogue deletion executed over TDS against a real Fabric Warehouse."""

from __future__ import annotations

from dataclasses import replace
from uuid import uuid4

from support.weaver_test import weaver_test

from weaver.catalogue.render import (
    VALUES_ROWS,
    InstallationScope,
    render_delete_obsolete,
)
from weaver.catalogue.tables import REGISTRY


@weaver_test(remote=True, resources={"tds"})
def test_a_large_keep_relation_is_accepted_by_the_catalogue_warehouse(
    session_catalogue_sql,
):
    """More than one constructor remains one safe, executable delete.

    The random scope has no catalogue rows, so the statement is compiled and
    executed by Fabric without changing an installation another test owns.
    """

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

    executor = session_catalogue_sql
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
        if created_table:
            executor.execute_script(f"drop table if exists [_].[{table.name}];")
        if created_schema:
            executor.execute_script("drop schema if exists [_];")
