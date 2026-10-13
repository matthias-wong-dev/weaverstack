"""Installation identity replacement preserves a real Warehouse table."""

from uuid import uuid4

import pytest
from support.weaver_test import weaver_test

from weaver.build_bundle.catalogue_actions import render_catalogue_upgrade
from weaver.catalogue.connection import CatalogueConnection
from weaver.catalogue.installation import (
    installation_schema_upgrade,
    render_installation_upgrade,
    resolve_installations,
)
from weaver.catalogue.reader import read_table
from weaver.catalogue.state import Catalogue
from weaver.catalogue.tables import INSTALLATION
from weaver.catalogue.tsql import literal


@pytest.fixture
def installation_identity_target(
    session_catalogue_sql, warehouse_session, fabric_workspace
):
    resolver = warehouse_session.resolver(fabric_workspace)
    target = resolver.resolve(fabric_workspace.catalogue_item, item_type="Warehouse")
    return resolver, target


@weaver_test(remote=True, resources=("tds",))
@pytest.mark.parametrize("ids", ["nullable", "missing"])
def test_installation_replacement_preserves_rows_audits_and_rolls_back(
    ids,
    session_catalogue_sql,
    installation_identity_target,
):
    sql = session_catalogue_sql
    resolver, target = installation_identity_target
    schema = "InstallationIdentity_" + uuid4().hex
    prefix = f"[{schema}]."
    table = prefix + "[Installation]"
    stage = prefix + "[Installation_identity_stage]"

    def in_schema(statement):
        return statement.replace("[_].", prefix).replace(
            "TABLE_SCHEMA = N'_'", f"TABLE_SCHEMA = {literal(schema)}"
        )

    connection = CatalogueConnection(lambda statement: sql.query(in_schema(statement)))
    sql.execute_script(f"CREATE SCHEMA [{schema}];")
    try:
        id_columns = (
            ", [Workspace ID] varchar(128) NULL, [Item ID] varchar(128) NULL"
            if ids == "nullable"
            else ""
        )
        sql.execute_script(
            f"CREATE TABLE {table} ([Item type] varchar(128) NOT NULL, [Item name] varchar(128) NOT NULL, "
            f"[Target name] varchar(128) NOT NULL, [Weaver version] varchar(128) NOT NULL{id_columns}, "
            "[Signature] varchar(128) NOT NULL, [Row insert datetime] datetime2(6) NOT NULL, "
            "[Row update datetime] datetime2(6) NOT NULL, [Row delete datetime] datetime2(6) NOT NULL);"
        )
        values = ", NULL, " + literal(target.name) if ids == "nullable" else ""
        sql.execute_script(
            f"INSERT INTO {table} VALUES "
            f"(N'Warehouse', N'_weaver', {literal(target.name)}, N'old'{values}, N'catalogue-signature', "
            "'2024-01-01T01:02:03.123456', '2024-02-02T02:03:04.234567', '9999-12-31T23:59:59.999999'), "
            f"(N'Warehouse', N'Retained', {literal(target.name)}, N'old'{values}, N'unselected-signature', "
            "'2023-01-01T01:02:03.123456', '2023-02-02T02:03:04.234567', '9999-12-31T23:59:59.999999');"
        )
        original = sql.query(f"SELECT * FROM {table} ORDER BY [Item name]")
        upgrade = installation_schema_upgrade(connection)
        catalogue = Catalogue({}, installation_upgrade=upgrade)
        rows = read_table(connection, INSTALLATION)
        resolve_installations(catalogue, rows, resolver=resolver)
        script = in_schema(render_installation_upgrade(upgrade))
        cutover = f"DROP TABLE {table};"
        assert cutover in script
        interrupted = script.replace(
            cutover, f"THROW 50000, N''test rollback'', 1;\n{cutover}"
        )
        with pytest.raises(Exception, match="test rollback"):
            sql.execute_script(interrupted)
        assert sql.query(f"SELECT * FROM {table} ORDER BY [Item name]") == original
        assert (
            sql.query(f"SELECT OBJECT_ID({literal(stage)}) AS stage_id")[0]["stage_id"]
            is None
        )
        sql.execute_script(script)
        physical = sql.query(
            "SELECT COLUMN_NAME, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS "
            f"WHERE TABLE_SCHEMA = {literal(schema)} AND TABLE_NAME = N'Installation' ORDER BY ORDINAL_POSITION"
        )
        assert [row["COLUMN_NAME"] for row in physical[:8]] == [
            c.public_name for c in INSTALLATION.columns
        ]
        assert all(row["IS_NULLABLE"] == "NO" for row in physical)
        after = sql.query(f"SELECT * FROM {table} ORDER BY [Item name]")
        assert len(after) == len(original) == 2
        retained = (
            "Item type",
            "Item name",
            "Target name",
            "Weaver version",
            "Signature",
            "Row insert datetime",
            "Row update datetime",
            "Row delete datetime",
        )
        for old, new in zip(original, after, strict=True):
            assert {name: old[name] for name in retained} == {
                name: new[name] for name in retained
            }
            assert new["Workspace name"] == resolver.workspace.name
            assert new["Workspace GUID"] == resolver.workspace.id
            assert new["Target item GUID"] == target.id
        connection.forget_shape()
        assert installation_schema_upgrade(connection) is None
        assert render_catalogue_upgrade(Catalogue({}), catalogue_target=target) is None
        assert (
            sql.query(
                f"SELECT name FROM sys.key_constraints WHERE parent_object_id = OBJECT_ID({literal(table)})"
            )[0]["name"]
            == "PK_Installation"
        )
    finally:
        sql.execute_script(
            f"DROP TABLE IF EXISTS {stage}; DROP TABLE IF EXISTS {table}; DROP SCHEMA [{schema}];"
        )
