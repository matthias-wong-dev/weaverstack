"""Delta identity through Weaver's production table and load boundaries."""

from __future__ import annotations

import json

import pytest
from support.weaver_test import weaver_test

SCHEMA = "DeltaIdentity"
AUDIT = (
    ("row_insert_datetime", "timestamp", True),
    ("row_update_datetime", "timestamp", True),
    ("row_delete_datetime", "timestamp", True),
)
SIGNATURE = ("row_signature", "string", True)
LIVE_DELETE_DATETIME = "9999-12-31 23:59:59.999999"

PYTHON_IDENTITY_SOURCE = '''
"""
Table ID: DeltaIdentity.PythonIdentity

Description: Identity table created from an authored Python declaration.

Lineage: Fabric identity acceptance fixture.

Primary key: CustomerId

Identity: CustomerKey

Schema:
  CustomerId: string
  Name: string
"""
from weaver import Table


class DeltaIdentity__PythonIdentity(Table):
    def read(self):
        raise AssertionError("build must not execute a Python table read method")
'''


def _payload(target: str, *, source_query: str | None) -> bytes:
    instruction = {
        "object": target,
        "schema_mode": "declared",
        "declared_columns": [
            ["CustomerId", "string", True],
            ["Name", "string", False],
        ],
        "source_query": source_query,
        "setup": [],
        "references": [["Primary key", "CustomerId"]],
        "identity_column": ["CustomerKey", "bigint", True],
        "audit_columns": [list(column) for column in AUDIT],
        "internal_columns": [list(SIGNATURE)],
        "column_mapping": True,
    }
    return (json.dumps(instruction, sort_keys=True) + "\n").encode()


def _authored_python_payload(destination) -> bytes:
    from weaver.declaration import read_source_document
    from weaver.declaration.ddl import SPARK_TABLE_EXECUTOR
    from weaver.declaration.model import LAKEHOUSE

    document = read_source_document(
        "DeltaIdentity__PythonIdentity.py",
        PYTHON_IDENTITY_SOURCE.lstrip().encode(),
        LAKEHOUSE,
    )
    ddl = document.create_ddl(destination=destination)
    instruction = json.loads(ddl.content)

    assert ddl.executor == SPARK_TABLE_EXECUTOR
    assert instruction["object"] == destination.qualify(SCHEMA, "PythonIdentity")
    assert instruction["identity_column"] == ["CustomerKey", "bigint", True]
    return ddl.content.encode()


def _action(name: str):
    from weaver.build_bundle.models import InstallAction

    return InstallAction(
        id=f"build-{name}",
        kind="build_table",
        resource_node_id=f"Lakehouse/Sales/Tables/{SCHEMA}.{name}",
        executor="spark_table",
        payload=f"payload/{name}.spark-table.json",
        payload_sha256="unused",
    )


def _insert_sql(target: str, customer: str) -> str:
    columns = (
        "CustomerId, Name, row_insert_datetime, row_update_datetime, "
        "row_delete_datetime, row_signature"
    )
    return (
        f"INSERT INTO {target} ({columns}) VALUES "
        f"('{customer}', '{customer}', current_timestamp(), current_timestamp(), "
        f"CAST('{LIVE_DELETE_DATETIME}' AS timestamp), 'signature')"
    )


@weaver_test(remote=True, resources={"livy", "rest", "tds"})
def test_console_builds_python_and_sql_authored_identity_tables(
    fabric_workspace,
    fabric_target_lakehouse,
    weaver_session,
):
    from factories import bound_target
    from support.weaver_test import register_session

    from weaver.build_bundle import Installer, execute_install_action
    from weaver.build_bundle.executors.base import InstallationContext, ResolvedTarget
    from weaver.sessions import ConsoleSession
    from weaver.sql import SqlEndpoint
    from weaver.targets import ItemRef, WarehouseTarget

    resolver = weaver_session.resolver(fabric_workspace)
    item = ItemRef(fabric_target_lakehouse.name)
    destination = resolver.spark_destination(item)
    installer = Installer(weaver_session, workspace=fabric_workspace)
    target = ResolvedTarget(
        bound=bound_target(id="identity-target", item_id=item.name),
        lakehouse=item,
        destination=destination,
    )
    context = InstallationContext(
        create_delta_table=installer.delta_table_creator(),
        spark_sql=installer.spark_sql(),
        spark_sql_batch=installer.spark_sql_batch(),
        resolver=resolver,
        store=installer.store,
        target=target,
        targets={target.bound.id: target},
    )
    schema = destination.qualified_schema(SCHEMA)
    python_table = destination.qualify(SCHEMA, "PythonIdentity")
    sql_table = destination.qualify(SCHEMA, "SqlIdentity")

    weaver_session.execute_spark_sql_batch(
        [f"DROP SCHEMA IF EXISTS {schema} CASCADE", f"CREATE SCHEMA {schema}"],
        exact_case=True,
        workspace=fabric_workspace,
    )
    try:
        python_result = execute_install_action(
            _action("PythonIdentity"),
            _authored_python_payload(destination),
            context=context,
        )
        sql_result = execute_install_action(
            _action("SqlIdentity"),
            _payload(
                sql_table,
                source_query=(
                    "select cast('' as string) as CustomerId, "
                    "cast('' as string) as Name where 1 = 0"
                ),
            ),
            context=context,
        )
        assert python_result.status == "succeeded", python_result.error_message
        assert sql_result.status == "succeeded", sql_result.error_message

        for table, customer in ((python_table, "P"), (sql_table, "S")):
            weaver_session.execute_spark_sql(
                _insert_sql(table, customer),
                exact_case=True,
                workspace=fabric_workspace,
            )
            rows = weaver_session.execute_spark_sql(
                f"SELECT CustomerKey, CustomerId FROM {table}",
                exact_case=True,
                workspace=fabric_workspace,
            )
            assert rows[0]["CustomerId"] == customer
            assert int(rows[0]["CustomerKey"]) > 0

        described = weaver_session.execute_spark_sql(
            f"DESCRIBE TABLE {python_table}",
            exact_case=True,
            workspace=fabric_workspace,
        )
        identity = next(
            row for row in described if row.get("col_name") == "CustomerKey"
        )
        assert identity["data_type"] == "bigint"

        detail = weaver_session.execute_spark_sql(
            f"DESCRIBE DETAIL {python_table}",
            exact_case=True,
            workspace=fabric_workspace,
        )[0]
        features = {str(value).lower() for value in detail.get("tableFeatures") or ()}
        assert "identitycolumns" in features

        refresh = resolver.refresh_sql_endpoint(item)
        assert refresh["status"] == "Succeeded"
        physical_lakehouse = resolver.resolve(item, item_type="Lakehouse")
        lakehouse_payload = resolver.client.get_json(
            f"workspaces/{physical_lakehouse.workspace_id}/lakehouses/"
            f"{physical_lakehouse.id}"
        )
        endpoint_properties = lakehouse_payload["properties"]["sqlEndpointProperties"]
        endpoint = SqlEndpoint(
            server=endpoint_properties["connectionString"],
            database=physical_lakehouse.name,
            workspace_id=physical_lakehouse.workspace_id,
            warehouse_id=endpoint_properties["id"],
            warehouse_name=physical_lakehouse.name,
        )

        class EndpointResolver:
            def sql_endpoint(self, _target):
                return endpoint

        with ConsoleSession(
            workspace=fabric_workspace,
            resolver=EndpointResolver(),
            progress=False,
        ) as endpoint_session:
            register_session(endpoint_session)
            sql = endpoint_session.sql_executor(
                WarehouseTarget(ItemRef(physical_lakehouse.name)),
                workspace=fabric_workspace,
            )
            import time

            deadline = time.monotonic() + 180
            last_error = None
            endpoint_rows = []
            while time.monotonic() < deadline:
                try:
                    endpoint_rows = sql.query(
                        "SELECT [CustomerKey], [CustomerId] "
                        "FROM [DeltaIdentity].[PythonIdentity]"
                    )
                    if endpoint_rows:
                        break
                except Exception as exc:
                    last_error = exc
                time.sleep(5)
            assert endpoint_rows, last_error or "the SQL endpoint returned no rows"
            assert endpoint_rows[0]["CustomerId"] == "P"
            assert int(endpoint_rows[0]["CustomerKey"]) > 0

        with pytest.raises(Exception, match="EXPLICIT_INSERT|identity"):
            weaver_session.execute_spark_sql(
                (
                    f"INSERT INTO {python_table} "
                    "(CustomerKey, CustomerId, Name, row_insert_datetime, "
                    "row_update_datetime, row_delete_datetime, row_signature) "
                    "VALUES (999, 'X', 'X', current_timestamp(), current_timestamp(), "
                    f"CAST('{LIVE_DELETE_DATETIME}' AS timestamp), 'signature')"
                ),
                exact_case=True,
                workspace=fabric_workspace,
            )

        with pytest.raises(
            Exception, match="already exists|TABLE_OR_VIEW_ALREADY_EXISTS"
        ):
            weaver_session.create_delta_table(
                python_table,
                (
                    ("CustomerKey", "bigint", True),
                    ("CustomerId", "string", True),
                ),
                identity_column="CustomerKey",
                workspace=fabric_workspace,
            )
    finally:
        weaver_session.execute_spark_sql(
            f"DROP SCHEMA IF EXISTS {schema} CASCADE",
            exact_case=True,
            workspace=fabric_workspace,
        )


HOSTED = r"""
from weaver.declaration.model import ObjectId
from weaver.runtime.load_contract import LoadContract
from weaver.runtime.table_load import load_table
from weaver.sessions import NotebookSession
from weaver.spark import FabricSparkTarget
from weaver.workspaces import Workspace

session = NotebookSession(
    workspace=Workspace(workspace=WORKSPACE),
    spark=spark,
    resolver=object(),
    store=object(),
)
destination = FabricSparkTarget(workspace=WORKSPACE, lakehouse=LAKEHOUSE)
schema = destination.qualified_schema(SCHEMA)
keyed = destination.qualify(SCHEMA, "KeyedLifecycle")
replacement = destination.qualify(SCHEMA, "ReplacementLifecycle")
spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
spark.sql(f"CREATE SCHEMA {schema}")

identity = ("CustomerKey", "bigint", True)
business = (("CustomerId", "string", True), ("Name", "string", False))
audits = (
    ("row_insert_datetime", "timestamp", True),
    ("row_update_datetime", "timestamp", True),
    ("row_delete_datetime", "timestamp", True),
)
signature = ("row_signature", "string", True)

session.create_delta_table(
    keyed,
    (identity, *business, *audits, signature),
    identity_column="CustomerKey",
)
session.create_delta_table(
    replacement,
    (identity, *business, *audits),
    identity_column="CustomerKey",
)

def frame(*rows):
    return spark.createDataFrame(list(rows), "CustomerId string, Name string")

def ids(table):
    return {
        row["CustomerId"]: int(row["CustomerKey"])
        for row in spark.sql(
            f"SELECT CustomerKey, CustomerId FROM {table} ORDER BY CustomerId"
        ).collect()
    }

keyed_contract = LoadContract(
    object_id=ObjectId(SCHEMA, "KeyedLifecycle"),
    primary_key=("CustomerId",),
    identity_column="CustomerKey",
)
load_table(
    spark,
    contract=keyed_contract,
    lakehouse=destination,
    staging_frame=frame(("A", "one"), ("B", "two")),
)
first = ids(keyed)
load_table(
    spark,
    contract=keyed_contract,
    lakehouse=destination,
    staging_frame=frame(("A", "one"), ("B", "two")),
)
unchanged = ids(keyed)
load_table(
    spark,
    contract=keyed_contract,
    lakehouse=destination,
    staging_frame=frame(("A", "changed"), ("B", "two")),
)
updated = ids(keyed)
load_table(
    spark,
    contract=keyed_contract,
    lakehouse=destination,
    staging_frame=frame(("A", "changed")),
)
load_table(
    spark,
    contract=keyed_contract,
    lakehouse=destination,
    staging_frame=frame(("A", "changed"), ("B", "again")),
)
reinserted = ids(keyed)

replacement_contract = LoadContract(
    object_id=ObjectId(SCHEMA, "ReplacementLifecycle"),
    identity_column="CustomerKey",
)
load_table(
    spark,
    contract=replacement_contract,
    lakehouse=destination,
    staging_frame=frame(("D", "one"), ("E", "two")),
)
replacement_first = ids(replacement)
load_table(
    spark,
    contract=replacement_contract,
    lakehouse=destination,
    staging_frame=frame(("D", "one"), ("E", "two")),
)
replacement_second = ids(replacement)

explicit_error = None
try:
    spark.sql(
        f"INSERT INTO {keyed} "
        "(CustomerKey, CustomerId, Name, row_insert_datetime, "
        "row_update_datetime, row_delete_datetime, row_signature) "
        "VALUES (999, 'X', 'X', current_timestamp(), current_timestamp(), "
        "CAST('9999-12-31 23:59:59.999999' AS timestamp), 'signature')"
    )
except Exception as exc:
    explicit_error = str(exc)

strict_error = None
try:
    session.create_delta_table(
        keyed,
        (identity, *business, *audits, signature),
        identity_column="CustomerKey",
    )
except Exception as exc:
    strict_error = str(exc)

detail = spark.sql(f"DESCRIBE DETAIL {keyed}").collect()[0].asDict(recursive=True)
field = next(field for field in spark.table(keyed).schema.fields if field.name == "CustomerKey")
result = {
    "first": first,
    "unchanged": unchanged,
    "updated": updated,
    "reinserted": reinserted,
    "replacement_first": replacement_first,
    "replacement_second": replacement_second,
    "explicit_error": explicit_error,
    "strict_error": strict_error,
    "identity_type": field.dataType.simpleString(),
    "identity_nullable": field.nullable,
    "features": detail.get("tableFeatures") or [],
}
spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
emit(result)
"""


@weaver_test(hosted=True)
def test_notebook_identity_survives_keyed_updates_and_regenerates_on_replacement(
    fabric_workspace,
    fabric_target_lakehouse,
    livy_session,
):
    source = (
        f"WORKSPACE = {fabric_workspace.workspace!r}\n"
        f"LAKEHOUSE = {fabric_target_lakehouse.name!r}\n"
        f"SCHEMA = {SCHEMA!r}\n" + HOSTED
    )

    seen = livy_session.run(source).payload

    assert seen["identity_type"] == "bigint"
    assert seen["identity_nullable"] is False
    assert "identitycolumns" in {str(value).lower() for value in seen["features"]}
    assert seen["first"] == seen["unchanged"] == seen["updated"]
    assert seen["reinserted"]["A"] == seen["first"]["A"]
    assert seen["reinserted"]["B"] != seen["first"]["B"]
    assert all(
        seen["replacement_second"][key] != seen["replacement_first"][key]
        for key in ("D", "E")
    )
    assert "EXPLICIT_INSERT" in seen["explicit_error"]
    assert "already exists" in seen["strict_error"].lower()
