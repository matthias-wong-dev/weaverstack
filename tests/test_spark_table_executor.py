"""The ``spark_table`` executor, driven with a fake session — no JVM.

The install-time behaviour (run the query, read its shape, validate, create the
table) is proven end to end against real Delta in
``tests/spark/test_sql_table_build.py``. These tests pin the executor's own
logic cheaply: what SQL it generates, and that it surfaces every column
violation the plan lists — without paying for a Spark session.
"""

from __future__ import annotations

import json

import pytest

from weaver.build_bundle.executors.base import InstallationContext
from weaver.build_bundle.executors.spark_table import SparkTableExecutor
from weaver.build_bundle.models import BuildAction
from weaver.errors import BuildError, InstallError


class _FakeType:
    def __init__(self, simple: str) -> None:
        self._simple = simple

    def simpleString(self) -> str:
        return self._simple


class _FakeField:
    def __init__(self, name: str, simple: str) -> None:
        self.name = name
        self.dataType = _FakeType(simple)


class _FakeSchema:
    def __init__(self, fields: list[_FakeField]) -> None:
        self.fields = fields


class _FakeFrame:
    def __init__(self, fields: list[_FakeField]) -> None:
        self.schema = _FakeSchema(fields)


class _FakeSpark:
    """Returns a fixed query shape, and records the statements it is asked to run."""

    def __init__(self, query_fields: list[tuple[str, str]]) -> None:
        self._fields = [_FakeField(name, simple) for name, simple in query_fields]
        self.executed: list[str] = []

    def sql(self, statement: str):
        self.executed.append(statement)
        if statement.lstrip().upper().startswith("CREATE"):
            return None
        return _FakeFrame(self._fields)


AUDIT = [
    ["row_insert_datetime", "timestamp", True],
    ["row_update_datetime", "timestamp", True],
    ["row_delete_datetime", "timestamp", True],
]


def _payload(**overrides) -> bytes:
    payload = {
        "object": "Sales.Customer",
        "schema_mode": "inferred",
        "declared_columns": None,
        "source_query": "select CustomerId, CustomerName from Sales.Raw",
        "references": [["Primary key", "CustomerId"]],
        "identity": None,
        "audit_columns": AUDIT,
        "column_mapping": True,
    }
    payload.update(overrides)
    return (json.dumps(payload) + "\n").encode("utf-8")


def _run(spark, payload: bytes):
    action = BuildAction(
        id="build-delta-Sales.Customer",
        kind="build_table",
        resource_node_id="delta:Sales.Customer",
        executor="spark_table",
        payload="payload/x.spark-table.json",
        payload_sha256="x",
    )
    context = InstallationContext(
        spark=spark, resolver=None, store=None, snapshot=None, target=None
    )
    return SparkTableExecutor().execute(action, payload, context)


def _create_statement(spark) -> str:
    return next(s for s in spark.executed if s.lstrip().upper().startswith("CREATE"))


# --- generation -------------------------------------------------------------


def test_inferred_table_uses_query_types_and_appends_not_null_audit_columns():
    spark = _FakeSpark([("CustomerId", "int"), ("CustomerName", "string")])
    details = _run(spark, _payload())

    statement = _create_statement(spark)
    assert statement.startswith("CREATE OR REPLACE TABLE Sales.Customer (\n")
    # CustomerId is the primary key, so it is not null even when inferred;
    # CustomerName is not, so it stays nullable.
    assert "`CustomerId` int NOT NULL" in statement
    assert "`CustomerName` string,\n" in statement
    assert "`CustomerName` string NOT NULL" not in statement
    # Every audit column is not null.
    assert "`row_insert_datetime` timestamp NOT NULL" in statement
    assert "`row_update_datetime` timestamp NOT NULL" in statement
    assert "`row_delete_datetime` timestamp NOT NULL" in statement
    assert "USING delta" in statement
    assert "delta.columnMapping.mode" in statement
    assert details["columns"][:2] == ["CustomerId", "CustomerName"]


def test_the_not_null_header_marks_inferred_columns_not_null():
    spark = _FakeSpark([("CustomerId", "int"), ("CustomerName", "string"), ("Note", "string")])
    _run(
        spark,
        _payload(
            references=[["Primary key", "CustomerId"], ["Not null", "CustomerName"]],
        ),
    )
    statement = _create_statement(spark)
    # The primary key and the Not null column are not null; Note is nullable.
    assert "`CustomerId` int NOT NULL" in statement
    assert "`CustomerName` string NOT NULL" in statement
    assert "`Note` string,\n" in statement
    assert "`Note` string NOT NULL" not in statement


def test_the_identity_column_leads_as_a_not_null_bigint():
    spark = _FakeSpark([("CustomerName", "string")])
    _run(
        spark,
        _payload(
            identity_column=["CustomerKey", "bigint", True],
            references=[["Primary key", "CustomerKey"]],
        ),
    )
    statement = _create_statement(spark)
    # The Weaver-managed surrogate is created first, as a plain not-null bigint —
    # no GENERATED/identity keyword; a later load populates it.
    assert statement.startswith(
        "CREATE OR REPLACE TABLE Sales.Customer (\n    `CustomerKey` bigint NOT NULL,\n"
    )
    assert "generated" not in statement.lower()
    assert "identity" not in statement.lower()


def test_an_identity_colliding_with_a_query_column_fails_install():
    spark = _FakeSpark([("CustomerId", "int"), ("CustomerKey", "int")])
    with pytest.raises(BuildError, match="Identity 'CustomerKey' collides"):
        _run(spark, _payload(identity_column=["CustomerKey", "bigint", True], references=[]))


def test_declared_table_uses_declared_types_and_nullability_not_the_query():
    spark = _FakeSpark([("CustomerId", "int"), ("CustomerName", "string")])
    _run(
        spark,
        _payload(
            schema_mode="declared",
            declared_columns=[
                ["CustomerId", "bigint", True],
                ["CustomerName", "string", False],
            ],
        ),
    )
    statement = _create_statement(spark)
    # The declaration asked for bigint NOT NULL; the query's int is ignored.
    assert "`CustomerId` bigint NOT NULL" in statement
    assert "`CustomerName` string,\n" in statement


def test_column_names_are_case_sensitive_against_the_declaration():
    spark = _FakeSpark([("customerid", "int")])
    with pytest.raises(BuildError, match="not returned by the query under the same case"):
        _run(
            spark,
            _payload(
                schema_mode="declared",
                declared_columns=[["CustomerId", "bigint", True]],
                references=[],
            ),
        )


# --- validation failures the plan enumerates --------------------------------


def test_a_declared_column_missing_from_the_query_fails_install():
    spark = _FakeSpark([("CustomerId", "int")])
    with pytest.raises(BuildError, match="not returned by the query under the same case: CustomerName"):
        _run(
            spark,
            _payload(
                schema_mode="declared",
                declared_columns=[
                    ["CustomerId", "bigint", True],
                    ["CustomerName", "string", False],
                ],
                references=[],
            ),
        )


def test_an_undeclared_extra_query_column_fails_install():
    spark = _FakeSpark([("CustomerId", "int"), ("Extra", "string")])
    with pytest.raises(BuildError, match="not in the declared schema"):
        _run(
            spark,
            _payload(
                schema_mode="declared",
                declared_columns=[["CustomerId", "bigint", True]],
                references=[],
            ),
        )


def test_case_colliding_query_output_names_fail_install():
    spark = _FakeSpark([("CustomerId", "int"), ("customerid", "bigint")])
    with pytest.raises(BuildError, match="collide by name"):
        _run(spark, _payload(references=[]))


def test_a_primary_key_naming_a_missing_column_fails_install():
    spark = _FakeSpark([("CustomerName", "string")])
    with pytest.raises(BuildError, match="Primary key names column 'CustomerId'"):
        _run(spark, _payload())


def test_a_query_column_colliding_with_an_audit_column_is_refused():
    spark = _FakeSpark([("CustomerId", "int"), ("row_insert_datetime", "string")])
    with pytest.raises(InstallError, match="reserved for Weaver's audit columns"):
        _run(spark, _payload(references=[]))


def test_a_missing_session_is_a_clear_install_error():
    with pytest.raises(InstallError, match="needs a Spark session"):
        _run(None, _payload())
