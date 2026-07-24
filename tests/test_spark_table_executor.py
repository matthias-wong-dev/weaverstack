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
    ["Row_insert_datetime", "timestamp"],
    ["Row_update_datetime", "timestamp"],
    ["Row_delete_datetime", "timestamp"],
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


def test_inferred_table_uses_query_types_and_appends_audit_columns():
    spark = _FakeSpark([("CustomerId", "int"), ("CustomerName", "string")])
    details = _run(spark, _payload())

    statement = _create_statement(spark)
    assert statement.startswith("CREATE OR REPLACE TABLE Sales.Customer (\n")
    assert "`CustomerId` int" in statement
    assert "`CustomerName` string" in statement
    assert "`Row_insert_datetime` timestamp" in statement
    assert "`Row_delete_datetime` timestamp" in statement
    assert "USING delta" in statement
    assert "delta.columnMapping.mode" in statement
    assert details["columns"][:2] == ["CustomerId", "CustomerName"]


def test_declared_table_uses_declared_types_not_the_query_types():
    spark = _FakeSpark([("CustomerId", "int"), ("CustomerName", "string")])
    _run(
        spark,
        _payload(
            schema_mode="declared",
            declared_columns=[["CustomerId", "bigint"], ["CustomerName", "string"]],
        ),
    )
    statement = _create_statement(spark)
    # The declaration asked for bigint; the query's int is ignored.
    assert "`CustomerId` bigint" in statement


# --- validation failures the plan enumerates --------------------------------


def test_a_declared_column_missing_from_the_query_fails_install():
    spark = _FakeSpark([("CustomerId", "int")])
    with pytest.raises(BuildError, match="not returned by the query: CustomerName"):
        _run(
            spark,
            _payload(
                schema_mode="declared",
                declared_columns=[["CustomerId", "bigint"], ["CustomerName", "string"]],
                references=[],
            ),
        )


def test_an_undeclared_extra_query_column_fails_install():
    spark = _FakeSpark([("CustomerId", "int"), ("Extra", "string")])
    with pytest.raises(BuildError, match="not in the declared schema: Extra"):
        _run(
            spark,
            _payload(
                schema_mode="declared",
                declared_columns=[["CustomerId", "bigint"]],
                references=[],
            ),
        )


def test_duplicate_query_output_names_fail_install():
    spark = _FakeSpark([("CustomerId", "int"), ("customerid", "bigint")])
    with pytest.raises(BuildError, match="duplicate output column"):
        _run(spark, _payload(references=[]))


def test_a_primary_key_naming_a_missing_column_fails_install():
    spark = _FakeSpark([("CustomerName", "string")])
    with pytest.raises(BuildError, match="Primary key names column 'CustomerId'"):
        _run(spark, _payload())


def test_a_query_column_colliding_with_an_audit_column_is_refused():
    spark = _FakeSpark([("CustomerId", "int"), ("Row_insert_datetime", "string")])
    with pytest.raises(InstallError, match="reserved for Weaver's audit columns"):
        _run(spark, _payload(references=[]))


def test_a_missing_session_is_a_clear_install_error():
    with pytest.raises(InstallError, match="needs a Spark session"):
        _run(None, _payload())
