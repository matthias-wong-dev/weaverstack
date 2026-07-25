"""The self-contained Warehouse estate, built and installed against Fabric.

The ``warehouse-estate`` fixture (wired in ``build_envs`` as
``WAREHOUSE_ESTATE_FIXTURE``) is a closed Warehouse graph: its base tables seed
themselves from literal ``VALUES``, so no Lakehouse is needed. It is provisioned
and installed **once per module** (``warehouse_estate``) — one disposable
Warehouse, one install — and every assertion reads the Warehouse catalogue back
to prove the self-contained scripts really run.

Fabric-only: a Warehouse cannot be exercised locally, so the local coverage of
these same scripts is the generation-level ``tests/test_warehouse_estate.py``.
"""

from __future__ import annotations

import pytest
from build_envs import WAREHOUSE_ESTATE_FIXTURE

pytestmark = [
    pytest.mark.fabric,
    pytest.mark.parametrize("ses_fixture", [WAREHOUSE_ESTATE_FIXTURE], indirect=True),
]

AUDIT = {"Row insert datetime", "Row update datetime", "Row delete datetime"}


def _catalogue(env):
    rows = env.query(
        "select table_schema, table_name, table_type from information_schema.tables "
        "where table_schema in (N'Wh', N'Rpt')"
    )
    return {(r["table_schema"], r["table_name"], r["table_type"].strip()) for r in rows}


def _by_name(columns):
    return {column["name"]: column for column in columns}


def _count(env, qualified):
    return env.query(f"select count(*) as n from {qualified}")[0]["n"]


def test_every_object_is_built_in_dependency_order(warehouse_estate):
    env, bundle = warehouse_estate.env, warehouse_estate.bundle

    catalogue = _catalogue(env)
    tables = {(s, n) for s, n, kind in catalogue if kind == "BASE TABLE"}
    views = {(s, n) for s, n, kind in catalogue if kind == "VIEW"}
    assert {("Wh", "Customer"), ("Wh", "Product"), ("Wh", "CustomerOrder"),
            ("Wh", "CustomerDim")} <= tables
    assert ("Rpt", "CustomerSummary") in views

    at = {
        a.resource_node_id: seq.number
        for seq, _, a in bundle.plan.actions()
        if a.resource_node_id is not None
    }
    assert at["sql:Wh.Customer"] < at["sql:Wh.CustomerOrder"]
    assert at["sql:Wh.CustomerOrder"] < at["sql:Rpt.CustomerSummary"]


def test_tables_are_built_empty(warehouse_estate):
    env = warehouse_estate.env
    for qualified in ("Wh.Customer", "Wh.Product", "Wh.CustomerOrder", "Wh.CustomerDim"):
        assert _count(env, qualified) == 0


def test_a_declared_table_carries_its_declared_types(warehouse_estate):
    columns = _by_name(warehouse_estate.env.columns("Wh.Product"))
    assert columns["ProductId"]["type"] == "int"
    assert columns["ProductName"]["type"] == "varchar"
    assert columns["Price"]["type"] == "decimal"


def test_the_dimension_has_a_weaver_managed_bigint_surrogate(warehouse_estate):
    columns = _by_name(warehouse_estate.env.columns("Wh.CustomerDim"))
    # A plain bigint Weaver adds; a later load populates it.
    assert columns["CustomerKey"]["type"] == "bigint"
    assert {"CustomerId", "CustomerName"} <= set(columns)


def test_primary_key_and_audit_columns_are_physically_not_nullable(warehouse_estate):
    # The Warehouse equivalent of the Spark nullability check, read from the
    # catalogue: the primary key and all three audit columns are NOT NULL.
    columns = _by_name(warehouse_estate.env.columns("Wh.Customer"))
    assert columns["CustomerId"]["nullable"] is False
    for audit in AUDIT:
        assert columns[audit]["nullable"] is False
