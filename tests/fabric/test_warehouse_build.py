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
    pytest.mark.parametrize("weaver_repo_fixture", [WAREHOUSE_ESTATE_FIXTURE], indirect=True),
]

AUDIT = {"Row insert datetime", "Row update datetime", "Row delete datetime"}


#: Schemas Weaver never manages, excluded so the catalogue view shows exactly the
#: user objects — including any orphan schema a prune is expected to remove.
SYSTEM_SCHEMAS = {"dbo", "guest", "information_schema", "sys", "queryinsights", "_rsc"}


def _catalogue(env):
    # Fabric Warehouses use a case-sensitive collation — INFORMATION_SCHEMA and
    # its columns must be referenced in their exact (upper) case. Everything
    # non-system is returned, so an orphan schema is visible before and after a
    # prune rather than filtered out of the assertion.
    rows = env.query(
        "select TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE from INFORMATION_SCHEMA.TABLES"
    )
    return {
        (r["TABLE_SCHEMA"], r["TABLE_NAME"], r["TABLE_TYPE"].strip())
        for r in rows
        if r["TABLE_SCHEMA"].lower() not in SYSTEM_SCHEMAS
    }


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
    item = "Warehouse/Reporting"
    assert at[f"{item}/Wh.Customer"] < at[f"{item}/Wh.CustomerOrder"]
    assert at[f"{item}/Wh.CustomerOrder"] < at[f"{item}/Rpt.CustomerSummary"]


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


def test_prune_reconciles_unmanaged_objects_and_spares_the_managed_set(warehouse_estate):
    """Runs last in this module, so it reuses the installed estate: seed orphans
    beside it, rebuild with reconciliation on, and prove the frozen T-SQL drops
    remove exactly the unmanaged objects."""

    env = warehouse_estate.env
    env.seed_orphans()

    seeded = _catalogue(env)
    assert ("Wh", "OldTable", "BASE TABLE") in seeded
    assert ("Legacy", "Thing", "BASE TABLE") in seeded

    # The build inspects the catalogue now and freezes one drop per orphan.
    bundle = env.generate(
        bundle_name="whprune", prune=True
    )
    prune_kinds = {a.kind for _, _, a in bundle.plan.actions() if a.kind.startswith("prune")}
    assert {"prune_table", "prune_view", "prune_schema"} <= prune_kinds

    outcome = env.install(bundle)
    assert outcome.status == "succeeded", outcome.action_error

    after = _catalogue(env)
    # Every orphan is gone, including the whole orphan schema.
    assert not [row for row in after if row[0] == "Legacy"]
    assert ("Wh", "OldTable", "BASE TABLE") not in after
    assert ("Wh", "OldView", "VIEW") not in after
    # And the managed estate is untouched.
    assert {("Wh", "Customer"), ("Wh", "Product"), ("Wh", "CustomerOrder"),
            ("Wh", "CustomerDim")} <= {(s, n) for s, n, _ in after}
    assert ("Rpt", "CustomerSummary", "VIEW") in after
