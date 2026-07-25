"""The self-contained Warehouse estate, built and installed against Fabric.

The ``warehouse-estate`` fixture (wired in ``conftest`` as
``WAREHOUSE_ESTATE_FIXTURE``) is a closed Warehouse graph: its base tables seed
themselves from literal ``VALUES``, so no Lakehouse is needed. This drives the
``warehouse_build_env`` — desktop generation, T-SQL install over the disposable
Warehouse's live SQL endpoint — and reads the Warehouse catalogue back to prove
the self-contained scripts really run: base tables, a dependent table, a
Weaver-managed identity dimension and a reporting view, each an empty structure.

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

REPO = "WhEstate"


def _catalogue(build_env) -> set[tuple[str, str, str]]:
    rows = build_env.query(
        "select table_schema, table_name, table_type from information_schema.tables "
        "where table_schema in (N'Wh', N'Rpt')"
    )
    return {
        (r["table_schema"], r["table_name"], r["table_type"].strip()) for r in rows
    }


def _columns(build_env, schema, name) -> dict[str, str]:
    rows = build_env.query(
        "select column_name, data_type from information_schema.columns "
        f"where table_schema = N'{schema}' and table_name = N'{name}'"
    )
    return {r["column_name"]: r["data_type"] for r in rows}


def _count(build_env, qualified) -> int:
    return build_env.query(f"select count(*) as n from {qualified}")[0]["n"]


@pytest.fixture
def installed_estate(warehouse_build_env):
    warehouse_build_env.install_repo(REPO)
    bundle = warehouse_build_env.generate(repository_name=REPO)
    outcome = warehouse_build_env.install(bundle)
    assert outcome.status == "succeeded", outcome.action_error
    return warehouse_build_env, bundle


def test_every_object_is_built_in_dependency_order(installed_estate):
    build_env, bundle = installed_estate

    catalogue = _catalogue(build_env)
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


def test_tables_are_built_empty_with_audit_columns(installed_estate):
    build_env, _ = installed_estate

    for qualified in ("Wh.Customer", "Wh.Product", "Wh.CustomerOrder", "Wh.CustomerDim"):
        assert _count(build_env, qualified) == 0

    audit = {"Row insert datetime", "Row update datetime", "Row delete datetime"}
    assert audit <= set(_columns(build_env, "Wh", "Customer"))


def test_a_declared_table_carries_its_declared_types(installed_estate):
    build_env, _ = installed_estate
    columns = _columns(build_env, "Wh", "Product")
    assert columns["ProductId"] == "int"
    assert columns["ProductName"] == "varchar"
    assert columns["Price"] == "decimal"


def test_the_dimension_has_a_weaver_managed_bigint_surrogate(installed_estate):
    build_env, _ = installed_estate
    columns = _columns(build_env, "Wh", "CustomerDim")
    # The surrogate is a plain bigint Weaver adds; a later load populates it.
    assert columns["CustomerKey"] == "bigint"
    assert {"CustomerId", "CustomerName"} <= set(columns)
