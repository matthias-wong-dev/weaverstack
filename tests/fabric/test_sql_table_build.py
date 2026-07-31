"""SQL-backed Spark table build, on local Spark.

Both schema modes come from one self-contained Weaver document fixture (``sql-table-build``,
wired in ``build_envs`` as ``SQL_TABLE_FIXTURE``): ``Sales.InferredCustomer``
takes its shape from its query, ``Sales.DeclaredCustomer`` declares a wider one.
The estate is provisioned and installed **once per module** (``local_lakehouse_estate``)
and every assertion below reuses it.

**Local only.** Query-shape inference, declared not-null, audit columns and
dependency order are all decided before anything reaches a workspace, and the
emulator runs the same Spark SQL. The Fabric half was a second full install
asserting the identical things. What Fabric alone can answer about a built table
— that it landed in the destination rather than the attached Lakehouse, that its
storage is where the resolver says — is asserted in ``test_build_bundle``.
"""

from __future__ import annotations

import pytest
from build_envs import SQL_TABLE_FIXTURE

pytestmark = pytest.mark.parametrize("weaver_repo_fixture", [SQL_TABLE_FIXTURE], indirect=True)

AUDIT = {"row_insert_datetime", "row_update_datetime", "row_delete_datetime"}


def _by_name(columns):
    return {column["name"].lower(): column for column in columns}


def _count(env, table):
    return next(iter(env.query(f"SELECT count(*) AS n FROM {table}")[0].values()))


def test_both_schema_modes_build_the_main_tables_empty(local_lakehouse_estate):
    env = local_lakehouse_estate.env
    tables = {
        row["tableName"].lower()
        for row in env.query(f"SHOW TABLES IN {env.schema_name('Sales')}")
    }
    assert {"customer", "inferredcustomer", "declaredcustomer"} <= tables

    for table in ("{{object:Sales.InferredCustomer}}", "{{object:Sales.DeclaredCustomer}}"):
        columns = _by_name(env.columns(table))
        assert {"customerid", "customername"} <= set(columns)
        assert AUDIT <= set(columns)
        # Build creates structure, not data — the base read() would raise if run.
        assert _count(env, table) == 0


def test_inferred_types_come_from_the_query_declared_from_the_declaration(local_lakehouse_estate):
    env = local_lakehouse_estate.env
    # The base types CustomerId as int; the inferred table follows the query, the
    # declared table its wider declaration — the same on both engines.
    inferred = _by_name(env.columns("{{object:Sales.InferredCustomer}}"))
    declared = _by_name(env.columns("{{object:Sales.DeclaredCustomer}}"))
    assert inferred["customerid"]["type"] == "int"
    assert declared["customerid"]["type"] == "bigint"


def test_primary_key_and_audit_columns_are_physically_not_nullable(local_lakehouse_estate):
    env = local_lakehouse_estate.env
    for table in ("{{object:Sales.InferredCustomer}}", "{{object:Sales.DeclaredCustomer}}"):
        columns = _by_name(env.columns(table))
        # The primary key and all three audit columns carry NOT NULL through to
        # the physical Delta schema, inferred or declared, on both engines.
        assert columns["customerid"]["nullable"] is False
        for audit in AUDIT:
            assert columns[audit]["nullable"] is False
        # A non-key business column stays nullable.
        assert columns["customername"]["nullable"] is True


def test_dependency_order_places_the_base_before_its_readers(local_lakehouse_estate):
    at = {
        action.resource_node_id: seq.number
        for seq, _, action in local_lakehouse_estate.bundle.plan.actions()
        if action.resource_node_id is not None
    }
    base = next(n for n in at if n.endswith("Sales.Customer"))
    for reader in (
        "Lakehouse/Sales/Sales.InferredCustomer",
        "Lakehouse/Sales/Sales.DeclaredCustomer",
    ):
        assert at[base] < at[reader]
