"""SQL-backed Spark table build — the same assertions on local Spark and Fabric.

Both schema modes come from one self-contained SES fixture (``sql-table-build``,
wired in ``conftest`` as ``SQL_TABLE_FIXTURE``): ``Sales.InferredCustomer`` takes
its shape from its query, ``Sales.DeclaredCustomer`` declares a wider one. The
body is transport-neutral — it drives a ``BuildEnv`` and reads back through its
``query`` — so one small set of assertions covers the local emulator and Fabric,
and all environment setup lives in ``conftest``.
"""

from __future__ import annotations

import pytest
from build_envs import SQL_TABLE_FIXTURE, lakehouse_environments

from weaver import RepositoryRef

pytestmark = pytest.mark.parametrize("ses_fixture", [SQL_TABLE_FIXTURE], indirect=True)

REPO = "SqlTables"
AUDIT = {"row_insert_datetime", "row_update_datetime", "row_delete_datetime"}


def _tables(build_env, schema):
    return {row["tableName"].lower() for row in build_env.query(f"SHOW TABLES IN {schema}")}


def _columns(build_env, table):
    return {
        row["col_name"]: row["data_type"]
        for row in build_env.query(f"DESCRIBE TABLE {table}")
        if row["col_name"] and not row["col_name"].startswith("#")
    }


def _count(build_env, table):
    return next(iter(build_env.query(f"SELECT count(*) AS n FROM {table}")[0].values()))


def _install(build_env):
    build_env.install_repo(REPO)
    bundle = build_env.generate(repository_name=REPO)
    outcome = build_env.install(bundle)
    assert outcome.status == "succeeded", outcome.action_error
    return bundle


@lakehouse_environments
def test_both_schema_modes_build_only_the_main_table(build_env):
    _install(build_env)

    tables = _tables(build_env, "Sales")
    assert {"customer", "inferredcustomer", "declaredcustomer"} <= tables
    # Only the authored main tables — no view, no *_Current, no empty *_History.
    assert "inferredcustomer_current" not in tables
    assert "customer_history" not in tables

    for table in ("Sales.InferredCustomer", "Sales.DeclaredCustomer"):
        columns = {name.lower() for name in _columns(build_env, table)}
        assert {"customerid", "customername"} <= columns
        assert AUDIT <= columns
        # Build creates structure, not data — the base read() would raise if run.
        assert _count(build_env, table) == 0


@lakehouse_environments
def test_inferred_types_come_from_the_query_declared_from_the_declaration(build_env):
    _install(build_env)

    inferred = {n.lower(): t for n, t in _columns(build_env, "Sales.InferredCustomer").items()}
    declared = {n.lower(): t for n, t in _columns(build_env, "Sales.DeclaredCustomer").items()}
    # The base types CustomerId as int; the inferred table follows the query, the
    # declared table its wider declaration — the same on both engines.
    assert inferred["customerid"] == "int"
    assert declared["customerid"] == "bigint"


@lakehouse_environments
def test_dependency_order_places_the_base_before_its_readers(build_env):
    bundle = _install(build_env)

    at = {
        action.resource_node_id: seq.number
        for seq, _, action in bundle.plan.actions()
        if action.resource_node_id is not None
    }
    base = next(n for n in at if n.endswith("Sales.Customer"))
    for reader in ("delta:Sales.InferredCustomer", "delta:Sales.DeclaredCustomer"):
        assert at[base] < at[reader]


@lakehouse_environments
def test_rebuilding_is_deterministic_and_idempotent(build_env):
    build_env.install_repo(REPO)
    first = build_env.generate(repository_name=REPO)
    assert build_env.install(first).status == "succeeded"

    second = build_env.generate(bundle_name="rebuild", repository_name=REPO)
    assert second.bundle_id == first.bundle_id
    assert build_env.install(second).status == "succeeded"
