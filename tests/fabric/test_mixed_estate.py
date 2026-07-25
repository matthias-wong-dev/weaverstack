"""The Lakehouse side of a mixed estate, on local Spark and Fabric alike.

The ``mixed-estate`` fixture (wired in ``conftest`` as ``MIXED_ESTATE_FIXTURE``)
chains a Folder, a Python Delta table, an inferred Spark SQL table and a Spark
view — plus Warehouse T-SQL objects that read the Lakehouse by hard-coded
three-part name. Binding the Lakehouse builds the whole Lakehouse chain in one
call and transparently omits the Warehouse leaves (built separately). One
transport-neutral body runs the same assertions in both environments.
"""

from __future__ import annotations

import pytest
from build_envs import MIXED_ESTATE_FIXTURE, lakehouse_environments

from weaver import FolderTarget

pytestmark = pytest.mark.parametrize("ses_fixture", [MIXED_ESTATE_FIXTURE], indirect=True)

REPO = "Mixed"
AUDIT = {"row_insert_datetime", "row_update_datetime", "row_delete_datetime"}


def _folder(build_env, schema, name):
    return build_env.resolver.folder_object(
        FolderTarget(lakehouse=build_env.target), schema, name
    )


def _columns(build_env, table):
    return {
        row["col_name"].lower()
        for row in build_env.query(f"DESCRIBE TABLE {table}")
        if row["col_name"] and not row["col_name"].startswith("#")
    }


def _count(build_env, table):
    return next(iter(build_env.query(f"SELECT count(*) AS n FROM {table}")[0].values()))


@lakehouse_environments
def test_the_whole_lakehouse_chain_builds_in_one_call(build_env):
    build_env.install_repo(REPO)
    bundle = build_env.generate(repository_name=REPO)

    # The Warehouse leaves are omitted from this Lakehouse-only build.
    assert {n.node_id for n in bundle.plan.omitted_nodes} == {
        "sql:Wh.CustomerReport",
        "sql:Wh.ActiveReport",
    }

    outcome = build_env.install(bundle)
    assert outcome.status == "succeeded", outcome.action_error

    # The Folder is a real directory under Files.
    assert build_env.store.exists(_folder(build_env, "Raw", "Orders"))

    # The Python base table and the inferred Spark table exist, empty, with audit
    # columns — build creates structure, never data (the base read() would raise).
    for table in ("Sales.Customer", "Sales.CustomerEnriched"):
        columns = _columns(build_env, table)
        assert {"customerid", "customername"} <= columns
        assert AUDIT <= columns
        assert _count(build_env, table) == 0

    # The Spark view resolves through the chain and is queryable.
    view_columns = {
        row["col_name"].lower()
        for row in build_env.query("DESCRIBE TABLE Sales.ActiveCustomer")
        if row["col_name"] and not row["col_name"].startswith("#")
    }
    assert {"customerid", "customername"} <= view_columns
    views = {row["viewName"].lower() for row in build_env.query("SHOW VIEWS IN Sales")}
    assert "activecustomer" in views
