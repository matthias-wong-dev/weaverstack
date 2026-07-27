"""The Lakehouse side of a mixed estate, on local Spark and Fabric alike.

The ``mixed-estate`` fixture (wired in ``build_envs`` as ``MIXED_ESTATE_FIXTURE``)
chains a Folder, a Python Delta table, an inferred Spark SQL table and a Spark
view — plus Warehouse T-SQL objects that read the Lakehouse by hard-coded
three-part name. Binding the Lakehouse builds the whole Lakehouse chain in one
call and transparently omits the Warehouse leaves (built separately). The estate
is installed once per module (``lakehouse_estate``) and every assertion reuses it.
"""

from __future__ import annotations

import pytest
from build_envs import MIXED_ESTATE_FIXTURE

from weaver import FolderTarget

pytestmark = pytest.mark.parametrize("ses_fixture", [MIXED_ESTATE_FIXTURE], indirect=True)

AUDIT = {"row_insert_datetime", "row_update_datetime", "row_delete_datetime"}


def _folder(env, schema, name):
    return env.resolver.folder_object(FolderTarget(lakehouse=env.target), schema, name)


def _count(env, table):
    return next(iter(env.query(f"SELECT count(*) AS n FROM {table}")[0].values()))


def test_the_warehouse_leaves_are_omitted_from_the_lakehouse_build(lakehouse_estate):
    """Only the bound item is built; the Warehouse item is out of scope.

    Every other unbound item is omitted too — Weaver's own generated
    ``Lakehouse/_weaver`` among them — so this names the Warehouse documents
    rather than asserting the whole set.
    """

    omitted = {node.node_id for node in lakehouse_estate.bundle.plan.omitted_nodes}
    assert {
        "Warehouse/Reporting/Wh.CustomerReport",
        "Warehouse/Reporting/Wh.ActiveReport",
    } <= omitted
    assert not any(node.startswith("Lakehouse/Sales/") for node in omitted)


def test_the_folder_and_tables_are_built_empty_with_audit_columns(lakehouse_estate):
    env = lakehouse_estate.env

    # The Folder is a real directory under Files.
    assert env.store.exists(_folder(env, "Raw", "Orders"))

    # The Python base table and the inferred Spark table exist, empty, with audit
    # columns — build creates structure, never data (the base read() would raise).
    for table in ("{{object:Sales.Customer}}", "{{object:Sales.CustomerEnriched}}"):
        columns = {column["name"].lower() for column in env.columns(table)}
        assert {"customerid", "customername"} <= columns
        assert AUDIT <= columns
        assert _count(env, table) == 0


def test_the_spark_view_resolves_through_the_chain(lakehouse_estate):
    env = lakehouse_estate.env
    view_columns = {
        column["name"].lower()
        for column in env.columns("{{object:Sales.ActiveCustomer}}")
    }
    assert {"customerid", "customername"} <= view_columns
    views = {
        row["viewName"].lower()
        for row in env.query(f"SHOW VIEWS IN {env.schema_name('Sales')}")
    }
    assert "activecustomer" in views
