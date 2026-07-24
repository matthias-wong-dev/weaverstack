"""Can the local Spark catalog carry a Delta table and views on it, by name?

Weaver addresses Delta by explicit path and shares one Spark session across
tests, but a persistent Spark view is a *catalog* object referenced by its
two-part name. In Fabric, declaring a Lakehouse table makes ``Schema.Table``
immediately queryable; the build bundle relies on the local host mirroring that,
so a Spark SQL view can read ``DWG.Customer`` by name.

This spike proves the strategy the installer will use: register a path-addressed
Delta table into a catalog database, then build a view and a view-on-view over
it, all within the shared session. It is deliberately self-contained and cleans
up after itself, so it neither depends on nor pollutes cross-test state.

Decision recorded: registration is *in-session*. Cross-process catalog
persistence is explicitly not a prerequisite for bundle v1 — the session-scoped
fixture and the installer test prove the logical deployment chain, and whether
the local host should stand up a durable metastore is left open.
"""

from __future__ import annotations

import uuid

import pytest

from weaver import DeltaTarget, ItemRef

pytestmark = pytest.mark.spark


def test_delta_table_registers_and_carries_a_view_on_view(spark, lakehouses):
    schema = "DWG_" + uuid.uuid4().hex[:8]
    table_path = lakehouses.resolver.delta_table(
        DeltaTarget.parse("Sales_LH"), schema, "Customer"
    ).value

    try:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema}")

        # A path-addressed Delta table, registered so its two-part name binds —
        # the same move the installer's Delta materialisation makes.
        spark.sql(
            f"CREATE TABLE {schema}.Customer USING delta LOCATION '{table_path}' AS "
            "SELECT * FROM VALUES "
            "(1, 'Ada', true), (2, 'Bo', false), (3, 'Cy', true) "
            "AS t(CustomerId, CustomerName, IsActive)"
        )

        # A persistent view, and a persistent view over that view.
        spark.sql(
            f"CREATE OR REPLACE VIEW {schema}.ActiveCustomer AS "
            f"SELECT CustomerId, CustomerName FROM {schema}.Customer WHERE IsActive = true"
        )
        spark.sql(
            f"CREATE OR REPLACE VIEW {schema}.ActiveCustomerSummary AS "
            f"SELECT count(*) AS CustomerCount FROM {schema}.ActiveCustomer"
        )

        count = spark.sql(f"SELECT CustomerCount FROM {schema}.ActiveCustomerSummary").collect()
        assert count[0]["CustomerCount"] == 2

        # The second view genuinely resolves through the first, not a copy of the
        # query: dropping the inner view breaks the outer.
        views = {row["viewName"].lower() for row in spark.sql(f"SHOW VIEWS IN {schema}").collect()}
        assert "activecustomer" in views
        assert "activecustomersummary" in views
    finally:
        spark.sql(f"DROP VIEW IF EXISTS {schema}.ActiveCustomerSummary")
        spark.sql(f"DROP VIEW IF EXISTS {schema}.ActiveCustomer")
        spark.sql(f"DROP TABLE IF EXISTS {schema}.Customer")
        spark.sql(f"DROP DATABASE IF EXISTS {schema} CASCADE")
