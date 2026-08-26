"""The catalogue shares a Warehouse with a user-owned schema.

Weaver owns ``_`` in the catalogue Warehouse and nothing else, so a neighbour's
schema sits beside it. This reads the Warehouse and finds both.

That a build reconciling the catalogue leaves the neighbour alone was a hosted
claim here, costing a full cross-item build for one assertion. The acceptance
journey builds and reconciles the same catalogue.
"""

from __future__ import annotations

import pytest
from support.weaver_test import weaver_test

NEIGHBOUR_SCHEMA = "Finance"
NEIGHBOUR_TABLE = "Ledger"
NEIGHBOUR_VIEW = "OpenLedger"


@pytest.fixture
def neighbour(catalogue_sql):
    """A schema, a table and a view a user owns, inside the catalogue Warehouse."""

    catalogue_sql.execute_script(
        f"if not exists (select 1 from sys.schemas where name = N'{NEIGHBOUR_SCHEMA}')"
        f" exec('create schema [{NEIGHBOUR_SCHEMA}]');"
    )
    catalogue_sql.execute_script(
        f"drop view if exists [{NEIGHBOUR_SCHEMA}].[{NEIGHBOUR_VIEW}];"
    )
    catalogue_sql.execute_script(
        f"drop table if exists [{NEIGHBOUR_SCHEMA}].[{NEIGHBOUR_TABLE}];"
    )
    catalogue_sql.execute_script(
        f"create table [{NEIGHBOUR_SCHEMA}].[{NEIGHBOUR_TABLE}] "
        "([Entry id] int not null, [Amount] decimal(10,2) not null);"
    )
    catalogue_sql.execute_script(
        f"insert into [{NEIGHBOUR_SCHEMA}].[{NEIGHBOUR_TABLE}] "
        "([Entry id], [Amount]) values (1, 10.00), (2, 20.00);"
    )
    catalogue_sql.execute_script(
        f"create view [{NEIGHBOUR_SCHEMA}].[{NEIGHBOUR_VIEW}] as "
        f"select [Entry id], [Amount] from [{NEIGHBOUR_SCHEMA}].[{NEIGHBOUR_TABLE}];"
    )
    return catalogue_sql


@pytest.fixture
def catalogue_sql(session_catalogue_sql):
    """TDS against the Warehouse the catalogue lives in."""

    return session_catalogue_sql


def _objects(sql, schema: str) -> set[str]:
    rows = sql.query(
        "select TABLE_NAME from INFORMATION_SCHEMA.TABLES "
        f"where TABLE_SCHEMA = N'{schema}'"
    )
    return {str(row["TABLE_NAME"]) for row in rows}


@weaver_test(remote=True, resources={"tds"})
def test_the_catalogue_warehouse_holds_both_schemas(neighbour):
    schemas = {
        str(row["SCHEMA_NAME"])
        for row in neighbour.query(
            "select SCHEMA_NAME from INFORMATION_SCHEMA.SCHEMATA"
        )
    }

    assert {"_", NEIGHBOUR_SCHEMA} <= schemas
