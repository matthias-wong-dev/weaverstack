"""The Weaver catalogue sharing a Warehouse with somebody else's schemas.

``catalogue="Warehouse/Curated"`` is a supported arrangement, not a workaround:
Weaver owns the ``_`` schema of its host and nothing else there. That the
*rendered statements* stay inside ``_`` is asserted without a tenant, in
``tests/test_shared_catalogue_host_invariant.py``. What needs a real Warehouse is
the other half — that a build reconciling ``_`` against a live estate leaves a
neighbour's schema, table and view exactly where they were.

The suite's catalogue Warehouse is the host. Seeding a neighbour into it is the
faithful test: the objects sit in the same database as ``_``, and a reconciliation
that reached too far would take them.
"""

from __future__ import annotations

import pytest

import weaver

pytestmark = [pytest.mark.fabric, pytest.mark.hosted]

#: A neighbour's schema in the catalogue's Warehouse. Neutral, and nothing to do
#: with Weaver — which is the point.
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
def catalogue_sql(fabric_workspace):
    """TDS against the Warehouse the catalogue lives in."""

    from weaver.fabric import FabricResolver, desktop_sql_executor
    from weaver.targets import WarehouseTarget

    target = WarehouseTarget(warehouse=fabric_workspace.catalogue_item)
    executor = desktop_sql_executor(
        target, fabric_workspace, resolver=FabricResolver(fabric_workspace)
    )
    try:
        yield executor
    finally:
        executor.close()


def _objects(sql, schema: str) -> set[str]:
    rows = sql.query(
        "select TABLE_NAME from INFORMATION_SCHEMA.TABLES "
        f"where TABLE_SCHEMA = N'{schema}'"
    )
    return {str(row["TABLE_NAME"]) for row in rows}


def test_a_build_reconciling_the_catalogue_leaves_a_neighbour_untouched(
    neighbour,
    weaver_session,
    fabric_workspace,
    fabric_target_lakehouse,
    tmp_path_factory,
):
    """One build against a shared catalogue host, and the neighbour survives it.

    A build reconciles ``_`` — it deletes obsolete rows, merges current ones and
    creates any catalogue table that is missing. All of that runs in the same
    Warehouse the neighbour is in.
    """

    from support.build_envs import CROSS_ITEM_JOURNEY_FIXTURE, DESKTOP_JOURNEY_NAMES

    before = _objects(neighbour, NEIGHBOUR_SCHEMA)
    assert before == {NEIGHBOUR_TABLE, NEIGHBOUR_VIEW}, (
        "the neighbour was not seeded, so this would pass for the wrong reason"
    )

    estate = CROSS_ITEM_JOURNEY_FIXTURE.renamed(
        tmp_path_factory.mktemp("shared-host"), DESKTOP_JOURNEY_NAMES
    )
    lakehouse = f"Lakehouse/{fabric_target_lakehouse.name}"
    built = weaver.build(
        str(estate.path),
        bind=[f"{lakehouse}=Stock"],
        session=weaver_session,
    )
    assert built.status == "succeeded", [
        (failure.action_id, failure.error_message) for failure in built.errors
    ]

    # The neighbour's objects, its rows and its view are all still there.
    assert _objects(neighbour, NEIGHBOUR_SCHEMA) == before
    rows = neighbour.query(
        f"select count(*) as n from [{NEIGHBOUR_SCHEMA}].[{NEIGHBOUR_VIEW}]"
    )
    assert rows[0]["n"] == 2

    # And Weaver's own schema is there beside it, holding what it certified.
    certified = neighbour.query(
        "select count(*) as n from [_].[Registry] where [Item name] = N'Stock'"
    )
    assert certified[0]["n"] > 0


def test_the_catalogue_warehouse_holds_both_schemas(neighbour):
    """The arrangement itself: `_` and a user's schema, in one Warehouse."""

    schemas = {
        str(row["SCHEMA_NAME"])
        for row in neighbour.query(
            "select SCHEMA_NAME from INFORMATION_SCHEMA.SCHEMATA"
        )
    }

    assert {"_", NEIGHBOUR_SCHEMA} <= schemas
