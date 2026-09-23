"""What only a real workspace answers about a Session's reused resources.

Reuse itself, one credential, resolver, Livy session and connection per
Session, is proven against a `TestSession` in `tests/test_session_representation.py`
and `tests/test_session_resource_cycle.py`. What stays here is Fabric's: a
Lakehouse and its SQL endpoint share a name, and a failed statement leaves a
real TDS connection usable.
"""

from __future__ import annotations

import pytest
from support.weaver_test import weaver_test

from weaver.fabric.resources import LAKEHOUSE
from weaver.sessions.resources import ResourceState
from weaver.targets import ItemRef


@weaver_test(remote=True, resources={"rest"})
def test_a_lakehouse_and_a_warehouse_of_the_same_name_stay_distinct(
    fresh_weaver_session, fabric_workspace, fabric_target_lakehouse
):
    """Identity is workspace + type + name, and the cache key must say so.

    A Lakehouse generates a SQL endpoint of its own name, so a cache keyed on
    the name alone would hand a Lakehouse back for its endpoint.
    """

    from weaver.fabric.resources import SQL_ENDPOINT

    reference = ItemRef(fabric_target_lakehouse.name)
    lakehouse = fresh_weaver_session.resolve_item(
        reference, item_type=LAKEHOUSE, workspace=fabric_workspace
    )
    endpoint = fresh_weaver_session.resolve_item(
        reference, item_type=SQL_ENDPOINT, workspace=fabric_workspace
    )

    assert lakehouse.id != endpoint.id


@weaver_test(remote=True, resources={"tds"})
def test_a_failed_statement_leaves_the_connection_healthy(
    ready_warehouse_session, fabric_workspace, disposable_warehouse
):
    """A statement fault is not a resource fault.

    This is the distinction that lets a console survive a mistake: a bad query
    reports, and the next command runs on the same connection rather than
    waiting for a new one.
    """

    from weaver.sql import SqlError

    with pytest.raises(SqlError):
        ready_warehouse_session.query_tsql(
            "SELECT * FROM dbo.a_table_that_is_not_there",
            target=disposable_warehouse.target,
            workspace=fabric_workspace,
        )

    rows = ready_warehouse_session.query_tsql(
        "SELECT 1 AS one",
        target=disposable_warehouse.target,
        workspace=fabric_workspace,
    )

    assert list(rows) == [{"one": 1}]
    scope = ready_warehouse_session.scope(fabric_workspace)
    assert scope._sql[disposable_warehouse.target.warehouse.name].state is (
        ResourceState.READY
    )
