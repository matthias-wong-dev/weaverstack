"""One Session across a run of commands, holding one of each expensive thing.

The architectural claim, against a real workspace: a console that runs several
commands acquires one credential, one resolver with one item cache, one Livy
session and one TDS connection per Warehouse — and closes only what it opened.

Deliberately cheap. Proving reuse does not require doing expensive work twice;
it requires showing that the second command finds what the first left behind.
"""

from __future__ import annotations

import pytest

from weaver.fabric.resources import LAKEHOUSE
from weaver.session.resources import ResourceState
from weaver.targets import ItemRef

pytestmark = [pytest.mark.fabric, pytest.mark.remote]


def test_one_resolver_serves_every_command_in_the_session(
    weaver_session, fabric_workspace, fabric_target_lakehouse
):
    first = weaver_session.resolver(fabric_workspace)
    second = weaver_session.resolver(fabric_workspace)

    assert first is second


def test_the_second_command_does_not_re_ask_what_the_first_resolved(
    weaver_session, fabric_workspace, fabric_target_lakehouse
):
    resolver = weaver_session.resolver(fabric_workspace)
    reference = ItemRef(fabric_target_lakehouse.name)

    weaver_session.resolve_item(
        reference, item_type=LAKEHOUSE, workspace=fabric_workspace
    )
    before = resolver.cache_hits
    weaver_session.resolve_item(
        reference, item_type=LAKEHOUSE, workspace=fabric_workspace
    )

    assert resolver.cache_hits == before + 1


def test_a_lakehouse_and_a_warehouse_of_the_same_name_stay_distinct(
    weaver_session, fabric_workspace, fabric_target_lakehouse
):
    """Identity is workspace + type + name, and the cache key must say so.

    A Lakehouse generates a SQL endpoint of its own name, so a cache keyed on
    the name alone would hand a Lakehouse back for its endpoint.
    """

    from weaver.fabric.resources import SQL_ENDPOINT

    reference = ItemRef(fabric_target_lakehouse.name)
    lakehouse = weaver_session.resolve_item(
        reference, item_type=LAKEHOUSE, workspace=fabric_workspace
    )
    endpoint = weaver_session.resolve_item(
        reference, item_type=SQL_ENDPOINT, workspace=fabric_workspace
    )

    assert lakehouse.id != endpoint.id


def test_the_session_starts_no_livy_of_its_own_when_it_was_given_one(
    weaver_session, fabric_workspace, livy_session
):
    scope = weaver_session.scope(fabric_workspace)

    assert scope.livy.get() is livy_session
    assert scope.livy.attempts == 1


def test_one_connection_per_warehouse_serves_every_command(
    weaver_session, fabric_workspace, disposable_warehouse
):
    first = weaver_session.sql_executor(
        disposable_warehouse.target, workspace=fabric_workspace
    )
    second = weaver_session.sql_executor(
        disposable_warehouse.target, workspace=fabric_workspace
    )

    assert first is second


def test_a_failed_statement_leaves_the_connection_healthy(
    weaver_session, fabric_workspace, disposable_warehouse
):
    """A statement fault is not a resource fault.

    This is the distinction that lets a console survive a mistake: a bad query
    reports, and the next command runs on the same connection rather than
    waiting for a new one.
    """

    from weaver.sql import SqlError

    with pytest.raises(SqlError):
        weaver_session.query_tsql(
            "SELECT * FROM dbo.a_table_that_is_not_there",
            target=disposable_warehouse.target,
            workspace=fabric_workspace,
        )

    rows = weaver_session.query_tsql(
        "SELECT 1 AS one",
        target=disposable_warehouse.target,
        workspace=fabric_workspace,
    )

    assert list(rows) == [{"one": 1}]
    scope = weaver_session.scope(fabric_workspace)
    assert scope._sql[disposable_warehouse.target.warehouse.name].state is (
        ResourceState.READY
    )


def test_the_session_records_what_it_spent(weaver_session, fabric_workspace):
    weaver_session.resolve_item(
        ItemRef(fabric_workspace.weaver_lakehouse),
        item_type=LAKEHOUSE,
        workspace=fabric_workspace,
    )

    assert weaver_session.telemetry.lifetime > 0
    assert "resolve.item" in weaver_session.telemetry.measures
