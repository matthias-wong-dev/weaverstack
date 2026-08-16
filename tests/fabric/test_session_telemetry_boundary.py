"""Production resource boundaries emit semantically attributed telemetry."""

from __future__ import annotations

import pytest
from support.weaver_test import weaver_test

from weaver.fabric import LAKEHOUSE, onelake_url
from weaver.locations import Location
from weaver.sessions import ConsoleSession
from weaver.targets import ItemRef


@pytest.fixture
def telemetry_console():
    from support.weaver_test import register_session

    with ConsoleSession(progress=False) as session:
        yield register_session(session)


def _event(session, resource):
    return next(
        event
        for event in reversed(session.telemetry.events())
        if event.resource == resource
    )


@weaver_test(remote=True, resources={"rest"})
def test_item_resolution_records_rest_under_its_reporting_context(
    telemetry_console, fabric_workspace, fabric_target_lakehouse
):
    with telemetry_console.task("Telemetry"):
        with telemetry_console.step("Resolve item"):
            telemetry_console.resolve_item(
                ItemRef(fabric_target_lakehouse.name),
                item_type=LAKEHOUSE,
                workspace=fabric_workspace,
            )

    event = _event(telemetry_console, "rest")
    assert (event.task, event.step, event.substep) == (
        "Telemetry",
        "Resolve item",
        None,
    )


@weaver_test(remote=True, resources={"tds"})
def test_warehouse_query_records_tds_under_its_reporting_context(
    ready_warehouse_session, fabric_workspace, disposable_warehouse
):
    with ready_warehouse_session.task("Telemetry"):
        with ready_warehouse_session.step("Read catalogue"):
            rows = ready_warehouse_session.query_tsql(
                "SELECT 1 AS value",
                target=disposable_warehouse.target,
                workspace=fabric_workspace,
            )

    assert list(rows) == [{"value": 1}]
    event = _event(ready_warehouse_session, "tds")
    assert (event.task, event.step, event.substep) == (
        "Telemetry",
        "Read catalogue",
        None,
    )


@weaver_test(remote=True, resources={"livy"})
def test_spark_sql_records_livy_under_its_reporting_context(
    weaver_session, fabric_workspace
):
    with weaver_session.task("Telemetry"):
        with weaver_session.step("Execute Spark SQL"):
            rows = weaver_session.execute_spark_sql(
                "SELECT 1 AS value", workspace=fabric_workspace
            )

    assert rows == [{"value": 1}]
    event = _event(weaver_session, "livy")
    assert (event.task, event.step, event.substep) == (
        "Telemetry",
        "Execute Spark SQL",
        None,
    )


@weaver_test(remote=True, resources={"onelake"})
def test_session_transport_store_records_onelake_under_its_reporting_context(
    telemetry_console, fabric_workspace, fabric_workspace_item, fabric_staging_lakehouse
):
    location = Location(
        onelake_url(
            fabric_workspace_item.id,
            fabric_staging_lakehouse.id,
            "Files/weaver_telemetry/probe.txt",
        )
    )
    with telemetry_console.task("Telemetry"):
        with telemetry_console.step("Write repository"):
            store = telemetry_console.transport_store(fabric_workspace)
            store.write(location, b"telemetry\n")
            assert store.read(location) == b"telemetry\n"
            store.delete(location)

    event = _event(telemetry_console, "onelake")
    assert (event.task, event.step, event.substep) == (
        "Telemetry",
        "Write repository",
        None,
    )
