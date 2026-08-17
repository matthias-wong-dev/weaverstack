"""Session telemetry records resource events under reporting context."""

from __future__ import annotations

import pytest
from support.weaver_test import weaver_test

from weaver.sessions import ConsoleSession, SessionTelemetry
from weaver.sessions.sql import SessionSqlExecutor


@weaver_test()
def test_a_resource_event_inherits_task_step_and_substep_context():
    with ConsoleSession(progress=False) as session:
        with session.task("Build"):
            with session.step("Install"):
                with session.substep("Sales.Customer"):
                    with session.telemetry.external("livy", "submit"):
                        pass

    (event,) = session.telemetry.events()
    assert (event.resource, event.operation) == ("livy", "submit")
    assert (event.task, event.step, event.substep) == (
        "Build",
        "Install",
        "Sales.Customer",
    )
    assert not event.failed


@weaver_test()
def test_a_failed_external_operation_is_recorded_without_hiding_its_exception():
    telemetry = SessionTelemetry()

    with pytest.raises(RuntimeError, match="unavailable"):
        with telemetry.external("tds", "query"):
            raise RuntimeError("unavailable")

    (event,) = telemetry.events()
    assert event.failed
    assert telemetry.resources_used() == {"tds"}
    assert telemetry.by_resource()["tds"].failures == 1


@weaver_test()
def test_resource_and_semantic_aggregates_preserve_unattributed_work():
    telemetry = SessionTelemetry()
    with telemetry.external("rest", "get"):
        pass
    with ConsoleSession(telemetry=telemetry, progress=False) as session:
        with session.task("Load"):
            with session.step("Read catalogue"):
                with telemetry.external("tds", "query"):
                    pass

    assert set(telemetry.by_resource()) == {"rest", "tds"}
    assert set(telemetry.by_task()) == {"<unattributed>", "Load"}
    assert telemetry.by_step()[("Load", "Read catalogue")].calls == 1
    assert telemetry.by_resource_and_task()[("tds", "Load")].calls == 1


@weaver_test()
def test_a_session_sql_executor_records_every_tds_operation():
    from unittest.mock import Mock

    backing = Mock()
    backing.query.return_value = []
    backing.query_result_sets.return_value = ()
    backing.call_procedure.return_value = {}
    telemetry = SessionTelemetry()
    executor = SessionSqlExecutor(backing, telemetry)

    executor.execute("delete from T")
    executor.execute_script("create table T (x int)")
    executor.query("select * from T")
    executor.query_result_sets("select 1; select 2")
    executor.call_procedure("p", outputs=(("n", "int"),))
    executor.call_procedure_with_results("p", outputs=(("n", "int"),))

    assert [event.operation for event in telemetry.events()] == [
        "execute",
        "execute_script",
        "query",
        "query_result_sets",
        "call_procedure",
        "call_procedure_with_results",
    ]
    assert telemetry.resources_used() == {"tds"}
