"""Asynchronous Warehouse append and its Session durability barrier."""

from __future__ import annotations

import queue
import threading
import time

import pytest
from support.weaver_test import weaver_test
from support.workspaces import given_workspace

from weaver.catalogue.flusher import FlushError, WarehouseFlusher
from weaver.catalogue.tables import LOG
from weaver.errors import CommandError
from weaver.sessions import ConsoleSession
from weaver.sessions.testing import TestSession
from weaver.targets import WarehouseTarget


class Recorder:
    def __init__(self, fail: Exception | None = None) -> None:
        self.statements: list[str] = []
        self.fail = fail
        self.started = threading.Event()
        self.release = threading.Event()
        self.block = False

    def __call__(self, statement: str) -> None:
        self.started.set()
        if self.block:
            self.release.wait(timeout=5)
        self.statements.append(statement)
        if self.fail is not None:
            raise self.fail


def a_row(workflow: str = "w-1", **overrides) -> dict:
    row = {
        "log_sk": "sk-1",
        "workflow_id": workflow,
        "task_type": "load",
        "target_type": "lakehouse",
        "target_name": "Sales",
        "schema_name": "DWG",
        "object_name": "Customer",
        "result": "succeeded",
        "message": None,
        "details": None,
    }
    row.update(overrides)
    return row


def a_flusher(execute, **kwargs) -> WarehouseFlusher:
    from weaver.catalogue.flusher import FlusherKey

    return WarehouseFlusher(
        LOG,
        execute=execute,
        key=FlusherKey(workspace="Demo", warehouse="Weaver", schema="_", table="Log"),
        **kwargs,
    )


# --- accepting a row ----------------------------------------------------------


@weaver_test()
def test_submit_does_not_wait_for_the_warehouse():
    recorder = Recorder()
    recorder.block = True
    flusher = a_flusher(recorder)

    flusher.submit(a_row())
    assert recorder.started.wait(timeout=5)
    assert recorder.statements == []

    recorder.release.set()
    flusher.close()
    assert len(recorder.statements) == 1


@weaver_test()
def test_every_accepted_row_is_written():
    recorder = Recorder()
    flusher = a_flusher(recorder)

    for index in range(10):
        flusher.submit(a_row(object_name=f"Table{index}"))
    flusher.close()

    written = "\n".join(recorder.statements)
    for index in range(10):
        assert f"Table{index}" in written


@weaver_test()
def test_a_worker_tds_event_keeps_the_context_that_queued_it():
    with ConsoleSession(progress=False) as session:

        def execute(statement):
            with session.telemetry.external("tds", "execute"):
                pass

        flusher = a_flusher(
            execute,
            capture_context=session.telemetry.capture_context,
            use_context=session.telemetry.use_context,
        )
        with session.task("Load"):
            with session.step("Logging"):
                flusher.submit(a_row())
        flusher.flush()
        flusher.close()

    (event,) = session.telemetry.events()
    assert (event.task, event.step, event.substep) == ("Load", "Logging", None)


@weaver_test()
def test_rows_are_written_in_the_order_they_were_submitted():
    recorder = Recorder()
    flusher = a_flusher(recorder)

    for index in range(20):
        flusher.submit(a_row(object_name=f"Table{index:02d}"))
    flusher.close()

    written = "\n".join(recorder.statements)
    positions = [written.index(f"Table{index:02d}") for index in range(20)]
    assert positions == sorted(positions)


@weaver_test()
def test_rows_may_batch_into_one_statement():
    recorder = Recorder()
    recorder.block = True
    flusher = a_flusher(recorder, batch_rows=5)

    for index in range(5):
        flusher.submit(a_row(object_name=f"Table{index}"))
    recorder.release.set()
    flusher.close()

    assert len(recorder.statements) < 5


@weaver_test()
def test_a_row_reaches_the_public_column_names():
    recorder = Recorder()
    flusher = a_flusher(recorder)

    flusher.submit(a_row())
    flusher.close()

    (statement,) = recorder.statements
    assert "[Workflow ID]" in statement
    assert "[Log SK]" in statement
    assert "[Row insert datetime]" in statement
    assert "N'Succeeded'" in statement
    assert "succeeded" not in statement


# --- failure ------------------------------------------------------------------


@weaver_test()
def test_a_background_failure_is_surfaced_by_flush():
    flusher = a_flusher(Recorder(fail=RuntimeError("the warehouse said no")))

    flusher.submit(a_row())

    with pytest.raises(FlushError, match="the warehouse said no"):
        flusher.flush()


@weaver_test()
def test_a_background_failure_is_surfaced_by_close():
    flusher = a_flusher(Recorder(fail=RuntimeError("the warehouse said no")))

    flusher.submit(a_row())

    with pytest.raises(FlushError, match="the warehouse said no"):
        flusher.close()


@weaver_test()
def test_a_closed_flusher_accepts_no_more_rows():
    flusher = a_flusher(Recorder())
    flusher.submit(a_row())
    flusher.close()

    with pytest.raises(FlushError, match="accepts no more rows"):
        flusher.submit(a_row())


@weaver_test()
def test_a_flusher_nothing_was_submitted_to_writes_nothing():
    recorder = Recorder()
    flusher = a_flusher(recorder)

    flusher.flush()
    flusher.close()

    assert recorder.statements == []


# --- the Session owns them ----------------------------------------------------


def _session():
    return TestSession(workspace=given_workspace(catalogue="Warehouse/Weaver"))


@weaver_test()
def test_opening_a_session_creates_no_flusher():
    with _session() as session:
        assert session._flushers == {}


@weaver_test()
def test_a_session_reuses_one_flusher_for_one_stream():
    warehouse = WarehouseTarget.parse("Weaver")
    with _session() as session:
        first = session.flusher(LOG, warehouse=warehouse)
        second = session.flusher(LOG, warehouse=warehouse)

        assert first is second


@weaver_test()
def test_two_warehouses_are_two_streams():
    with _session() as session:
        first = session.flusher(LOG, warehouse=WarehouseTarget.parse("Weaver"))
        second = session.flusher(LOG, warehouse=WarehouseTarget.parse("Other"))

        assert first is not second


@weaver_test()
def test_session_close_writes_every_accepted_row():
    session = _session()
    flusher = session.flusher(LOG, warehouse=WarehouseTarget.parse("Weaver"))
    for index in range(4):
        flusher.submit(a_row(object_name=f"Table{index}"))

    session.close()

    written = "\n".join(session.tsql)
    for index in range(4):
        assert f"Table{index}" in written


@weaver_test()
def test_the_final_flush_happens_while_the_session_can_still_write():
    written_while_open: list[bool] = []
    session = _session()
    holding = threading.Event()
    release = threading.Event()
    flusher = session.flusher(LOG, warehouse=WarehouseTarget.parse("Weaver"))

    def write(_statement: str) -> None:
        holding.set()
        release.wait(timeout=5)
        written_while_open.append(not session.closed)

    flusher._execute = write
    flusher.submit(a_row())
    assert holding.wait(timeout=5), "the worker never reached the write"

    threading.Timer(0.2, release.set).start()
    session.close()

    assert written_while_open == [True], (
        "the row was written after the Session had been marked closed"
    )


@weaver_test()
def test_a_closed_session_hands_out_no_flusher():
    session = _session()
    session.close()

    with pytest.raises(CommandError, match="closed"):
        session.flusher(LOG, warehouse=WarehouseTarget.parse("Weaver"))


@weaver_test()
def test_session_close_surfaces_a_background_failure():
    session = _session()
    flusher = session.flusher(LOG, warehouse=WarehouseTarget.parse("Weaver"))
    flusher._execute = Recorder(fail=RuntimeError("the warehouse said no"))
    flusher.submit(a_row())

    with pytest.raises(FlushError, match="the warehouse said no"):
        session.close()

    assert not session.closed


@weaver_test()
def test_close_reports_a_worker_that_did_not_stop():
    recorder = Recorder()
    recorder.block = True
    flusher = a_flusher(recorder)
    flusher.submit(a_row())
    assert recorder.started.wait(timeout=5)
    worker = flusher._worker

    with pytest.raises(FlushError, match="worker did not stop"):
        flusher.close(timeout=0.1)

    assert flusher._worker is worker
    assert worker is not None and worker.is_alive()
    recorder.release.set()
    flusher.close()


@weaver_test()
def test_a_flusher_timeout_stops_session_teardown():
    recorder = Recorder()
    recorder.block = True
    session = _session()
    flusher = session.flusher(LOG, warehouse=WarehouseTarget.parse("Weaver"))
    flusher._execute = recorder
    flusher.submit(a_row())
    assert recorder.started.wait(timeout=5)
    close = flusher.close
    flusher.close = lambda: close(timeout=0.1)

    with pytest.raises(FlushError, match="worker did not stop"):
        session.close()

    assert not session.closed
    recorder.release.set()
    flusher.close = close
    session.close()


@weaver_test()
def test_a_flusher_writes_through_the_session(monkeypatch):
    seen: list[tuple[str, object]] = []
    session = _session()
    monkeypatch.setattr(
        session,
        "execute_tsql",
        lambda statement, *, target, workspace=None: seen.append((statement, target)),
    )
    warehouse = WarehouseTarget.parse("Weaver")
    flusher = session.flusher(LOG, warehouse=warehouse)
    flusher.submit(a_row())
    session.close()

    (statement, target) = seen[0]
    assert "INSERT INTO [_].[Log]" in statement
    assert target is warehouse


@weaver_test()
def test_a_slow_worker_does_not_hang_a_flush_for_ever():
    recorder = Recorder()
    recorder.block = True
    flusher = a_flusher(recorder)
    flusher.submit(a_row())
    assert recorder.started.wait(timeout=5)

    started = time.monotonic()
    with pytest.raises(FlushError, match="unwritten"):
        flusher.flush(timeout=0.2)
    assert time.monotonic() - started < 5

    recorder.release.set()


# --- accepting a row while the Session is closing -----------------------------


class GatedQueue(queue.Queue):
    def __init__(self) -> None:
        super().__init__()
        self.gate_on: str | None = None
        self.entered = threading.Event()
        self.proceed = threading.Event()

    def put(self, item, *args, **kwargs):
        if isinstance(item, dict) and item.get("object_name") == self.gate_on:
            self.entered.set()
            self.proceed.wait(timeout=5)
        super().put(item, *args, **kwargs)


@weaver_test()
def test_a_row_accepted_before_close_is_written_even_if_close_overtakes_it():
    recorder = Recorder()
    flusher = a_flusher(recorder)
    gated = GatedQueue()
    flusher._queue = gated
    gated.gate_on = "Second"

    flusher.submit(a_row(object_name="First"))
    flusher.flush()

    late = threading.Thread(target=lambda: flusher.submit(a_row(object_name="Second")))
    late.start()
    assert gated.entered.wait(timeout=5)

    closing = threading.Thread(target=flusher.close)
    closing.start()
    time.sleep(0.05)
    gated.proceed.set()
    late.join(timeout=5)
    closing.join(timeout=5)

    written = "\n".join(recorder.statements)
    assert "First" in written
    assert "Second" in written


@weaver_test()
def test_a_stream_opened_while_the_session_is_closing_is_refused():
    session = _session()
    started = threading.Event()
    release = threading.Event()
    written = []
    flusher = session.flusher(LOG, warehouse=WarehouseTarget.parse("Weaver"))

    def write(_statement: str) -> None:
        started.set()
        release.wait(timeout=5)
        written.append(True)

    flusher._execute = write
    flusher.submit(a_row())
    assert started.wait(timeout=5)

    closing = threading.Thread(target=session.close)
    closing.start()
    time.sleep(0.05)

    with pytest.raises(CommandError, match="closing"):
        session.flusher(LOG, warehouse=WarehouseTarget.parse("Other"))

    release.set()
    closing.join(timeout=5)
    assert written == [True]


@weaver_test()
def test_run_logs_inherit_the_session_workflow():
    from weaver.catalogue.state import catalogue_for
    from weaver.run import open_run_log

    with _session() as session:
        with session.workflow("compose-1"):
            catalogue = catalogue_for(session, session.workspace)
            load = open_run_log(catalogue, task_type="load", session=session)
            test = open_run_log(catalogue, task_type="test", session=session)

    assert load.workflow_id == "compose-1"
    assert test.workflow_id == "compose-1"
