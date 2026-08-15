"""What the Warehouse flusher promises a caller who does not wait.

Asynchronous logging is only safe if the contract is exact: a row a caller
handed over is either written or reported as unwritten, and a Session that
closed normally has written everything it accepted. Every claim below is one
sentence of that contract.
"""

from __future__ import annotations

import queue
import threading
import time

import pytest
from support.workspaces import given_workspace

from weaver.catalogue.flusher import FlushError, WarehouseFlusher
from weaver.catalogue.tables import LOG
from weaver.errors import CommandError
from weaver.sessions.testing import TestSession
from weaver.targets import WarehouseTarget


class Recorder:
    """Stands in for the Warehouse, and records the statements it was given."""

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


def test_submit_does_not_wait_for_the_warehouse():
    """The whole reason the flusher exists: a settled node does not block."""

    recorder = Recorder()
    recorder.block = True
    flusher = a_flusher(recorder)

    flusher.submit(a_row())
    # The worker is inside the write and is not coming back yet.
    assert recorder.started.wait(timeout=5)
    assert recorder.statements == []

    recorder.release.set()
    flusher.close()
    assert len(recorder.statements) == 1


def test_every_accepted_row_is_written():
    recorder = Recorder()
    flusher = a_flusher(recorder)

    for index in range(10):
        flusher.submit(a_row(object_name=f"Table{index}"))
    flusher.close()

    written = "\n".join(recorder.statements)
    assert flusher.written == 10
    for index in range(10):
        assert f"Table{index}" in written


def test_rows_are_written_in_the_order_they_were_submitted():
    recorder = Recorder()
    flusher = a_flusher(recorder)

    for index in range(20):
        flusher.submit(a_row(object_name=f"Table{index:02d}"))
    flusher.close()

    written = "\n".join(recorder.statements)
    positions = [written.index(f"Table{index:02d}") for index in range(20)]
    assert positions == sorted(positions)


def test_rows_may_batch_into_one_statement():
    """One INSERT per batch, not one per row — the cost this design is about."""

    recorder = Recorder()
    recorder.block = True
    flusher = a_flusher(recorder, batch_rows=5)

    for index in range(5):
        flusher.submit(a_row(object_name=f"Table{index}"))
    recorder.release.set()
    flusher.close()

    assert len(recorder.statements) < 5
    assert flusher.written == 5


def test_a_row_reaches_the_public_column_names():
    recorder = Recorder()
    flusher = a_flusher(recorder)

    flusher.submit(a_row())
    flusher.close()

    (statement,) = recorder.statements
    assert "[Workflow ID]" in statement
    assert "[Log SK]" in statement
    assert "[Row insert datetime]" in statement
    # And the frozen public vocabulary, not the internal spelling.
    assert "N'Succeeded'" in statement
    assert "succeeded" not in statement


# --- failure ------------------------------------------------------------------


def test_a_background_failure_is_surfaced_by_flush():
    """A run must never read an empty table as an empty run."""

    flusher = a_flusher(Recorder(fail=RuntimeError("the warehouse said no")))

    flusher.submit(a_row())

    with pytest.raises(FlushError, match="the warehouse said no"):
        flusher.flush()


def test_a_background_failure_is_surfaced_by_close():
    flusher = a_flusher(Recorder(fail=RuntimeError("the warehouse said no")))

    flusher.submit(a_row())

    with pytest.raises(FlushError, match="the warehouse said no"):
        flusher.close()


def test_a_closed_flusher_accepts_no_more_rows():
    flusher = a_flusher(Recorder())
    flusher.submit(a_row())
    flusher.close()

    with pytest.raises(FlushError, match="accepts no more rows"):
        flusher.submit(a_row())


def test_a_flusher_nothing_was_submitted_to_writes_nothing():
    recorder = Recorder()
    flusher = a_flusher(recorder)

    flusher.flush()
    flusher.close()

    assert recorder.statements == []


# --- the Session owns them ----------------------------------------------------


def _session():
    return TestSession(workspace=given_workspace(catalogue="Warehouse/Weaver"))


def test_opening_a_session_creates_no_flusher():
    """Most Sessions never log; none should pay a worker or a connection for it."""

    with _session() as session:
        assert session._flushers == {}


def test_a_session_reuses_one_flusher_for_one_stream():
    warehouse = WarehouseTarget.parse("Weaver")
    with _session() as session:
        first = session.flusher(LOG, warehouse=warehouse)
        second = session.flusher(LOG, warehouse=warehouse)

        assert first is second


def test_two_warehouses_are_two_streams():
    with _session() as session:
        first = session.flusher(LOG, warehouse=WarehouseTarget.parse("Weaver"))
        second = session.flusher(LOG, warehouse=WarehouseTarget.parse("Other"))

        assert first is not second


def test_session_close_writes_every_accepted_row():
    """Normal completion is the durability barrier the design promises."""

    session = _session()
    flusher = session.flusher(LOG, warehouse=WarehouseTarget.parse("Weaver"))
    for index in range(4):
        flusher.submit(a_row(object_name=f"Table{index}"))

    session.close()

    assert flusher.written == 4
    assert flusher.submitted == 4


def test_the_final_flush_happens_while_the_session_can_still_write():
    """The order in `Session.close` *is* the durability guarantee.

    A flusher writes through the Session, and a closed Session refuses to hand
    out a scope. So marking the Session closed before draining would fail
    exactly the writes the barrier exists to complete — and only when the worker
    is still behind, which is to say only under load.
    """

    written_while_open: list[bool] = []
    session = _session()
    holding = threading.Event()
    release = threading.Event()
    flusher = session.flusher(LOG, warehouse=WarehouseTarget.parse("Weaver"))

    def write(_statement: str) -> None:
        # Held until `close` is under way, so the write lands *during* the
        # barrier rather than before it — which is the only moment the ordering
        # is observable, and the moment a busy run is always in.
        holding.set()
        release.wait(timeout=5)
        # What a real Session does here is ask for a scope, which a closed one
        # refuses. Recorded rather than raised so a failure names the cause.
        written_while_open.append(not session.closed)

    flusher._execute = write
    flusher.submit(a_row())
    assert holding.wait(timeout=5), "the worker never reached the write"

    # Let go a moment after close has begun.
    threading.Timer(0.2, release.set).start()
    session.close()

    assert written_while_open == [True], (
        "the row was written after the Session had been marked closed"
    )


def test_a_closed_session_hands_out_no_flusher():
    session = _session()
    session.close()

    with pytest.raises(CommandError, match="closed"):
        session.flusher(LOG, warehouse=WarehouseTarget.parse("Weaver"))


def test_session_close_surfaces_a_background_failure():
    session = _session()
    flusher = session.flusher(LOG, warehouse=WarehouseTarget.parse("Weaver"))
    flusher._execute = Recorder(fail=RuntimeError("the warehouse said no"))
    flusher.submit(a_row())

    with pytest.raises(FlushError, match="the warehouse said no"):
        session.close()


def test_a_flusher_writes_through_the_session(monkeypatch):
    """The statement reaches the Session's own T-SQL capability, not a new one."""

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


def test_a_slow_worker_does_not_hang_a_flush_for_ever():
    """A wedged connection must not hold a finished run open indefinitely."""

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
    """A queue that holds one row's ``put`` open, to catch a submit in flight."""

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


def test_a_row_accepted_before_close_is_written_even_if_close_overtakes_it():
    """Accepting a row and queueing it is one step, or the row can be lost.

    A flusher is shared by every caller appending to one table, so a submit can
    be in flight when another thread closes the Session. If the stop sentinel
    can be queued between the two, the worker stops before reaching the row and
    ``close`` returns reporting nothing wrong.
    """

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

    assert flusher.submitted == 2
    assert flusher.written == 2, "a row the flusher accepted never reached the table"


def test_a_stream_opened_while_the_session_is_closing_is_refused():
    """A flusher created after the drain began would never be drained.

    `close` takes the flushers it knows about and waits for them. One handed
    out after that would hold rows nobody waits for, and the Session would
    return having lost them without saying so.
    """

    session = _session()
    started = threading.Event()
    release = threading.Event()
    flusher = session.flusher(LOG, warehouse=WarehouseTarget.parse("Weaver"))

    def write(_statement: str) -> None:
        started.set()
        release.wait(timeout=5)

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
    assert flusher.written == 1
