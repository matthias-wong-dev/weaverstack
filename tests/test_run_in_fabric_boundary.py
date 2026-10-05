"""A run with Spark work, sent whole from a client to Fabric.

The client stages the arguments, submits one statement and presents what Fabric
reports while it runs. Fabric is doubled here as a Session whose ``execute_python``
does what the submitted statement does: it reads the stage, writes its progress
and leaves the report. Whether Fabric really does that is the Fabric suite's.
"""

from __future__ import annotations

import ast
import hashlib
import json
import re

import pytest
from support.weaver_test import weaver_test

from weaver.errors import OutcomeUnknown
from weaver.fabric import LivyError
from weaver.locations import Location
from weaver.run.result import RunError
from weaver.sessions.program import FabricRun
from weaver.sessions.run_in_fabric import (
    PROGRESS,
    REQUEST,
    RESULT,
    RUN_TIMEOUT,
    progress_written,
)
from weaver.sessions.testing import TestSession
from weaver.store import StoreError
from weaver.workspaces import Workspace

WORKSPACE = Workspace(
    workspace="Analytics", catalogue="Warehouse/Weaver", environment="weaver"
)
FILES = "abfss://ws@onelake.dfs.fabric.microsoft.com/Sales/Files"


class Store:
    """OneLake as both sides reach it: bytes by location."""

    def __init__(self) -> None:
        self.files: dict[str, bytes] = {}

    def make_directory(self, location) -> None:
        pass

    def write(self, location, data: bytes) -> None:
        self.files[location.value] = data

    def read(self, location) -> bytes:
        if location.value not in self.files:
            raise StoreError(f"cannot read {location.value}")
        return self.files[location.value]

    def delete(self, location, *, recursive: bool = False) -> None:
        for key in [key for key in self.files if key.startswith(location.value)]:
            del self.files[key]


class Resolver:
    def files_root(self, item):
        return Location(FILES)


class Fabric(TestSession):
    """A client whose submitted statement runs as Fabric would run it."""

    def __init__(self, *, progress=(), report=None, lost=False, tamper=False):
        super().__init__(workspace=WORKSPACE, resolver=Resolver(), store=Store())
        self.offer_spark_home(["Sales"])
        self.programs = []
        self.requests = []
        self._progress = list(progress)
        self._report = {"status": "succeeded"} if report is None else report
        self._lost = lost
        self._tamper = tamper

    def execute_python(self, program, *, workspace=None, timeout=None):
        self.programs.append(program)
        store = self.scope(workspace).store
        stage = Location(re.search(r"stage='([^']+)'", program.source).group(1))
        self.requests.append(json.loads(store.read(stage / REQUEST)))
        store.write(stage / PROGRESS, json.dumps(self._progress).encode())
        if self._lost:
            raise LivyError("the connection was reset")
        data = json.dumps({"report": self._report, "warnings": ["mind"]}).encode()
        store.write(stage / RESULT, data + (b" " if self._tamper else b""))
        return {"bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}


def _entry(*, session, workspace, **arguments):  # pragma: no cover - Fabric's
    return {}


def _run(session, **arguments):
    return FabricRun(
        name="load",
        needs_spark=True,
        call=lambda here: "here",
        entry=_entry,
        arguments=lambda: arguments,
        decode=lambda carried: ("decoded", carried),
    )


def _send(session, run):
    from weaver.sessions.run_in_fabric import send

    return send(session, run, workspace=WORKSPACE)


# --- what crosses -------------------------------------------------------------


@weaver_test()
def test_the_arguments_are_staged_and_the_report_decoded():
    session = Fabric(report={"status": "succeeded", "nodes": []})

    report = _send(session, _run(session, request={"items": ["Lakehouse/Sales"]}))

    assert session.requests == [{"request": {"items": ["Lakehouse/Sales"]}}]
    assert report == ("decoded", {"status": "succeeded", "nodes": []})


@weaver_test()
def test_a_run_crosses_once_and_is_never_sent_again():
    """It records itself, so a resubmitted statement would run it twice."""

    session = Fabric()

    _send(session, _run(session))

    (program,) = session.programs
    assert program.resubmit is False
    assert program.timeout == RUN_TIMEOUT
    ast.parse(program.source)
    assert "run_staged(_entry, session=session" in program.source
    assert "NotebookSession(workspace=workspace, spark=spark)" in program.source


@weaver_test()
def test_fabric_warnings_reach_the_client():
    session = Fabric()

    _send(session, _run(session))

    assert "mind" in session.warnings


@weaver_test()
def test_the_stage_is_removed_once_the_report_is_read():
    session = Fabric()

    _send(session, _run(session))

    assert session.scope(WORKSPACE).store.files == {}


# --- what can go wrong --------------------------------------------------------


@weaver_test()
def test_a_lost_response_leaves_the_outcome_unknown():
    session = Fabric(lost=True)

    with pytest.raises(OutcomeUnknown, match="catalogue log records what it did"):
        _send(session, _run(session))

    assert len(session.programs) == 1
    assert session.scope(WORKSPACE).store.files == {}


@weaver_test()
def test_a_report_that_differs_from_its_receipt_is_refused():
    session = Fabric(tamper=True)

    with pytest.raises(RunError, match="incomplete"):
        _send(session, _run(session))


# --- what the operator sees ---------------------------------------------------


def _frames(*records):
    return [dict(kind="substep", detail=None, **record) for record in records]


@weaver_test()
def test_each_node_is_shown_with_the_time_fabric_measured():
    session = Fabric(
        progress=[
            {"kind": "step", "event": "started", "id": 1, "name": "Execute"},
            *_frames(
                {"event": "started", "id": 2, "name": "Load Sales.Customer"},
                {
                    "event": "completed",
                    "id": 2,
                    "name": "Load Sales.Customer",
                    "failed": False,
                    "note": "(read 5, +5 ~0 -0 !0)",
                    "elapsed": 7.5,
                },
            ),
            {"kind": "step", "event": "completed", "id": 1, "name": "Execute"},
        ]
    )

    _send(session, _run(session))

    shown = {frame.name: frame for frame in session.timings}
    assert shown["Load Sales.Customer"].elapsed == 7.5
    assert shown["Load Sales.Customer"].note == "(read 5, +5 ~0 -0 !0)"
    assert not shown["Load Sales.Customer"].failed
    assert "Execute" in shown


@weaver_test()
def test_a_node_fabric_never_finished_is_shown_as_failed():
    session = Fabric(
        progress=_frames({"event": "started", "id": 2, "name": "Load Sales.Order"}),
        lost=True,
    )

    with pytest.raises(OutcomeUnknown):
        _send(session, _run(session))

    (frame,) = [frame for frame in session.timings if frame.name == "Load Sales.Order"]
    assert frame.failed


# --- where a run goes ---------------------------------------------------------


@weaver_test()
def test_a_client_sends_a_run_with_spark_work_and_runs_anything_else_itself():
    session = TestSession(workspace=WORKSPACE)
    session.answer_python({"status": "succeeded"})

    sent = session.execute_run(_run(session, items=["Lakehouse/Sales"]))
    here = session.execute_run(
        FabricRun(
            name="load",
            needs_spark=False,
            call=lambda here: "here",
            entry=_entry,
            arguments=dict,
            decode=dict,
        )
    )

    assert sent == ("decoded", {"status": "succeeded"})
    assert here == "here"
    assert [call.kind for call in session.calls] == ["run"]
    assert session.calls[0].body == {
        "name": "load",
        "arguments": {"items": ["Lakehouse/Sales"]},
    }


@weaver_test()
def test_in_fabric_a_run_with_spark_work_runs_in_place():
    session = TestSession(workspace=WORKSPACE, executes_here=True)

    assert session.execute_run(_run(session)) == "here"
    assert session.calls == []


# --- Fabric's half ------------------------------------------------------------


@weaver_test()
def test_fabric_writes_every_step_and_sub_step_it_presents():
    session = TestSession(workspace=WORKSPACE, executes_here=True)
    store = Store()
    location = Location(FILES) / PROGRESS

    with progress_written(session, store, location):
        with session.step("Execute"):
            frame = session.open_concurrent_substep("Load Sales.Customer")
            frame.note = "(read 1, +1 ~0 -0 !0)"
            session.close_concurrent_substep(frame, elapsed=3.0)

    records = json.loads(store.read(location))
    assert [(one["kind"], one["event"], one["name"]) for one in records] == [
        ("step", "started", "Execute"),
        ("substep", "started", "Load Sales.Customer"),
        ("substep", "completed", "Load Sales.Customer"),
        ("step", "completed", "Execute"),
    ]
    assert records[2]["elapsed"] == 3.0
    assert records[2]["note"] == "(read 1, +1 ~0 -0 !0)"
    assert records[1]["id"] == records[2]["id"]


@weaver_test()
def test_fabric_leaves_the_report_and_returns_its_identity(monkeypatch):
    from weaver.run import entry

    store = Store()
    root = Location(FILES) / "run"
    store.write(root / REQUEST, json.dumps({"rows": 3}).encode())
    monkeypatch.setattr("weaver.fabric.store.FabricStore", lambda: store)
    session = TestSession(workspace=WORKSPACE, executes_here=True)
    session.warnings.append("mind")

    def counted(*, session, workspace, rows):
        return {"rows": rows}

    receipt = entry.run_staged(
        counted, session=session, workspace=WORKSPACE, stage=root.value
    )

    data = store.read(root / RESULT)
    assert json.loads(data) == {"report": {"rows": 3}, "warnings": ["mind"]}
    assert receipt == {
        "bytes": len(data),
        "sha256": hashlib.sha256(data).hexdigest(),
    }


@weaver_test()
def test_a_node_that_failed_before_counting_anything_crosses_back():
    """A failure Fabric settled with only an error still decodes, counting zero."""

    from weaver.load_report import FAILED, LoadNodeReport, LoadRunReport

    report = LoadRunReport(
        requested=("Lakehouse/Sales",),
        status="failed",
        dry_run=False,
        fault_tolerant=True,
        nodes=(
            LoadNodeReport(
                node_id="load:Lakehouse/Sales/Tables/Sales.Customer",
                logical_id=None,
                physical_target="Lakehouse/Sales",
                primitive_kind="python_table",
                dispatch_location=None,
                status=FAILED,
                executed=True,
            ),
        ),
    )
    carried = report.to_mapping()
    carried["nodes"][0]["rows"] = {"succeeded": False, "error_message": "no result"}

    (node,) = LoadRunReport.from_mapping(json.loads(json.dumps(carried))).nodes

    assert node.status == FAILED
    assert node.result.error_message == "no result"
    assert node.result.rows_read == 0


@weaver_test()
def test_a_named_test_run_keeps_the_diagnostic_rows_that_crossed_beside_it():
    """The report never holds diagnostic rows, so Fabric sends them alongside."""

    from weaver.operations.test import _decoded
    from weaver.test_report import ValidationNodeReport, ValidationRunReport

    report = ValidationRunReport(
        status="failed",
        nodes=(
            ValidationNodeReport(
                logical_id="Lakehouse/Sales/Tables/Sales.Customer",
                kind="Test",
                physical_target="Lakehouse/Sales",
                primitive_kind="python_validation",
                dispatch_location="",
                status="failed",
                executed=True,
            ),
        ),
    )
    carried = report.to_mapping()
    carried["diagnostics"] = {
        "Lakehouse/Sales/Tables/Sales.Customer": [{"Id": 1, "Side": "missing"}]
    }

    (node,) = _decoded(json.loads(json.dumps(carried))).nodes

    assert node.diagnostics == ({"Id": 1, "Side": "missing"},)


# --- one Livy interpreter, many runs -------------------------------------------

#: The Sessions each program opened, in the order it opened them.
OPENED: list = []


def _recording_entry(*, session, workspace, fail=False):
    """Records a Log row as a run does, through a Session-owned flusher."""

    from weaver.catalogue.tables import LOG
    from weaver.run.record import open_run_record

    OPENED.append(session)
    record = open_run_record(None, task_type="load", session=session)
    session.flusher(LOG, warehouse="Weaver").submit({"workflow_id": record.workflow_id})
    if fail:
        raise RuntimeError("the run failed")
    return {"workflow_id": record.workflow_id}


class Interpreter(Fabric):
    """A client whose statement runs the generated program in this process.

    One Livy interpreter runs every statement a client sends, so what one program
    leaves behind is still there for the next.
    """

    def __init__(self, monkeypatch):
        super().__init__()
        self.written: list[str] = []
        store = self.scope(WORKSPACE).store
        monkeypatch.setattr("weaver.fabric.store.FabricStore", lambda: store)
        monkeypatch.setattr(
            "weaver.sessions.notebook.NotebookSession.execute_tsql",
            lambda session, statement, **kwargs: self.written.append(statement),
        )

    def execute_python(self, program, *, workspace=None, timeout=None):
        self.programs.append(program)
        emitted: list = []
        exec(program.source, {"spark": object(), "emit": emitted.append})  # noqa: S102
        return emitted[0]


def _recorded(**arguments):
    return FabricRun(
        name="load",
        needs_spark=True,
        call=lambda here: "here",
        entry=_recording_entry,
        arguments=lambda: arguments,
        decode=lambda carried: carried,
    )


def _workers() -> set:
    """Weaver's worker threads alive in this process, whoever started them."""

    import threading

    return {
        thread
        for thread in threading.enumerate()
        if thread.name.startswith(("weaver-flusher", "weaver-progress"))
    }


@weaver_test()
def test_each_run_closes_the_session_it_opened_in_fabric(monkeypatch):
    """A Session left open keeps its flushers' workers alive in the interpreter."""

    OPENED.clear()
    client = Interpreter(monkeypatch)

    from weaver.sessions.run_in_fabric import send

    for _ in range(3):
        before = _workers()
        assert send(client, _recorded(), workspace=WORKSPACE)["workflow_id"]
        assert not _workers() - before

    assert len(OPENED) == 3 and all(session.closed for session in OPENED)
    assert len(client.written) == 3


@weaver_test()
def test_a_run_that_fails_in_fabric_still_closes_its_session(monkeypatch):
    from weaver.sessions.run_in_fabric import send

    OPENED.clear()
    client = Interpreter(monkeypatch)
    before = _workers()

    with pytest.raises(RuntimeError, match="the run failed"):
        send(client, _recorded(fail=True), workspace=WORKSPACE)

    (session,) = OPENED
    assert session.closed
    assert not _workers() - before
    # What the run recorded before it failed was still written.
    assert len(client.written) == 1


@weaver_test()
def test_runs_sent_from_one_workflow_record_that_workflow_in_fabric(monkeypatch):
    from weaver.sessions.run_in_fabric import send

    client = Interpreter(monkeypatch)

    with client.workflow("workflow-1"):
        load = send(client, _recorded(), workspace=WORKSPACE)
        test = send(client, _recorded(), workspace=WORKSPACE)
    alone = send(client, _recorded(), workspace=WORKSPACE)

    assert load["workflow_id"] == test["workflow_id"] == "workflow-1"
    assert alone["workflow_id"] not in ("workflow-1", None)
    assert sum("workflow-1" in statement for statement in client.written) == 2


@weaver_test()
def test_a_run_in_fabric_starts_without_an_earlier_runs_mounts(monkeypatch):
    """The host's mount outlives the run that made it, and can still list a
    file deleted through OneLake since."""

    from types import SimpleNamespace

    from weaver import lakehouse
    from weaver.run import entry

    landing = "abfss://ws@onelake.dfs.fabric.microsoft.com/Landing"
    unmounted = []
    monkeypatch.setattr(
        lakehouse,
        "_notebook_utils",
        lambda: SimpleNamespace(fs=SimpleNamespace(unmount=unmounted.append)),
    )
    monkeypatch.setitem(lakehouse._MOUNTS, landing, "/synfs/weaver/Landing")
    store = Store()
    root = Location(FILES) / "run"
    store.write(root / REQUEST, b"{}")
    monkeypatch.setattr("weaver.fabric.store.FabricStore", lambda: store)
    seen = []

    def listing(*, session, workspace):
        seen.append(dict(lakehouse._MOUNTS))
        return {}

    entry.run_staged(
        listing,
        session=TestSession(workspace=WORKSPACE, executes_here=True),
        workspace=WORKSPACE,
        stage=root.value,
    )

    assert unmounted == ["/weaver/Landing"]
    assert seen == [{}]
