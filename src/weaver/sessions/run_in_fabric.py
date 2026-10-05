"""A load or test run with Spark work, sent whole from a client to Fabric.

The client stages the run's arguments in the Lakehouse its Spark session
attaches to and submits one Livy statement. Fabric schedules the whole graph,
writes the frames it presents beside the arguments while it runs, and leaves its
report there. The client presents those frames as they arrive, with the times
Fabric measured.

A run records itself in the catalogue, so a submission that may have reached
Fabric is never sent again. When its result is lost the outcome is unknown, and
the catalogue says what it did.
"""

from __future__ import annotations

import hashlib
import json
import threading
from contextlib import contextmanager
from uuid import uuid4

from ..errors import CommandError, OutcomeUnknown
from .base import STEP, SUBSTEP
from .program import FabricProgram

#: Seconds between Fabric's progress writes, and between a client's reads.
PROGRESS_INTERVAL = 2.0
#: How long a run may take. A load takes as long as its data does, so this only
#: ends a wait that can no longer be answered.
RUN_TIMEOUT = 24 * 3600.0

REQUEST = "request.json"
PROGRESS = "progress.json"
RESULT = "result.json"


# --- the client --------------------------------------------------------------


def send(session, run, *, workspace=None):
    """Run ``run`` in Fabric, present its progress here, and return its report."""

    from ..fabric.onelake import abfss_path
    from ..run.result import RunError
    from ..targets import ItemRef
    from ..workspaces import CARRIER_AREA

    scope = session.scope(workspace)
    home = scope.spark_home
    if home is None:
        raise CommandError(
            f"Cannot run this {run.name} in Fabric: it reads no Lakehouse for "
            "Spark to attach to."
        )
    store = scope.store
    area = scope.resolver.files_root(ItemRef(home)) / CARRIER_AREA
    stage = area / uuid4().hex
    store.make_directory(stage)
    try:
        store.write(stage / REQUEST, json.dumps(run.arguments()).encode("utf-8"))
        program = _program(
            run, scope.workspace, abfss_path(stage), workflow_id=session.workflow_id
        )
        following = _Following(session, store, stage / PROGRESS)
        try:
            receipt = session.execute_python(program, workspace=workspace)
        except OutcomeUnknown as exc:
            raise OutcomeUnknown(
                f"The {run.name} sent to Fabric did not report back: {exc}. It may "
                "have run; the catalogue log records what it did."
            ) from exc
        finally:
            following.stop()
        data = store.read(stage / RESULT)
        if hashlib.sha256(data).hexdigest() != (receipt or {}).get("sha256"):
            raise RunError(f"The {run.name} report Fabric left is incomplete.")
        carried = json.loads(data)
    finally:
        _remove(store, stage, area)
    for warning in carried.get("warnings") or ():
        session.warn(warning)
    return run.decode(carried["report"])


def _workspace_literal(workspace) -> str:
    if workspace is None:
        return "None"
    environment = None if workspace.environment is None else str(workspace.environment)
    return (
        f"Workspace(workspace={workspace.workspace!r}, "
        f"catalogue={workspace.catalogue!r}, "
        f"environment={environment!r})"
    )


def _program(run, workspace, stage: str, *, workflow_id=None) -> FabricProgram:
    """The statement Fabric runs.

    The Livy interpreter outlives it, so the Session it opens is closed with it.
    ``workflow_id`` is the client's workflow, which the run records under.
    """

    entry = run.entry
    source = (
        "from weaver.workspaces import Workspace\n"
        "from weaver.sessions import NotebookSession\n"
        "from weaver.run.entry import run_staged\n"
        f"from {entry.__module__} import {entry.__name__}\n"
        f"workspace = {_workspace_literal(workspace)}\n"
        "with NotebookSession(workspace=workspace, spark=spark) as session:\n"
        f"    receipt = run_staged({entry.__name__}, session=session, "
        f"workspace=workspace, stage={stage!r}, workflow_id={workflow_id!r})\n"
        "emit(receipt)\n"
    )

    def call():
        raise CommandError(f"A {run.name} runs in Fabric through its source.")

    return FabricProgram(
        name=run.name,
        call=call,
        source=source,
        timeout=RUN_TIMEOUT,
        resubmit=False,
    )


def _remove(store, stage, area) -> None:
    try:
        store.delete(stage, recursive=True)
        # Non-recursive, so another invocation's stage keeps the area.
        store.delete(area)
    except Exception:  # noqa: BLE001 - the report is the outcome
        pass


class _Following:
    """Present the frames Fabric reports, as they arrive."""

    def __init__(self, session, store, location) -> None:
        self._session = session
        self._store = store
        self._location = location
        self._seen = 0
        self._substeps: dict = {}
        self._steps: list[str] = []
        self._done = threading.Event()
        context = session.telemetry.capture_context()

        def follow():
            with session.telemetry.use_context(context):
                while not self._done.wait(PROGRESS_INTERVAL):
                    self._read()

        self._reader = threading.Thread(
            target=follow, name="weaver-progress", daemon=True
        )
        self._reader.start()

    def _read(self) -> None:
        try:
            records = json.loads(self._store.read(self._location))
        except Exception:  # noqa: BLE001 - read again at the next interval
            return
        for record in records[self._seen :]:
            try:
                self._present(record)
            except Exception:  # noqa: BLE001 - presentation never changes an outcome
                pass
        self._seen = len(records)

    def _present(self, record: dict) -> None:
        session = self._session
        if record["kind"] == STEP:
            if record["event"] == "started":
                session.step_started(record["name"], record.get("detail"))
                self._steps.append(record["name"])
            elif record["name"] in self._steps:
                self._steps.remove(record["name"])
                if record.get("failed"):
                    session.step_failed(record["name"])
                else:
                    session.step_completed(record["name"])
            return
        if record["event"] == "started":
            self._substeps[record["id"]] = session.open_concurrent_substep(
                record["name"], record.get("detail")
            )
            return
        frame = self._substeps.pop(record["id"], None)
        if frame is None:
            return
        frame.failed = bool(record.get("failed"))
        frame.note = record.get("note")
        session.close_concurrent_substep(frame, elapsed=record.get("elapsed"))

    def stop(self) -> None:
        self._done.set()
        self._reader.join(PROGRESS_INTERVAL * 5)
        self._read()
        # Whatever Fabric never reported finished did not finish.
        for frame in self._substeps.values():
            frame.failed = True
            self._session.close_concurrent_substep(frame)
        self._substeps.clear()
        for name in reversed(self._steps):
            self._session.step_failed(name)
        self._steps.clear()


# --- Fabric ------------------------------------------------------------------


@contextmanager
def progress_written(session, store, location):
    """Write each Step and Sub-step ``session`` presents to ``location``.

    The whole record is rewritten beside the run, so a write never delays a
    node or changes an outcome. The last write follows the run.
    """

    records: list[dict] = []
    lock = threading.Lock()
    done = threading.Event()

    def observe(frame, event: str) -> None:
        if frame.kind not in (STEP, SUBSTEP):
            return
        record = {
            "kind": frame.kind,
            "event": event,
            "id": id(frame),
            "name": frame.name,
            "detail": frame.detail,
        }
        if event != "started":
            record.update(failed=frame.failed, note=frame.note, elapsed=frame.elapsed)
        with lock:
            records.append(record)

    written = 0

    def write() -> None:
        nonlocal written
        with lock:
            snapshot = list(records)
        if len(snapshot) == written:
            return
        try:
            store.write(location, json.dumps(snapshot).encode("utf-8"))
            written = len(snapshot)
        except Exception:  # noqa: BLE001 - progress never changes an outcome
            pass

    def keep_writing() -> None:
        while not done.wait(PROGRESS_INTERVAL):
            write()

    writer = threading.Thread(target=keep_writing, name="weaver-progress", daemon=True)
    with session.observing(observe):
        writer.start()
        try:
            yield
        finally:
            done.set()
            writer.join(PROGRESS_INTERVAL * 5)
            write()


__all__ = ["PROGRESS_INTERVAL", "progress_written", "send"]
