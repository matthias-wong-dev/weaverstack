"""What an intolerant run does when something fails, and what it leaves behind.

Two claims that have to hold together, and the order between them is the whole
design:

.. code-block:: text

    every planned node is classified and recorded
    the completion document is written
    ── only then ──
    weaver.load raises

Raising earlier would leave nodes with no durable outcome at all, and "nothing
was written for it" cannot be told apart from "the run died before reaching it".
Not raising would make ``fault_tolerant=False`` — a caller saying *stop if
anything fails* — indistinguishable from success to anyone who did not read the
report.

The session is prepared rather than acquired and dispatch is injected, because
these are orchestration claims. What a primitive does when it refuses rows is
proved against that primitive; what orchestration does with the refusal is
proved here, and standing up four engines to assert it would be asserting the
engines.
"""

from __future__ import annotations

import json
from dataclasses import dataclass

import pytest

from weaver.errors import LoadError
from weaver.load import run_load
from weaver.load_report import (
    BLOCKED,
    FAILED,
    PENDING,
    SKIPPED,
    SUCCEEDED,
    SUCCEEDED_WITH_REJECTS,
    TASK_FAILED,
    TASK_PARTIALLY_SUCCEEDED,
    TASK_SUCCEEDED_WITH_REJECTS,
    LoadResult,
)
from weaver.load_plan import PhysicalTargetRef
from weaver.load_resolution import LoadEnvironment
from weaver.locations import Location
from weaver.resolution import LocalResolver
from weaver.store import FilesystemStore
from weaver.task_logging import COMPLETE_STEP, PLAN_FILE, open_task_log
from weaver.workspaces import LocalWorkspace

from factories import (
    installed_catalogue,
    installed_inventories,
    load_estate,
    load_estate_bindings,
)

RAW = PhysicalTargetRef("lakehouse", "Raw_LH")
REPORTING = PhysicalTargetRef("warehouse", "Reporting_WH")

ORDER = "load:Lakehouse/Raw_LH/Sales.Order"
DAILY = "load:Lakehouse/Raw_LH/Sales.Daily"
EXPORT = "load:Lakehouse/Raw_LH/Sales.Export"
REFRESH = "refresh:Lakehouse/Raw_LH"
SUMMARY = "load:Warehouse/Reporting_WH/Sales.Summary"

OK = LoadResult(succeeded=True, rows_read=3, rows_inserted=3)


class Unused:
    """Stands in wherever a real capability would be, and is never reached."""

    def __getattr__(self, name):
        def refuse(*args, **kwargs):
            raise AssertionError(f"dispatch is injected; {name} must not be called")

        return refuse


@dataclass
class PreparedSession:
    """A load session whose state is prepared and whose log is a real one."""

    catalogue: object
    inventories: dict
    workspace: object
    resolver: object
    log_root: Location
    store: object

    def read_catalogue(self):
        return self.catalogue

    def environment(self, dag):
        return LoadEnvironment(
            resolver=self.resolver,
            inventories=self.inventories,
            store=self.store,
            spark=Unused(),
            sql={"Reporting_WH": Unused()},
            workspace=self.workspace,
        )

    def open_log(self):
        return open_task_log(
            task_type="load", folder=self.log_root, store=self.store
        )


class Refreshing(LocalResolver):
    def refresh_sql_endpoint(self, item):
        return None


@pytest.fixture
def session(tmp_path):
    repository = load_estate(tmp_path / "repository")
    bindings = load_estate_bindings()
    workspace = LocalWorkspace(
        workspace=str(tmp_path / "estate"), weaver_lakehouse="Weaver_LH"
    )
    root = Location(str(tmp_path / "log"))
    FilesystemStore().make_directory(root)
    return PreparedSession(
        catalogue=installed_catalogue(repository, bindings),
        inventories=installed_inventories(repository, bindings),
        workspace=workspace,
        resolver=Refreshing(workspace),
        log_root=root,
        store=FilesystemStore(),
    )


@pytest.fixture
def dispatched(monkeypatch):
    """Answer each node by id: a result to return, or an exception to raise."""

    import weaver.load_execution as module

    answers: dict = {}
    calls: list[str] = []

    def dispatch(resolved, *, fault_tolerant, environment):
        node_id = resolved.node_id
        calls.append(node_id)
        answer = answers.get(node_id, OK)
        if isinstance(answer, BaseException):
            raise answer
        return answer

    monkeypatch.setattr(module, "dispatch_load_node", dispatch)
    dispatch.answers = answers
    dispatch.calls = calls
    return dispatch


def _run(session, *, fault_tolerant=False, targets=(RAW, REPORTING)):
    return run_load(
        session, requested=targets, fault_tolerant=fault_tolerant, dry_run=False
    )


def _written(session) -> list:
    """Every file the run's task folder holds, as Locations."""

    store = FilesystemStore()
    (partition,) = [entry.location for entry in store.list(session.log_root)]
    (task,) = [entry.location for entry in store.list(partition)]
    return [entry.location for entry in store.list(task)]


def _records(session) -> list[dict]:
    """Every node-result record the run wrote, as parsed JSON."""

    store = FilesystemStore()
    return [
        json.loads(store.read(location).decode("utf-8"))
        for location in _written(session)
        if not location.value.endswith(PLAN_FILE)
        and f"_{COMPLETE_STEP}_" not in location.value
    ]


def _completion(session) -> dict:
    store = FilesystemStore()
    (found,) = [
        location
        for location in _written(session)
        if f"_{COMPLETE_STEP}_" in location.value
    ]
    return json.loads(store.read(found).decode("utf-8"))


# --- an intolerant run raises -------------------------------------------------


def test_a_primitive_that_refused_rows_raises(session, dispatched):
    """The refusal shape: the primitive raised carrying what it counted."""

    dispatched.answers[ORDER] = LoadError(
        "rows were rejected and fault_tolerant = 0",
        result=LoadResult(succeeded=False, rows_read=9, rows_rejected=2),
    )

    with pytest.raises(LoadError, match="rejected"):
        _run(session)


def test_an_authored_exception_raises(session, dispatched):
    dispatched.answers[ORDER] = RuntimeError("the source system was unreachable")

    with pytest.raises(LoadError, match="unreachable"):
        _run(session)


def test_a_warehouse_procedure_failure_raises(session, dispatched):
    dispatched.answers[SUMMARY] = LoadError("the procedure does not exist")

    with pytest.raises(LoadError, match="procedure does not exist"):
        _run(session)


def test_an_endpoint_refresh_failure_raises(session, dispatched):
    dispatched.answers[REFRESH] = LoadError("the endpoint could not be refreshed")

    with pytest.raises(LoadError, match="endpoint could not be refreshed"):
        _run(session)


def test_a_primitive_that_returned_failure_raises(session, dispatched):
    """A returned failure and a raised one are the same outcome to a caller."""

    dispatched.answers[ORDER] = LoadResult.failure("the merge found no key")

    with pytest.raises(LoadError, match="no key"):
        _run(session)


def test_the_exception_names_the_node_that_failed(session, dispatched):
    dispatched.answers[ORDER] = RuntimeError("boom")

    with pytest.raises(LoadError) as raised:
        _run(session)

    assert ORDER in str(raised.value)


def test_the_exception_carries_the_partial_report_and_the_evidence(session, dispatched):
    dispatched.answers[ORDER] = LoadError(
        "rows were rejected",
        result=LoadResult(succeeded=False, rows_read=9, rows_rejected=2),
    )

    with pytest.raises(LoadError) as raised:
        _run(session)

    error = raised.value
    assert error.report is not None
    # Other branches had already completed, so the run is partial rather than
    # wholly failed — and the exception is raised on the node, not the tally.
    assert error.report.status in (TASK_FAILED, TASK_PARTIALLY_SUCCEEDED)
    assert error.report.by_node[ORDER].status == FAILED
    assert error.task_log
    # The counts the primitive managed before refusing, so a caller need not go
    # to the reject table to find out how much was refused.
    assert error.result.rows_rejected == 2


# --- what does not run --------------------------------------------------------


def test_a_descendant_of_a_failed_node_does_not_execute(session, dispatched):
    dispatched.answers[ORDER] = RuntimeError("boom")

    with pytest.raises(LoadError):
        _run(session)

    assert SUMMARY not in dispatched.calls


def test_nothing_new_is_scheduled_after_an_intolerant_failure(session, dispatched):
    """Fail-fast stops scheduling. It does not stop reporting."""

    dispatched.answers[EXPORT] = RuntimeError("boom")

    with pytest.raises(LoadError) as raised:
        _run(session)

    statuses = {n.node_id: n.status for n in raised.value.report.nodes}
    assert statuses[EXPORT] == FAILED
    assert PENDING in statuses.values() or BLOCKED in statuses.values()
    assert all(not n.executed for n in raised.value.report.nodes if n.status == PENDING)


# --- a tolerant run reports ---------------------------------------------------


def test_a_tolerant_run_returns_its_report_rather_than_raising(session, dispatched):
    dispatched.answers[ORDER] = RuntimeError("boom")

    report = _run(session, fault_tolerant=True)

    assert report.by_node[ORDER].status == FAILED
    assert report.status in (TASK_FAILED, TASK_PARTIALLY_SUCCEEDED)


def test_a_tolerant_run_continues_independent_branches(session, dispatched):
    dispatched.answers[ORDER] = RuntimeError("boom")

    _run(session, fault_tolerant=True)

    # Sales.Export is upstream of nothing that failed, so it still runs.
    assert EXPORT in dispatched.calls


def test_a_tolerant_run_still_blocks_descendants(session, dispatched):
    """Tolerance decides whether *independent* branches continue, never whether
    a node may run on a dependency that did not."""

    dispatched.answers[ORDER] = RuntimeError("boom")

    report = _run(session, fault_tolerant=True)

    assert SUMMARY not in dispatched.calls
    assert report.by_node[SUMMARY].status == BLOCKED


# --- the distinction rejects create -------------------------------------------


def test_tolerated_rejects_are_not_a_failed_node(session, dispatched):
    """The primitive wrote the valid rows and reported the refusal."""

    dispatched.answers[ORDER] = LoadResult(
        succeeded=False, rows_read=9, rows_inserted=7, rows_rejected=2
    )

    report = _run(session, fault_tolerant=True)

    assert report.by_node[ORDER].status == SUCCEEDED_WITH_REJECTS
    assert report.status == TASK_SUCCEEDED_WITH_REJECTS
    # Rejects do not block: the valid work completed and what it wrote is there.
    assert SUMMARY in dispatched.calls


def test_a_raised_rejection_is_a_failed_node_however_it_was_counted(
    session, dispatched
):
    """The case a count alone cannot answer.

    A refusal and a tolerated load both come back with ``succeeded=False`` and
    ``rows_rejected > 0``, and they mean opposite things — one wrote the valid
    rows, the other wrote nothing. What separates them is that the refusal
    *raised*, which is why the outcome keeps the exception rather than inferring
    from the counts.
    """

    dispatched.answers[ORDER] = LoadError(
        "rows were rejected and fault_tolerant = 0",
        result=LoadResult(succeeded=False, rows_read=9, rows_rejected=2),
    )

    report = _run(session, fault_tolerant=True).by_node[ORDER]

    assert report.status == FAILED
    assert report.result.rows_rejected == 2


# --- durable evidence ---------------------------------------------------------


def test_every_planned_node_receives_exactly_one_final_record(session, dispatched):
    dispatched.answers[ORDER] = RuntimeError("boom")

    with pytest.raises(LoadError) as raised:
        _run(session)

    planned = [node.node_id for node in raised.value.report.nodes]
    recorded = [record["node_id"] for record in _records(session)]

    assert sorted(recorded) == sorted(planned)
    assert len(recorded) == len(set(recorded))


def test_a_record_says_whether_the_node_executed_and_what_became_of_it(
    session, dispatched
):
    dispatched.answers[ORDER] = RuntimeError("boom")

    with pytest.raises(LoadError):
        _run(session)

    by_node = {record["node_id"]: record for record in _records(session)}

    assert by_node[ORDER]["executed"] is True
    assert by_node[ORDER]["status"] == FAILED
    assert by_node[SUMMARY]["executed"] is False
    assert by_node[SUMMARY]["status"] in (BLOCKED, PENDING)


def test_a_blocked_node_receives_evidence_of_its_own(session, dispatched):
    dispatched.answers[ORDER] = RuntimeError("boom")

    report = _run(session, fault_tolerant=True)
    blocked = [n.node_id for n in report.nodes if n.status == BLOCKED]
    recorded = {record["node_id"] for record in _records(session)}

    assert blocked
    assert set(blocked) <= recorded


def test_a_pending_node_receives_evidence_of_its_own(session, dispatched):
    dispatched.answers[EXPORT] = RuntimeError("boom")

    with pytest.raises(LoadError) as raised:
        _run(session)

    pending = [n.node_id for n in raised.value.report.nodes if n.status == PENDING]
    recorded = {record["node_id"] for record in _records(session)}

    assert set(pending) <= recorded


def test_the_completion_document_is_written_before_the_run_raises(session, dispatched):
    """A decided failure is a finished task.

    The absence of a completion document has to keep meaning *interrupted* — a
    crash, a lost session — rather than "an ordinary load failed", or the one
    signal that distinguishes them is spent on the common case.
    """

    dispatched.answers[ORDER] = RuntimeError("boom")

    with pytest.raises(LoadError):
        _run(session)

    completion = _completion(session)

    assert completion["final_status"] in (TASK_FAILED, TASK_PARTIALLY_SUCCEEDED)
    assert completion["failed_steps"] >= 1


def test_a_successful_intolerant_run_returns_normally(session, dispatched):
    report = _run(session)

    assert report.status == SUCCEEDED
    assert all(node.status == SUCCEEDED for node in report.nodes)


def test_no_record_is_ever_rewritten(session, dispatched):
    """Immutability is what makes the log readable after an interruption."""

    dispatched.answers[ORDER] = RuntimeError("boom")

    with pytest.raises(LoadError):
        _run(session)

    names = [location.value.rsplit("/", 1)[-1] for location in _written(session)]

    assert len(names) == len(set(names))
