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
Not raising would make ``fault_tolerant=False``, a caller saying *stop if
anything fails*, indistinguishable from success to anyone who did not read the
report.

The session is prepared rather than acquired and dispatch is injected, because
these are orchestration claims. What a primitive does when it refuses rows is
proved against that primitive; what orchestration does with the refusal is
proved here, and standing up four engines to assert it would be asserting the
engines.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from factories import (
    installed_catalogue,
    load_estate,
    load_estate_bindings,
)
from support.weaver_test import weaver_test
from support.workspaces import InventoryClient, given_workspace

from weaver.catalogue.tables import RESULT_VOCABULARY
from weaver.declaration.model import WeaverItemId
from weaver.errors import LoadError
from weaver.fabric.resolution import FabricResolver
from weaver.load_report import (
    BLOCKED,
    FAILED,
    PENDING,
    SUCCEEDED,
    SUCCEEDED_WITH_REJECTS,
    TASK_FAILED,
    TASK_PARTIALLY_SUCCEEDED,
    TASK_SUCCEEDED_WITH_REJECTS,
    LoadResult,
)
from weaver.operations.load import run_load
from weaver.run import RunState
from weaver.store import FilesystemStore

if TYPE_CHECKING:  # names used only in annotations
    from weaver.lakehouse import Lakehouse

#: What a request names. The node ids below stay physical: that is where the
#: work runs.
RAW = WeaverItemId.parse("Lakehouse/Raw")
REPORTING = WeaverItemId.parse("Warehouse/Reporting")

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
class Prepared:
    """Catalogue state and the Session a run reaches engines through."""

    catalogue: Lakehouse / object
    workspace: object
    session: object


class Refreshing(FabricResolver):
    def refresh_sql_endpoint(self, item):
        return None


@pytest.fixture
def session(tmp_path):
    repository = load_estate(tmp_path / "repository")
    bindings = load_estate_bindings()
    workspace = given_workspace(catalogue="Warehouse/Weaver_LH")
    from support.sessions import given_session

    resolver = Refreshing(
        workspace,
        client=InventoryClient(
            workspace.workspace,
            [("Lakehouse", name) for name in ("Weaver_LH", "Raw_LH")],
        ),
        base_url=Path(tmp_path).as_posix(),
    )
    store = FilesystemStore()
    opened = given_session(workspace=workspace, resolver=resolver, store=store)
    return Prepared(
        # Writing through the Session, so a claim about the statements a run
        # submits still sees them.
        catalogue=installed_catalogue(repository, bindings, session=opened),
        workspace=workspace,
        session=opened,
    )


@pytest.fixture
def dispatched(monkeypatch):
    """Answer each node by id: a result to return, or an exception to raise."""

    import weaver.run as module

    answers: dict = {}
    calls: list[str] = []

    def dispatch(node, **asked):
        calls.append(node.node_id)
        answer = answers.get(node.node_id, OK)
        if isinstance(answer, BaseException):
            raise answer
        return answer

    # The one seam a run crosses. `run_load` reads it from the package at call
    # time, so patching the name here is what a controlled dispatch does.
    monkeypatch.setattr(module, "dispatch_primitive", dispatch)
    dispatch.answers = answers
    dispatch.calls = calls
    return dispatch


def _run(session, *, fault_tolerant=False, targets=(RAW, REPORTING)):
    return run_load(
        session.session,
        workspace=session.workspace,
        state=RunState(catalogue=session.catalogue),
        requested=targets,
        fault_tolerant=fault_tolerant,
        dry_run=False,
    )


def _log_statements(session) -> list[str]:
    """Every ``_.Log`` append the run made, flushed and read back.

    The whole path, not a shortcut through it: the Runner constructed the rows,
    the Session-managed flusher batched them, and the Session executed the
    T-SQL. Flushed here because a caller that never waited is exactly what the
    flusher promises, the rows are in flight until something asks.
    """

    session.session.flush()
    return [
        statement
        for call in session.session.calls
        if call.kind == "tsql"
        for statement in call.body
        if "INSERT INTO [_].[Log]" in statement
    ]


def _recorded_nodes(session) -> list[str]:
    """The node each appended row is about, in the order they were written."""

    statements = "\n".join(_log_statements(session))
    found = [
        (statements.index(node), node)
        for node in (ORDER, DAILY, EXPORT, REFRESH, SUMMARY)
        if node in statements
    ]
    return [node for _at, node in sorted(found)]


#: Every value the public ``[Result]`` vocabulary holds, so a row carrying a new
#: one is read as that rather than falling through to whichever value happens to
#: appear next in the statement.
RESULTS = tuple(RESULT_VOCABULARY.values()) + ("Rejected",)


def _result_for(session, node_id: str) -> str:
    """The public ``[Result]`` value one node's row carries."""

    for statement in _log_statements(session):
        for row in statement.split("),\n"):
            if node_id in row:
                for value in RESULTS:
                    if f"N'{value}'" in row:
                        return value
    raise AssertionError(f"no _.Log row was written for {node_id}")


# --- an intolerant run raises -------------------------------------------------


@weaver_test()
def test_a_primitive_that_refused_rows_raises(session, dispatched):
    """The refusal shape: the primitive raised carrying what it counted."""

    dispatched.answers[ORDER] = LoadError(
        "rows were rejected and fault_tolerant = 0",
        result=LoadResult(succeeded=False, rows_read=9, rows_rejected=2),
    )

    with pytest.raises(LoadError, match="rejected"):
        _run(session)


@weaver_test()
def test_an_authored_exception_raises(session, dispatched):
    dispatched.answers[ORDER] = RuntimeError("the source system was unreachable")

    with pytest.raises(LoadError, match="unreachable"):
        _run(session)


@weaver_test()
def test_a_warehouse_procedure_failure_raises(session, dispatched):
    dispatched.answers[SUMMARY] = LoadError("the procedure does not exist")

    with pytest.raises(LoadError, match="procedure does not exist"):
        _run(session)


@weaver_test()
def test_an_endpoint_refresh_failure_raises(session, dispatched):
    dispatched.answers[REFRESH] = LoadError("the endpoint could not be refreshed")

    with pytest.raises(LoadError, match="endpoint could not be refreshed"):
        _run(session)


@weaver_test()
def test_a_primitive_that_returned_failure_raises(session, dispatched):
    """A returned failure and a raised one are the same outcome to a caller."""

    dispatched.answers[ORDER] = LoadResult.failure("the merge found no key")

    with pytest.raises(LoadError, match="no key"):
        _run(session)


@weaver_test()
def test_the_exception_names_the_node_that_failed(session, dispatched):
    dispatched.answers[ORDER] = RuntimeError("boom")

    with pytest.raises(LoadError) as raised:
        _run(session)

    assert ORDER in str(raised.value)


@weaver_test()
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
    # wholly failed, and the exception is raised on the node, not the tally.
    assert error.report.status in (TASK_FAILED, TASK_PARTIALLY_SUCCEEDED)
    assert error.report.by_node[ORDER].status == FAILED
    assert error.workflow_id
    # The counts the primitive managed before refusing, so a caller need not go
    # to the reject table to find out how much was refused.
    assert error.result.rows_rejected == 2


# --- what does not run --------------------------------------------------------


@weaver_test()
def test_a_descendant_of_a_failed_node_does_not_execute(session, dispatched):
    dispatched.answers[ORDER] = RuntimeError("boom")

    with pytest.raises(LoadError):
        _run(session)

    assert SUMMARY not in dispatched.calls


@weaver_test()
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


@weaver_test()
def test_a_tolerant_run_returns_its_report_rather_than_raising(session, dispatched):
    dispatched.answers[ORDER] = RuntimeError("boom")

    report = _run(session, fault_tolerant=True)

    assert report.by_node[ORDER].status == FAILED
    assert report.status in (TASK_FAILED, TASK_PARTIALLY_SUCCEEDED)


@weaver_test()
def test_a_tolerant_run_continues_independent_branches(session, dispatched):
    dispatched.answers[ORDER] = RuntimeError("boom")

    _run(session, fault_tolerant=True)

    # Sales.Export is upstream of nothing that failed, so it still runs.
    assert EXPORT in dispatched.calls


@weaver_test()
def test_a_tolerant_run_still_blocks_descendants(session, dispatched):
    """Tolerance decides whether independent branches continue, never whether
    a node may run on a dependency that did not."""

    dispatched.answers[ORDER] = RuntimeError("boom")

    report = _run(session, fault_tolerant=True)

    assert SUMMARY not in dispatched.calls
    assert report.by_node[SUMMARY].status == BLOCKED


# --- the distinction rejects create -------------------------------------------


@weaver_test()
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


@weaver_test()
def test_a_raised_rejection_is_a_failed_node_however_it_was_counted(
    session, dispatched
):
    """The case a count alone cannot answer.

    A refusal and a tolerated load both come back with ``succeeded=False`` and
    ``rows_rejected > 0``, and they mean opposite things. One wrote the valid
    rows, the other wrote nothing. What separates them is that the refusal
    raised, which is why the outcome keeps the exception rather than inferring
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


@weaver_test()
def test_every_planned_node_receives_exactly_one_final_record(session, dispatched):
    dispatched.answers[ORDER] = RuntimeError("boom")

    with pytest.raises(LoadError) as raised:
        _run(session)

    planned = [node.node_id for node in raised.value.report.nodes]
    recorded = _recorded_nodes(session)

    assert sorted(recorded) == sorted(planned)
    assert len(recorded) == len(set(recorded))


@weaver_test()
def test_a_record_says_what_became_of_the_node(session, dispatched):
    """The frozen public vocabulary, and the node's own detail beside it.

    A dispatch that threw is an Error rather than a Failure: the load produced
    no judgement about anything, and deciding whether to look at the
    data or at the code needs the two kept apart.
    """

    dispatched.answers[ORDER] = RuntimeError("boom")

    with pytest.raises(LoadError):
        _run(session)

    assert _result_for(session, ORDER) == "Error"
    assert _result_for(session, SUMMARY) == "Blocked"
    # And the detail no single value carries: whether the
    # node touched the target at all.
    statements = "\n".join(_log_statements(session))
    assert "executed" in statements


@weaver_test()
def test_a_blocked_node_receives_evidence_of_its_own(session, dispatched):
    dispatched.answers[ORDER] = RuntimeError("boom")

    report = _run(session, fault_tolerant=True)
    blocked = [n.node_id for n in report.nodes if n.status == BLOCKED]

    assert blocked
    assert set(blocked) <= set(_recorded_nodes(session))
    assert _result_for(session, blocked[0]) == "Blocked"


@weaver_test()
def test_a_pending_node_receives_evidence_of_its_own(session, dispatched):
    """Never reached is an outcome, and it is not the same as blocked."""

    dispatched.answers[EXPORT] = RuntimeError("boom")

    with pytest.raises(LoadError) as raised:
        _run(session)

    pending = [n.node_id for n in raised.value.report.nodes if n.status == PENDING]

    assert set(pending) <= set(_recorded_nodes(session))
    for node in pending:
        # Pending rather than Skipped: nothing decided not to run it, and no
        # outcome was established for this incarnation.
        assert _result_for(session, node) == "Pending"


@weaver_test()
def test_every_node_is_recorded_before_the_run_raises(session, dispatched):
    """A decided failure is a finished task, and its evidence is complete.

    There is no completion row to look for, a workflow is its rows. So what
    has to hold is that every planned node was already recorded when the run
    raised, or "no row for this node" would mean both interrupted and *an
    ordinary load failed*.
    """

    dispatched.answers[ORDER] = RuntimeError("boom")

    with pytest.raises(LoadError) as raised:
        _run(session)

    assert sorted(_recorded_nodes(session)) == sorted(
        node.node_id for node in raised.value.report.nodes
    )
    assert _result_for(session, ORDER) == "Error"


@weaver_test()
def test_a_successful_intolerant_run_returns_normally(session, dispatched):
    report = _run(session)

    assert report.status == SUCCEEDED
    assert all(node.status == SUCCEEDED for node in report.nodes)


@weaver_test()
def test_the_log_is_appended_to_and_never_updated(session, dispatched):
    """Immutability is what makes the log readable after an interruption."""

    dispatched.answers[ORDER] = RuntimeError("boom")

    with pytest.raises(LoadError):
        _run(session)

    statements = _log_statements(session)

    assert statements
    assert all(statement.startswith("INSERT INTO") for statement in statements)
    assert not any("UPDATE" in statement for statement in statements)
    assert not any("DELETE" in statement for statement in statements)
