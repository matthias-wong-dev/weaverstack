"""Sequential execution over a prepared graph, with fake dispatchers.

These are orchestration claims and not primitive ones. What is asserted is the
order nodes ran in, what each was told, what happened to the graph after one of
them failed, and how a transport's answer became a normalised result — none of
which needs a Warehouse, a Spark session or a real deployed module. Standing up
four engines to assert an ordering would be asserting the engines.

The dispatcher is a spy: it records what it was handed and answers plainly, so a
test asserts on *what reached the boundary* rather than on a call signature.
"""

from __future__ import annotations

import pytest

from weaver.errors import LoadError
from weaver.load_execution import execute_load_plan
from weaver.load_plan import LoadDag, PhysicalTargetRef
from weaver.load_report import (
    BLOCKED,
    DISPATCH_EXCEPTION,
    FAILED,
    PENDING,
    PRIMITIVE_FAILURE,
    PRIMITIVE_REJECTS,
    RESULT_CONTRACT_INVALID,
    SKIPPED,
    SUCCEEDED,
    SUCCEEDED_WITH_REJECTS,
    LoadResult,
)
from weaver.load_resolution import LoadEnvironment, resolve_load_plan
from weaver.resolution import LocalResolver
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


class SpyDispatcher:
    """Records every dispatch and answers with whatever it was told to."""

    def __init__(self, results=None) -> None:
        self.dispatched: list[str] = []
        self.tolerance: list[bool] = []
        self._results = dict(results or {})

    def __call__(self, resolved, *, fault_tolerant, environment):
        self.dispatched.append(resolved.node_id)
        self.tolerance.append(fault_tolerant)
        answer = self._results.get(resolved.node_id, LoadResult(succeeded=True))
        if isinstance(answer, Exception):
            raise answer
        return answer


class Refreshing(LocalResolver):
    """A resolver that can refresh, so the barrier is a real node here."""

    def refresh_sql_endpoint(self, item):
        return {"lakehouse": item.name}


def forbidden(*_args, **_kwargs):
    raise AssertionError("dispatch must not be called")


@pytest.fixture
def plan(tmp_path):
    repository = load_estate(tmp_path)
    bindings = load_estate_bindings()
    dag = LoadDag.from_catalogue(
        installed_catalogue(repository, bindings), targets=(RAW, REPORTING)
    )
    environment = LoadEnvironment(
        resolver=Refreshing(
            LocalWorkspace(workspace=".local", weaver_lakehouse="Weaver_LH")
        ),
        inventories=installed_inventories(repository, bindings),
    )
    return resolve_load_plan(dag, environment=environment), environment


def run(plan, dispatch, **extra):
    resolved, environment = plan
    return execute_load_plan(
        resolved, environment=environment, dispatch=dispatch, **extra
    )


def statuses(reports):
    return {report.node_id: report.status for report in reports}


# --- order and tolerance ------------------------------------------------------


def test_load_execution_runs_nodes_in_topological_order(plan):
    spy = SpyDispatcher()

    run(plan, spy)

    assert spy.dispatched == [EXPORT, ORDER, DAILY, REFRESH, SUMMARY]


def test_load_execution_runs_endpoint_refresh_at_its_dag_position(plan):
    spy = SpyDispatcher()

    run(plan, spy)

    assert spy.dispatched.index(ORDER) < spy.dispatched.index(REFRESH)
    assert spy.dispatched.index(REFRESH) < spy.dispatched.index(SUMMARY)


def test_load_execution_passes_fault_tolerant_to_every_primitive(plan):
    spy = SpyDispatcher()

    run(plan, spy, fault_tolerant=True)

    assert spy.tolerance == [True] * 5


def test_load_execution_is_deterministic(plan):
    once, again = SpyDispatcher(), SpyDispatcher()

    first = run(plan, once)
    second = run(plan, again)

    assert once.dispatched == again.dispatched
    assert [report.node_id for report in first] == [
        report.node_id for report in second
    ]


# --- normalising what a primitive answered ------------------------------------


def test_load_execution_normalises_primitive_results(plan):
    result = LoadResult(
        succeeded=True, rows_read=7, rows_inserted=5, rows_updated=2
    )
    reports = run(plan, SpyDispatcher({ORDER: result}))

    order = {report.node_id: report for report in reports}[ORDER]
    assert order.status == SUCCEEDED
    assert order.executed
    assert order.result == result
    assert order.to_mapping()["rows"]["rows_inserted"] == 5


def test_load_execution_reports_rejects_without_calling_them_a_failure(plan):
    """Valid rows landed; some were refused. Downstream may still read them."""

    rejected = LoadResult(
        succeeded=False,
        rows_read=10,
        rows_inserted=8,
        rows_rejected=2,
        error_message="rows were rejected and excluded from the load",
    )
    spy = SpyDispatcher({ORDER: rejected})

    reports = run(plan, spy, fault_tolerant=True)

    assert statuses(reports)[ORDER] == SUCCEEDED_WITH_REJECTS
    assert statuses(reports)[DAILY] == SUCCEEDED
    assert [
        message.code
        for report in reports
        for message in report.messages
    ] == [PRIMITIVE_REJECTS]


def test_load_execution_records_a_primitive_that_reported_failure(plan):
    failure = LoadResult.failure("the source view does not resolve")
    reports = run(plan, SpyDispatcher({ORDER: failure}), fault_tolerant=True)

    order = {report.node_id: report for report in reports}[ORDER]
    assert order.status == FAILED
    assert [message.code for message in order.messages] == [PRIMITIVE_FAILURE]


def test_load_execution_converts_an_unexpected_exception_into_a_failed_result(plan):
    reports = run(
        plan,
        SpyDispatcher({ORDER: RuntimeError("the connection dropped")}),
        fault_tolerant=True,
    )

    order = {report.node_id: report for report in reports}[ORDER]
    assert order.status == FAILED
    assert order.result.error_message == "RuntimeError: the connection dropped"
    assert [message.code for message in order.messages] == [DISPATCH_EXCEPTION]


def test_load_execution_keeps_the_counts_a_failed_primitive_carried(plan):
    """A load that failed having written four hundred rows is not one that wrote none."""

    carried = LoadResult.failure("rejected", rows_read=400, rows_rejected=400)
    reports = run(
        plan,
        SpyDispatcher({ORDER: LoadError("rejected", result=carried)}),
        fault_tolerant=True,
    )

    order = {report.node_id: report for report in reports}[ORDER]
    assert order.result.rows_read == 400


def test_load_execution_refuses_a_result_that_is_not_one(plan):
    class NotAResult:
        pass

    reports = run(
        plan, SpyDispatcher({ORDER: NotAResult()}), fault_tolerant=True
    )

    order = {report.node_id: report for report in reports}[ORDER]
    assert order.status == FAILED
    assert [message.code for message in order.messages] == [RESULT_CONTRACT_INVALID]


# --- failure propagation ------------------------------------------------------


def test_load_execution_stops_after_failure_when_not_fault_tolerant(plan):
    spy = SpyDispatcher({EXPORT: RuntimeError("gone")})

    reports = run(plan, spy, fault_tolerant=False)

    assert spy.dispatched == [EXPORT]
    assert statuses(reports) == {
        EXPORT: FAILED,
        # Never started, and not blocked: nothing they depend on failed. Saying
        # "pending" is what keeps the one node that actually broke findable.
        ORDER: PENDING,
        DAILY: PENDING,
        # These two do depend on the folder load — every selected load in a
        # Lakehouse feeds its barrier — so for them the failure really did
        # propagate.
        REFRESH: BLOCKED,
        SUMMARY: BLOCKED,
    }


def test_load_execution_continues_independent_branches_when_fault_tolerant(plan):
    spy = SpyDispatcher({EXPORT: RuntimeError("gone")})

    reports = run(plan, spy, fault_tolerant=True)

    assert spy.dispatched == [EXPORT, ORDER, DAILY]
    assert statuses(reports)[ORDER] == SUCCEEDED
    assert statuses(reports)[DAILY] == SUCCEEDED


def test_load_execution_blocks_descendants_of_failed_nodes(plan):
    spy = SpyDispatcher({ORDER: RuntimeError("gone")})

    reports = run(plan, spy, fault_tolerant=True)

    assert statuses(reports) == {
        EXPORT: SUCCEEDED,
        ORDER: FAILED,
        DAILY: BLOCKED,
        REFRESH: BLOCKED,
        SUMMARY: BLOCKED,
    }


def test_fault_tolerance_never_executes_a_node_whose_dependency_failed(plan):
    spy = SpyDispatcher({ORDER: RuntimeError("gone")})

    run(plan, spy, fault_tolerant=True)

    assert DAILY not in spy.dispatched
    assert SUMMARY not in spy.dispatched


def test_load_execution_reports_a_blocked_node_as_not_executed(plan):
    reports = run(
        plan, SpyDispatcher({ORDER: RuntimeError("gone")}), fault_tolerant=True
    )

    daily = {report.node_id: report for report in reports}[DAILY]
    assert not daily.executed
    assert daily.result is None


# --- steps reach the caller as they complete ----------------------------------


def test_load_execution_hands_each_executed_step_to_its_observer(plan):
    seen = []

    run(plan, SpyDispatcher(), on_step=seen.append)

    assert [report.node_id for report in seen] == [
        EXPORT,
        ORDER,
        DAILY,
        REFRESH,
        SUMMARY,
    ]
    assert all(report.executed for report in seen)


def test_a_node_the_host_cannot_perform_is_skipped_not_failed(tmp_path):
    """The emulator has no SQL endpoint, so the barrier is omitted, not broken."""

    repository = load_estate(tmp_path)
    bindings = load_estate_bindings()
    dag = LoadDag.from_catalogue(
        installed_catalogue(repository, bindings), targets=(RAW, REPORTING)
    )
    environment = LoadEnvironment(
        resolver=LocalResolver(
            LocalWorkspace(workspace=".local", weaver_lakehouse="Weaver_LH")
        ),
        inventories=installed_inventories(repository, bindings),
    )
    spy = SpyDispatcher()

    reports = execute_load_plan(
        resolve_load_plan(dag, environment=environment),
        environment=environment,
        dispatch=spy,
    )

    assert statuses(reports)[REFRESH] == SKIPPED
    assert REFRESH not in spy.dispatched
    # And a skip clears the way rather than blocking it: the barrier was never
    # needed here, because there is no endpoint for anything to read through.
    assert statuses(reports)[SUMMARY] == SUCCEEDED
