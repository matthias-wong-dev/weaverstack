import threading
import time

import pytest
from support.weaver_test import weaver_test
from test_mutation_executor_primitive import sealed
from test_mutation_plan_representation import _action
from test_run_cycle import Outcome, Target, node, runner

import weaver
from weaver.errors import CommandError
from weaver.run.runner import Lanes, RunRequest
from weaver.sessions.archive_runtime import execution_capacity
from weaver.workspaces import BuildConcurrency, ExecutionSettings, Workspace


@weaver_test()
@pytest.mark.parametrize("operation", [weaver.build, weaver.load, weaver.test])
@pytest.mark.parametrize("value", [0, -1, True, False, 1.5, "2", "bad"])
def test_invalid_concurrency_fails_before_workspace_or_source(operation, value):
    with pytest.raises(CommandError, match="concurrency must be a positive integer"):
        operation(concurrency=value)


@weaver_test()
@pytest.mark.parametrize("kind", ["load", "test"])
def test_request_round_trip_and_legacy_default(kind):
    from weaver.declaration.model import WeaverItemId

    made = getattr(RunRequest, kind)(
        [WeaverItemId.parse("Warehouse/Sales")], concurrency=2
    )
    assert made.concurrency == 2
    assert RunRequest.from_mapping(made.to_mapping()) == made
    legacy = made.to_mapping()
    legacy.pop("concurrency")
    assert RunRequest.from_mapping(legacy).concurrency is None


class Probe:
    def __init__(self, fail=None):
        self.lock = threading.Lock()
        self.active = 0
        self.peak = 0
        self.by_lane = {}
        self.peaks = {}
        self.events = []
        self.fail = fail

    def execute(self, key, lane):
        with self.lock:
            self.active += 1
            self.peak = max(self.peak, self.active)
            self.by_lane[lane] = self.by_lane.get(lane, 0) + 1
            self.peaks[lane] = max(self.peaks.get(lane, 0), self.by_lane[lane])
            self.events.append((key, "start"))
        time.sleep(0.025)
        with self.lock:
            self.events.append((key, "end"))
            self.active -= 1
            self.by_lane[lane] -= 1
        if key == self.fail:
            raise RuntimeError("controlled failure")

    def dispatch(self, n, **asked):
        self.execute(n.node_id, Lanes().of(n))
        return Outcome()


@weaver_test()
@pytest.mark.parametrize("total", [1, 2, 3])
def test_real_runner_total_intersects_multiple_resource_lanes(total):
    nodes = [node("s" + str(i)) for i in range(4)]
    nodes += [
        node(
            "w" + str(i),
            target=Target("Sales", "Warehouse"),
            primitive_kind="warehouse_procedure",
        )
        for i in range(4)
    ]
    nodes += [
        node(
            "x" + str(i),
            target=Target("Other", "Warehouse"),
            primitive_kind="warehouse_procedure",
        )
        for i in range(4)
    ]
    made = runner(nodes=nodes, edges=[("s0", "s1")], concurrency=total)
    probe = Probe()
    result = made.run(
        session=object(), dispatch=probe.dispatch, lanes=Lanes(spark=1, warehouse=2)
    )
    assert result.succeeded
    assert probe.peak == total
    assert probe.peaks[("spark",)] == 1
    assert probe.peaks[("warehouse", "Sales")] <= 2
    assert probe.events.index(("s0", "end")) < probe.events.index(("s1", "start"))
    assert probe.active == 0


@weaver_test()
def test_total_one_fail_fast_has_no_queued_dispatch():
    made = runner(
        nodes=[node("a"), node("b"), node("c")], edges=[("a", "b")], concurrency=1
    )
    probe = Probe(fail="a")
    result = made.run(session=object(), dispatch=probe.dispatch, lanes=Lanes())
    assert {n.node_id: n.status for n in result.nodes} == {
        "a": "failed",
        "b": "blocked",
        "c": "pending",
    }
    assert probe.events == [("a", "start"), ("a", "end")]


@weaver_test()
@pytest.mark.parametrize("total", [1, 2, 3, 100, None])
def test_build_capacity_retains_resource_limits_and_workspace(total):
    workspace = Workspace(
        workspace="Demo",
        execution=ExecutionSettings(
            build=BuildConcurrency(
                warehouse_concurrency=1, spark_concurrency=2, onelake_concurrency=3
            )
        ),
    )
    plan = sealed(
        [
            _action("a", resources=("warehouse:sales",)),
            _action("b", resources=("spark",)),
            _action("c", resources=("onelake:sales",)),
        ]
    )
    original = workspace.execution
    workers, limits = execution_capacity(plan, workspace, concurrency=total)
    assert workers == (32 if total is None else min(total, 32))
    assert limits == {"warehouse:sales": 1, "spark": 2, "onelake:sales": 3}
    assert workspace.execution is original
    assert plan.bundle_id == sealed(list(a for _, _, a in plan.actions())).bundle_id


@weaver_test()
@pytest.mark.parametrize("total", [1, 2, 3])
def test_actual_build_scheduler_uses_capacity(total):
    from weaver.mutation import MutationExecutor
    from weaver.mutation.executor import Completed, MutationDriver

    plan = sealed([_action(str(i), resources=("onelake:sales",)) for i in range(6)])
    probe = Probe()

    def execute(request):
        probe.execute(request.action.id, "onelake")
        return Completed()

    workers, limits = execution_capacity(plan, concurrency=total)
    report = MutationExecutor(
        {"folder": MutationDriver(execute)}, workers=workers, limits=limits
    ).execute(plan, {})
    assert report.succeeded
    assert probe.peak == total
    assert probe.active == 0
