from dataclasses import replace

import pytest
from support.weaver_test import weaver_test
from test_mutation_plan_representation import _action, _plan

from weaver.mutation.bundle import compute_bundle_id


def sealed(actions):
    plan = _plan(actions)
    return replace(plan, bundle_id=compute_bundle_id(plan))


class Clock:
    def __init__(self):
        self.now = 0.0

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


@weaver_test()
def test_pending_releases_single_worker_for_completed_parent_child():
    import weaver.mutation as mutation

    assert hasattr(mutation, "MutationExecutor"), "internal executor is missing"
    from weaver.mutation.executor import Completed, MutationDriver, Pending

    clock = Clock()
    calls = []

    def run(request):
        calls.append((request.action.id, request.continuation, clock.now))
        if request.action.id == "await" and request.continuation is None:
            return Pending("observing", 10)
        return Completed()

    plan = sealed((_action("await"), _action("parent"), _action("child", ("parent",))))
    identity = plan.bundle_id
    report = mutation.MutationExecutor(
        {"folder": MutationDriver(run)}, workers=1, clock=clock
    ).execute(plan)
    assert [c[0] for c in calls] == ["await", "parent", "child", "await"]
    assert [r.action_id for r in report.results] == ["await", "parent", "child"]
    assert all(r.status == "succeeded" for r in report.results)
    assert report.by_id["await"].attempts == 1
    assert report.by_id["await"].observations == 1
    assert plan.bundle_id == identity == compute_bundle_id(plan)


@weaver_test()
def test_resources_are_acquired_atomically_in_stable_order():
    from weaver.mutation import MutationExecutor
    from weaver.mutation.executor import Completed, MutationDriver, Pending

    clock = Clock()
    calls = []

    def run(request):
        calls.append((request.action.id, request.resource_keys, clock.now))
        if request.action.id == "probe" and request.continuation is None:
            return Pending("wait", 5)
        return Completed()

    plan = sealed(
        (
            _action("probe", resources=("tds:shared", "spark:shared")),
            _action("useful", resources=("spark:shared", "tds:shared")),
        )
    )
    report = MutationExecutor(
        {"folder": MutationDriver(run)},
        workers=2,
        clock=clock,
        limits={"spark:shared": 1, "tds:shared": 1},
    ).execute(plan, {})
    assert calls == [
        ("probe", ("spark:shared", "tds:shared"), 0),
        ("useful", ("spark:shared", "tds:shared"), 0),
        ("probe", ("spark:shared", "tds:shared"), 5),
    ]
    assert all(r.status == "succeeded" for r in report.results)


@weaver_test()
def test_completed_parent_unlocks_child_while_other_worker_runs():
    from threading import Event

    from weaver.mutation import MutationExecutor
    from weaver.mutation.executor import Completed, MutationDriver

    child_started = Event()
    unrelated_running = Event()
    seen = []

    def run(request):
        name = request.action.id
        if name == "unrelated":
            unrelated_running.set()
            assert child_started.wait(2), "whole layer was joined"
        elif name == "parent":
            assert unrelated_running.wait(2)
        else:
            child_started.set()
        seen.append(name)
        return Completed()

    report = MutationExecutor({"folder": MutationDriver(run)}, workers=2).execute(
        sealed(
            (_action("unrelated"), _action("parent"), _action("child", ("parent",)))
        ),
        {},
    )
    assert seen.index("child") < seen.index("unrelated")
    assert all(r.status == "succeeded" for r in report.results)


@weaver_test()
@pytest.mark.parametrize("failure", ["failed", "uncertain", "exception", "malformed"])
def test_non_success_suppresses_success_edges_without_replay(failure):
    import weaver.mutation.executor as runtime

    assert hasattr(runtime, "Failed"), "typed failure outcomes are missing"
    calls = []

    def run(request):
        calls.append(request.action.id)
        if request.action.id == "root":
            if failure == "exception":
                raise OSError("lost response")
            if failure == "malformed":
                return None
            return (
                runtime.Failed("statement rejected")
                if failure == "failed"
                else runtime.Uncertain("lost acknowledgement")
            )
        return runtime.Completed()

    report = runtime.MutationExecutor(
        {"folder": runtime.MutationDriver(run)}, workers=1
    ).execute(
        sealed(
            (
                _action("root"),
                _action("child", ("root",)),
                _action("ordered", settle_after=("root",)),
                _action("after-blocked", settle_after=("child",)),
                _action("independent"),
            )
        ),
        {},
    )
    assert report.by_id["root"].status == (
        "failed" if failure == "failed" else "uncertain"
    )
    assert report.by_id["child"].status == "blocked"
    assert report.by_id["ordered"].status == (
        "succeeded" if failure == "failed" else "not_dispatched"
    )
    assert report.by_id["after-blocked"].status == "succeeded"
    assert calls.count("root") == 1
    assert report.by_id["independent"].status == "succeeded"


@weaver_test()
@pytest.mark.parametrize("late", [False, True])
def test_absolute_deadline_survives_pending_and_rejects_late_success(late):
    from weaver.mutation.executor import (
        Completed,
        MutationDriver,
        MutationExecutor,
        Pending,
    )

    clock = Clock()
    deadlines = []

    def run(request):
        deadlines.append(request.deadline)
        if clock.now == 0:
            return Pending("known-handle", 2)
        if late:
            clock.now = 6
            return Completed()
        return Pending("known-handle", clock.now + 2)

    report = MutationExecutor(
        {"folder": MutationDriver(run)}, clock=clock, timeout=5
    ).execute(sealed((_action("await"), _action("certify", ("await",)))), {})
    assert deadlines == ([5, 5] if late else [5, 5, 5])
    assert report.by_id["await"].status == "uncertain"
    assert "deadline" in report.by_id["await"].error
    assert report.by_id["certify"].status == "blocked"
    assert clock.now == (6 if late else 5)
    assert report.by_id["await"].attempts == 1


def operation_plan():
    from weaver.mutation import DriverContract, ResultReference

    actions = (
        _action("start", executor="refresh_start", exclusions=("endpoint:Sales",)),
        _action("second", executor="refresh_start", exclusions=("endpoint:Sales",)),
        _action(
            "await",
            ("start",),
            executor="refresh_await",
            result_from=ResultReference("start", "refresh-handle"),
        ),
        _action(
            "second-await",
            ("second",),
            executor="refresh_await",
            result_from=ResultReference("second", "refresh-handle"),
        ),
        _action("independent"),
    )
    base = _plan(())
    plan = replace(
        base,
        sequences=(
            replace(
                base.sequences[0],
                batches=(replace(base.sequences[0].batches[0], actions=actions),),
            ),
        ),
        driver_contracts=(
            DriverContract(
                "refresh_start", None, produces="refresh-handle", starts_operation=True
            ),
            DriverContract(
                "refresh_await", None, consumes="refresh-handle", settles_operation=True
            ),
        ),
        required_completion=("await", "second-await"),
    )
    return replace(plan, bundle_id=compute_bundle_id(plan))


@weaver_test()
def test_typed_operation_acknowledgement_retains_lease_until_settlement():
    import weaver.mutation.executor as runtime

    assert hasattr(runtime, "TypedValue"), "operation result ledger is missing"
    clock = Clock()
    calls = []
    persisted = []
    plan = operation_plan()
    contracts = {c.executor: c for c in plan.driver_contracts}

    def run(request):
        calls.append((request.action.id, clock.now))
        if request.action.executor == "refresh_start":
            return runtime.Completed(
                runtime.TypedValue("refresh-handle", "handle-" + request.action.id)
            )
        if request.action.executor == "refresh_await":
            assert any(
                e.kind == "acknowledged"
                and e.action_id == request.action.result_from.action_id
                for e in persisted
            )
            assert (
                request.input.value == "handle-" + request.action.result_from.action_id
            )
            assert request.deadline == (5 if request.action.id == "await" else 7)
            if request.continuation is None:
                return runtime.Pending("observe", clock.now + 2)
        return runtime.Completed()

    drivers = {
        name: runtime.MutationDriver(run, contract=c) for name, c in contracts.items()
    }
    drivers["folder"] = runtime.MutationDriver(run)
    report = runtime.MutationExecutor(
        drivers, clock=clock, timeout=5, journal=persisted.append
    ).execute(plan, {})
    assert calls == [
        ("start", 0),
        ("await", 0),
        ("independent", 0),
        ("await", 2),
        ("second", 2),
        ("second-await", 2),
        ("second-await", 4),
    ]
    assert all(r.status == "succeeded" for r in report.results)
    assert all(op.status == "settled" for op in report.operations)
    assert persisted == list(report.ledger)


@weaver_test()
@pytest.mark.parametrize("mode", ["direct", "decoded", "bundle"])
@pytest.mark.parametrize(
    "fault", ["payload", "identity", "unsealed", "unsupported", "contract", "resource"]
)
def test_all_inputs_are_validated_before_any_dispatch(tmp_path, mode, fault):
    import hashlib

    from weaver.errors import BuildError
    from weaver.locations import Location
    from weaver.mutation import MutationPlan
    from weaver.mutation.bundle import BuildBundle, write_bundle
    from weaver.mutation.executor import Completed, MutationDriver, MutationExecutor
    from weaver.store import FilesystemStore

    data = b"binary\x00\xff"
    plan = sealed(
        (
            _action(
                "a",
                executor="load_file",
                payload="payload/a.payload",
                payload_sha256=hashlib.sha256(data).hexdigest(),
            ),
        )
    )
    payloads = {"payload/a.payload": data}
    store = FilesystemStore()
    location = Location(str(tmp_path / "bundle"))
    write_bundle(location, plan=plan, payloads=payloads, store=store)
    calls = []
    driver = MutationDriver(lambda request: calls.append(request) or Completed())
    drivers = {"load_file": driver}
    if fault == "payload":
        payloads["payload/a.payload"] = b"changed"
        store.write(location.join("payload", "a.payload"), b"changed")
    elif fault == "identity":
        object.__setattr__(plan, "required_completion", ("a",))
    elif fault == "unsealed":
        plan = replace(plan, bundle_id="")
    elif fault == "unsupported":
        drivers = {}
    elif fault == "contract":
        from weaver.mutation import DriverContract

        drivers = {"load_file": replace(driver, contract=DriverContract("wrong", None))}
    else:
        action = replace(next(plan.actions())[2], resources=("unknown-lane",))
        plan = sealed((action,))
    with pytest.raises(BuildError):
        if mode == "decoded":
            plan = MutationPlan.from_mapping(plan.to_mapping())
        if mode == "bundle":
            bundle = BuildBundle(location, plan, store)
            payloads = {
                action.payload: store.read(
                    bundle.location.join(*action.payload.split("/"))
                )
                for _, _, action in bundle.plan.actions()
                if action.payload is not None
            }
            plan = bundle.plan
            assert isinstance(plan, MutationPlan)
        MutationExecutor(drivers).execute(plan, payloads)
    assert calls == []


@weaver_test()
@pytest.mark.parametrize("policy", ["continue_independent", "fail_fast", "cancel"])
def test_stop_policy_drains_dispatched_work_and_stops_new_admission(policy):
    from threading import Event

    from weaver.mutation.executor import (
        Completed,
        Failed,
        MutationDriver,
        MutationExecutor,
    )

    cancellation = Event()
    second_running = Event()
    released = Event()
    calls = []

    def run(request):
        calls.append(request.action.id)
        if request.action.id == "first":
            assert second_running.wait(2)
            if policy == "cancel":
                cancellation.set()
            return Failed("known failure")
        if request.action.id == "second":
            second_running.set()
            assert released.wait(2)
        return Completed()

    report = MutationExecutor(
        {"folder": MutationDriver(run)},
        workers=2,
        failure_policy="continue_independent" if policy == "cancel" else policy,
        journal=lambda event: (
            released.set()
            if event.kind == "terminal" and event.action_id == "first"
            else None
        ),
    ).execute(
        sealed((_action("first"), _action("second"), _action("third"))),
        {},
        cancellation=cancellation,
    )
    assert report.by_id["second"].status == "succeeded"
    assert report.by_id["third"].status == (
        "succeeded" if policy == "continue_independent" else "not_dispatched"
    )
    assert ("third" in calls) == (policy == "continue_independent")


@weaver_test()
def test_cancellation_keeps_pending_remote_operation_uncertain():
    from threading import Event

    from weaver.mutation.executor import (
        Completed,
        MutationDriver,
        MutationExecutor,
        Pending,
    )

    cancellation = Event()

    def run(request):
        if request.action.id == "pending":
            cancellation.set()
            return Pending("known-handle", 10)
        return Completed()

    report = MutationExecutor({"folder": MutationDriver(run)}, clock=Clock()).execute(
        sealed((_action("pending"), _action("not-started"))),
        {},
        cancellation=cancellation,
    )
    assert report.by_id["pending"].status == "uncertain"
    assert report.by_id["pending"].observations == 0
    assert report.by_id["not-started"].status == "not_dispatched"


@weaver_test()
@pytest.mark.parametrize(
    "fault", ["none", "missing", "duplicate", "extra", "malformed", "member_failure"]
)
def test_batch_results_validate_exact_members_before_success(fault):
    from weaver.mutation.executor import (
        Completed,
        Failed,
        MutationDriver,
        MutationExecutor,
    )

    batches = []

    def batch(requests, emit):
        batches.append(tuple(r.action.id for r in requests))
        outcomes = [("a", Completed()), ("b", Completed())]
        if fault == "missing":
            outcomes.pop()
        if fault == "duplicate":
            outcomes[1] = outcomes[0]
        if fault == "extra":
            outcomes.append(("extra", Completed()))
        if fault == "malformed":
            outcomes[1] = ("b", True)
        if fault == "member_failure":
            outcomes[1] = ("b", Failed("rejected"))
        return outcomes

    driver = MutationDriver(lambda request: Completed(), batch=batch, batch_size=2)
    report = MutationExecutor({"folder": driver}, workers=1).execute(
        sealed((_action("a"), _action("b"), _action("certify", ("a", "b")))), {}
    )
    assert batches[0] == ("a", "b")
    if fault == "none":
        assert all(r.status == "succeeded" for r in report.results)
    elif fault == "member_failure":
        assert report.by_id["a"].status == "succeeded"
        assert report.by_id["b"].status == "failed"
        assert report.by_id["certify"].status == "blocked"
    else:
        assert report.by_id["a"].status == report.by_id["b"].status == "uncertain"
        assert report.by_id["certify"].status == "blocked"


@weaver_test()
def test_incremental_batch_member_unlocks_child_before_batch_finishes():
    from threading import Event

    from weaver.mutation.executor import Completed, MutationDriver, MutationExecutor

    child = Event()
    seen = []

    def batch(requests, emit):
        assert [r.action.id for r in requests] == ["a", "b"]
        emit("a", Completed())
        assert child.wait(2), "member completion was held until batch completion"
        seen.append("b")
        return [("b", Completed())]

    def run(request):
        seen.append(request.action.id)
        child.set()
        return Completed()

    report = MutationExecutor(
        {"folder": MutationDriver(run, batch=batch, batch_size=2)}, workers=2
    ).execute(sealed((_action("a"), _action("b"), _action("child", ("a",)))), {})
    assert seen == ["child", "b"]
    assert all(r.status == "succeeded" for r in report.results)


@weaver_test()
def test_report_separates_active_wait_and_ready_queue_time_with_worker_context():
    from weaver.mutation.executor import (
        Completed,
        MutationDriver,
        MutationExecutor,
        Pending,
    )
    from weaver.sessions.telemetry import SessionTelemetry, TelemetryContext

    clock = Clock()
    telemetry = SessionTelemetry()
    context = TelemetryContext(task="Build", step="physical", substep="frozen")
    captured = []

    def run(request):
        captured.append(telemetry.capture_context())
        if request.action.id == "probe":
            if request.continuation is None:
                clock.now += 2
                return Pending("observe", 10)
            clock.now += 1
        else:
            clock.now += 3
        return Completed()

    with telemetry.use_context(context):
        report = MutationExecutor({"folder": MutationDriver(run)}, clock=clock).execute(
            sealed((_action("probe"), _action("useful"))), {}
        )
        assert telemetry.capture_context() == context
    assert captured == [context, context, context]
    assert report.by_id["probe"].active_seconds == 3
    assert report.by_id["probe"].wait_seconds == 8
    assert report.by_id["useful"].active_seconds == 3
    assert report.by_id["useful"].ready_queue_seconds == 2
    assert report.by_id["useful"].resource_queue_seconds == 0


@weaver_test()
@pytest.mark.parametrize("fault", ["none", "past", "nan", "infinity"])
def test_malformed_pending_cannot_replay_mutation_or_spin(fault):
    from weaver.mutation.executor import (
        Completed,
        MutationDriver,
        MutationExecutor,
        Pending,
    )

    clock = Clock()
    calls = []

    def run(request):
        calls.append(request)
        if len(calls) > 1:
            return Completed()
        return Pending(
            None if fault == "none" else "state",
            {"none": 1, "past": -1, "nan": float("nan"), "infinity": float("inf")}[
                fault
            ],
        )

    report = MutationExecutor(
        {"folder": MutationDriver(run)}, clock=clock, timeout=2
    ).execute(sealed((_action("a"),)), {})
    assert report.by_id["a"].status == "uncertain"
    assert len(calls) == 1
    assert "invalid Pending" in report.by_id["a"].error


@weaver_test()
@pytest.mark.parametrize(
    "policy",
    [
        {"workers": 0},
        {"workers": True},
        {"timeout": 0},
        {"timeout": float("nan")},
        {"limits": {"lane": 0}},
        {"failure_policy": "guess"},
    ],
)
def test_execution_policy_requires_positive_finite_bounds(policy):
    from weaver.errors import BuildError
    from weaver.mutation.executor import MutationExecutor

    with pytest.raises(BuildError):
        MutationExecutor({}, **policy)


@weaver_test()
@pytest.mark.parametrize("outcome", ["success", "failure", "uncertain", "cancel"])
def test_build_planner_preserves_admitted_member_continuation(outcome):
    from threading import Event

    from support.mutation_plans import warehouse_prune_plan

    from weaver.mutation.executor import (
        Completed,
        Failed,
        MutationDriver,
        MutationExecutor,
        Uncertain,
    )

    plan, payloads = warehouse_prune_plan()
    members = plan.sequences[0].batches[0].actions
    calls = []
    cancel = Event()

    def run(request):
        calls.append(request.action.id)
        if request.action.id == members[0].id:
            if outcome == "failure":
                return Failed("statement rejected")
            if outcome == "uncertain":
                return Uncertain("lost response")
            if outcome == "cancel":
                cancel.set()
        return Completed()

    report = MutationExecutor({"tsql": MutationDriver(run)}).execute(
        plan, payloads, cancellation=cancel
    )
    if outcome == "failure":
        assert calls == [m.id for m in members]
        assert report.by_id["complete-batch:prune"].status == "blocked"
    elif outcome == "success":
        assert calls == [m.id for m in members] + ["later", "next"]
        assert all(r.status == "succeeded" for r in report.results)
    else:
        assert calls == [members[0].id]
    if outcome != "success":
        assert report.by_id["later"].status != "succeeded"
        assert report.by_id["next"].status != "succeeded"


@weaver_test()
@pytest.mark.parametrize("missing", [False, True])
def test_physical_adapter_runs_existing_executor_on_owned_lane(missing):
    import hashlib

    import weaver.mutation.executor as runtime

    assert hasattr(runtime, "physical_driver"), "physical capability adapter is missing"
    from weaver.build_bundle.executors.base import InstallationContext
    from weaver.build_bundle.executors.tsql import TSqlExecutor
    from weaver.errors import BuildError

    calls = []

    class Sql:
        def execute_script(self, statement):
            calls.append(statement)

    context = InstallationContext(
        resolver=None, store=None, target=None, sql=None if missing else Sql()
    )
    payload = b"select 1;"
    plan = sealed(
        (
            _action(
                "sql",
                executor="tsql",
                payload="payload/script.sql",
                payload_sha256=hashlib.sha256(payload).hexdigest(),
            ),
        )
    )
    driver = runtime.physical_driver(
        TSqlExecutor(),
        {"sales": context},
        lane="tds:shared",
        required_capabilities=("sql",),
    )
    executor = runtime.MutationExecutor({"tsql": driver}, limits={"tds:shared": 1})
    if missing:
        with pytest.raises(BuildError, match="sql"):
            executor.execute(plan, {"payload/script.sql": payload})
        assert calls == []
    else:
        report = executor.execute(plan, {"payload/script.sql": payload})
        assert calls == ["select 1;"]
        assert report.by_id["sql"].status == "succeeded"


@weaver_test()
def test_failed_durable_acknowledgement_preserves_handle_without_replay():
    from weaver.mutation.executor import (
        Completed,
        MutationDriver,
        MutationExecutor,
        TypedValue,
    )

    plan = operation_plan()
    calls = []

    def run(request):
        calls.append(request.action.id)
        return Completed(TypedValue("refresh-handle", "known-handle"))

    def journal(event):
        if event.kind == "acknowledged":
            raise OSError("journal unavailable")

    drivers = {
        c.executor: MutationDriver(run, contract=c) for c in plan.driver_contracts
    }
    drivers["folder"] = MutationDriver(run)
    report = MutationExecutor(drivers, journal=journal).execute(plan, {})
    assert calls == ["start"]
    assert report.by_id["start"].status == "uncertain"
    assert report.operations[0].handle.value == "known-handle"
    assert report.operations[0].status == "uncertain"
    assert report.journal_errors == ("journal unavailable",)


@weaver_test()
def test_journal_checkpoints_completions_and_flushes_before_handle_consumption(
    tmp_path,
):
    import json
    from dataclasses import asdict

    import weaver.mutation.executor as runtime

    assert hasattr(runtime, "MutationJournal"), "buffered invocation journal is missing"
    path = tmp_path / "ledger.jsonl"
    chunks = []

    def write(events):
        chunks.append(events)
        with path.open("a") as stream:
            for event in events:
                stream.write(json.dumps(asdict(event)) + "\n")

    plan = operation_plan()

    def run(request):
        if request.action.executor == "refresh_start":
            return runtime.Completed(runtime.TypedValue("refresh-handle", "known"))
        if request.action.executor == "refresh_await":
            rows = [json.loads(line) for line in path.read_text().splitlines()]
            assert any(
                row["kind"] == "acknowledged"
                and row["action_id"] == request.action.result_from.action_id
                for row in rows
            )
        return runtime.Completed()

    drivers = {
        c.executor: runtime.MutationDriver(run, contract=c)
        for c in plan.driver_contracts
    }
    drivers["folder"] = runtime.MutationDriver(run)
    report = runtime.MutationExecutor(
        drivers, journal=runtime.MutationJournal(write)
    ).execute(plan, {})
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    assert len(rows) == len(report.ledger)
    assert {row["plan_id"] for row in rows} == {plan.bundle_id}
    assert {row["invocation_id"] for row in rows} == {report.invocation_id}
    assert len(chunks) < len(rows)


@weaver_test()
def test_supported_pending_cancellation_uses_owned_lane_and_known_outcome():
    from threading import Event

    from weaver.mutation.executor import (
        Failed,
        MutationDriver,
        MutationExecutor,
        Pending,
    )

    cancellation = Event()
    calls = []

    def run(request):
        calls.append("dispatch")
        cancellation.set()
        return Pending("known-operation", 10)

    def cancel(request):
        assert request.continuation == "known-operation"
        assert request.resource_keys == ("spark:shared",)
        calls.append("cancel")
        return Failed("confirmed cancelled by driver")

    driver = MutationDriver(run, cancel=cancel)
    report = MutationExecutor(
        {"folder": driver}, clock=Clock(), limits={"spark:shared": 1}
    ).execute(
        sealed((_action("pending", resources=("spark:shared",)), _action("new"))),
        {},
        cancellation=cancellation,
    )
    assert calls == ["dispatch", "cancel"]
    assert report.by_id["pending"].status == "failed"
    assert report.by_id["pending"].attempts == 1
    assert report.by_id["new"].status == "not_dispatched"


@weaver_test()
@pytest.mark.parametrize("bad", ["failed", "pending"])
def test_malformed_batch_values_cannot_release_other_members(bad):
    from weaver.mutation.executor import (
        Completed,
        Failed,
        MutationDriver,
        MutationExecutor,
        Pending,
    )

    def batch(requests, emit):
        return [
            ("a", Completed()),
            ("b", Failed(None) if bad == "failed" else Pending(None, 1)),
        ]

    driver = MutationDriver(lambda request: Completed(), batch=batch, batch_size=2)
    report = MutationExecutor({"folder": driver}).execute(
        sealed((_action("a"), _action("b"))), {}
    )
    assert all(r.status == "uncertain" for r in report.results)


@weaver_test()
def test_batch_size_one_keeps_single_dispatch():
    from weaver.mutation.executor import Completed, MutationDriver, MutationExecutor

    calls = []

    def run(request):
        calls.append(request.action.id)
        return Completed()

    def batch(requests, emit):
        raise AssertionError("batch exceeded its bound")

    report = MutationExecutor(
        {"folder": MutationDriver(run, batch=batch, batch_size=1)}
    ).execute(sealed((_action("a"), _action("b"))), {})
    assert calls == ["a", "b"]
    assert all(r.status == "succeeded" for r in report.results)


@weaver_test()
def test_worker_base_exception_returns_uncertainty_without_losing_completion():
    from weaver.mutation.executor import MutationDriver, MutationExecutor

    def run(request):
        raise SystemExit("worker stopped after possible submission")

    report = MutationExecutor({"folder": MutationDriver(run)}).execute(
        sealed((_action("a"),)), {}
    )
    assert report.by_id["a"].status == "uncertain"


@weaver_test()
def test_malformed_incremental_identity_cannot_crash_coordinator():
    from weaver.mutation.executor import Completed, MutationDriver, MutationExecutor

    def batch(requests, emit):
        emit([], Completed())
        return [("a", Completed()), ("b", Completed())]

    report = MutationExecutor(
        {
            "folder": MutationDriver(
                lambda request: Completed(), batch=batch, batch_size=2
            )
        }
    ).execute(sealed((_action("a"), _action("b"))), {})
    assert all(r.status == "uncertain" for r in report.results)


@weaver_test()
def test_uncertain_observation_retains_operation_exclusion_without_replay():
    from weaver.mutation.executor import (
        Completed,
        MutationDriver,
        MutationExecutor,
        Pending,
        TypedValue,
        Uncertain,
    )

    clock = Clock()
    plan = operation_plan()
    calls = []

    def run(request):
        calls.append(request.action.id)
        if request.action.executor == "refresh_start":
            return Completed(TypedValue("refresh-handle", "known"))
        if request.action.executor == "refresh_await":
            if request.continuation is None:
                return Pending("observe-known", 1)
            return Uncertain("observation lost")
        return Completed()

    drivers = {
        c.executor: MutationDriver(run, contract=c) for c in plan.driver_contracts
    }
    drivers["folder"] = MutationDriver(run)
    report = MutationExecutor(drivers, clock=clock).execute(plan, {})
    assert calls == ["start", "await", "independent", "await"]
    assert report.retained_exclusions == (("endpoint:Sales", "start"),)
    assert report.operations[0].status == "uncertain"
    assert not report.succeeded


@weaver_test()
def test_shared_inner_pool_lane_prevents_outer_pool_multiplication():
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier, Lock

    from weaver.mutation.executor import Completed, MutationDriver, MutationExecutor

    active = 0
    peak = 0
    lock = Lock()

    def batch(requests, emit):
        barrier = Barrier(2)

        def record(request):
            nonlocal active, peak
            with lock:
                active += 1
                peak = max(peak, active)
            barrier.wait(timeout=2)
            with lock:
                active -= 1
            return request.action.id, Completed()

        with ThreadPoolExecutor(max_workers=2) as inner:
            return list(inner.map(record, requests))

    driver = MutationDriver(
        lambda request: Completed(),
        batch=batch,
        batch_size=2,
        resources=("delta:inner-pool",),
        serial_resources=("delta:inner-pool",),
    )
    plan = sealed(tuple(_action(str(i)) for i in range(4)))
    report = MutationExecutor(
        {"folder": driver}, workers=4, limits={"delta:inner-pool": 1}
    ).execute(plan, {})
    assert peak == 2
    assert all(r.status == "succeeded" for r in report.results)


@weaver_test()
def test_failed_dispatch_journal_prevents_mutation_admission():
    from weaver.mutation.executor import Completed, MutationDriver, MutationExecutor

    calls = []

    def run(request):
        calls.append(request.action.id)
        return Completed()

    def journal(event):
        raise OSError("dispatch journal unavailable")

    report = MutationExecutor({"folder": MutationDriver(run)}, journal=journal).execute(
        sealed((_action("a"), _action("b"))), {}
    )
    assert calls == []
    assert report.by_id["a"].status == "not_dispatched"
    assert report.by_id["a"].attempts == 0
    assert not report.succeeded


@weaver_test()
@pytest.mark.parametrize("fault", ["resource", "override"])
def test_completion_gates_cannot_bypass_runtime_validation(fault):
    from weaver.errors import BuildError
    from weaver.mutation.executor import Completed, MutationDriver, MutationExecutor

    calls = []
    driver = MutationDriver(lambda request: calls.append(request) or Completed())
    gate = _action(
        "gate",
        executor="completion_gate",
        resources=("unknown",) if fault == "resource" else (),
    )
    with pytest.raises(BuildError):
        MutationExecutor(
            {"completion_gate": driver} if fault == "override" else {}
        ).execute(sealed((gate,)), {})
    assert calls == []


@weaver_test()
@pytest.mark.parametrize("lane", ["rest:shared", "onelake:shared"])
def test_declared_io_limits_bound_concurrent_recording_calls(lane):
    from threading import Barrier, Lock

    from weaver.mutation.executor import Completed, MutationDriver, MutationExecutor

    barrier = Barrier(2)
    lock = Lock()
    active = 0
    peak = 0

    def run(request):
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
        barrier.wait(timeout=2)
        with lock:
            active -= 1
        return Completed()

    plan = sealed(tuple(_action(str(i), resources=(lane,)) for i in range(6)))
    report = MutationExecutor(
        {"folder": MutationDriver(run)}, workers=4, limits={lane: 2}
    ).execute(plan, {})
    assert peak == 2
    assert report.succeeded


@weaver_test()
@pytest.mark.parametrize("bad", [None, "untyped", "wrong-type"])
def test_invalid_typed_starter_result_provides_no_success_evidence(bad):
    from weaver.mutation.executor import (
        Completed,
        MutationDriver,
        MutationExecutor,
        TypedValue,
    )

    plan = operation_plan()
    calls = []

    def run(request):
        calls.append(request.action.id)
        return Completed(TypedValue("wrong", "handle") if bad == "wrong-type" else bad)

    drivers = {
        c.executor: MutationDriver(run, contract=c) for c in plan.driver_contracts
    }
    drivers["folder"] = MutationDriver(lambda request: Completed())
    report = MutationExecutor(drivers).execute(plan, {})
    assert calls == ["start"]
    assert report.by_id["start"].status == "uncertain"
    assert report.by_id["await"].status == "blocked"


@weaver_test()
def test_required_physical_skip_is_a_failure():
    from weaver.build_bundle.executors.base import InstallationContext, SkippedExecution
    from weaver.mutation.executor import MutationExecutor, physical_driver

    class Executor:
        def execute(self, action, payload, context):
            return SkippedExecution()

    context = InstallationContext(None, None, None)
    driver = physical_driver(
        Executor(), {"sales": context}, lane="owned", required_capabilities=()
    )
    report = MutationExecutor({"folder": driver}, limits={"owned": 1}).execute(
        sealed((_action("a"),)), {}
    )
    assert report.by_id["a"].status == "failed"
    assert not report.succeeded


@weaver_test()
@pytest.mark.parametrize("mode", ["direct", "decoded", "bundle"])
def test_successful_execution_preserves_frozen_binary_payloads(tmp_path, mode):
    import hashlib

    from weaver.locations import Location
    from weaver.mutation import MutationPlan
    from weaver.mutation.bundle import load_bundle, write_bundle
    from weaver.mutation.executor import Completed, MutationDriver, MutationExecutor
    from weaver.store import FilesystemStore

    payload = b"\x00\xff\x80frozen\r\n"
    plan = sealed(
        (
            _action(
                "binary",
                executor="load_file",
                payload="payload/binary.payload",
                payload_sha256=hashlib.sha256(payload).hexdigest(),
            ),
        )
    )
    identity = plan.bundle_id
    payloads = {"payload/binary.payload": payload}
    if mode == "decoded":
        plan = MutationPlan.from_mapping(plan.to_mapping())
    if mode == "bundle":
        store = FilesystemStore()
        bundle = write_bundle(
            Location(str(tmp_path / "bundle")),
            plan=plan,
            payloads=payloads,
            store=store,
        )
        bundle = load_bundle(bundle.location, store=store)
        plan = bundle.plan
        assert isinstance(plan, MutationPlan)
        payloads = {
            action.payload: store.read(bundle.location.join(*action.payload.split("/")))
            for _, _, action in plan.actions()
            if action.payload is not None
        }
    seen = []

    def run(request):
        seen.append(request.payload)
        return Completed()

    report = MutationExecutor({"load_file": MutationDriver(run)}).execute(
        plan, payloads
    )
    assert seen == [payload]
    assert report.plan_id == identity
    assert report.succeeded


@weaver_test()
def test_presentation_sequences_do_not_create_hidden_barriers():
    from weaver.mutation.executor import (
        Completed,
        MutationDriver,
        MutationExecutor,
        Pending,
    )

    base = _plan((_action("await"),))
    first = base.sequences[0]
    second = replace(
        first,
        number=2,
        batches=(replace(first.batches[0], id="second", actions=(_action("useful"),)),),
    )
    plan = replace(base, sequences=(first, second))
    plan = replace(plan, bundle_id=compute_bundle_id(plan))
    seen = []
    clock = Clock()

    def run(request):
        seen.append((request.action.id, clock.now))
        if request.action.id == "await" and request.continuation is None:
            return Pending("observe", 5)
        return Completed()

    report = MutationExecutor({"folder": MutationDriver(run)}, clock=clock).execute(
        plan, {}
    )
    assert seen == [("await", 0), ("useful", 0), ("await", 5)]
    assert report.succeeded


@weaver_test()
def test_batch_result_decode_failure_preserves_incremental_success():
    from weaver.mutation.executor import Completed, MutationDriver, MutationExecutor

    def batch(requests, emit):
        emit("a", Completed())

        def remaining():
            raise OSError("malformed result decoder")
            yield

        return remaining()

    report = MutationExecutor(
        {
            "folder": MutationDriver(
                lambda request: Completed(), batch=batch, batch_size=2
            )
        }
    ).execute(sealed((_action("a"), _action("b"))), {})
    assert report.by_id["a"].status == "succeeded"
    assert report.by_id["b"].status == "uncertain"


@weaver_test()
def test_late_starter_acknowledgement_retains_known_handle_without_success():
    from weaver.mutation.executor import (
        Completed,
        MutationDriver,
        MutationExecutor,
        TypedValue,
    )

    plan = operation_plan()
    clock = Clock()

    def run(request):
        if request.action.executor == "refresh_start":
            clock.now = 6
            return Completed(TypedValue("refresh-handle", "late-known-handle"))
        return Completed()

    drivers = {
        c.executor: MutationDriver(run, contract=c) for c in plan.driver_contracts
    }
    drivers["folder"] = MutationDriver(run)
    report = MutationExecutor(drivers, clock=clock, timeout=5).execute(plan, {})
    assert report.by_id["start"].status == "uncertain"
    assert report.by_id["await"].status == "blocked"
    assert report.operations[0].handle.value == "late-known-handle"
    assert report.operations[0].status == "uncertain"
    assert report.retained_exclusions == (("endpoint:Sales", "start"),)


@weaver_test()
def test_action_exclusion_survives_other_settlement_until_pending_expiry():
    from weaver.mutation import ResultReference
    from weaver.mutation.executor import (
        Completed,
        MutationDriver,
        MutationExecutor,
        Pending,
        TypedValue,
        Uncertain,
    )

    base = operation_plan()
    batch = base.sequences[0].batches[0]
    actions = list(batch.actions)
    actions[2] = replace(actions[2], exclusions=("endpoint:Sales",))
    actions.append(
        _action(
            "other-settlement",
            ("start",),
            executor="refresh_await",
            result_from=ResultReference("start", "refresh-handle"),
        )
    )
    plan = replace(
        base,
        bundle_id="",
        sequences=(
            replace(base.sequences[0], batches=(replace(batch, actions=actions),)),
        ),
    )
    plan = replace(plan, bundle_id=compute_bundle_id(plan))

    class AdvancingClock(Clock):
        def sleep(self, seconds):
            assert seconds > 0, "resource-blocked continuation must not spin"
            super().sleep(seconds)

    clock = AdvancingClock()

    def run(request):
        if request.action.id == "start":
            return Completed(TypedValue("refresh-handle", "known"))
        if request.action.id == "second":
            return Uncertain("second operation unknown")
        if request.action.id == "await":
            return Pending("remaining-observation", clock.now + 1)
        return Completed()

    drivers = {
        c.executor: MutationDriver(run, contract=c) for c in plan.driver_contracts
    }
    drivers["folder"] = MutationDriver(run)
    report = MutationExecutor(drivers, clock=clock, timeout=3).execute(plan, {})
    assert clock.now == 3
    assert report.by_id["await"].status == "uncertain"
    assert report.operations[0].status == "settled"
