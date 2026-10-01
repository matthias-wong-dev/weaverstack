from dataclasses import replace

import pytest
from support.weaver_test import weaver_test
from test_mutation_executor_primitive import Clock, sealed
from test_mutation_plan_representation import _action, _plan

from weaver.mutation import DriverContract, MutationPlan, PhysicalScope, ResultReference
from weaver.mutation.bundle import compute_bundle_id
from weaver.mutation.executor import (
    Completed,
    MutationDriver,
    MutationExecutor,
    Pending,
    TypedValue,
)


def settlers_plan(*, inherited=False, mode="direct"):
    base = _plan(())
    exclusions = ("endpoint", "writer") if inherited else ("endpoint",)
    actions = (
        _action("start", executor="start", exclusions=exclusions),
        *(
            _action(
                name,
                ("start",),
                executor="settle",
                result_from=ResultReference("start", "handle"),
                exclusions=("endpoint", "writer"),
                writes=(PhysicalScope("sales", "Files/Shared"),),
            )
            for name in ("a", "b")
        ),
        _action(
            "third",
            ("a",),
            exclusions=("writer",),
            writes=(PhysicalScope("sales", "Files/Shared"),),
        ),
    )
    plan = replace(
        base,
        sequences=(
            replace(
                base.sequences[0],
                batches=(replace(base.sequences[0].batches[0], actions=actions),),
            ),
        ),
        driver_contracts=(
            DriverContract("start", None, produces="handle", starts_operation=True),
            DriverContract("settle", None, consumes="handle", settles_operation=True),
        ),
        required_completion=("a", "b"),
    )
    plan = replace(plan, bundle_id=compute_bundle_id(plan))
    return MutationPlan.from_mapping(plan.to_mapping()) if mode == "decoded" else plan


@weaver_test()
@pytest.mark.parametrize("mode", ["direct", "decoded"])
@pytest.mark.parametrize("inherited", [False, True])
def test_same_operation_writers_keep_individual_exclusions_across_pending(
    mode, inherited
):
    plan = settlers_plan(inherited=inherited, mode=mode)
    clock = Clock()
    calls = []
    live = set()

    def run(request):
        name = request.action.id
        calls.append((name, clock.now))
        if name == "start":
            return Completed(TypedValue("handle", "known"))
        if request.continuation is None:
            assert not live, f"writer entered with live exclusion: {live}"
            live.add(name)
            if name in {"a", "b"}:
                return Pending(name, clock.now + 1)
        live.remove(name)
        return Completed()

    drivers = {
        c.executor: MutationDriver(run, contract=c) for c in plan.driver_contracts
    }
    drivers["folder"] = MutationDriver(run)
    report = MutationExecutor(drivers, workers=2, clock=clock).execute(plan, {})
    assert report.succeeded, report
    assert calls == [("start", 0), ("a", 0), ("a", 1), ("b", 1), ("b", 2), ("third", 2)]
    assert report.retained_exclusions == ()
    assert report.operations[0].status == "settled"


@weaver_test()
@pytest.mark.parametrize("broken", [False, True])
def test_buffered_journal_persists_group_admission_before_physical_dispatch(broken):
    from weaver.mutation.executor import MutationJournal

    persisted = []
    chunks = []
    calls = []

    def write(events):
        chunks.append(events)
        if broken:
            raise OSError("durable sink unavailable")
        persisted.extend(events)

    def batch(requests, emit):
        calls.append(tuple(r.action.id for r in requests))
        assert {r.action.id for r in requests} <= {
            e.action_id for e in persisted if e.kind == "dispatched"
        }, "physical mutation preceded durable admission"
        assert not any(e.kind == "terminal" for e in persisted)
        return [(r.action.id, Completed()) for r in requests]

    def run(request):
        raise AssertionError("compatible roots should be grouped")

    report = MutationExecutor(
        {"folder": MutationDriver(run, batch=batch, batch_size=2)},
        journal=MutationJournal(write, checkpoint_size=64),
    ).execute(
        sealed(
            (
                _action("a"),
                _action("b"),
                _action("certify", ("a", "b"), executor="completion_gate"),
            )
        ),
        {},
    )
    if broken:
        assert calls == []
        assert report.journal_errors == ("durable sink unavailable",)
        assert report.by_id["a"].status == report.by_id["b"].status == "not_dispatched"
        assert report.by_id["certify"].status == "blocked"
        assert report.by_id["a"].attempts == report.by_id["b"].attempts == 0
    else:
        assert calls == [("a", "b")]
        assert report.succeeded
        assert persisted == list(report.ledger)
        assert [e.action_id for e in chunks[0]] == ["a", "b"]
        assert len(chunks) == 3


def with_actions(plan, actions, *, required_completion=()):
    sequence = plan.sequences[0]
    plan = replace(
        plan,
        bundle_id="",
        sequences=(
            replace(sequence, batches=(replace(sequence.batches[0], actions=actions),)),
        ),
        required_completion=required_completion,
    )
    return replace(plan, bundle_id=compute_bundle_id(plan))


@weaver_test()
@pytest.mark.parametrize("typed", [False, True])
def test_late_known_failure_preserves_truth_and_settlement_successors(typed):
    from weaver.mutation.executor import Failed

    clock = Clock()
    if typed:
        base = settlers_plan()
        actions = tuple(a for _, _, a in base.actions())[:2]
        failed_id = "a"
        plan = with_actions(
            base,
            (
                *actions,
                _action("ordered", settle_after=(failed_id,)),
                _action("certify", (failed_id,), executor="completion_gate"),
            ),
            required_completion=(failed_id,),
        )
    else:
        failed_id = "failing"
        plan = sealed(
            (
                _action(failed_id, exclusions=("writer",)),
                _action("ordered", settle_after=(failed_id,)),
                _action("certify", (failed_id,), executor="completion_gate"),
            )
        )
    calls = []

    def run(request):
        calls.append(request.action.id)
        if request.action.id == "start":
            return Completed(TypedValue("handle", "known"))
        if request.action.id == failed_id:
            clock.now = 6
            return Failed("confirmed terminal failure")
        return Completed()

    drivers = {
        c.executor: MutationDriver(run, contract=c) for c in plan.driver_contracts
    }
    drivers["folder"] = MutationDriver(run)
    report = MutationExecutor(drivers, clock=clock, timeout=5).execute(plan, {})
    assert report.by_id[failed_id].status == "failed"
    assert report.by_id[failed_id].error == "confirmed terminal failure"
    assert report.by_id["ordered"].status == "succeeded"
    assert report.by_id["certify"].status == "blocked"
    assert report.retained_exclusions == ()
    assert calls == (["start", "a", "ordered"] if typed else ["failing", "ordered"])
    if typed:
        assert report.operations[0].status == "settled"
        assert report.operations[0].deadline == 5


@weaver_test()
@pytest.mark.parametrize("support", ["confirmed", "unsupported", "unresolved"])
def test_cancellation_arriving_inside_idle_sleep_preserves_cancel_opportunity(support):
    from threading import Event, Thread

    from weaver.mutation.executor import Failed

    entered = Event()
    release = Event()
    cancellation = Event()

    class PausingClock(Clock):
        def __init__(self):
            super().__init__()
            self.sleeps = []

        def sleep(self, seconds):
            self.sleeps.append(seconds)
            entered.set()
            assert release.wait(2), "test did not release idle wait"
            super().sleep(seconds)

    clock = PausingClock()
    base = settlers_plan()
    actions = tuple(a for _, _, a in base.actions())[:2]
    plan = with_actions(
        base, (*actions, _action("new", ("a",))), required_completion=("a",)
    )
    calls = []
    reports = []
    errors = []

    def run(request):
        calls.append(request.action.id)
        if request.action.id == "start":
            return Completed(TypedValue("handle", "known"))
        return Pending("known-operation", 100)

    def cancel(request):
        calls.append("cancel")
        assert request.cancelling
        assert request.continuation == "known-operation"
        assert request.deadline == 5
        assert clock.now < request.deadline
        return (
            Failed("confirmed cancellation")
            if support == "confirmed"
            else Pending("still-running", 101)
        )

    drivers = {
        c.executor: MutationDriver(
            run,
            contract=c,
            cancel=cancel
            if c.executor == "settle" and support != "unsupported"
            else None,
        )
        for c in plan.driver_contracts
    }
    drivers["folder"] = MutationDriver(run)

    def execute():
        try:
            reports.append(
                MutationExecutor(drivers, clock=clock, timeout=5).execute(
                    plan, {}, cancellation=cancellation
                )
            )
        except BaseException as exc:
            errors.append(exc)

    thread = Thread(target=execute)
    thread.start()
    try:
        assert entered.wait(2), "coordinator never entered idle waiting"
        cancellation.set()
    finally:
        release.set()
        thread.join(2)
    assert not thread.is_alive()
    assert not errors
    assert len(clock.sleeps) == 1
    assert 0 < clock.sleeps[0] <= 0.05, "idle wait hides cancellation until deadline"
    report = reports[0]
    assert calls == (
        ["start", "a"] if support == "unsupported" else ["start", "a", "cancel"]
    )
    assert report.by_id["new"].status != "succeeded"
    assert report.by_id["a"].attempts == 1
    if support == "confirmed":
        assert report.by_id["a"].status == "failed"
        assert report.by_id["a"].error == "confirmed cancellation"
        assert report.operations[0].status == "settled"
        assert report.retained_exclusions == ()
    else:
        assert report.by_id["a"].status == "uncertain"
        assert report.operations[0].status == "uncertain"
        assert ("endpoint", "start") in report.retained_exclusions
        assert ("writer", "a") in report.retained_exclusions


@weaver_test()
@pytest.mark.parametrize("mode", ["direct", "decoded"])
def test_one_settler_finishing_cannot_release_another_running_writer(mode):
    from threading import Event

    base = settlers_plan()
    start, a, b, third = (a for _, _, a in base.actions())
    plan = with_actions(
        base,
        (
            start,
            replace(
                a, exclusions=("writer-a",), writes=(PhysicalScope("sales", "Files/A"),)
            ),
            replace(b, exclusions=("writer",)),
            third,
            _action("release-b", ("a",)),
        ),
        required_completion=("a", "b"),
    )
    if mode == "decoded":
        plan = MutationPlan.from_mapping(plan.to_mapping())
    b_running = Event()
    release_b = Event()
    b_live = Event()
    overlap = []
    calls = []

    def journal(event):
        if event.kind == "dispatched" and event.action_id == "third":
            overlap.append(b_live.is_set())

    def run(request):
        name = request.action.id
        calls.append(name)
        if name == "start":
            return Completed(TypedValue("handle", "known"))
        if name == "a":
            assert b_running.wait(2)
        elif name == "b":
            b_live.set()
            b_running.set()
            assert release_b.wait(2)
            b_live.clear()
        elif name == "release-b":
            release_b.set()
        return Completed()

    drivers = {
        c.executor: MutationDriver(run, contract=c) for c in plan.driver_contracts
    }
    drivers["folder"] = MutationDriver(run)
    report = MutationExecutor(drivers, workers=3, journal=journal).execute(plan, {})
    assert report.succeeded, report
    assert overlap == [False]
    assert calls.index("release-b") < calls.index("third")
    assert report.retained_exclusions == ()


@weaver_test()
@pytest.mark.parametrize("fault", ["acknowledgement", "checkpoint", "final"])
def test_buffered_journal_faults_stop_admission_without_erasing_observed_truth(fault):
    from weaver.mutation.executor import MutationJournal

    base = settlers_plan()
    start, a, _, _ = (a for _, _, a in base.actions())
    plan = (
        with_actions(base, (start, a), required_completion=("a",))
        if fault == "acknowledgement"
        else sealed((_action("root"), _action("child", ("root",))))
    )
    persisted = []
    calls = []

    def write(events):
        if (
            fault == "acknowledgement"
            and any(e.kind == "acknowledged" for e in events)
            or fault == "checkpoint"
            and any(e.kind == "terminal" for e in events)
            or fault == "final"
            and events[-1].kind == "terminal"
            and events[-1].action_id == "child"
        ):
            raise OSError(fault + " sink unavailable")
        persisted.extend(events)

    def run(request):
        calls.append(request.action.id)
        assert any(
            e.kind == "dispatched" and e.action_id == request.action.id
            for e in persisted
        )
        if request.action.id == "start":
            return Completed(TypedValue("handle", "known"))
        return Completed()

    drivers = {
        c.executor: MutationDriver(run, contract=c) for c in plan.driver_contracts
    }
    drivers["folder"] = MutationDriver(run)
    report = MutationExecutor(
        drivers,
        journal=MutationJournal(
            write, checkpoint_size=1 if fault == "checkpoint" else 64
        ),
    ).execute(plan, {})
    assert report.journal_errors == (fault + " sink unavailable",)
    assert not report.succeeded
    if fault == "acknowledgement":
        assert calls == ["start"]
        assert report.by_id["start"].status == "uncertain"
        assert report.by_id["a"].status == "blocked"
        assert report.operations[0].handle.value == "known"
        assert report.retained_exclusions == (("endpoint", "start"),)
    elif fault == "checkpoint":
        assert calls == ["root"]
        assert report.by_id["root"].status == "succeeded"
        assert report.by_id["child"].status == "not_dispatched"
    else:
        assert calls == ["root", "child"]
        assert all(r.status == "succeeded" for r in report.results)


@weaver_test()
@pytest.mark.parametrize("typed", [False, True])
@pytest.mark.parametrize(
    "outcome", ["completed", "pending", "uncertain", "malformed-failure"]
)
def test_expired_nonfailure_outcomes_cannot_settle_or_certify(typed, outcome):
    from weaver.mutation.executor import Failed, Uncertain

    base = settlers_plan()
    actions = tuple(a for _, _, a in base.actions())[:2]
    root = "a" if typed else "root"
    successors = (
        _action("ordered", settle_after=(root,)),
        _action("certify", (root,), executor="completion_gate"),
    )
    plan = (
        with_actions(base, (*actions, *successors), required_completion=(root,))
        if typed
        else sealed((_action(root, exclusions=("writer",)), *successors))
    )
    clock = Clock()
    calls = []

    def run(request):
        calls.append(request.action.id)
        if request.action.id == "start":
            return Completed(TypedValue("handle", "known"))
        clock.now = 6
        return {
            "completed": Completed(),
            "pending": Pending("known", 100),
            "uncertain": Uncertain("lost response"),
            "malformed-failure": Failed(""),
        }[outcome]

    drivers = {
        c.executor: MutationDriver(run, contract=c) for c in plan.driver_contracts
    }
    drivers["folder"] = MutationDriver(run)
    report = MutationExecutor(drivers, clock=clock, timeout=5).execute(plan, {})
    assert report.by_id[root].status == "uncertain"
    assert report.by_id["ordered"].status == "not_dispatched"
    assert report.by_id["certify"].status == "blocked"
    assert report.retained_exclusions
    assert calls == (["start", "a"] if typed else ["root"])
    if typed:
        assert report.operations[0].status == "uncertain"


@weaver_test()
@pytest.mark.parametrize("stop", ["fail_fast", "cancel"])
def test_late_known_failure_remains_failed_when_policy_stops_successors(stop):
    from threading import Event

    from weaver.mutation.executor import Failed

    clock = Clock()
    cancellation = Event()
    calls = []

    def run(request):
        calls.append(request.action.id)
        clock.now = 6
        if stop == "cancel":
            cancellation.set()
        return Failed("confirmed terminal failure")

    report = MutationExecutor(
        {"folder": MutationDriver(run)},
        clock=clock,
        timeout=5,
        failure_policy="fail_fast" if stop == "fail_fast" else "continue_independent",
    ).execute(
        sealed(
            (
                _action("root", exclusions=("writer",)),
                _action("ordered", settle_after=("root",)),
            )
        ),
        {},
        cancellation=cancellation,
    )
    assert calls == ["root"]
    assert report.by_id["root"].status == "failed"
    assert report.by_id["root"].error == "confirmed terminal failure"
    assert report.by_id["ordered"].status == "not_dispatched"
    assert report.retained_exclusions == ()


@weaver_test()
def test_idle_cancellation_checks_sleep_without_spinning_or_resetting_deadline():
    from threading import Event

    cancellation = Event()

    class BoundedClock(Clock):
        def __init__(self):
            super().__init__()
            self.sleeps = []

        def sleep(self, seconds):
            assert 0 < seconds <= 0.05
            self.sleeps.append(seconds)
            super().sleep(seconds)

    clock = BoundedClock()
    calls = []

    def run(request):
        calls.append((request.action.id, request.deadline))
        return Pending("known", 100)

    report = MutationExecutor(
        {"folder": MutationDriver(run)}, clock=clock, timeout=0.2
    ).execute(
        sealed((_action("await", exclusions=("writer",)),)),
        {},
        cancellation=cancellation,
    )
    assert calls == [("await", 0.2)]
    assert len(clock.sleeps) == 4
    assert clock.now == 0.2
    assert report.by_id["await"].status == "uncertain"
    assert report.retained_exclusions == (("writer", "await"),)


@weaver_test()
def test_default_clock_idle_wait_can_be_interrupted_by_cancellation_event():
    from threading import Event

    from weaver.mutation.executor import Failed

    class CancelInsideWait(Event):
        def __init__(self):
            super().__init__()
            self.waits = []

        def wait(self, timeout=None):
            self.waits.append(timeout)
            self.set()
            return True

    cancellation = CancelInsideWait()
    calls = []

    def run(request):
        calls.append("run")
        return Pending("known", request.deadline + 100)

    def cancel(request):
        calls.append("cancel")
        return Failed("confirmed cancellation")

    report = MutationExecutor(
        {"folder": MutationDriver(run, cancel=cancel)}, timeout=0.2
    ).execute(sealed((_action("pending"),)), {}, cancellation=cancellation)
    assert len(cancellation.waits) == 1, (
        "default idle wait did not observe event interruption"
    )
    assert 0 < cancellation.waits[0] <= 0.05
    assert calls == ["run", "cancel"]
    assert report.by_id["pending"].status == "failed"
