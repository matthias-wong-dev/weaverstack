import pytest
from support.weaver_test import weaver_test
from test_mutation_executor_primitive import sealed
from test_mutation_plan_representation import _action

from weaver.errors import BuildError
from weaver.mutation.executor import (
    Completed,
    LedgerEvent,
    MutationDriver,
    MutationExecutor,
    MutationJournal,
    MutationResult,
)


def event(plan, kind, action, value=None, at=0):
    return LedgerEvent(kind, action, at, value, plan.bundle_id, "invocation")


@weaver_test()
def test_recovery_keeps_out_of_order_success_and_distinguishes_unadmitted():
    from weaver.mutation import recovery

    plan = sealed(
        (
            _action("first"),
            _action("second"),
            _action("child", ("first",)),
            _action("last"),
        )
    )
    events = (
        event(plan, "dispatched", "second"),
        event(
            plan,
            "terminal",
            "second",
            MutationResult("second", "succeeded", attempts=1),
        ),
        event(plan, "dispatched", "first"),
    )
    recovered = recovery.recover(plan, events, invocation_id="invocation")
    assert [r.status for r in recovered.results] == [
        "uncertain",
        "succeeded",
        "blocked",
        "not_dispatched",
    ]
    assert recovered.by_id["second"].attempts == 1
    assert not recovered.succeeded
    assert (
        recovery.recover(plan, (), invocation_id="invocation").by_id["first"].status
        == "not_dispatched"
    )


@weaver_test()
def test_durable_checkpoints_round_trip_real_executor_without_growing_reports():
    from weaver.sessions import mutation_receipts

    storage = {}
    plan = sealed((_action("one"), _action("two", ("one",))))
    sink = mutation_receipts.DurableJournal(
        lambda path, data: storage.__setitem__(path, data),
        "result",
        plan.bundle_id,
        "invocation",
    )
    calls = []

    def run(request):
        head = mutation_receipts.loads(storage["result"])
        events = mutation_receipts.read_journal(head, storage.__getitem__, "result")
        assert any(
            e.kind == "dispatched" and e.action_id == request.action.id for e in events
        )
        calls.append(request.action.id)
        return Completed()

    report = MutationExecutor(
        {"folder": MutationDriver(run)},
        journal=MutationJournal(sink, checkpoint_size=2),
    ).execute(plan, invocation_id="invocation")
    head = mutation_receipts.loads(storage["result"])
    events = mutation_receipts.read_journal(head, storage.__getitem__, "result")
    from weaver.mutation.recovery import recover

    assert recover(plan, events, invocation_id="invocation").results == report.results
    encoded = mutation_receipts.encode_report(report)
    assert (
        mutation_receipts.decode_report(
            plan, encoded, invocation_id="invocation"
        ).results
        == report.results
    )
    assert calls == ["one", "two"]
    assert all(
        len(mutation_receipts.loads(data)["events"]) <= 2
        for name, data in storage.items()
        if name != "result"
    )
    chunk = next(name for name in storage if name != "result")
    storage[chunk] += b" "
    with pytest.raises(ValueError, match="receipt"):
        mutation_receipts.read_journal(head, storage.__getitem__, "result")


@weaver_test()
@pytest.mark.parametrize(
    "fault", ["duplicate", "unknown", "identity", "causal", "missing-admission", "type"]
)
def test_recovery_rejects_untrusted_action_evidence(fault):
    from weaver.mutation import recovery

    plan = sealed((_action("first"), _action("child", ("first",))))
    events = [
        event(plan, "dispatched", "first"),
        event(
            plan, "terminal", "first", MutationResult("first", "succeeded", attempts=1)
        ),
    ]
    if fault == "duplicate":
        events.append(events[-1])
    elif fault == "unknown":
        events.append(event(plan, "dispatched", "other"))
    elif fault == "identity":
        events.append(
            LedgerEvent(
                "dispatched", "child", 1, plan_id=plan.bundle_id, invocation_id="other"
            )
        )
    elif fault == "causal":
        events = [event(plan, "dispatched", "child")]
    elif fault == "missing-admission":
        events = events[1:]
    else:
        events[-1] = event(plan, "terminal", "first", {"status": "succeeded"})
    with pytest.raises(BuildError):
        recovery.recover(plan, events, invocation_id="invocation")


@weaver_test()
@pytest.mark.parametrize(
    "fault", ["attempts", "observations", "not-dispatched", "success-input"]
)
def test_recovery_rejects_terminal_counters_and_typed_input_forgery(fault):
    from weaver.mutation.recovery import recover

    plan = sealed((_action("first"),))
    result = MutationResult("first", "succeeded", attempts=1)
    events = [event(plan, "dispatched", "first")]
    if fault == "attempts":
        result = MutationResult("first", "succeeded", attempts=0)
    elif fault == "observations":
        result = MutationResult("first", "succeeded", attempts=1, observations=2)
    elif fault == "not-dispatched":
        result = MutationResult(
            "first", "not_dispatched", attempts=1, error="not started"
        )
    else:
        result = MutationResult("first", "succeeded", attempts=1)
        events = []
    events.append(event(plan, "terminal", "first", result))
    with pytest.raises(BuildError):
        recover(plan, events, invocation_id="invocation")


@weaver_test()
@pytest.mark.parametrize(
    "fault",
    ["nan-poll", "infinite-poll", "bool-poll", "admitted-unstarted", "pending-success"],
)
def test_recovery_rejects_malformed_continuation_and_terminal_state(fault):
    from weaver.mutation.executor import Pending
    from weaver.mutation.recovery import recover

    plan = sealed((_action("first"),))
    events = [event(plan, "dispatched", "first")]
    if fault == "admitted-unstarted":
        events.append(
            event(
                plan,
                "terminal",
                "first",
                MutationResult("first", "not_dispatched", error="not started"),
            )
        )
    else:
        poll = {
            "nan-poll": float("nan"),
            "infinite-poll": float("inf"),
            "bool-poll": True,
            "pending-success": 1,
        }[fault]
        events.append(event(plan, "pending", "first", Pending({"ticket": "id"}, poll)))
        if fault == "pending-success":
            events.append(
                event(
                    plan,
                    "terminal",
                    "first",
                    MutationResult("first", "succeeded", attempts=1),
                )
            )
    with pytest.raises(BuildError):
        recover(plan, events, invocation_id="invocation")


@weaver_test()
def test_recovery_rejects_bare_external_results_without_prerequisite_receipts():
    from weaver.mutation.recovery import recover

    plan = sealed((_action("first"), _action("child", ("first",))))
    events = (
        event(plan, "dispatched", "child"),
        event(
            plan, "terminal", "child", MutationResult("child", "succeeded", attempts=1)
        ),
    )
    with pytest.raises(BuildError, match="prerequisite"):
        recover(
            plan,
            events,
            invocation_id="invocation",
            selected=("child",),
            prerequisites=(MutationResult("first", "succeeded", attempts=1),),
        )
