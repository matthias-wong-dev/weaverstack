from dataclasses import replace
from threading import Event

import pytest
from support.weaver_test import weaver_test
from test_mutation_executor_primitive import sealed
from test_mutation_plan_representation import _action

from weaver.errors import BuildError
from weaver.mutation import (
    DriverContract,
    MutationBatch,
    MutationPlan,
    MutationSequence,
    ResultReference,
)
from weaver.mutation.bundle import compute_bundle_id
from weaver.mutation.executor import (
    Completed,
    MutationDriver,
    MutationExecutor,
    TypedValue,
)
from weaver.mutation.recovery import recover
from weaver.sessions.mutation_receipts import decode_report, encode_report


def typed_plan(actions, required):
    contracts = (
        DriverContract("start", None, produces="handle", starts_operation=True),
        DriverContract("await", None, consumes="handle", settles_operation=True),
    )
    template = sealed((_action("template"),))
    plan = MutationPlan(
        targets=template.targets,
        execution=template.execution,
        sequences=(
            MutationSequence(1, "typed", (MutationBatch("typed", "sales", actions),)),
        ),
        driver_contracts=contracts,
        required_completion=required,
    )
    return replace(plan, bundle_id=compute_bundle_id(plan)), contracts


def await_action(key, owner):
    return _action(
        key, (owner,), executor="await", result_from=ResultReference(owner, "handle")
    )


def round_trip(plan, report):
    return decode_report(
        plan, encode_report(report), invocation_id=report.invocation_id
    )


@weaver_test()
def test_typed_acknowledgements_recover_in_frozen_order():
    plan, (start, awaiter) = typed_plan(
        (
            _action("a", executor="start"),
            _action("b", executor="start"),
            await_action("aa", "a"),
            await_action("bb", "b"),
        ),
        ("aa", "bb"),
    )
    released = Event()

    def run(request):
        if request.action.id == "a":
            assert released.wait(2)
        return Completed(TypedValue("handle", {"id": request.action.id}))

    def journal(event):
        if event.kind == "terminal" and event.action_id == "b":
            released.set()

    report = MutationExecutor(
        {
            "start": MutationDriver(run, contract=start),
            "await": MutationDriver(lambda r: Completed(), contract=awaiter),
        },
        workers=2,
        journal=journal,
    ).execute(plan)
    assert report.succeeded
    assert [e.action_id for e in report.ledger if e.kind == "acknowledged"] == [
        "b",
        "a",
    ]
    recovered = recover(plan, report.ledger, invocation_id=report.invocation_id)
    assert [o.action_id for o in recovered.operations] == ["a", "b"]
    assert recovered.operations == report.operations
    assert round_trip(plan, report) == report


@weaver_test()
def test_uncertain_observer_preserves_known_operation_settlement():
    from weaver.mutation.executor import Uncertain

    plan, (start, awaiter) = typed_plan(
        (
            _action("start", executor="start"),
            await_action("known", "start"),
            await_action("unknown", "start"),
        ),
        ("known", "unknown"),
    )
    report = MutationExecutor(
        {
            "start": MutationDriver(
                lambda r: Completed(TypedValue("handle", {"id": "op"})), contract=start
            ),
            "await": MutationDriver(
                lambda r: (
                    Completed()
                    if r.action.id == "known"
                    else Uncertain("lost response")
                ),
                contract=awaiter,
            ),
        }
    ).execute(plan)
    assert [r.status for r in report.results] == ["succeeded", "succeeded", "uncertain"]
    assert report.operations[0].status == "settled"
    recovered = recover(plan, report.ledger, invocation_id=report.invocation_id)
    assert recovered.operations == report.operations
    assert round_trip(plan, report) == report


@weaver_test()
@pytest.mark.parametrize("checkpoint_size", [1, 64])
def test_failed_buffered_admission_preserves_observed_success(checkpoint_size):
    from weaver.mutation.executor import MutationJournal

    plan = sealed((_action("first"), _action("second", ("first",))))
    persisted, calls, faults = [], [], []

    def sink(events):
        if any(e.kind == "dispatched" and e.action_id == "second" for e in events):
            faults.append(events)
            raise OSError("second admission write failed")
        persisted.extend(events)

    report = MutationExecutor(
        {"folder": MutationDriver(lambda r: calls.append(r.action.id) or Completed())},
        journal=MutationJournal(sink, checkpoint_size=checkpoint_size),
    ).execute(plan)
    assert calls == ["first"]
    assert len(faults) == 1
    assert report.journal_errors == ("second admission write failed",)
    assert [r.status for r in report.results] == ["succeeded", "not_dispatched"]
    assert report.by_id["second"].attempts == 0
    assert recover(plan, persisted, invocation_id=report.invocation_id).by_id[
        "first"
    ].status == ("succeeded" if checkpoint_size == 1 else "uncertain")
    assert round_trip(plan, report) == report
    with pytest.raises(BuildError, match="refused admission.*diagnostic"):
        round_trip(plan, replace(report, journal_errors=()))
    second = report.by_id["second"]
    forged = replace(second, status="succeeded", attempts=1, error=None)
    forged_ledger = tuple(
        replace(e, value=forged)
        if e.kind == "terminal" and e.action_id == "second"
        else e
        for e in report.ledger
    )
    with pytest.raises(BuildError, match="refused admission requires"):
        recover(plan, forged_ledger, invocation_id=report.invocation_id)
    missing_intent = tuple(
        e
        for e in report.ledger
        if not (e.kind == "dispatched" and e.action_id == "second")
    )
    with pytest.raises(BuildError, match="invalid refused admission"):
        recover(plan, missing_intent, invocation_id=report.invocation_id)


@weaver_test()
def test_refused_group_admission_never_dispatches_batch_members():
    from weaver.mutation.executor import MutationJournal

    plan = sealed(
        (_action("first"), _action("second", ("first",)), _action("third", ("first",)))
    )
    calls, batches, faults = [], [], []

    def sink(events):
        if any(e.kind == "dispatched" and e.action_id == "second" for e in events):
            faults.append(events)
            raise OSError("group admission unavailable")

    report = MutationExecutor(
        {
            "folder": MutationDriver(
                lambda r: calls.append(r.action.id) or Completed(),
                batch=lambda requests, emit: (
                    batches.append(requests)
                    or [(r.action.id, Completed()) for r in requests]
                ),
                batch_size=2,
            )
        },
        workers=2,
        journal=MutationJournal(sink),
    ).execute(plan)
    assert calls == ["first"]
    assert batches == []
    assert len(faults) == 1
    assert [r.status for r in report.results] == [
        "succeeded",
        "not_dispatched",
        "not_dispatched",
    ]
    assert [e.action_id for e in report.ledger if e.kind == "admission_refused"] == [
        "second",
        "third",
    ]
    assert round_trip(plan, report) == report
