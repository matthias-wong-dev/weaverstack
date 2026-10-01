from dataclasses import replace

import pytest
from support.weaver_test import weaver_test
from test_mutation_executor_primitive import sealed
from test_mutation_plan_representation import _action

from weaver.errors import BuildError
from weaver.mutation.executor import (
    Completed,
    Failed,
    MutationDriver,
    MutationExecutor,
    TypedValue,
)


@weaver_test()
@pytest.mark.parametrize("outcome", ["success", "failure"])
def test_selected_fragment_preserves_success_and_terminal_receipt_distinction(outcome):
    from weaver.mutation import fragments

    plan = sealed(
        (
            _action("parent"),
            _action("ordered", settle_after=("parent",)),
            _action("child", ("parent",)),
        )
    )
    source = MutationExecutor(
        {
            "folder": MutationDriver(
                lambda r: (
                    Failed("rejected")
                    if r.action.id == "parent" and outcome == "failure"
                    else Completed()
                )
            )
        }
    ).execute(plan)
    receipt = fragments.receipt_for(plan, source, "parent")
    calls = []
    driver = MutationDriver(lambda r: calls.append(r.action.id) or Completed())
    report = MutationExecutor({"folder": driver}).execute(
        plan, selected=("ordered",), prerequisites=(receipt,)
    )
    assert calls == ["ordered"]
    assert [r.action_id for r in report.results] == ["ordered"]
    assert report.plan_id == plan.bundle_id
    if outcome == "failure":
        with pytest.raises(BuildError, match="successful"):
            MutationExecutor({"folder": driver}).execute(
                plan, selected=("child",), prerequisites=(receipt,)
            )
    else:
        assert (
            MutationExecutor({"folder": driver})
            .execute(plan, selected=("child",), prerequisites=(receipt,))
            .succeeded
        )


@weaver_test()
@pytest.mark.parametrize(
    "fault", ["missing", "extra", "identity", "forged", "duplicate", "selection"]
)
def test_fragment_prerequisites_are_exact_and_evidenced_before_preflight(fault):
    from weaver.mutation import fragments

    plan = sealed((_action("parent"), _action("child", ("parent",)), _action("other")))
    source = MutationExecutor(
        {"folder": MutationDriver(lambda r: Completed())}
    ).execute(plan)
    receipt = fragments.receipt_for(plan, source, "parent")
    selected, receipts = ("child",), (receipt,)
    if fault == "missing":
        receipts = ()
    elif fault == "extra":
        receipts += (fragments.receipt_for(plan, source, "other"),)
    elif fault == "identity":
        receipts = (replace(receipt, plan_id="different"),)
    elif fault == "forged":
        receipts = (replace(receipt, ledger=()),)
    elif fault == "duplicate":
        receipts += receipts
    else:
        selected = ("unknown",)
    calls = []
    with pytest.raises(BuildError):
        MutationExecutor(
            {
                "folder": MutationDriver(
                    lambda r: Completed(), preflight=lambda *a: calls.append(a)
                )
            }
        ).execute(plan, selected=selected, prerequisites=receipts)
    assert calls == []


@weaver_test()
def test_fragment_typed_result_retains_producer_contract():
    from test_mutation_plan_representation import _plan

    from weaver.mutation import DriverContract, ResultReference, fragments
    from weaver.mutation.bundle import compute_bundle_id

    producer = DriverContract("produce", None, produces="snapshot")
    consumer = DriverContract("consume", None, consumes="snapshot")
    from weaver.mutation import MutationBatch, MutationPlan, MutationSequence

    base = _plan((_action("base"),))
    plan = MutationPlan(
        targets=base.targets,
        execution=base.execution,
        driver_contracts=(producer, consumer),
        sequences=(
            MutationSequence(
                1,
                "typed",
                (
                    MutationBatch(
                        "typed",
                        "sales",
                        (
                            _action("producer", executor="produce"),
                            _action(
                                "consumer",
                                ("producer",),
                                executor="consume",
                                result_from=ResultReference("producer", "snapshot"),
                            ),
                        ),
                    ),
                ),
            ),
        ),
    )
    plan = replace(plan, bundle_id=compute_bundle_id(plan))
    seen = []
    drivers = {
        "produce": MutationDriver(
            lambda r: Completed(TypedValue("snapshot", {"version": 7})),
            contract=producer,
        ),
        "consume": MutationDriver(
            lambda r: seen.append(r.input) or Completed(), contract=consumer
        ),
    }
    source = MutationExecutor(drivers).execute(plan)
    receipt = fragments.receipt_for(plan, source, "producer")
    from weaver.sessions.mutation_receipts import decode, dumps, encode, loads

    receipt = decode(loads(dumps(encode(receipt))))
    seen.clear()
    assert (
        MutationExecutor({"consume": drivers["consume"]})
        .execute(plan, selected=("consumer",), prerequisites=(receipt,))
        .succeeded
    )
    assert seen == [TypedValue("snapshot", {"version": 7})]
    with pytest.raises(BuildError):
        MutationExecutor(drivers).execute(
            plan,
            selected=("consumer",),
            prerequisites=(replace(receipt, contract=consumer),),
        )


@weaver_test()
def test_empty_plan_has_a_successful_empty_invocation():
    assert MutationExecutor({}).execute(sealed(())).succeeded


@weaver_test()
def test_operation_receipts_validate_acknowledgement_and_refuse_split_handles():
    from test_mutation_plan_representation import _plan

    from weaver.mutation import (
        DriverContract,
        MutationBatch,
        MutationPlan,
        MutationSequence,
        ResultReference,
    )
    from weaver.mutation.bundle import compute_bundle_id
    from weaver.mutation.fragments import receipt_for
    from weaver.sessions.mutation_receipts import decode_report, encode_report

    start = DriverContract("start", None, produces="handle", starts_operation=True)
    awaiter = DriverContract("await", None, consumes="handle", settles_operation=True)
    base = _plan((_action("base"),))
    actions = (
        _action("start", executor="start"),
        _action(
            "await",
            ("start",),
            executor="await",
            result_from=ResultReference("start", "handle"),
        ),
    )
    plan = MutationPlan(
        targets=base.targets,
        execution=base.execution,
        driver_contracts=(start, awaiter),
        required_completion=("await",),
        sequences=(
            MutationSequence(
                1, "operation", (MutationBatch("operation", "sales", actions),)
            ),
        ),
    )
    plan = replace(plan, bundle_id=compute_bundle_id(plan))
    drivers = {
        "start": MutationDriver(
            lambda r: Completed(TypedValue("handle", {"operation": "id"})),
            contract=start,
        ),
        "await": MutationDriver(lambda r: Completed(), contract=awaiter),
    }
    report = MutationExecutor(drivers).execute(plan)
    assert (
        decode_report(
            plan, encode_report(report), invocation_id=report.invocation_id
        ).operations
        == report.operations
    )
    assert report.operations[0].status == "settled"
    receipt = receipt_for(plan, report, "start")
    for selected, prerequisites in ((("start",), ()), (("await",), (receipt,))):
        with pytest.raises(BuildError, match="split"):
            MutationExecutor(drivers).execute(
                plan, selected=selected, prerequisites=prerequisites
            )
    forged = replace(
        report, ledger=tuple(e for e in report.ledger if e.kind != "acknowledged")
    )
    with pytest.raises(BuildError, match="acknowledgement"):
        decode_report(plan, encode_report(forged), invocation_id=report.invocation_id)


@weaver_test()
def test_fragment_refuses_dependency_on_local_successor():
    from weaver.mutation.fragments import receipt_for

    plan = sealed(
        (_action("first"), _action("local", ("first",)), _action("last", ("local",)))
    )
    report = MutationExecutor(
        {"folder": MutationDriver(lambda r: Completed())}
    ).execute(plan)
    receipt = receipt_for(plan, report, "local")
    with pytest.raises(BuildError, match="local successor"):
        MutationExecutor({"folder": MutationDriver(lambda r: Completed())}).execute(
            plan, selected=("first", "last"), prerequisites=(receipt,)
        )
