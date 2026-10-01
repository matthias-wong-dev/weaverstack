"""Validated invocation evidence; recovery never dispatches work."""

from dataclasses import replace
from math import isfinite

from ..errors import BuildError
from .executor import (
    LedgerEvent,
    MutationReport,
    MutationResult,
    Operation,
    Pending,
    TypedValue,
)
from .serialization import freeze_value
from .validation import validate_mutation_plan

TERMINAL = frozenset({"succeeded", "failed", "blocked"})
STATUSES = TERMINAL | {"uncertain", "not_dispatched"}


def validate_result(action, contract, result):
    if (
        not isinstance(result, MutationResult)
        or result.action_id != action.id
        or result.status not in STATUSES
    ):
        raise BuildError("invalid action result identity or status")
    for count in (result.attempts, result.observations):
        if type(count) is not int or count < 0:
            raise BuildError("invalid action result count")
    for value in (
        result.active_seconds,
        result.wait_seconds,
        result.ready_queue_seconds,
        result.resource_queue_seconds,
    ):
        if type(value) not in (int, float) or not isfinite(value) or value < 0:
            raise BuildError("invalid action result timing")
    if result.status != "succeeded":
        if not isinstance(result.error, str) or not result.error:
            raise BuildError("unsuccessful action result requires an error")
        if result.value is not None:
            raise BuildError("unsuccessful action cannot provide a result")
    elif result.error is not None:
        raise BuildError("successful action cannot carry an error")
    if result.status == "succeeded" and contract and contract.produces:
        if (
            not isinstance(result.value, TypedValue)
            or result.value.result_type != contract.produces
        ):
            raise BuildError("typed producer result differs from its contract")
    if isinstance(result.value, TypedValue):
        if not contract or contract.produces != result.value.result_type:
            raise BuildError("unexpected typed producer result")
        freeze_value(result.value.value)
    else:
        freeze_value(result.value)


def recover(plan, events, *, invocation_id, selected=None, prerequisites=()):
    from .fragments import validate_fragment

    chosen = tuple(a.id for _, _, a in plan.actions()) if selected is None else selected
    external = validate_fragment(plan, chosen, prerequisites)
    return _recover(
        plan, events, invocation_id=invocation_id, selected=chosen, external=external
    )


def _recover(plan, events, *, invocation_id, selected, external):
    validate_mutation_plan(plan)
    if not plan.bundle_id or not isinstance(invocation_id, str) or not invocation_id:
        raise BuildError("recovery requires sealed plan and invocation identities")
    actions = {a.id: a for _, _, a in plan.actions()}
    contracts = {c.executor: c for c in plan.driver_contracts}
    selected = tuple(actions) if selected is None else tuple(selected)
    if len(selected) != len(set(selected)) or not set(selected) <= actions.keys():
        raise BuildError("invalid recovered action selection")
    results = dict(external)
    admitted, refused, pending, operations = set(), set(), set(), {}
    observations = {}
    ledger = tuple(events)
    previous = -float("inf")
    for event in ledger:
        if (
            not isinstance(event, LedgerEvent)
            or event.plan_id != plan.bundle_id
            or event.invocation_id != invocation_id
            or event.action_id not in selected
        ):
            raise BuildError("journal identity differs from invocation")
        if (
            type(event.at) not in (int, float)
            or not isfinite(event.at)
            or event.at < previous
        ):
            raise BuildError("invalid journal clock")
        previous = event.at
        key = event.action_id
        action = actions[key]
        contract = contracts.get(action.executor)
        if key in results:
            raise BuildError("journal event follows terminal action")
        if key in refused and event.kind != "terminal":
            raise BuildError("journal event follows refused admission")
        if event.kind == "dispatched":
            if key in admitted or event.value is not None:
                raise BuildError("duplicate or malformed admission")
            if any(
                p not in results or results[p].status != "succeeded"
                for p in action.depends_on
            ) or any(
                p not in results or results[p].status not in TERMINAL
                for p in action.settle_after
            ):
                raise BuildError("admission lacks causal prerequisites")
            admitted.add(key)
        elif event.kind == "admission_refused":
            if (
                key not in admitted
                or key in pending
                or key in operations
                or key in observations
                or not isinstance(event.value, str)
                or not event.value
            ):
                raise BuildError("invalid refused admission")
            admitted.remove(key)
            refused.add(key)
        elif event.kind == "observed":
            if key not in pending or event.value is not None:
                raise BuildError("observation lacks continuation")
            pending.remove(key)
            observations[key] = observations.get(key, 0) + 1
        elif event.kind == "pending":
            if (
                key not in admitted
                or key in pending
                or not isinstance(event.value, Pending)
                or event.value.continuation is None
                or type(event.value.next_poll_at) not in (int, float)
                or not isfinite(event.value.next_poll_at)
                or event.value.next_poll_at <= event.at
            ):
                raise BuildError("invalid journal continuation")
            freeze_value(event.value.continuation)
            pending.add(key)
        elif event.kind == "acknowledged":
            op = event.value
            if (
                key not in admitted
                or key in operations
                or not contract
                or not contract.starts_operation
                or not isinstance(op, Operation)
                or op.action_id != key
                or not isinstance(op.handle, TypedValue)
                or op.handle.result_type != contract.produces
                or type(op.deadline) not in (int, float)
                or not isfinite(op.deadline)
                or op.status not in {"acknowledged", "uncertain"}
                or op.exclusions != action.exclusions
            ):
                raise BuildError("invalid typed operation acknowledgement")
            freeze_value(op.handle.value)
            operations[key] = op
        elif event.kind == "terminal":
            result = event.value
            validate_result(action, contract, result)
            if key in refused and result.status != "not_dispatched":
                raise BuildError("refused admission requires unstarted result")
            expired_observer = bool(
                result.status == "uncertain"
                and key not in admitted
                and contract
                and contract.settles_operation
                and action.result_from.action_id in operations
                and event.at >= operations[action.result_from.action_id].deadline
                and all(
                    p in results and results[p].status == "succeeded"
                    for p in action.depends_on
                )
                and all(
                    p in results and results[p].status in TERMINAL
                    for p in action.settle_after
                )
            )
            expected_attempts = (
                0
                if result.status in {"blocked", "not_dispatched"} or expired_observer
                else 1
            )
            if (
                result.attempts != expected_attempts
                or result.observations != observations.get(key, 0)
            ):
                raise BuildError("terminal counters differ from journal evidence")
            if result.status in {"blocked", "not_dispatched"} and key in admitted:
                raise BuildError("unstarted result follows durable admission")
            if result.status == "succeeded" and key in pending:
                raise BuildError("success lacks continuation observation")
            if (
                result.status in {"succeeded", "failed", "uncertain"}
                and key not in admitted
                and not expired_observer
            ):
                raise BuildError("terminal action lacks admission")
            if result.status == "blocked" and not any(
                p in results and results[p].status != "succeeded"
                for p in action.depends_on
            ):
                raise BuildError("blocked result lacks unsuccessful prerequisite")
            if result.status == "succeeded" and contract and contract.starts_operation:
                if (
                    key not in operations
                    or operations[key].handle != result.value
                    or operations[key].status != "acknowledged"
                ):
                    raise BuildError("successful operation lacks acknowledgement")
            if key in operations and result.status == "uncertain":
                operations[key] = replace(operations[key], status="uncertain")
            if (
                contract
                and contract.settles_operation
                and (key in admitted or expired_observer)
            ):
                owner = action.result_from.action_id
                if owner not in operations:
                    raise BuildError("settlement lacks operation acknowledgement")
                if operations[owner].status != "settled":
                    operations[owner] = replace(
                        operations[owner],
                        status="settled" if result.status in TERMINAL else "uncertain",
                    )
            results[key] = result
        else:
            raise BuildError("unknown journal event kind")
    for key in selected:
        if key not in results and key in admitted:
            results[key] = MutationResult(
                key,
                "uncertain",
                attempts=1,
                error="admitted action has no terminal receipt",
            )
            if key in operations:
                operations[key] = replace(operations[key], status="uncertain")
    # Causal closure may run against manifest order unrelated to topology.
    changed = True
    while changed:
        changed = False
        for key in selected:
            if key not in results and any(
                p in results and results[p].status != "succeeded"
                for p in actions[key].depends_on
            ):
                results[key] = MutationResult(
                    key, "blocked", error="unsuccessful prerequisite"
                )
                changed = True
    for key in selected:
        if key not in results:
            results[key] = MutationResult(
                key, "not_dispatched", error="no durable admission"
            )
    return MutationReport(
        plan.bundle_id,
        tuple(results[k] for k in actions if k in selected),
        ledger,
        tuple(operations[k] for k in actions if k in operations),
        invocation_id,
    )
