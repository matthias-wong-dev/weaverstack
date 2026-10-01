"""Frozen selections and causally validated prerequisite evidence."""

from dataclasses import dataclass

from ..errors import BuildError
from .executor import LedgerEvent, MutationResult
from .models import DriverContract
from .recovery import TERMINAL, _recover
from .validation import validate_mutation_plan


@dataclass(frozen=True)
class PrerequisiteReceipt:
    plan_id: str
    invocation_id: str
    action_id: str
    contract: DriverContract | None
    result: MutationResult
    ledger: tuple[LedgerEvent, ...]
    selected: tuple[str, ...]
    prerequisites: tuple["PrerequisiteReceipt", ...] = ()


def receipt_for(plan, report, action_id, *, selected=None, prerequisites=()):
    if report.plan_id != plan.bundle_id or action_id not in report.by_id:
        raise BuildError("receipt report differs from plan")
    contracts = {c.executor: c for c in plan.driver_contracts}
    action = next(a for _, _, a in plan.actions() if a.id == action_id)
    receipt = PrerequisiteReceipt(
        plan.bundle_id,
        report.invocation_id,
        action_id,
        contracts.get(action.executor),
        report.by_id[action_id],
        report.ledger,
        tuple(r.action_id for r in report.results)
        if selected is None
        else tuple(selected),
        tuple(prerequisites),
    )
    validate_receipt(plan, receipt)
    return receipt


def validate_receipt(plan, receipt, *, seen=()):
    actions = {a.id: a for _, _, a in plan.actions()}
    contracts = {c.executor: c for c in plan.driver_contracts}
    if (
        not isinstance(receipt, PrerequisiteReceipt)
        or receipt.plan_id != plan.bundle_id
        or receipt.action_id not in actions
    ):
        raise BuildError("prerequisite receipt identity differs")
    identity = (receipt.invocation_id, receipt.action_id)
    if identity in seen or len(seen) >= len(actions):
        raise BuildError("cyclic prerequisite receipts")
    action = actions[receipt.action_id]
    if receipt.contract != contracts.get(action.executor):
        raise BuildError("prerequisite producer contract differs")
    external = validate_fragment(
        plan, receipt.selected, receipt.prerequisites, seen=(*seen, identity)
    )
    recovered = _recover(
        plan,
        receipt.ledger,
        invocation_id=receipt.invocation_id,
        selected=receipt.selected,
        external=external,
    )
    if (
        recovered.by_id.get(receipt.action_id) != receipt.result
        or receipt.result.status not in TERMINAL
    ):
        raise BuildError("prerequisite receipt lacks verified terminal evidence")
    return receipt.result


def validate_fragment(plan, selected, prerequisites=(), *, seen=()):
    validate_mutation_plan(plan)
    actions = {a.id: a for _, _, a in plan.actions()}
    if (
        not isinstance(selected, (list, tuple))
        or (not selected and actions)
        or any(not isinstance(k, str) for k in selected)
        or len(selected) != len(set(selected))
        or not set(selected) <= actions.keys()
    ):
        raise BuildError("invalid fragment action selection")
    chosen = set(selected)
    required, successes = set(), set()
    for key in chosen:
        action = actions[key]
        successful = set(action.depends_on) | set(action.certifies)
        if action.result_from is not None:
            successful.add(action.result_from.action_id)
        required.update((successful | set(action.settle_after)) - chosen)
        successes.update(successful - chosen)
    if not isinstance(prerequisites, (list, tuple)) or any(
        not isinstance(r, PrerequisiteReceipt) for r in prerequisites
    ):
        raise BuildError("invalid prerequisite receipts")
    ids = [r.action_id for r in prerequisites]
    if len(ids) != len(set(ids)) or set(ids) != required:
        raise BuildError("fragment prerequisite inventory differs")
    contracts = {c.executor: c for c in plan.driver_contracts}
    for action in actions.values():
        contract = contracts.get(action.executor)
        if contract and contract.settles_operation:
            if (action.id in chosen) != (action.result_from.action_id in chosen):
                raise BuildError("split asynchronous operation is unsupported")
    for key in required:
        stack = list(actions[key].depends_on + actions[key].settle_after)
        visited = set()
        while stack:
            parent = stack.pop()
            if parent in chosen:
                raise BuildError(
                    "fragment prerequisite depends on an unfinished local successor"
                )
            if parent not in visited:
                visited.add(parent)
                stack.extend(actions[parent].depends_on + actions[parent].settle_after)
    results = {}
    for receipt in prerequisites:
        result = validate_receipt(plan, receipt, seen=seen)
        if receipt.action_id in successes and result.status != "succeeded":
            raise BuildError("fragment requires a successful prerequisite")
        results[receipt.action_id] = result
    return results
