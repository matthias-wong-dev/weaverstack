"""Shared structural, causal and destructive-intent validation."""

import re
from typing import Mapping

from ..errors import BuildError
from .models import MutationPlan


def validate_mutation_plan(plan: MutationPlan) -> None:
    from ..errors import GraphError
    from ..graph import Graph
    from .bundle import VALID_EXECUTORS, _check_payload_path, _validate_action_shape
    from .execution import BundleEnvironment, MutationExecution
    from .models import MutationAction, MutationBatch, MutationSequence
    from .serialization import require_string

    if not isinstance(plan.execution, MutationExecution):
        raise BuildError("execution requires a frozen mutation identity")
    if not isinstance(plan.bundle_id, str):
        raise BuildError("bundle_id must be a string")
    for name in ("workspace_id", "catalogue_target_id", "spark_home_target_id"):
        require_string(getattr(plan.execution, name), what=name, optional=True)
    environment = plan.execution.environment
    if environment is not None:
        if not isinstance(environment, BundleEnvironment):
            raise BuildError("Environment requires a frozen identity")
        require_string(environment.name, what="Environment name")
        for name in ("workspace", "item_id"):
            require_string(
                getattr(environment, name), what=f"Environment {name}", optional=True
            )
    if plan.build_envelope is not None:
        allowed = {
            "repository_name",
            "repository_signature",
            "selection",
            "omitted_nodes",
            "target_changes",
            "runtime_state",
            "runtime_state_established",
        }
        if (
            not isinstance(plan.build_envelope, Mapping)
            or set(plan.build_envelope) - allowed
        ):
            raise BuildError("invalid Build envelope fields")
    if type(plan.format_version) is not int or plan.format_version != 5:
        raise BuildError(
            f"Mutation format version {plan.format_version!r} is not supported"
        )
    require_string(plan.execution.workspace_name, what="workspace_name")
    targets = {}
    for target in plan.targets:
        for name, value in target.to_mapping().items():
            require_string(value, what=f"target {name}")
        if target.id in targets:
            raise BuildError(f"duplicate target {target.id!r}")
        if target.kind not in {"lakehouse", "warehouse"}:
            raise BuildError(f"unknown target kind {target.kind!r}")
        if (target.logical_item_type is None) != (target.logical_item_name is None):
            raise BuildError(f"incomplete logical item for target {target.id!r}")
        if (
            target.logical_item_type is not None
            and {"Lakehouse": "lakehouse", "Warehouse": "warehouse"}.get(
                target.logical_item_type
            )
            != target.kind
        ):
            raise BuildError(f"incompatible logical item for target {target.id!r}")
        targets[target.id] = target
    batches = set()
    numbers = []
    ids = set()
    for sequence in plan.sequences:
        if not isinstance(sequence, MutationSequence) or any(
            not isinstance(b, MutationBatch) for b in sequence.batches
        ):
            raise BuildError("common plan requires frozen mutation groups")
        if type(sequence.number) is not int or not isinstance(
            sequence.description, str
        ):
            raise BuildError("invalid sequence presentation")
        numbers.append(sequence.number)
        for batch in sequence.batches:
            require_string(batch.id, what="batch id")
            require_string(batch.target_id, what="batch target id")
            if batch.id in batches:
                raise BuildError(f"duplicate batch {batch.id!r}")
            batches.add(batch.id)
            if batch.target_id not in targets:
                raise BuildError(f"unknown target {batch.target_id!r}")
            for action in batch.actions:
                if not isinstance(action, MutationAction):
                    raise BuildError("mutation actions require explicit depends_on")
                for name in ("id", "kind", "executor", "target_id"):
                    require_string(getattr(action, name), what=f"action {name}")
                for name in ("resource_node_id", "source_path", "payload"):
                    require_string(
                        getattr(action, name), what=f"action {name}", optional=True
                    )
                if type(action.awaits_name_release) is not bool:
                    raise BuildError("awaits_name_release must be a boolean")
                if action.id in ids:
                    raise BuildError(f"duplicate action {action.id!r}")
                ids.add(action.id)
                if action.target_id != batch.target_id:
                    raise BuildError(
                        f"action {action.id!r} has inconsistent target binding"
                    )
                if action.payload is not None and action.payload_sha256 is None:
                    raise BuildError(f"action {action.id!r} has no payload hash")
                if action.payload_sha256 is not None and (
                    not isinstance(action.payload_sha256, str)
                    or re.fullmatch(r"[0-9a-f]{64}", action.payload_sha256) is None
                ):
                    raise BuildError(f"action {action.id!r} has invalid payload hash")
    if numbers != sorted(set(numbers)):
        raise BuildError("sequence numbers must be uniquely ordered")
    if plan.execution.catalogue_target_id is not None:
        catalogue = targets.get(plan.execution.catalogue_target_id)
        if catalogue is None or catalogue.kind != "warehouse":
            raise BuildError("invalid catalogue target")
    home_id = plan.execution.spark_home_target_id
    if home_id is not None and (
        home_id not in targets or targets[home_id].kind != "lakehouse"
    ):
        raise BuildError("invalid Spark attachment")
    contracts = {}
    for contract in plan.driver_contracts:
        require_string(contract.executor, what="driver executor")
        require_string(contract.produces, what="produced result type", optional=True)
        require_string(contract.consumes, what="consumed result type", optional=True)
        if (
            contract.executor in contracts
            or contract.executor in VALID_EXECUTORS
            or contract.executor == "completion_gate"
        ):
            raise BuildError(
                f"duplicate or built-in driver contract {contract.executor!r}"
            )
        if contract.payload_extension is not None and (
            not isinstance(contract.payload_extension, str)
            or not contract.payload_extension.startswith(".")
            or "/" in contract.payload_extension
            or "\\" in contract.payload_extension
        ):
            raise BuildError("invalid driver payload extension")
        if (
            type(contract.starts_operation) is not bool
            or type(contract.settles_operation) is not bool
        ):
            raise BuildError("driver completion policy must be boolean")
        if contract.starts_operation and (
            contract.produces is None or contract.settles_operation
        ):
            raise BuildError("operation starter requires a produced result type")
        if contract.settles_operation and contract.consumes is None:
            raise BuildError("operation settler requires a consumed result type")
        contracts[contract.executor] = contract
    omitted_ids = {
        node["node_id"] for node in (plan.build_envelope or {}).get("omitted_nodes", ())
    }
    actions = [a for _, _, a in plan.actions()]
    if (
        any(
            a.executor in {"spark_sql", "spark_sql_batch", "spark_table"}
            for a in actions
        )
        and home_id is None
    ):
        raise BuildError("Spark work requires a frozen Lakehouse attachment")
    for action in actions:
        if action.executor == "completion_gate":
            if action.payload is not None or action.payload_sha256 is not None:
                raise BuildError("completion gate must be payloadless")
        elif action.executor in contracts:
            contract = contracts[action.executor]
            if contract.payload_extension is None:
                if action.payload is not None or action.payload_sha256 is not None:
                    raise BuildError("unexpected extension payload")
            else:
                if action.payload is None or action.payload_sha256 is None:
                    raise BuildError("missing extension payload")
                _check_payload_path(action.payload)
                if not action.payload.endswith(contract.payload_extension):
                    raise BuildError("invalid extension payload")
        else:
            _validate_action_shape(action, omitted_ids)
        edges = (*action.depends_on, *action.settle_after)
        if len(edges) != len(set(edges)):
            raise BuildError(f"action {action.id!r} has a duplicate ordering edge")
    try:
        ordering = Graph(
            (a.id for a in actions),
            ((dep, a.id) for a in actions for dep in (*a.depends_on, *a.settle_after)),
        )
        success = Graph(
            (a.id for a in actions),
            ((dep, a.id) for a in actions for dep in a.depends_on),
        )
    except GraphError as exc:
        raise BuildError(str(exc)) from exc

    _validate_results_and_completion(plan, actions, contracts, success)
    _validate_scopes(plan, actions, ordering)
    if plan.bundle_id:
        from .bundle import compute_bundle_id

        if plan.bundle_id != compute_bundle_id(plan):
            raise BuildError("mutation bundle identity does not match its plan")


def _validate_results_and_completion(plan, actions, contracts, graph):
    from .models import ResultReference
    from .serialization import require_string

    by_id = {a.id: a for a in actions}
    ancestors = {}

    def before(node):
        if node not in ancestors:
            ancestors[node] = frozenset(graph.ancestors(node))
        return ancestors[node]

    if len(plan.required_completion) != len(set(plan.required_completion)):
        raise BuildError("duplicate required completion")
    for required in plan.required_completion:
        if required not in by_id:
            raise BuildError(f"unknown required completion {required!r}")
    settlements = {}
    starters = {
        a.id
        for a in actions
        if (c := contracts.get(a.executor)) is not None and c.starts_operation
    }
    for action in actions:
        contract = contracts.get(action.executor)
        expected = None if contract is None else contract.consumes
        reference = action.result_from
        if expected is None and reference is not None:
            raise BuildError(f"action {action.id!r} does not consume a result")
        if expected is not None:
            if not isinstance(reference, ResultReference):
                raise BuildError(
                    f"action {action.id!r} requires a typed result reference"
                )
            require_string(reference.action_id, what="result producer")
            require_string(reference.result_type, what="result type")
            producer = by_id.get(reference.action_id)
            producer_contract = (
                None if producer is None else contracts.get(producer.executor)
            )
            if (
                producer_contract is None
                or producer_contract.produces != expected
                or reference.result_type != expected
            ):
                raise BuildError(
                    f"action {action.id!r} has an incompatible result producer"
                )
            if reference.action_id not in before(action.id):
                raise BuildError(
                    f"result producer {reference.action_id!r} is not an ancestor of {action.id!r}"
                )
            if contract.settles_operation:
                if reference.action_id not in starters:
                    raise BuildError(
                        "operation settlement requires an operation starter"
                    )
                settlements.setdefault(reference.action_id, set()).add(action.id)
        if len(action.certifies) != len(set(action.certifies)):
            raise BuildError(f"duplicate certification member in {action.id!r}")
        for member in action.certifies:
            if member not in before(action.id):
                raise BuildError(
                    f"certification {action.id!r} lacks prerequisite {member!r}"
                )

    required_closure = set(plan.required_completion)
    for required in plan.required_completion:
        required_closure.update(before(required))
    for starter in starters:
        gates = settlements.get(starter, set())
        if not gates.intersection(required_closure):
            raise BuildError(f"operation {starter!r} has no required completion gate")
        for required in plan.required_completion:
            if starter in before(required) and not gates.intersection(
                before(required) | {required}
            ):
                raise BuildError(
                    f"required completion {required!r} precedes settlement of {starter!r}"
                )
        for action in actions:
            if (
                action.certifies
                and starter in before(action.id)
                and not gates.intersection(before(action.id))
            ):
                raise BuildError(
                    f"certification {action.id!r} precedes completion of {starter!r}"
                )


def _validate_scopes(plan, actions, graph):
    from .scopes import ScopeRules

    rules = ScopeRules(plan)
    check, covers, overlaps = rules.check, rules.covers, rules.overlaps

    for scope in plan.protected_scopes:
        check(scope)
    writers = []
    for action in actions:
        for name in ("resources", "exclusions"):
            values = getattr(action, name)
            if len(values) != len(set(values)):
                raise BuildError(f"duplicate {name} on action {action.id!r}")
        for scope in (*action.writes, *action.destructive_scopes):
            check(scope)
            if scope.target_id != action.target_id:
                raise BuildError(f"scope for action {action.id!r} crosses its target")
        for destructive in action.destructive_scopes:
            if not any(covers(write, destructive) for write in action.writes):
                raise BuildError(
                    f"destructive scope of {action.id!r} is outside its writes"
                )
            if any(
                overlaps(destructive, protected) for protected in plan.protected_scopes
            ):
                raise BuildError(
                    f"destructive scope of {action.id!r} overlaps a protected root"
                )
        if action.writes:
            writers.append(action)
    for index, left in enumerate(writers):
        for right in writers[index + 1 :]:
            if not any(overlaps(a, b) for a in left.writes for b in right.writes):
                continue
            if set(left.exclusions).intersection(right.exclusions):
                continue
            if left.id in graph.ancestors(right.id) or right.id in graph.ancestors(
                left.id
            ):
                continue
            raise BuildError(
                f"incompatible writers {left.id!r} and {right.id!r} require ordering or exclusion"
            )
