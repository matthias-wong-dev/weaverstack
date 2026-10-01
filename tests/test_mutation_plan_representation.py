import hashlib
from dataclasses import replace

import pytest
from support.weaver_test import weaver_test

from weaver.build_bundle.bundle import (
    compute_bundle_id,
    load_bundle,
    write_bundle,
)
from weaver.errors import BuildError
from weaver.locations import Location
from weaver.store import FilesystemStore


@weaver_test()
def test_physical_only_plan_round_trips_binary_payload(tmp_path):
    import weaver

    assert hasattr(weaver, "mutation"), "shared mutation representation is missing"
    from weaver.mutation import (
        BoundTarget,
        MutationAction,
        MutationBatch,
        MutationExecution,
        MutationPlan,
        MutationSequence,
    )

    data = b"\x00\xff\x80runtime\r\n"
    action = MutationAction(
        id="write-runtime",
        kind="write_file",
        resource_node_id="Files/runtime",
        executor="load_file",
        payload="payload/runtime.payload",
        payload_sha256=hashlib.sha256(data).hexdigest(),
        target_id="sales",
        depends_on=[],
    )
    plan = MutationPlan(
        targets=[BoundTarget("sales", "lakehouse", "sales-id")],
        sequences=[
            MutationSequence(
                1, "runtime", [MutationBatch("runtime", "sales", [action])]
            )
        ],
        execution=MutationExecution(workspace_name="Demo"),
    )
    plan = replace(plan, bundle_id=compute_bundle_id(plan))
    store = FilesystemStore()
    location = Location(str(tmp_path / "bundle"))
    bundle = write_bundle(
        location,
        plan=plan,
        payloads={action.payload: data},
        store=store,
    )
    assert bundle.plan == plan
    assert load_bundle(location, store=store).plan == plan
    assert store.read(location.join("payload", "runtime.payload")) == data
    assert plan.build_envelope is None


def _action(id="root", depends_on=(), **kwargs):
    from weaver.mutation import MutationAction

    fields = dict(
        id=id,
        kind="build_folder",
        resource_node_id="Files/Incoming",
        executor="folder",
        payload=None,
        payload_sha256=None,
        target_id="sales",
        depends_on=depends_on,
    )
    fields.update(kwargs)
    return MutationAction(**fields)


def _plan(actions):
    from weaver.mutation import (
        BoundTarget,
        MutationBatch,
        MutationExecution,
        MutationPlan,
        MutationSequence,
    )

    return MutationPlan(
        targets=(BoundTarget("sales", "lakehouse", "sales-id"),),
        sequences=(
            MutationSequence(1, "physical", (MutationBatch("work", "sales", actions),)),
        ),
        execution=MutationExecution(workspace_name="Demo"),
    )


@weaver_test()
@pytest.mark.parametrize(
    "dependencies,match",
    [
        (("missing",), "unknown"),
        (("root",), "itself"),
        (("other", "other"), "duplicate dependency"),
        (("other",), "cycle"),
    ],
)
def test_direct_and_decoded_plans_reject_invalid_topology(dependencies, match):
    from weaver.build_bundle.bundle import plan_from_yaml
    from weaver.mutation import MutationPlan

    valid = _plan((_action(), _action("other", ("root",))))
    mapping = valid.to_mapping()
    mapping["sequences"][0]["batches"][0]["actions"][0]["depends_on"] = list(
        dependencies
    )
    with pytest.raises(BuildError, match=match):
        _plan((_action(depends_on=dependencies), _action("other", ("root",))))
    with pytest.raises(BuildError, match=match):
        MutationPlan.from_mapping(mapping)
    import yaml

    with pytest.raises(BuildError, match=match):
        plan_from_yaml(yaml.safe_dump(mapping))


@weaver_test()
@pytest.mark.parametrize(
    "location,key,value",
    [
        ("plan", "format_version", True),
        ("plan", "format_version", "5"),
        ("plan", "unexpected_graph", []),
        ("action", "depends_on", None),
        ("action", "depends_on", "root"),
        ("action", "depends_on", [True]),
        ("action", "id", False),
        ("action", "awaits_name_release", "false"),
        ("action", "future_nodes", []),
        ("target", "kind", "unknown"),
        ("target", "id", ""),
        ("target", "credential", "secret"),
        ("execution", "operation_handle", "runtime-only"),
        ("action", "payload", "payload\\escape.payload"),
        ("action", "payload_sha256", "invalid"),
    ],
)
def test_decode_rejects_malformed_physical_intent(location, key, value):
    from weaver.mutation import MutationPlan

    mapping = _plan((_action(),)).to_mapping()
    subject = {
        "plan": mapping,
        "action": mapping["sequences"][0]["batches"][0]["actions"][0],
        "target": mapping["targets"][0],
        "execution": mapping["execution"],
    }[location]
    subject[key] = value
    with pytest.raises(BuildError):
        MutationPlan.from_mapping(mapping)


@weaver_test()
def test_decode_requires_explicit_root_dependencies():
    from weaver.mutation import MutationPlan

    mapping = _plan((_action(),)).to_mapping()
    del mapping["sequences"][0]["batches"][0]["actions"][0]["depends_on"]
    with pytest.raises(BuildError, match="depends_on"):
        MutationPlan.from_mapping(mapping)


@weaver_test()
@pytest.mark.parametrize(
    "fault",
    [
        "duplicate_action",
        "duplicate_target",
        "unknown_target",
        "wrong_binding",
        "invalid_hash",
    ],
)
def test_direct_plan_rejects_invalid_physical_bindings(fault):
    from weaver.mutation import BoundTarget

    valid = _plan((_action(),))
    with pytest.raises(BuildError):
        if fault == "duplicate_action":
            _plan((_action(), _action()))
        elif fault == "duplicate_target":
            replace(valid, targets=valid.targets * 2)
        elif fault == "unknown_target":
            replace(valid, targets=(BoundTarget("other", "lakehouse", "other-id"),))
        elif fault == "wrong_binding":
            _plan((_action(target_id="other"),))
        else:
            _plan(
                (
                    _action(
                        executor="load_file",
                        payload="payload/a.payload",
                        payload_sha256="invalid",
                    ),
                )
            )


@weaver_test()
def test_dependency_permutations_have_one_owned_canonical_identity():
    from weaver.build_bundle.bundle import plan_from_yaml, plan_to_yaml
    from weaver.mutation import MutationPlan

    dependencies = ["a", "b"]
    actions = [_action("a"), _action("b"), _action("child", dependencies)]
    envelope = {"repository_name": "Source", "selection": {"selected": ["a"]}}
    plan = replace(_plan(actions), build_envelope=envelope)
    other = replace(
        _plan([_action("a"), _action("b"), _action("child", ["b", "a"])]),
        build_envelope=envelope,
    )
    identity = compute_bundle_id(plan)
    assert identity == compute_bundle_id(other)
    dependencies.clear()
    actions.clear()
    envelope["selection"]["selected"].append("b")
    assert compute_bundle_id(plan) == identity
    assert plan_from_yaml(plan_to_yaml(plan)) == plan
    assert MutationPlan.from_mapping(plan.to_mapping()) == plan
    with pytest.raises(TypeError):
        plan.build_envelope["selection"]["selected"] = ()
    assert compute_bundle_id(
        _plan((_action(), _action("child", ("root",))))
    ) != compute_bundle_id(_plan((_action(), _action("child"))))


@weaver_test()
def test_typed_operation_contract_round_trips_completion_intent():
    import weaver.mutation as mutation

    assert hasattr(mutation, "DriverContract"), "typed driver contract is missing"
    from weaver.build_bundle.bundle import plan_from_yaml, plan_to_yaml
    from weaver.mutation import DriverContract, ResultReference

    data = b"{}"
    start = _action(
        "start",
        executor="refresh_start",
        payload="payload/refresh.json",
        payload_sha256=hashlib.sha256(data).hexdigest(),
    )
    await_action = _action(
        "await",
        ("start",),
        executor="refresh_await",
        result_from=ResultReference("start", "refresh_operation"),
    )
    certify = _action(
        "certify", ("await",), executor="completion_gate", certifies=("start",)
    )
    contracts = (
        DriverContract(
            "refresh_start",
            ".json",
            produces="refresh_operation",
            starts_operation=True,
        ),
        DriverContract(
            "refresh_await", None, consumes="refresh_operation", settles_operation=True
        ),
    )
    # Supply the extension contracts in the same construction as their actions.
    base = _plan((_action(),))
    sequence = replace(
        base.sequences[0],
        batches=(
            replace(
                base.sequences[0].batches[0], actions=(start, await_action, certify)
            ),
        ),
    )
    plan = replace(
        base,
        sequences=(sequence,),
        driver_contracts=contracts,
        required_completion=("certify",),
    )
    assert plan_from_yaml(plan_to_yaml(plan)) == plan
    assert compute_bundle_id(plan) != compute_bundle_id(
        replace(plan, required_completion=("await",))
    )


def _operation_plan():
    from weaver.mutation import DriverContract, ResultReference

    base = _plan((_action(),))
    actions = (
        _action(
            "start",
            executor="refresh_start",
            payload="payload/refresh.json",
            payload_sha256=hashlib.sha256(b"{}").hexdigest(),
        ),
        _action(
            "await",
            ("start",),
            executor="refresh_await",
            result_from=ResultReference("start", "refresh_operation"),
        ),
        _action(
            "certify", ("await",), executor="completion_gate", certifies=("start",)
        ),
    )
    sequence = replace(
        base.sequences[0],
        batches=(replace(base.sequences[0].batches[0], actions=actions),),
    )
    return replace(
        base,
        sequences=(sequence,),
        driver_contracts=(
            DriverContract(
                "refresh_start",
                ".json",
                produces="refresh_operation",
                starts_operation=True,
            ),
            DriverContract(
                "refresh_await",
                None,
                consumes="refresh_operation",
                settles_operation=True,
            ),
        ),
        required_completion=("certify",),
    )


@weaver_test()
@pytest.mark.parametrize(
    "fault",
    [
        "unknown_producer",
        "wrong_type",
        "nonancestor",
        "missing_result",
        "missing_completion",
        "early_certification",
        "unknown_required",
        "duplicate_contract",
        "builtin_override",
    ],
)
def test_typed_references_require_causal_settlement_before_certification(fault):
    from weaver.mutation import MutationPlan

    plan = _operation_plan()
    mapping = plan.to_mapping()
    actions = mapping["sequences"][0]["batches"][0]["actions"]
    if fault == "unknown_producer":
        actions[1]["result_from"]["action_id"] = "missing"
    elif fault == "wrong_type":
        actions[1]["result_from"]["result_type"] = "query_shape"
    elif fault == "nonancestor":
        actions[1]["depends_on"] = []
    elif fault == "missing_result":
        actions[1]["result_from"] = None
    elif fault == "missing_completion":
        mapping["driver_contracts"][1]["settles_operation"] = False
    elif fault == "early_certification":
        actions[2]["depends_on"] = ["start"]
    elif fault == "unknown_required":
        mapping["required_completion"] = ["missing"]
    elif fault == "duplicate_contract":
        mapping["driver_contracts"].append(mapping["driver_contracts"][0])
    else:
        mapping["driver_contracts"][0]["executor"] = "folder"
    with pytest.raises(BuildError):
        MutationPlan.from_mapping(mapping)
    # Reconstruct the same intent with frozen types, bypassing the plan decoder.
    from weaver.mutation import DriverContract, MutationSequence

    with pytest.raises(BuildError):
        replace(
            plan,
            sequences=tuple(
                MutationSequence.from_mapping(s) for s in mapping["sequences"]
            ),
            driver_contracts=tuple(
                DriverContract.from_mapping(c) for c in mapping["driver_contracts"]
            ),
            required_completion=mapping["required_completion"],
        )


@weaver_test()
def test_scoped_destructive_intent_round_trips_owned_exclusions():
    import weaver.mutation as mutation

    assert hasattr(mutation, "PhysicalScope"), "physical destructive scope is missing"
    from weaver.mutation import MutationPlan, PhysicalScope

    scope = PhysicalScope("sales", "Files/Incoming")
    exclusions = ["sales-files"]
    resources = ["onelake"]
    scopes = [scope]
    first = _action(
        "delete",
        writes=scopes,
        destructive_scopes=scopes,
        exclusions=exclusions,
        resources=resources,
    )
    second = _action("recreate", writes=scopes, exclusions=exclusions)
    plan = replace(
        _plan((first, second)),
        protected_scopes=(PhysicalScope("sales", "Files/Protected"),),
    )
    identity = compute_bundle_id(plan)
    scopes.clear()
    exclusions.clear()
    resources.clear()
    assert compute_bundle_id(plan) == identity
    assert MutationPlan.from_mapping(plan.to_mapping()) == plan
    assert compute_bundle_id(replace(plan, protected_scopes=())) != identity
    ordered = _plan((first, replace(second, depends_on=("delete",), exclusions=())))
    assert MutationPlan.from_mapping(ordered.to_mapping()) == ordered


@weaver_test()
@pytest.mark.parametrize(
    "fault",
    [
        "protected",
        "unexcluded",
        "unknown_scope_target",
        "scope_escape",
        "inconsistent_destructive",
        "duplicate_exclusion",
        "wrong_action_target",
    ],
)
def test_scoped_writers_reject_incompatible_destructive_intent(fault):
    from weaver.mutation import MutationPlan, PhysicalScope

    scope = PhysicalScope("sales", "Files/Incoming")
    first = _action(
        "delete", writes=(scope,), destructive_scopes=(scope,), exclusions=("files",)
    )
    second = _action("recreate", writes=(scope,), exclusions=("files",))
    plan = _plan((first, second))
    mapping = plan.to_mapping()
    actions = mapping["sequences"][0]["batches"][0]["actions"]
    if fault == "protected":
        mapping["protected_scopes"] = [
            {"target_id": "sales", "path": "Files/Incoming/Keep"}
        ]
    elif fault == "unexcluded":
        actions[1]["exclusions"] = []
    elif fault == "unknown_scope_target":
        actions[0]["writes"][0]["target_id"] = "unknown"
    elif fault == "scope_escape":
        actions[0]["destructive_scopes"][0]["path"] = "../escape"
    elif fault == "inconsistent_destructive":
        actions[0]["destructive_scopes"][0]["path"] = "Files/Other"
    elif fault == "duplicate_exclusion":
        actions[1]["exclusions"] = ["files", "files"]
    else:
        mapping["targets"].append(
            {"id": "other", "kind": "lakehouse", "item_id": "other-id"}
        )
        actions[0]["destructive_scopes"][0]["target_id"] = "other"
    with pytest.raises(BuildError):
        MutationPlan.from_mapping(mapping)


@weaver_test()
def test_common_plan_refuses_caller_owned_legacy_group_graph():
    from weaver.build_bundle import BuildBatch, BuildSequence

    plan = _plan((_action(),))
    mutable_batches = [BuildBatch("work", "sales", [_action()])]
    with pytest.raises(BuildError, match="frozen mutation"):
        replace(plan, sequences=[BuildSequence(1, "physical", mutable_batches)])


@weaver_test()
@pytest.mark.parametrize("mode", ["direct", "decoded"])
@pytest.mark.parametrize(
    "field,value",
    [
        ("sequences", None),
        ("targets", [None]),
        ("execution", []),
        ("driver_contracts", [None]),
        ("build_envelope", {"receipt": "runtime"}),
    ],
)
def test_decoded_plan_refuses_invalid_serialization_containers(field, value, mode):
    from weaver.mutation import MutationPlan

    plan = _plan((_action(),))
    mapping = plan.to_mapping()
    mapping[field] = value
    with pytest.raises(BuildError):
        if mode == "decoded":
            MutationPlan.from_mapping(mapping)
        else:
            replace(plan, **{field: value})


@weaver_test()
def test_every_required_completion_covers_its_inflight_starters():
    from weaver.mutation import MutationPlan

    mapping = _operation_plan().to_mapping()
    actions = mapping["sequences"][0]["batches"][0]["actions"]
    actions[2]["depends_on"] = ["start"]
    actions[2]["certifies"] = []
    mapping["required_completion"] = ["await", "certify"]
    with pytest.raises(BuildError, match="completion"):
        MutationPlan.from_mapping(mapping)


@weaver_test()
def test_payload_contract_requires_hash_before_freezing():
    with pytest.raises(BuildError, match="hash"):
        _plan((_action(executor="load_file", payload="payload/runtime.payload"),))


@weaver_test()
@pytest.mark.parametrize(
    "field",
    [
        "workspace_id",
        "catalogue_target_id",
        "spark_home_target_id",
        "environment",
        "bundle_id",
    ],
)
def test_execution_identity_refuses_mutable_runtime_values(field):
    plan = _plan((_action(),))
    with pytest.raises(BuildError):
        if field == "bundle_id":
            replace(plan, bundle_id=[])
        else:
            replace(plan, execution=replace(plan.execution, **{field: []}))


@weaver_test()
@pytest.mark.parametrize("mode", ["direct", "decoded"])
def test_batch_binding_requires_an_immutable_target_id(mode):
    from weaver.mutation import MutationPlan

    plan = _plan((_action(),))
    with pytest.raises(BuildError, match="target"):
        if mode == "decoded":
            mapping = plan.to_mapping()
            mapping["sequences"][0]["batches"][0]["target_id"] = []
            MutationPlan.from_mapping(mapping)
        else:
            batch = replace(plan.sequences[0].batches[0], target_id=[])
            replace(plan, sequences=(replace(plan.sequences[0], batches=(batch,)),))


@weaver_test()
def test_decoded_mutation_identity_requires_its_manifest_fields():
    from weaver.mutation import MutationPlan

    mapping = _plan((_action(),)).to_mapping()
    del mapping["bundle_id"]
    with pytest.raises(BuildError, match="bundle_id"):
        MutationPlan.from_mapping(mapping)
