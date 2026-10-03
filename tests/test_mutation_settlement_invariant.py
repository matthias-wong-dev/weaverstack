from dataclasses import replace

import pytest
from support.weaver_test import weaver_test
from test_mutation_plan_representation import _action, _operation_plan, _plan

from weaver.errors import BuildError
from weaver.mutation import MutationPlan, PhysicalScope


def _reconstruct(base, mapping, mode):
    if mode == "decoded":
        return MutationPlan.from_mapping(mapping)
    batch = base.sequences[0].batches[0]
    fields = mapping["sequences"][0]["batches"][0]["actions"]
    actions = tuple(
        replace(
            action,
            depends_on=values["depends_on"],
            settle_after=values["settle_after"],
            certifies=values["certifies"],
            writes=tuple(PhysicalScope(**scope) for scope in values["writes"]),
        )
        for action, values in zip(batch.actions, fields, strict=True)
    )
    return replace(
        base,
        sequences=(
            replace(base.sequences[0], batches=(replace(batch, actions=actions),)),
        ),
        required_completion=mapping["required_completion"],
    )


@weaver_test()
@pytest.mark.parametrize("mode", ["direct", "decoded"])
@pytest.mark.parametrize(
    "fault,match",
    [
        ("unknown", "unknown"),
        ("self", "itself"),
        ("duplicate", "duplicate"),
        ("cross_duplicate", "duplicate"),
        ("mixed_cycle", "cycle"),
        ("settlement_cycle", "cycle"),
    ],
)
def test_success_and_settlement_union_requires_valid_acyclic_edges(mode, fault, match):
    base = _plan((_action("a"), _action("b")))
    mapping = base.to_mapping()
    a, b = mapping["sequences"][0]["batches"][0]["actions"]
    b["settle_after"] = ["a"]
    if fault == "unknown":
        b["settle_after"] = ["missing"]
    elif fault == "self":
        b["settle_after"] = ["b"]
    elif fault == "duplicate":
        b["settle_after"] = ["a", "a"]
    elif fault == "cross_duplicate":
        b["depends_on"] = ["a"]
    elif fault == "mixed_cycle":
        a["depends_on"] = ["b"]
    else:
        a["settle_after"] = ["b"]
    with pytest.raises(BuildError, match=match):
        _reconstruct(base, mapping, mode)


@weaver_test()
@pytest.mark.parametrize("mode", ["direct", "decoded"])
def test_settlement_order_serializes_physical_writers_without_success_claim(mode):
    base = _plan((_action("a"), _action("b")))
    mapping = base.to_mapping()
    a, b = mapping["sequences"][0]["batches"][0]["actions"]
    for action in (a, b):
        action["writes"] = [PhysicalScope("sales", "Files/Incoming").to_mapping()]
    b["settle_after"] = ["a"]
    plan = _reconstruct(base, mapping, mode)
    assert tuple(a.depends_on for _, _, a in plan.actions()) == ((), ())
    assert MutationPlan.from_mapping(plan.to_mapping()) == plan


@weaver_test()
@pytest.mark.parametrize("mode", ["direct", "decoded"])
@pytest.mark.parametrize(
    "claim", ["result", "certification", "completion", "async_certification"]
)
def test_settlement_ancestors_never_prove_successful_production(mode, claim):
    base = _operation_plan()
    mapping = base.to_mapping()
    _, await_action, certify = mapping["sequences"][0]["batches"][0]["actions"]
    if claim == "result":
        await_action["depends_on"] = []
        await_action["settle_after"] = ["start"]
    elif claim == "async_certification":
        certify["depends_on"] = ["start"]
        certify["settle_after"] = ["await"]
    else:
        certify["depends_on"] = []
        certify["settle_after"] = ["await"]
        if claim == "completion":
            certify["certifies"] = []
    with pytest.raises(BuildError, match="ancestor|prerequisite|completion|settlement"):
        _reconstruct(base, mapping, mode)


@weaver_test()
def test_settlement_links_have_owned_canonical_signature_bearing_identity():
    from weaver.mutation.bundle import compute_bundle_id, plan_from_yaml, plan_to_yaml

    links = ["b", "a"]
    actions = [_action("a"), _action("b"), _action("c", settle_after=links)]
    plan = _plan(actions)
    reordered = _plan(
        (_action("a"), _action("b"), _action("c", settle_after=["a", "b"]))
    )
    identity = compute_bundle_id(plan)
    assert identity == compute_bundle_id(reordered)
    assert identity != compute_bundle_id(
        _plan((_action("a"), _action("b"), _action("c")))
    )
    links.clear()
    actions.clear()
    assert compute_bundle_id(plan) == identity
    assert plan_from_yaml(plan_to_yaml(plan)) == plan
    assert MutationPlan.from_mapping(plan.to_mapping()) == plan
    assert tuple(a.settle_after for _, _, a in plan.actions()) == ((), (), ("a", "b"))


@weaver_test()
@pytest.mark.parametrize("mode", ["direct", "decoded"])
@pytest.mark.parametrize("value", [None, "a", [True], [""]])
def test_settlement_links_require_an_explicit_string_collection(mode, value):
    base = _plan((_action("a"), _action("b")))
    mapping = base.to_mapping()
    mapping["sequences"][0]["batches"][0]["actions"][1]["settle_after"] = value
    with pytest.raises(BuildError, match="settle_after"):
        _reconstruct(base, mapping, mode)


@weaver_test()
def test_decoded_action_requires_explicit_settlement_links():
    mapping = _plan((_action(),)).to_mapping()
    del mapping["sequences"][0]["batches"][0]["actions"][0]["settle_after"]
    with pytest.raises(BuildError, match="settle_after"):
        MutationPlan.from_mapping(mapping)


@weaver_test()
@pytest.mark.parametrize("outcome", ["blocked", "uncertain", "pending"])
def test_runtime_outcomes_cannot_be_frozen_as_settlement_evidence(outcome):
    plan = _plan((_action("a"), _action("b", settle_after=("a",))))
    mapping = plan.to_mapping()
    a, b = mapping["sequences"][0]["batches"][0]["actions"]
    assert b["depends_on"] == []
    assert b["settle_after"] == ["a"]
    a["outcome"] = outcome
    with pytest.raises(BuildError, match="unknown fields"):
        MutationPlan.from_mapping(mapping)
