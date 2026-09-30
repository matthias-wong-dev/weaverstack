from dataclasses import replace

import pytest
from support.weaver_test import weaver_test

from weaver.errors import BuildError
from weaver.mutation import (
    BoundTarget,
    MutationAction,
    MutationBatch,
    MutationExecution,
    MutationPlan,
    MutationSequence,
    PhysicalScope,
)


def _plan(targets, actions, *, workspace_id=None, protected=()):
    return MutationPlan(
        targets=targets,
        sequences=(
            MutationSequence(
                1,
                "physical",
                tuple(
                    MutationBatch(
                        target.id,
                        target.id,
                        tuple(a for a in actions if a.target_id == target.id),
                    )
                    for target in targets
                ),
            ),
        ),
        execution=MutationExecution("Demo", workspace_id=workspace_id),
        protected_scopes=protected,
    )


def _writer(target_id, **fields):
    return MutationAction(
        id=target_id,
        kind="prune_folder",
        resource_node_id="folder:Protected",
        executor="folder",
        payload=None,
        payload_sha256=None,
        target_id=target_id,
        depends_on=fields.pop("depends_on", ()),
        writes=(PhysicalScope(target_id, "Files/Protected"),),
        **fields,
    )


def _alias_targets(left_workspace, right_workspace):
    return (
        BoundTarget(
            "sales", "lakehouse", "same-item", item_name="Sales", **left_workspace
        ),
        BoundTarget(
            "alias", "lakehouse", "same-item", item_name="Renamed", **right_workspace
        ),
    )


@weaver_test()
@pytest.mark.parametrize("mode", ["direct", "decoded"])
@pytest.mark.parametrize(
    "left,right,default",
    [
        (
            {"workspace_id": "workspace", "workspace_name": "Before"},
            {"workspace_id": "workspace", "workspace_name": "After"},
            None,
        ),
        ({"workspace_id": "workspace"}, {}, "workspace"),
        ({}, {}, None),
        ({"workspace_id": "workspace"}, {"workspace_name": "Other"}, "default"),
        ({"workspace_name": "Before"}, {"workspace_name": "After"}, None),
    ],
)
def test_protected_roots_cover_physical_aliases_without_display_identity(
    mode, left, right, default
):
    targets = _alias_targets(left, right)
    action = _writer(
        "alias", destructive_scopes=(PhysicalScope("alias", "Files/Protected"),)
    )
    base = _plan(targets, (action,), workspace_id=default)
    protected = (PhysicalScope("sales", "Files/Protected/Keep"),)
    with pytest.raises(BuildError, match="protected root"):
        if mode == "direct":
            replace(base, protected_scopes=protected)
        else:
            mapping = base.to_mapping()
            mapping["protected_scopes"] = [s.to_mapping() for s in protected]
            MutationPlan.from_mapping(mapping)


@weaver_test()
@pytest.mark.parametrize("mode", ["direct", "decoded"])
@pytest.mark.parametrize("policy", ["unordered", "ordered", "settled", "excluded"])
@pytest.mark.parametrize(
    "left,right,default",
    [
        ({"workspace_id": "workspace"}, {"workspace_id": "workspace"}, None),
        ({"workspace_id": "workspace"}, {}, "workspace"),
        ({}, {}, None),
        ({"workspace_id": "workspace"}, {"workspace_name": "Other"}, "default"),
        ({"workspace_name": "Before"}, {"workspace_name": "After"}, None),
    ],
)
def test_physical_alias_writers_require_enforced_order_or_exclusion(
    mode, policy, left, right, default
):
    targets = _alias_targets(left, right)
    exclusions = ("item-files",) if policy == "excluded" else ()
    first = _writer("sales", exclusions=exclusions)
    second = _writer(
        "alias",
        depends_on=("sales",) if policy == "ordered" else (),
        settle_after=("sales",) if policy == "settled" else (),
        exclusions=exclusions,
    )
    base = _plan(targets, (first,), workspace_id=default)

    def construct():
        if mode == "direct":
            batches = (
                base.sequences[0].batches[0],
                MutationBatch("alias", "alias", (second,)),
            )
            return replace(
                base, sequences=(replace(base.sequences[0], batches=batches),)
            )
        mapping = base.to_mapping()
        mapping["sequences"][0]["batches"][1]["actions"] = [second.to_mapping()]
        return MutationPlan.from_mapping(mapping)

    if policy == "unordered":
        with pytest.raises(BuildError, match="incompatible writers"):
            construct()
    else:
        plan = construct()
        assert MutationPlan.from_mapping(plan.to_mapping()) == plan


@weaver_test()
@pytest.mark.parametrize("difference", ["workspace", "item", "kind", "path"])
def test_distinct_physical_bindings_remain_valid_without_live_resolution(difference):
    targets = list(_alias_targets({"workspace_id": "left"}, {"workspace_id": "left"}))
    if difference == "workspace":
        targets[1] = replace(targets[1], workspace_id="right")
    elif difference == "item":
        targets[1] = replace(targets[1], item_id="other-item")
    elif difference == "kind":
        targets[1] = replace(targets[1], kind="warehouse")
    first, second = _writer("sales"), _writer("alias")
    if difference == "path":
        second = replace(second, writes=(PhysicalScope("alias", "Files/Other"),))
    plan = _plan(
        targets, (first, second), protected=(PhysicalScope("sales", "Files/Keep"),)
    )
    assert MutationPlan.from_mapping(plan.to_mapping()) == plan
    protected = replace(first, destructive_scopes=first.writes)
    if difference != "path":
        plan = _plan(
            targets,
            (protected,),
            protected=(PhysicalScope("alias", "Files/Protected"),),
        )
        assert MutationPlan.from_mapping(plan.to_mapping()) == plan
