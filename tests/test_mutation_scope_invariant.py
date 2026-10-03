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
@pytest.mark.parametrize("mode", ["direct", "decoded"])
@pytest.mark.parametrize(
    "path",
    [
        "Files/Protected ",
        " Files/Protected",
        "Files/Protected\t",
        "Files/ Protected",
        "Files/Protected /Keep",
        "Files/Schema/ Folder",
    ],
)
def test_noncanonical_protected_root_cannot_bypass_scope_checks(mode, path):
    targets = _alias_targets({}, {})
    action = _writer(
        "sales", destructive_scopes=(PhysicalScope("sales", "Files/Protected"),)
    )
    base = _plan(targets, (action,))
    scope = PhysicalScope("alias", path)
    with pytest.raises(BuildError, match="canonical.*scope path|scope path.*canonical"):
        if mode == "direct":
            replace(base, protected_scopes=(scope,))
        else:
            mapping = base.to_mapping()
            mapping["protected_scopes"] = [scope.to_mapping()]
            MutationPlan.from_mapping(mapping)


@weaver_test()
@pytest.mark.parametrize("mode", ["direct", "decoded"])
@pytest.mark.parametrize("policy", ["unordered", "ordered", "settled", "excluded"])
@pytest.mark.parametrize(
    "path", ["Files/Protected ", "Files/ Protected", "Files/Protected /Keep"]
)
def test_noncanonical_writer_path_cannot_bypass_scope_checks(mode, policy, path):
    targets = _alias_targets({}, {})
    exclusions = ("item-files",) if policy == "excluded" else ()
    first = _writer("sales", exclusions=exclusions)
    second = replace(
        _writer(
            "alias",
            depends_on=("sales",) if policy == "ordered" else (),
            settle_after=("sales",) if policy == "settled" else (),
            exclusions=exclusions,
        ),
        resource_node_id="folder:" + path.removeprefix("Files/"),
        writes=(PhysicalScope("alias", path),),
    )
    base = _plan(targets, (first,))
    with pytest.raises(BuildError, match="scope path must be canonical"):
        if mode == "direct":
            batches = (
                base.sequences[0].batches[0],
                MutationBatch("alias", "alias", (second,)),
            )
            replace(base, sequences=(replace(base.sequences[0], batches=batches),))
        else:
            mapping = base.to_mapping()
            mapping["sequences"][0]["batches"][1]["actions"] = [second.to_mapping()]
            MutationPlan.from_mapping(mapping)


@weaver_test()
@pytest.mark.parametrize("mode", ["direct", "decoded"])
@pytest.mark.parametrize("scenario", ["protected", "writers"])
@pytest.mark.parametrize(
    "name", [" Protected", "Protected ", "\tProtected", "Protected\t"]
)
def test_resolved_folder_component_alias_is_refused(mode, scenario, name):
    from weaver.build_bundle.executors.base import InstallationContext, ResolvedTarget
    from weaver.build_bundle.executors.folder import FolderExecutor
    from weaver.fabric.resolution import FabricResolver
    from weaver.fabric.resources import Item, WorkspaceItem
    from weaver.targets import ItemRef
    from weaver.workspaces import Workspace

    class NoNetwork:
        def __getattr__(self, name):
            raise AssertionError(f"unexpected external operation: {name}")

    target = BoundTarget(
        "sales",
        "lakehouse",
        "11111111-1111-4111-8111-111111111111",
        workspace_id="22222222-2222-4222-8222-222222222222",
    )
    resolver = FabricResolver(Workspace(workspace="Demo"), client=NoNetwork())
    resolver._workspace = WorkspaceItem(target.workspace_id, "Demo")
    resolver._items[target.item_id + ":Lakehouse"] = Item(
        target.item_id, "Sales", "Lakehouse", target.workspace_id
    )
    context = InstallationContext(
        resolver=resolver,
        store=NoNetwork(),
        target=ResolvedTarget(target, ItemRef(target.item_id)),
    )
    executor = FolderExecutor()
    canonical = executor._location("folder:Protected", context)
    assert executor._location("folder:" + name, context) == canonical
    scope = PhysicalScope("sales", "Files/" + name)
    padded = replace(
        _writer("sales"),
        id="padded",
        resource_node_id="folder:" + name,
        writes=(scope,),
        destructive_scopes=(scope,) if scenario == "protected" else (),
    )
    existing = (
        (replace(_writer("sales"), id="canonical"),) if scenario == "writers" else ()
    )
    protected = (
        (PhysicalScope("sales", "Files/Protected"),) if scenario == "protected" else ()
    )
    base = _plan((target,), existing)
    with pytest.raises(BuildError, match="scope path must be canonical"):
        if mode == "direct":
            _plan((target,), (*existing, padded), protected=protected)
        else:
            mapping = base.to_mapping()
            mapping["sequences"][0]["batches"][0]["actions"].append(padded.to_mapping())
            mapping["protected_scopes"] = [scope.to_mapping() for scope in protected]
            MutationPlan.from_mapping(mapping)


@weaver_test()
@pytest.mark.parametrize("mode", ["direct", "decoded"])
@pytest.mark.parametrize("path", ["", "Files/Protected", "Files/Monthly Reports"])
def test_canonical_scope_path_is_accepted_without_changing_identity(mode, path):
    from weaver.locations import Location

    targets = _alias_targets({}, {})
    action = replace(_writer("sales"), writes=(PhysicalScope("sales", path),))
    plan = _plan(targets, (action,))
    if mode == "decoded":
        plan = MutationPlan.from_mapping(plan.to_mapping())
    assert next(plan.actions())[2].writes[0].path == path
    if path:
        root = Location("https://example.invalid/item")
        assert root.join(path).value == root.value + "/" + path


@weaver_test()
@pytest.mark.parametrize("mode", ["direct", "decoded"])
@pytest.mark.parametrize("path", ["Files/Other", "Files/ProtectedCopy"])
def test_genuinely_distinct_scope_paths_remain_accepted(mode, path):
    targets = _alias_targets({}, {})
    first = _writer(
        "sales", destructive_scopes=(PhysicalScope("sales", "Files/Protected"),)
    )
    second = replace(_writer("alias"), writes=(PhysicalScope("alias", path),))
    plan = _plan(
        targets,
        (first, second),
        protected=(PhysicalScope("alias", path),),
    )
    if mode == "decoded":
        plan = MutationPlan.from_mapping(plan.to_mapping())
    assert tuple(action.writes[0].path for _, _, action in plan.actions()) == (
        "Files/Protected",
        path,
    )


@weaver_test()
@pytest.mark.parametrize("mode", ["direct", "decoded"])
@pytest.mark.parametrize(
    "identity", ["item_id", "workspace_id", "execution_workspace_id"]
)
def test_noncanonical_physical_identity_cannot_bypass_alias_checks(mode, identity):
    targets = _alias_targets({"workspace_id": "workspace"}, {})
    action = _writer(
        "alias", destructive_scopes=(PhysicalScope("alias", "Files/Protected"),)
    )
    base = _plan(targets, (action,), workspace_id="workspace")
    protected = (PhysicalScope("sales", "Files/Protected"),)
    changes = {identity: "same-item " if identity == "item_id" else "workspace "}
    with pytest.raises(BuildError, match="physical identity must be canonical"):
        if mode == "direct":
            if identity == "execution_workspace_id":
                replace(
                    base,
                    execution=replace(base.execution, workspace_id=changes[identity]),
                    protected_scopes=protected,
                )
            else:
                replace(
                    base,
                    targets=(targets[0], replace(targets[1], **changes)),
                    protected_scopes=protected,
                )
        else:
            mapping = base.to_mapping()
            if identity == "execution_workspace_id":
                mapping["execution"]["workspace_id"] = changes[identity]
            else:
                mapping["targets"][1].update(changes)
            mapping["protected_scopes"] = [scope.to_mapping() for scope in protected]
            MutationPlan.from_mapping(mapping)


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
