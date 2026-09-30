from support.bundles import given_execution, with_catalogue
from support.weaver_test import weaver_test

from weaver.build_bundle import (
    BoundTarget,
    BuildBatch,
    BuildPlan,
    BuildSelection,
    BuildSequence,
    Impact,
    InstallAction,
    OmittedNode,
)
from weaver.build_bundle.changes import TargetChange
from weaver.catalogue.runtime_state import (
    RuntimeStateEstablishment,
    RuntimeStateInvalidation,
)
from weaver.graph import Graph


def _legacy_plan():
    target = BoundTarget("sales", "lakehouse", "sales-id")
    actions = tuple(
        InstallAction(
            str(i),
            "build_folder",
            f"Files/Incoming/{i}",
            "folder",
            None,
            None,
            source_path=f"Sales/Files/Incoming.{i}.yml",
        )
        for i in range(1000)
    )
    sequences = (
        BuildSequence(
            10,
            "first",
            (
                BuildBatch("first", "sales", actions[:400]),
                BuildBatch("second", "sales", actions[400:800]),
            ),
        ),
        BuildSequence(20, "last", (BuildBatch("third", "sales", actions[800:]),)),
    )
    targets = with_catalogue((target,))
    return BuildPlan(
        format_version=4,
        bundle_id="",
        repository_name="Source",
        repository_signature="source-signature",
        targets=targets,
        sequences=sequences,
        selection=BuildSelection(Impact((), (), ()), (), (), ()),
        execution=given_execution(targets, sequences),
        omitted_nodes=(OmittedNode("omitted", "target_unbound"),),
        target_changes={"sales": (TargetChange("add", "folder", "Incoming/0", "0"),)},
        runtime_state=(
            RuntimeStateInvalidation("LoadStatus", ({"object_name": "Customer"},)),
        ),
        runtime_state_established=(
            RuntimeStateEstablishment(
                "LoadStatus", ({"object_name": "Customer", "status": "Pending"},)
            ),
        ),
    )


@weaver_test()
def test_compatibility_compiler_preserves_envelope_with_linear_batch_barriers():
    import weaver.mutation as mutation

    assert hasattr(mutation, "compile_legacy_build"), (
        "compatibility compiler is missing"
    )
    from weaver.build_bundle.bundle import compute_bundle_id
    from weaver.mutation import MutationPlan, compile_legacy_build

    legacy = _legacy_plan()
    original_mapping = legacy.to_mapping()
    compiled = compile_legacy_build(legacy)
    actions = {a.id: a for _, _, a in compiled.actions()}
    graph = Graph(actions, ((p, a.id) for a in actions.values() for p in a.depends_on))
    assert len(actions) == 1003
    assert sum(len(a.depends_on) for a in actions.values()) <= 2000
    assert actions["0"].depends_on == ()
    assert "399" in graph.ancestors("400")
    assert "799" in graph.ancestors("800")
    assert "0" not in graph.ancestors("1")
    assert actions["999"].source_path == "Sales/Files/Incoming.999.yml"
    assert actions["999"].target_id == "sales"
    assert compiled.required_completion
    assert compiled.bundle_id == compute_bundle_id(compiled)
    assert MutationPlan.from_mapping(compiled.to_mapping()) == compiled
    envelope_keys = {
        "repository_name",
        "repository_signature",
        "selection",
        "omitted_nodes",
        "target_changes",
        "runtime_state",
        "runtime_state_established",
    }
    envelope = compiled.to_mapping()["build_envelope"]
    assert envelope == {key: original_mapping[key] for key in envelope_keys}
    assert legacy.to_mapping() == original_mapping
    original_groups = {
        b.id: tuple(a.id for a in b.actions)
        for s in legacy.sequences
        for b in s.batches
    }
    compiled_groups = {
        b.id: tuple(a.id for a in b.actions)
        for s in compiled.sequences
        for b in s.batches
    }
    assert all(
        compiled_groups[id] == members for id, members in original_groups.items()
    )
