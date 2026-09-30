from dataclasses import replace

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
    assert sum(len(a.settle_after) for a in actions.values()) == 997
    assert (
        sum(len(a.depends_on) + len(a.settle_after) for a in actions.values()) <= 3000
    )
    assert actions["1"].settle_after == ("0",)
    assert actions["400"].settle_after == ()
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


def _warehouse_prune_plan():
    from weaver.build_bundle.prune import (
        TargetInventory,
        managed_warehouse_sets,
        render_warehouse_inventory_prune,
    )

    target = BoundTarget("sales", "warehouse", "warehouse-id")
    payloads = {}
    actions, _ = render_warehouse_inventory_prune(
        target,
        TargetInventory(
            target_id=target.id,
            kind=target.kind,
            target_name="Sales",
            schemas=("Legacy",),
            tables=("Legacy.Thing",),
            views=("Legacy.Report",),
        ),
        managed_warehouse_sets({}),
        payloads,
    )
    from weaver.build_bundle.stages import PlannedStage, enumerate_stages

    numbered, payloads, _ = enumerate_stages(
        (
            PlannedStage(
                "prune",
                "prune",
                (BuildBatch("prune", target.id, tuple(actions)),),
                payloads=payloads,
            ),
        )
    )
    actions = numbered[0].batches[0].actions
    sequences = (
        BuildSequence(
            10,
            "prune",
            (
                BuildBatch("prune", target.id, tuple(actions)),
                BuildBatch(
                    "later-batch", target.id, (replace(actions[0], id="later"),)
                ),
            ),
        ),
        BuildSequence(
            20,
            "next sequence",
            (BuildBatch("next", target.id, (replace(actions[0], id="next"),)),),
        ),
    )
    targets = with_catalogue((target,))
    return replace(
        _legacy_plan(),
        targets=targets,
        sequences=sequences,
        execution=given_execution(targets, sequences),
    ), payloads


@weaver_test()
def test_real_warehouse_prune_freezes_member_settlement_order():
    from weaver.mutation import compile_legacy_build

    legacy, payloads = _warehouse_prune_plan()
    members = legacy.sequences[0].batches[0].actions
    assert [a.kind for a in members] == ["prune_view", "prune_table", "prune_schema"]
    assert [payloads[a.payload].decode().strip() for a in members] == [
        "drop view if exists [Legacy].[Report];",
        "drop table if exists [Legacy].[Thing];",
        "drop schema if exists [Legacy];",
    ]
    compiled = compile_legacy_build(legacy)
    actions = {a.id: a for _, _, a in compiled.actions()}
    order = Graph(
        actions,
        (
            (p, a.id)
            for a in actions.values()
            for p in (*a.depends_on, *a.settle_after)
        ),
    ).order()
    assert [id for id in order if id in {a.id for a in members}] == [
        a.id for a in members
    ]
    for previous, current in zip(members, members[1:]):
        assert actions[current.id].settle_after == (previous.id,)
        assert actions[current.id].depends_on == ()
    assert actions["complete-batch:prune"].depends_on == tuple(
        sorted(a.id for a in members)
    )
    assert actions["later"].depends_on == ("complete-batch:prune",)
    assert actions["next"].depends_on == ("complete-batch:later-batch",)


@weaver_test()
def test_legacy_prune_continues_admitted_members_then_stops_batches_and_sequences(
    tmp_path,
):
    from support.sessions import given_installer

    from weaver.locations import Location
    from weaver.mutation import compile_legacy_build
    from weaver.mutation.bundle import write_bundle
    from weaver.store import FilesystemStore

    legacy, payloads = _warehouse_prune_plan()
    members = legacy.sequences[0].batches[0].actions

    class RecordingFailure:
        def __init__(self):
            self.calls = []

        def execute(self, action, payload, context):
            self.calls.append(action.id)
            if action.id == members[0].id:
                raise RuntimeError("known statement failure")

    executor = RecordingFailure()
    store = FilesystemStore()
    bundle = write_bundle(
        Location(str(tmp_path / "legacy")), plan=legacy, payloads=payloads, store=store
    )
    report = given_installer(store=store, executors={"tsql": executor}).install(bundle)
    outcomes = {r.action_id: r.status for r in report.action_results()}
    assert executor.calls == [a.id for a in members]
    assert [outcomes[a.id] for a in members] == ["failed", "succeeded", "succeeded"]
    assert outcomes["later"] == outcomes["next"] == "skipped"
    assert report.status == "failed"
    compiled = compile_legacy_build(legacy)
    actions = {a.id: a for _, _, a in compiled.actions()}
    assert actions[members[1].id].settle_after == (members[0].id,)
    assert actions[members[1].id].depends_on == ()
    assert set(actions["complete-batch:prune"].depends_on) == {a.id for a in members}
    assert actions["later"].depends_on == ("complete-batch:prune",)
    assert actions["next"].depends_on == ("complete-batch:later-batch",)
