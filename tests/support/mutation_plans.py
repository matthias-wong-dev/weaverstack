"""Forward-plan fixtures built through the production stage numbering seam."""

from dataclasses import fields, replace

from support.bundles import serial_sequences
from weaver.build_bundle.models import BuildBatch, BuildSequence, InstallAction
from weaver.build_bundle.prune import (
    TargetInventory,
    managed_warehouse_sets,
    render_warehouse_inventory_prune,
)
from weaver.build_bundle.stages import (
    PlannedStage,
    enumerate_stages,
)
from weaver.mutation import BoundTarget, MutationExecution, MutationPlan
from weaver.mutation.bundle import compute_bundle_id


def warehouse_prune_plan():
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
    numbered, payloads, _, _ = enumerate_stages(
        (
            PlannedStage(
                "prune",
                "prune",
                (BuildBatch("prune", target.id, tuple(actions)),),
                payloads=payloads,
            ),
        ),
        targets=(target,),
        completion_target_id=target.id,
    )
    members = tuple(
        InstallAction(**{f.name: getattr(a, f.name) for f in fields(InstallAction)})
        for a in numbered[0].batches[0].actions
        if a.executor != "completion_gate"
    )
    sequences = serial_sequences(
        (
            BuildSequence(
                10,
                "prune",
                (
                    BuildBatch("prune", target.id, members),
                    BuildBatch(
                        "later-batch", target.id, (replace(members[0], id="later"),)
                    ),
                ),
            ),
            BuildSequence(
                20,
                "next sequence",
                (BuildBatch("next", target.id, (replace(members[0], id="next"),)),),
            ),
        )
    )
    plan = MutationPlan(
        targets=(target,),
        sequences=sequences,
        execution=MutationExecution(workspace_name="Demo"),
    )
    return replace(plan, bundle_id=compute_bundle_id(plan)), payloads
