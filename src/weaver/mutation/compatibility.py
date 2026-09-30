"""Planning-only conversion of format-4 batch admission and member order.

The compatibility plan serializes admitted members, including optimized legacy
groups. Normal format-4 installation retains its grouped and concurrent dispatch.
"""

from dataclasses import replace

from ..errors import BuildError
from .bundle import compute_bundle_id, validate_plan_structure
from .models import MutationAction, MutationBatch, MutationPlan, MutationSequence


def compile_legacy_build(plan) -> MutationPlan:
    from ..build_bundle.models import BuildPlan

    if (
        not isinstance(plan, BuildPlan)
        or type(plan.format_version) is not int
        or plan.format_version != 4
    ):
        raise BuildError("compatibility compilation requires a format-4 Build plan")
    validate_plan_structure(plan)
    action_ids = {a.id for _, _, a in plan.actions()}
    batch_ids = {b.id for s in plan.sequences for b in s.batches}
    previous = None
    sequences = []
    for sequence in plan.sequences:
        batches = []
        for batch in sequence.batches:
            dependencies = () if previous is None else (previous,)
            actions = tuple(
                MutationAction(
                    **a.to_mapping(),
                    target_id=batch.target_id,
                    depends_on=dependencies,
                    settle_after=() if index == 0 else (batch.actions[index - 1].id,),
                )
                for index, a in enumerate(batch.actions)
            )
            batches.append(MutationBatch(batch.id, batch.target_id, actions))
            if not actions:
                continue
            gate_id = f"complete-batch:{batch.id}"
            gate_batch_id = f"completion:{batch.id}"
            if gate_id in action_ids or gate_batch_id in batch_ids:
                raise BuildError(
                    f"compatibility completion identity collides with {batch.id!r}"
                )
            action_ids.add(gate_id)
            batch_ids.add(gate_batch_id)
            gate = MutationAction(
                id=gate_id,
                kind="completion_gate",
                resource_node_id=None,
                executor="completion_gate",
                payload=None,
                payload_sha256=None,
                target_id=batch.target_id,
                depends_on=tuple(a.id for a in actions),
            )
            batches.append(MutationBatch(gate_batch_id, batch.target_id, (gate,)))
            previous = gate_id
        sequences.append(
            MutationSequence(sequence.number, sequence.description, tuple(batches))
        )
    envelope = plan.to_mapping()
    for key in ("format_version", "bundle_id", "targets", "sequences", "execution"):
        del envelope[key]
    compiled = MutationPlan(
        targets=plan.targets,
        sequences=tuple(sequences),
        execution=plan.execution,
        build_envelope=envelope,
        required_completion=() if previous is None else (previous,),
    )
    return replace(compiled, bundle_id=compute_bundle_id(compiled))
