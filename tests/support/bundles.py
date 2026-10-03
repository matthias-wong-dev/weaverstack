"""Execution descriptors for bundles a test builds by hand.

A planner-generated bundle names the catalogue Warehouse it publishes to and,
when its actions need Spark, the Lakehouse a session attaches to. A hand-built
plan declares the same things through :func:`given_execution` rather than
leaving the installer to infer them.
"""

from __future__ import annotations

from weaver.build_bundle.execution import (
    SPARK_EXECUTORS,
    BundleEnvironment,
    BundleExecution,
)
from weaver.build_bundle.targets import BoundTarget

#: Matches ``support.sessions.NOWHERE`` so a hand-built bundle and the Session
#: installing it share one scope.
WORKSPACE = "Demo"

CATALOGUE_TARGET = BoundTarget(
    id="warehouse-Weaver",
    kind="warehouse",
    item_id="Weaver",
    item_name="Weaver",
)


def given_execution(
    targets,
    sequences=(),
    *,
    workspace_name: str = WORKSPACE,
    workspace_id: str | None = None,
    environment: BundleEnvironment | None = None,
) -> BundleExecution:
    """The descriptor for a plan over ``targets`` running ``sequences``."""

    catalogue = next(
        (target for target in targets if target.kind == "warehouse"),
        CATALOGUE_TARGET,
    )
    home = None
    if _needs_spark(sequences):
        home = next(
            (target.id for target in targets if target.kind == "lakehouse"), None
        )
    return BundleExecution(
        workspace_name=workspace_name,
        workspace_id=workspace_id,
        catalogue_target_id=catalogue.id,
        environment=environment,
        spark_home_target_id=home,
    )


def with_catalogue(targets) -> tuple:
    """``targets`` plus the catalogue Warehouse, as a real plan carries it."""

    targets = tuple(targets)
    if any(target.kind == "warehouse" for target in targets):
        return targets
    return targets + (CATALOGUE_TARGET,)


def _needs_spark(sequences) -> bool:
    return any(
        action.executor in SPARK_EXECUTORS
        for sequence in sequences
        for batch in sequence.batches
        for action in batch.actions
    )


__all__ = ["CATALOGUE_TARGET", "WORKSPACE", "given_execution", "with_catalogue"]


def serial_sequences(sequences):
    """Chain a draft's actions in sequence and batch order.

    Each batch's actions settle in turn, a gate requires the whole batch, and the
    next batch requires that gate. Planner output carries its own DAG instead.
    """

    from weaver.mutation.models import MutationAction, MutationBatch, MutationSequence

    previous = None
    result = []
    for sequence in sequences:
        batches = []
        for batch in sequence.batches:
            actions = tuple(
                MutationAction(
                    **action.to_mapping(),
                    target_id=batch.target_id,
                    depends_on=() if previous is None else (previous,),
                    settle_after=() if index == 0 else (batch.actions[index - 1].id,),
                )
                for index, action in enumerate(batch.actions)
            )
            batches.append(MutationBatch(batch.id, batch.target_id, actions))
            if actions:
                gate = MutationAction(
                    id="complete-batch:" + batch.id,
                    kind="completion_gate",
                    resource_node_id=None,
                    executor="completion_gate",
                    payload=None,
                    payload_sha256=None,
                    target_id=batch.target_id,
                    depends_on=tuple(a.id for a in actions),
                )
                batches.append(
                    MutationBatch("completion:" + batch.id, batch.target_id, (gate,))
                )
                previous = gate.id
        result.append(
            MutationSequence(sequence.number, sequence.description, tuple(batches))
        )
    return tuple(result)


def endpoint_refresh_sequence(target_id: str, *, number: int = 1):
    """One Lakehouse endpoint refresh, started and awaited, and its contracts."""

    from weaver.build_bundle.executors.sql_endpoint_refresh import (
        AWAIT_EXECUTOR,
        CONTRACTS,
        REFRESH_RESULT,
        START_EXECUTOR,
    )
    from weaver.mutation.models import (
        MutationAction,
        MutationBatch,
        MutationSequence,
        ResultReference,
    )

    def action(id, executor, **options):
        return MutationAction(
            id=id,
            kind=executor,
            resource_node_id=None,
            executor=executor,
            payload=None,
            payload_sha256=None,
            target_id=target_id,
            **{"depends_on": (), **options},
        )

    sequence = MutationSequence(
        number,
        "refresh mutated Lakehouse SQL endpoints",
        (
            MutationBatch(
                "refresh",
                target_id,
                (
                    action("start-refresh", START_EXECUTOR),
                    action(
                        "await-refresh",
                        AWAIT_EXECUTOR,
                        depends_on=("start-refresh",),
                        result_from=ResultReference("start-refresh", REFRESH_RESULT),
                    ),
                ),
            ),
        ),
    )
    return sequence, CONTRACTS, ("await-refresh",)


def runs_before(plan, first: str, second: str) -> bool:
    """Whether the plan's DAG orders action ``first`` before action ``second``."""

    from weaver.graph import Graph

    actions = [action for _s, _b, action in plan.actions()]
    graph = Graph(
        (action.id for action in actions),
        (
            (edge, action.id)
            for action in actions
            for edge in (*action.depends_on, *action.settle_after)
        ),
    )
    return first in graph.ancestors(second)


def given_build_plan(
    *,
    format_version=5,
    bundle_id="",
    targets=(),
    sequences=(),
    execution,
    repository_name="",
    repository_signature="",
    selection,
    omitted_nodes=(),
    target_changes=None,
    runtime_state=(),
    runtime_state_established=(),
    driver_contracts=(),
    required_completion=(),
):
    """Build a forward MutationPlan from a test's draft physical stages."""
    from weaver.mutation import MutationAction, MutationPlan

    if not all(
        isinstance(action, MutationAction)
        for sequence in sequences
        for batch in sequence.batches
        for action in batch.actions
    ):
        sequences = serial_sequences(sequences)
    return MutationPlan(
        format_version=format_version,
        bundle_id=bundle_id,
        targets=targets,
        sequences=sequences,
        execution=execution,
        driver_contracts=driver_contracts,
        required_completion=required_completion,
        build_envelope={
            "repository_name": repository_name,
            "repository_signature": repository_signature,
            "selection": selection.to_mapping(),
            "omitted_nodes": [node.to_mapping() for node in omitted_nodes],
            "target_changes": {
                key: [change.to_mapping() for change in value]
                for key, value in (target_changes or {}).items()
            },
            "runtime_state": [one.to_mapping() for one in runtime_state],
            "runtime_state_established": [
                one.to_mapping() for one in runtime_state_established
            ],
        },
    )


def build_metadata(plan):
    """Decode the planner's frozen Build envelope for fixture assertions."""
    from types import SimpleNamespace

    from weaver.build_bundle.changes import TargetChange
    from weaver.build_bundle.incremental import BuildSelection
    from weaver.build_bundle.models import OmittedNode
    from weaver.catalogue.runtime_state import (
        RuntimeStateEstablishment,
        RuntimeStateInvalidation,
    )

    envelope = plan.build_envelope
    return SimpleNamespace(
        repository_name=envelope["repository_name"],
        repository_signature=envelope["repository_signature"],
        selection=BuildSelection.from_mapping(envelope["selection"]),
        omitted_nodes=tuple(
            OmittedNode.from_mapping(n) for n in envelope["omitted_nodes"]
        ),
        target_changes={
            key: tuple(TargetChange.from_mapping(c) for c in changes)
            for key, changes in envelope["target_changes"].items()
        },
        runtime_state=tuple(
            RuntimeStateInvalidation.from_mapping(n) for n in envelope["runtime_state"]
        ),
        runtime_state_established=tuple(
            RuntimeStateEstablishment.from_mapping(n)
            for n in envelope["runtime_state_established"]
        ),
    )
