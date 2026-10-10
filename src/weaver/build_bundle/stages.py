"""Build stages, their numbering, and the physical DAG they compile to.

Stages group and order actions for presentation. Execution order comes only from
the dependency keys each stage declares; see :mod:`.dependencies`.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Iterable, Mapping, Sequence

from ..errors import BuildError
from .changes import TargetChange
from .changes import merge as merge_changes
from .models import BuildBatch, BuildSequence
from .payloads import payload_path

#: Presentation order. It implies no execution barrier.
PRUNE = "prune"
DROP = "drop"
SCHEMA = "schema"
SHORTCUT = "shortcut"
BUILD = "build"
REFRESH = "refresh"
RUNTIME = "runtime"
CATALOGUE = "catalogue"

_PHASE_ORDER = (
    PRUNE,
    DROP,
    SCHEMA,
    SHORTCUT,
    BUILD,
    REFRESH,
    RUNTIME,
    CATALOGUE,
)
_PHASE_RANK = {phase: rank for rank, phase in enumerate(_PHASE_ORDER)}


@dataclass(frozen=True)
class PlannedStage:
    """Unnumbered target batches, payloads, declared changes and dependency keys.

    ``index`` orders dependency layers within a phase for presentation. Payload
    keys are bare filenames until final numbering assigns their directories.
    ``provides``, ``requires`` and ``follows`` map an action id to keys; see
    :mod:`.dependencies`.
    """

    phase: str
    description: str
    batches: tuple[BuildBatch, ...]
    slug: str = ""
    index: int = 0
    payloads: Mapping[str, bytes] = field(default_factory=dict)
    changes: Mapping[str, tuple[TargetChange, ...]] = field(default_factory=dict)
    provides: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    requires: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    follows: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    #: Action id to the ``(producer action id, result type)`` it consumes.
    results: Mapping[str, tuple[str, str]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.phase not in _PHASE_RANK:
            raise BuildError(f"unknown planned stage phase {self.phase!r}")
        for filename in self.payloads:
            if "/" in filename or not filename:
                raise BuildError(
                    f"stage {self.phase!r} payload key must be a bare filename, "
                    f"got {filename!r}"
                )

    @property
    def action_ids(self) -> tuple[str, ...]:
        return tuple(a.id for batch in self.batches for a in batch.actions)

    def declaring(self, *, provides=(), requires=(), follows=()) -> "PlannedStage":
        """Add the same keys to every action in this stage."""

        def extended(current, keys):
            if not keys:
                return current
            return {
                action: (*current.get(action, ()), *keys) for action in self.action_ids
            } | {k: v for k, v in current.items() if k not in self.action_ids}

        return replace(
            self,
            provides=extended(self.provides, tuple(provides)),
            requires=extended(self.requires, tuple(requires)),
            follows=extended(self.follows, tuple(follows)),
        )

    @property
    def payload_slug(self) -> str:
        return self.slug or self.phase

    @property
    def rank(self) -> tuple[int, int]:
        return (_PHASE_RANK[self.phase], self.index)


def merge_layer_stages(stages: Iterable[PlannedStage]) -> tuple[PlannedStage, ...]:
    """Merge one kind of same-phase, same-index work within a dependency layer.

    A merged stage keeps one description, so only stages that describe the same
    work merge.
    """

    grouped: dict[tuple[int, int], dict[str, list[PlannedStage]]] = {}
    for stage in stages:
        grouped.setdefault(stage.rank, {}).setdefault(stage.description, []).append(
            stage
        )

    merged: list[PlannedStage] = []
    for group in (
        group for rank in sorted(grouped) for group in grouped[rank].values()
    ):
        first = group[0]
        payloads: dict[str, bytes] = {}
        for stage in group:
            if stage.payload_slug != first.payload_slug:
                raise BuildError(
                    f"stages merged into one sequence disagree about their payload "
                    f"directory: {first.payload_slug!r} and {stage.payload_slug!r}"
                )
            for filename, content in stage.payloads.items():
                if payloads.setdefault(filename, content) != content:
                    raise BuildError(
                        f"two merged stages disagree about payload {filename!r}"
                    )
        merged.append(
            replace(
                first,
                batches=tuple(batch for stage in group for batch in stage.batches),
                payloads=payloads,
                changes=merge_changes(*(stage.changes for stage in group)),
                provides=_merged(stage.provides for stage in group),
                requires=_merged(stage.requires for stage in group),
                follows=_merged(stage.follows for stage in group),
                results={k: v for stage in group for k, v in stage.results.items()},
            )
        )
    return tuple(merged)


def _merged(mappings) -> dict[str, tuple[str, ...]]:
    result: dict[str, tuple[str, ...]] = {}
    for mapping in mappings:
        for action, keys in mapping.items():
            result[action] = (*result.get(action, ()), *keys)
    return result


def enumerate_stages(
    stages: Sequence[PlannedStage],
    *,
    targets,
    completion_target_id: str,
) -> tuple[
    tuple[BuildSequence, ...],
    dict[str, bytes],
    dict[str, tuple[TargetChange, ...]],
    tuple[str, ...],
]:
    """Number populated stages and compile their keys into the physical DAG.

    Returns the sequences, payloads by resolved path, declared changes, and the
    action whose success is the plan's required completion.
    """

    sequences: list[BuildSequence] = []
    payloads: dict[str, bytes] = {}
    changes: list[Mapping[str, tuple[TargetChange, ...]]] = []
    # Empty stages leave no numbering gap.
    populated = [stage for stage in stages if stage.batches]
    for number, stage in enumerate(populated, start=1):
        resolved = {}
        for filename, content in stage.payloads.items():
            path = payload_path(number, stage.payload_slug, filename)
            resolved[filename] = path
            payloads[path] = content
        changes.append(stage.changes)
        sequences.append(
            BuildSequence(
                number=number,
                description=stage.description,
                batches=tuple(
                    _numbered(batch, number, resolved) for batch in stage.batches
                ),
            )
        )
    compiled, required = _compile(
        sequences,
        populated,
        payloads,
        targets={target.id: target for target in targets},
        completion_target_id=completion_target_id,
    )
    return compiled, payloads, merge_changes(*changes), required


def _numbered(
    batch: BuildBatch, number: int, payloads: Mapping[str, str]
) -> BuildBatch:
    actions = []
    for action in batch.actions:
        if action.payload is None:
            actions.append(action)
            continue
        resolved = payloads.get(action.payload)
        if resolved is None:
            raise BuildError(
                f"action {action.id!r} names payload {action.payload!r}, which its "
                "stage did not supply"
            )
        actions.append(replace(action, payload=resolved))
    return replace(batch, id=f"{number:03d}-{batch.id}", actions=tuple(actions))


_SPARK = frozenset({"spark_sql", "spark_sql_batch", "spark_table"})
_TDS = frozenset(
    {
        "tsql",
        "tsql_batch",
        "runtime_state",
        "semantic_catalogue",
        "await_endpoint_objects",
    }
)
_ONELAKE = frozenset({"folder", "load_file"})
#: Session-scoped temporary views authored as table setup can collide.
TEMPORARY_VIEWS = "spark:temporary-views"
SPARK = "spark"


def action_resources(action, target) -> tuple[str, ...]:
    """The constrained capability an action occupies while it runs.

    A Warehouse connection runs one statement at a time, so its key names the
    physical Warehouse. Waiting work holds no resource.
    """

    from .models import AWAIT_TABLE_SHORTCUTS

    if action.executor in _TDS:
        return (f"warehouse:{target.item_id}",)
    if action.executor in _SPARK or action.kind == AWAIT_TABLE_SHORTCUTS:
        return (SPARK,)
    if action.executor in _ONELAKE or action.executor == "shortcut_readiness":
        return (f"onelake:{target.item_id}",)
    if action.executor == "shortcut":
        return (f"shortcuts:{target.item_id}",)
    return ()


def _exclusions(action, payloads) -> tuple[str, ...]:
    if action.executor != "spark_table" or action.payload is None:
        return ()
    import json

    instruction = json.loads(payloads[action.payload].decode("utf-8"))
    return (TEMPORARY_VIEWS,) if instruction.get("setup") else ()


def _compile(sequences, stages, payloads, *, targets, completion_target_id):
    from ..mutation.models import (
        MutationAction,
        MutationBatch,
        MutationSequence,
        ResultReference,
    )
    from .dependencies import DECERTIFIED, PHYSICAL_COMPLETE, PREPARED, action_key

    declared = {}
    providers: dict[str, list[str]] = {}
    for stage in stages:
        for action_id in stage.action_ids:
            declared[action_id] = stage
            for key in (action_key(action_id), *stage.provides.get(action_id, ())):
                providers.setdefault(key, []).append(action_id)
    if not declared:
        return tuple(sequences), ()

    def resolve(keys, action_id):
        return {
            provider
            for key in keys
            for provider in providers.get(key, ())
            if provider != action_id
        }

    depends = {}
    settles = {}
    for action_id, stage in declared.items():
        depends[action_id] = resolve(stage.requires.get(action_id, ()), action_id)
        settles[action_id] = (
            resolve(stage.follows.get(action_id, ()), action_id) - depends[action_id]
        )
    gated = {
        action_id
        for action_id, stage in declared.items()
        if PHYSICAL_COMPLETE in stage.requires.get(action_id, ())
    }
    roots = set(providers.get(DECERTIFIED, ())) | set(providers.get(PREPARED, ()))
    for action_id in declared:
        if not depends[action_id] and action_id not in roots | gated:
            depends[action_id] = set(providers.get(PREPARED, ()))

    gates = {}
    if gated:
        # Success sinks carry every physical prerequisite through depends_on.
        # Publication certifies objects, not endpoint metadata, so refreshes
        # are outside its gate; the Build's completion still includes them.
        # The preparation roots are inside it, so publication never writes
        # the catalogue beside decertification, even with no physical work.
        physical = [
            a
            for a, stage in declared.items()
            if a in roots or stage.phase not in (CATALOGUE, REFRESH)
        ]
        needed = {p for a in physical for p in depends[a]}
        gate = _gate(
            "complete-physical-work",
            completion_target_id,
            [a for a in physical if a not in needed],
        )
        gates[min(gated, key=list(declared).index)] = gate
        for action_id in gated:
            depends[action_id].add(gate.id)
    needed = {p for edges in depends.values() for p in edges} | {
        p for gate in gates.values() for p in gate.depends_on
    }
    final = _gate(
        "complete-build",
        completion_target_id,
        [a for a in declared if a not in needed],
    )

    result = []
    for sequence in sequences:
        batches = []
        for batch in sequence.batches:
            for action in batch.actions:
                if action.id in gates:
                    gate = gates[action.id]
                    batches.append(
                        MutationBatch("completion:physical", gate.target_id, (gate,))
                    )
            target = targets[batch.target_id]
            batches.append(
                MutationBatch(
                    batch.id,
                    batch.target_id,
                    tuple(
                        MutationAction(
                            **action.to_mapping(),
                            target_id=batch.target_id,
                            depends_on=tuple(depends[action.id]),
                            settle_after=tuple(settles[action.id]),
                            resources=action_resources(action, target),
                            exclusions=_exclusions(action, payloads),
                            result_from=(
                                None
                                if action.id not in declared[action.id].results
                                else ResultReference(
                                    *declared[action.id].results[action.id]
                                )
                            ),
                        )
                        for action in batch.actions
                    ),
                )
            )
        result.append(
            MutationSequence(sequence.number, sequence.description, tuple(batches))
        )
    last = result[-1]
    result[-1] = MutationSequence(
        last.number,
        last.description,
        last.batches + (MutationBatch("completion:build", final.target_id, (final,)),),
    )
    return tuple(result), (final.id,)


def _gate(action_id, target_id, depends_on):
    from ..mutation.models import MutationAction

    return MutationAction(
        id=action_id,
        kind="completion_gate",
        resource_node_id=None,
        executor="completion_gate",
        payload=None,
        payload_sha256=None,
        target_id=target_id,
        depends_on=tuple(sorted(depends_on)),
    )
