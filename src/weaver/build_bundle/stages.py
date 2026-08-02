"""Logical stages, and the one place a sequence number is chosen.

A planning component answers *what* has to happen and in what order relative to
its own siblings. It does not answer *which sequence number* that is, because a
number is a property of the finished plan and nothing else: with one alias, one
schema and one endpoint-refresh stage per item, arithmetic over reserved regions
stops describing the plan and starts constraining it.

So each component returns :class:`PlannedStage` values — a phase, a description,
target-bound batches, and the payloads those batches need, keyed by bare
filename. The top-level planner concatenates the stages in execution order and
:func:`enumerate_stages` turns them into :class:`~weaver.build_bundle.models.BuildSequence`
values, numbering them 1, 2, 3 … and rewriting each payload into
``payload/<number>-<slug>/<filename>`` so the bundle directory still reads top to
bottom in deployment order.

Numbering last also means a stage cannot collide with another stage's region, and
there is no headroom to run out of: the number *describes* the order the plan
already has.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Iterable, Mapping, Sequence

from ..errors import BuildError
from .changes import TargetChange, merge as merge_changes
from .models import BuildBatch, BuildSequence
from .payloads import payload_path

#: The phases one item's work is made of, in the order they must run.
#:
#: Prune and managed drops come first because they are the destructive
#: reconciliation of what is already there. Schemas precede aliases so an alias
#: materialised as a Warehouse view has a schema to be created in, and aliases
#: precede builds so every document this item declares is built against a
#: namespace that already holds what the item imports. The refresh closes the
#: item: until a mutated Lakehouse's SQL endpoint has caught up, a dependent
#: item's view or shortcut would be built over metadata that does not describe it.
#: Load closes the item, after the refresh. Its artefacts depend on the item's
#: structural work being finished and on nothing within their own layer — a
#: deployed module and a generated procedure have no ordering between them,
#: because nothing here runs them.
PRUNE = "prune"
DROP = "drop"
SCHEMA = "schema"
ALIAS = "alias"
BUILD = "build"
REFRESH = "refresh"
LOAD = "load"
CATALOGUE = "catalogue"

_PHASE_ORDER = (PRUNE, DROP, SCHEMA, ALIAS, BUILD, REFRESH, LOAD, CATALOGUE)
_PHASE_RANK = {phase: rank for rank, phase in enumerate(_PHASE_ORDER)}


@dataclass(frozen=True)
class PlannedStage:
    """One barrier's worth of work, before it is given a number.

    ``phase`` and ``index`` place the stage among its siblings: ``index``
    separates the dependency layers within a phase, so two items in the same
    topological item layer can have their layer *n* merged into one barrier.

    ``slug`` names the stage's payload directory. ``payloads`` is keyed by bare
    filename within it, because the directory's name is not known until the
    stage has a number.

    ``changes`` is what this stage's actions will *mean* for each target, keyed
    by target id. Rendered beside the actions rather than inferred from them —
    see :mod:`weaver.build_bundle.changes` — so the statement of effect and the
    thing that has the effect are written in one place.
    """

    phase: str
    description: str
    batches: tuple[BuildBatch, ...]
    slug: str = ""
    index: int = 0
    payloads: Mapping[str, bytes] = field(default_factory=dict)
    changes: Mapping[str, tuple[TargetChange, ...]] = field(default_factory=dict)

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
    def payload_slug(self) -> str:
        return self.slug or self.phase

    @property
    def rank(self) -> tuple[int, int]:
        return (_PHASE_RANK[self.phase], self.index)


def merge_layer_stages(stages: Iterable[PlannedStage]) -> tuple[PlannedStage, ...]:
    """Fold same-phase, same-index stages from one item layer into one barrier.

    Items in the same topological layer have no ordering between them, so their
    work belongs in the same barriers: one batch per item, exactly as a
    single-layer build already produces. Merging here is what keeps the
    invariant that matters — nothing in a later item layer starts before this
    layer has completed — without serialising items that never needed it.
    """

    grouped: dict[tuple[int, int], list[PlannedStage]] = {}
    for stage in stages:
        grouped.setdefault(stage.rank, []).append(stage)

    merged: list[PlannedStage] = []
    for rank in sorted(grouped):
        group = grouped[rank]
        first = group[0]
        payloads: dict[str, bytes] = {}
        for stage in group:
            if stage.payload_slug != first.payload_slug:
                raise BuildError(
                    f"stages merged into one barrier disagree about their payload "
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
            )
        )
    return tuple(merged)


def enumerate_stages(
    stages: Sequence[PlannedStage],
) -> tuple[
    tuple[BuildSequence, ...],
    dict[str, bytes],
    dict[str, tuple[TargetChange, ...]],
]:
    """Number the assembled plan and resolve every payload path.

    Batch ids gain the same number prefix, so a batch is still identifiable in a
    report and still unique across the plan without any component having to know
    what else is being planned.
    """

    sequences: list[BuildSequence] = []
    payloads: dict[str, bytes] = {}
    changes: list[Mapping[str, tuple[TargetChange, ...]]] = []
    # An empty stage is not a barrier — it is a phase this build had no work for
    # — so it takes no number and leaves no gap.
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
    return tuple(sequences), payloads, merge_changes(*changes)


def _numbered(batch: BuildBatch, number: int, payloads: Mapping[str, str]) -> BuildBatch:
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
