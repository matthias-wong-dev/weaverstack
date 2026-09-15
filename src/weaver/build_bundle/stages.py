"""Logical build stages and final sequence numbering."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Iterable, Mapping, Sequence

from ..errors import BuildError
from .changes import TargetChange
from .changes import merge as merge_changes
from .models import BuildBatch, BuildSequence
from .payloads import payload_path

#: Prune and drops precede creation. Schemas precede shortcuts, which precede
#: builds. Endpoint refresh completes before runtime publication.
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
    """An unnumbered barrier with target batches, payloads, and declared changes.

    ``index`` separates dependency layers within a phase. Payload keys are bare
    filenames until final numbering assigns their directories.
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
    """Merge same-phase, same-index work without crossing dependency layers."""

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
    """Number populated stages and resolve their payload paths and batch ids."""

    sequences: list[BuildSequence] = []
    payloads: dict[str, bytes] = {}
    changes: list[Mapping[str, tuple[TargetChange, ...]]] = []
    # Empty stages are not barriers and leave no numbering gap.
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
