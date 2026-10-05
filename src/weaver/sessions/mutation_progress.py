"""A MutationPlan's stages as the operator follows them.

A stage is one of the plan's sequences: one kind of work on its targets.
Stages run as their actions' edges allow, so several may be underway at once.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass

#: Stage descriptions in the operator's words. Any other is shown as written.
_STAGES = {
    "reconcile and remove catalogue claims before physical work": (
        "Removing superseded catalogue entries"
    ),
    "stop recording materialised objects as borrowed": "Updating catalogue ownership",
    "drop selected rebuild dependency layer": "Removing objects for rebuild",
    "build dependency layer": "Building objects",
    "install runtime artefacts": "Installing load and test artefacts",
    "publish catalogue dictionaries and installations": "Updating catalogue definitions",
    "publish item registry last": "Finalising catalogue",
    "refresh mutated lakehouse sql endpoints": "Refreshing SQL endpoints",
    "record the views this build created": "Recording created Views",
    "prune unmanaged objects by logical item": "Removing unmanaged objects",
    "materialise item-owned shortcuts": "Creating shortcuts",
    "create item-owned schemas": "Creating schemas",
}

GATE = "completion_gate"


def stage_label(sequence, targets, count: int) -> str:
    text = (sequence.description or "").strip()
    said = _STAGES.get(text.casefold()) or text[:1].upper() + text[1:]
    names: list[str] = []
    for batch in sequence.batches:
        target = targets.get(batch.target_id)
        name = target.display if target is not None else batch.target_id
        if name not in names:
            names.append(name)
    actions = f"{count} {'action' if count == 1 else 'actions'}"
    return " · ".join(part for part in (", ".join(names), said, actions) if part)


@dataclass
class _Stage:
    label: str
    remaining: int
    failed: int = 0
    blocked: int = 0
    frame: object = None
    first: float | None = None
    last: float | None = None

    @property
    def note(self) -> str | None:
        said = [
            f"{count} {word}"
            for count, word in ((self.failed, "failed"), (self.blocked, "blocked"))
            if count
        ]
        return ", ".join(said) or None


class StageProgress:
    """Start and end lines for each stage of one plan's execution."""

    def __init__(self, plan, session) -> None:
        self._session = session
        self._lock = threading.Lock()
        targets = {target.id: target for target in plan.targets}
        self._stages: list[_Stage] = []
        self._of: dict[str, _Stage] = {}
        for sequence in plan.sequences:
            actions = [
                action.id
                for batch in sequence.batches
                for action in batch.actions
                if action.executor != GATE
            ]
            if not actions:
                continue
            stage = _Stage(stage_label(sequence, targets, len(actions)), len(actions))
            self._stages.append(stage)
            self._of.update(dict.fromkeys(actions, stage))

    def observe(self, event) -> None:
        """Present a stage as its first action starts and its last one ends."""

        stage = self._of.get(event.action_id)
        if stage is None:
            return
        with self._lock:
            if event.kind == "dispatched" and stage.frame is None:
                stage.frame = self._session.open_concurrent_substep(stage.label)
            elif event.kind == "terminal":
                self._count(stage, event)
                if not stage.remaining and stage.frame is not None:
                    stage.frame.failed = bool(stage.failed or stage.blocked)
                    stage.frame.note = stage.note
                    self._session.close_concurrent_substep(stage.frame)

    def replay(self, report) -> None:
        """Present each stage of a plan that ran elsewhere, once it has ended.

        Durations come from the executing host's ledger.
        """

        for event in report.ledger:
            stage = self._of.get(event.action_id)
            if stage is None:
                continue
            if event.kind == "dispatched" and stage.first is None:
                stage.first = event.at
            elif event.kind == "terminal":
                self._count(stage, event)
                stage.last = event.at
        for stage in self._stages:
            if stage.first is None:
                continue
            self._session.finished_substep(
                stage.label,
                elapsed=max((stage.last or stage.first) - stage.first, 0.0),
                failed=bool(stage.failed or stage.blocked),
                note=stage.note,
            )

    @staticmethod
    def _count(stage: _Stage, event) -> None:
        stage.remaining -= 1
        status = getattr(event.value, "status", None)
        if status in ("failed", "uncertain"):
            stage.failed += 1
        elif status == "blocked":
            stage.blocked += 1
