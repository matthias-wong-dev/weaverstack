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
    "publish the mirrored catalogue": "Forking the catalogue and binding items",
}

GATE = "completion_gate"
#: How a mirror names the stages of its catalogue build.
CATALOGUE = "catalogue: "


def _said(text: str) -> str:
    text = text.strip()
    if text.casefold().startswith(CATALOGUE):
        return "Catalogue: " + _said(text[len(CATALOGUE) :]).lower()
    if text.casefold().startswith("empty "):
        return "Wipe " + text[len("empty ") :]
    return _STAGES.get(text.casefold()) or text[:1].upper() + text[1:]


def stage_label(sequence, targets, count: int) -> str:
    said = _said(sequence.description or "")
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
    """Start and end lines for each stage of one plan's execution.

    Events may arrive twice, from a host's progress and again from its final
    ledger, and each is presented once. Durations come from the executing
    host's ledger clock.
    """

    def __init__(self, plan, session) -> None:
        self._session = session
        self._lock = threading.Lock()
        self._seen: set[tuple[str, str]] = set()
        targets = {target.id: target for target in plan.targets}
        self._of: dict[str, _Stage] = {}
        for sequence in plan.sequences:
            actions = [
                action.id
                for batch in sequence.batches
                for action in batch.actions
                if action.executor != GATE
            ]
            if actions:
                stage = _Stage(
                    stage_label(sequence, targets, len(actions)), len(actions)
                )
                self._of.update(dict.fromkeys(actions, stage))

    def observe(self, event) -> None:
        """Present a stage as its first action starts and its last one ends."""

        record = progress_record(event)
        if record is not None:
            self.follow(record)

    def follow(self, record: dict) -> None:
        stage = self._of.get(record["action_id"])
        if stage is None:
            return
        with self._lock:
            key = (record["kind"], record["action_id"])
            if key in self._seen:
                return
            self._seen.add(key)
            if record["kind"] == DISPATCHED:
                if stage.frame is None:
                    stage.first = record["at"]
                    stage.frame = self._session.open_concurrent_substep(stage.label)
                return
            stage.remaining -= 1
            stage.last = record["at"]
            if record["status"] in ("failed", "uncertain"):
                stage.failed += 1
            elif record["status"] == "blocked":
                stage.blocked += 1
            if not stage.remaining and stage.frame is not None:
                stage.frame.failed = bool(stage.failed or stage.blocked)
                stage.frame.note = stage.note
                self._session.close_concurrent_substep(
                    stage.frame, elapsed=max(stage.last - stage.first, 0.0)
                )

    def finish(self, report) -> None:
        """Present whatever the host's progress did not, from its final ledger."""

        for event in report.ledger:
            self.observe(event)


DISPATCHED = "dispatched"
TERMINAL = "terminal"


def progress_record(event) -> dict | None:
    """A ledger event as plain data, if it starts or ends an action."""

    if event.kind not in (DISPATCHED, TERMINAL):
        return None
    return {
        "kind": event.kind,
        "action_id": event.action_id,
        "at": event.at,
        "status": getattr(event.value, "status", None),
    }
