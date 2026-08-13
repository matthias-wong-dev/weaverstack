"""Serializable installation results, with one result per planned action."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

PENDING = "pending"
RUNNING = "running"
SUCCEEDED = "succeeded"
FAILED = "failed"
SKIPPED = "skipped"


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


@dataclass(frozen=True)
class ActionResult:
    """The outcome of one action."""

    action_id: str
    resource_node_id: str | None
    target_id: str
    executor: str
    status: str
    started_at: datetime | None = None
    finished_at: datetime | None = None
    duration_seconds: float | None = None
    error_type: str | None = None
    error_message: str | None = None
    details: dict[str, Any] | None = None
    #: The authored repository file this action came from, where it has one, so
    #: a failure can name the file to open rather than the artefact it became.
    source_path: str | None = None

    def to_mapping(self) -> dict[str, Any]:
        mapping: dict[str, Any] = {
            "action_id": self.action_id,
            "resource_node_id": self.resource_node_id,
            "target_id": self.target_id,
            "executor": self.executor,
            "status": self.status,
            "started_at": _iso(self.started_at),
            "finished_at": _iso(self.finished_at),
            "duration_seconds": self.duration_seconds,
        }
        if self.source_path is not None:
            mapping["source_path"] = self.source_path
        if self.error_type is not None:
            mapping["error_type"] = self.error_type
            mapping["error_message"] = self.error_message
        if self.details:
            mapping["details"] = self.details
        return mapping


@dataclass(frozen=True)
class SequenceResult:
    number: int
    description: str
    status: str
    actions: tuple[ActionResult, ...]

    def to_mapping(self) -> dict[str, Any]:
        return {
            "number": self.number,
            "description": self.description,
            "status": self.status,
            "actions": [action.to_mapping() for action in self.actions],
        }


@dataclass(frozen=True)
class InstallationReport:
    bundle_id: str
    status: str
    started_at: datetime
    finished_at: datetime | None
    sequences: tuple[SequenceResult, ...]

    @property
    def succeeded(self) -> bool:
        return self.status == SUCCEEDED

    def action_results(self):
        for sequence in self.sequences:
            for action in sequence.actions:
                yield action

    def to_mapping(self) -> dict[str, Any]:
        return {
            "bundle_id": self.bundle_id,
            "status": self.status,
            "started_at": _iso(self.started_at),
            "finished_at": _iso(self.finished_at),
            "sequences": [sequence.to_mapping() for sequence in self.sequences],
        }

    def to_yaml(self) -> str:
        import yaml

        return yaml.safe_dump(self.to_mapping(), sort_keys=False, allow_unicode=True)
