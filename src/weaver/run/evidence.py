"""What a run records about itself, in ``_.Log``.

One row per settled unit of work, appended as the Runner settles it. There is no
plan row and no completion row: a workflow is its rows, correlated by
``[Workflow ID]``, and a reader asking what a run did reads them rather than
reassembling a folder.

The Runner decides what happened and constructs the row; the flusher writes it.
Nothing here waits for the Warehouse.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from ..catalogue.tables import LOG
from .result import RunError

#: Run statuses, as the frozen public ``[Result]`` vocabulary spells them.
#: Several run statuses settle to one result: what a reader of the estate's
#: evidence asks is whether the work landed, and the detail is in ``[Details]``.
RESULT_FOR_STATUS = {
    "succeeded": "succeeded",
    "succeeded_with_rejects": "succeeded",
    # A validation that ran and found nothing.
    "passed": "succeeded",
    "failed": "failed",
    # The node could not be evaluated at all. Not a failure of the data, but
    # not a success either, and a reader asking "did this land" wants Failed.
    "invalid": "failed",
    "blocked": "blocked",
    "skipped": "skipped",
    # Never reached, because the run stopped before scheduling it.
    "pending": "skipped",
    # The two dry-run outcomes. A dry run writes nothing, so neither should
    # reach a row — they are mapped so that a change of mind about that does
    # not surface as a run failing at its last step.
    "validated": "skipped",
    "planned": "skipped",
}


def _result_for(status: str) -> str:
    try:
        return RESULT_FOR_STATUS[status]
    except KeyError:
        raise RunError(
            f"{status!r} has no place in the public Result vocabulary; add one "
            "deliberately rather than letting a run write an unknown value"
        ) from None


@dataclass
class RunLog:
    """One workflow's evidence, appended through a Session-managed flusher."""

    workflow_id: str
    task_type: str
    flusher: Any

    def submit(self, node) -> None:
        """Record one settled node."""

        self.flusher.submit(self.row(node))

    def row(self, node) -> dict:
        """The ``_.Log`` row one settled node produces.

        Physical identity, deliberately: the target the work touched and the
        object it touched there. Which logical item declared it is a question
        the catalogue answers, and duplicating it here would let the two drift.
        """

        target_type, _, target_name = str(node.physical_target).partition("/")
        schema, name = _object_parts(node.logical_id)
        started = _instant(node.started_at)
        completed = _instant(node.finished_at)
        return {
            "log_sk": uuid.uuid4().hex,
            "workflow_id": self.workflow_id,
            "task_type": self.task_type,
            "target_type": target_type or None,
            "target_name": target_name or None,
            "schema_name": schema,
            "object_name": name,
            "result": _result_for(node.status),
            "started_datetime": started,
            "completed_datetime": completed,
            "duration_milliseconds": _duration(started, completed),
            "message": _message(node),
            "details": _details(node),
        }


def _object_parts(logical_id: str | None) -> tuple[str | None, str | None]:
    """The schema and object a node's logical id names, if it names one.

    A logical id is ``<ItemType>/<ItemName>/<Schema>.<Object>``; a node such as
    an endpoint refresh has no object at all and says so with nulls.
    """

    if not logical_id:
        return None, None
    qualified = str(logical_id).rsplit("/", 1)[-1]
    schema, separator, name = qualified.rpartition(".")
    if not separator:
        return None, qualified or None
    return schema or None, name or None


def _instant(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value))
    except ValueError:
        return None


def _duration(started: datetime | None, completed: datetime | None) -> int | None:
    if started is None or completed is None:
        return None
    return int((completed - started).total_seconds() * 1000)


def _message(node) -> str | None:
    """One concise line, from the node's own messages."""

    for message in getattr(node, "messages", ()):
        text = getattr(message, "message", None) or str(message)
        if text:
            return text[:4000]
    return None


def _details(node) -> str | None:
    """The node's own mapping, as JSON.

    The mapping rather than the node: a mapping carries no diagnostics, so rows
    a check happened to select cannot reach the estate's evidence.
    """

    try:
        mapping = node.to_mapping()
    except Exception:  # noqa: BLE001 - evidence must not fail a run
        return None
    mapping.pop("diagnostics", None)
    text = json.dumps(mapping, default=str, sort_keys=True)
    return text[:4000]


def new_workflow_id() -> str:
    """One correlation identity for a whole workflow."""

    return uuid.uuid4().hex


def open_run_log(session, *, workspace=None, task_type: str, workflow_id=None):
    """Where this run's evidence goes — the sink, opened at the boundary.

    Downstream of the Runner by construction: a run is correct without one, and
    this is called by the operation that wants a durable record rather than by
    the thing doing the work. ``task_type`` is what the record says it was,
    because a load and a validation need the same capabilities and are not the
    same event.
    """

    from ..targets import WarehouseTarget

    workspace = workspace if workspace is not None else session.workspace
    if workspace is None or not workspace.catalogue:
        raise RunError("writing run evidence needs a Workspace with a catalogue")
    catalogue = WarehouseTarget(warehouse=workspace.catalogue_item)
    return RunLog(
        workflow_id=workflow_id or new_workflow_id(),
        task_type=task_type,
        flusher=session.flusher(LOG, warehouse=catalogue, workspace=workspace),
    )


__all__ = ["RESULT_FOR_STATUS", "RunLog", "new_workflow_id", "open_run_log"]
