"""What a run records about itself, in the catalogue's runtime tables.

One row per settled unit of work in ``_.Log``, appended as the run settles it,
and beside it the operational state that unit left behind:

.. code-block:: text

    every settled node       _.Log                append
    a load that executed     _.LoadStatus         merge on the object's identity
                             _.LoadStatistic      append
    a clean load             _.Bookmark           merge, to the instant it began
    a validation that ran    _.TestStatus         merge on the validation's identity

There is no plan row and no completion row: a workflow is its rows, correlated by
``[Workflow ID]``, and a reader asking what a run did reads them rather than
reassembling a folder.

Row construction is separate from writing, and both are separate from flushing.
The run decides what happened and builds the rows; the catalogue writes them on a
worker; :meth:`RunRecord.flush` is the durability barrier and the only place a
failure surfaces.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from ..catalogue.tables import (
    BLOCKED,
    BOOKMARK,
    ERROR,
    FAILED,
    LOAD_STATISTIC,
    LOAD_STATUS,
    LOG,
    PENDING,
    REJECTED,
    SKIPPED,
    SUCCEEDED,
    TEST_STATUS,
)
from .result import RunError

#: The task types a run records under. A load and a validation need the same
#: capabilities and are not the same event.
LOAD_TASK = "load"
TEST_TASK = "test"

#: Run and validation statuses, as the frozen public ``[Result]`` vocabulary
#: spells them. The distinctions kept here are the ones an operator acts on, so
#: nothing collapses two of them into one word.
RESULT_FOR_STATUS = {
    "succeeded": SUCCEEDED,
    # A load that wrote its valid rows and refused the rest. Neither a success
    # nor a failure: valid rows landed, and some did not.
    "succeeded_with_rejects": REJECTED,
    # A validation that ran and found nothing.
    "passed": SUCCEEDED,
    # Ran under Weaver's control and produced an unacceptable result: a
    # validation found discrepancies, a load refused a change larger than its
    # declared threshold. A node that *raised* is an Error instead — see
    # :func:`result_for`.
    "failed": FAILED,
    # The node could not be evaluated at all: resolution failed before dispatch.
    # Not a judgement about the data, and it must never be read as one.
    "invalid": ERROR,
    "blocked": BLOCKED,
    "skipped": SKIPPED,
    # Never reached, because the run stopped before scheduling it. No outcome was
    # established for this incarnation, which is what Pending means.
    "pending": PENDING,
    # The two dry-run outcomes. A dry run writes nothing, so neither should reach
    # a row — they are mapped so that a change of mind about that does not
    # surface as a run failing at its last step.
    "validated": PENDING,
    "planned": PENDING,
}


def result_for(node) -> str:
    """How one settled node's outcome is spelled in the ``_`` schema.

    A node that *raised* is an Error rather than a Failure whatever its status
    says. The two are not the same thing to act on: a validation that found
    discrepancies has told you something about the data, and one whose procedure
    threw has told you nothing at all.
    """

    status = node.status
    try:
        result = RESULT_FOR_STATUS[status]
    except KeyError:
        raise RunError(
            f"{status!r} has no place in the public Result vocabulary; add one "
            "deliberately rather than letting a run write an unknown value"
        ) from None
    if result == FAILED and getattr(node, "raised", False):
        return ERROR
    return result


# --- the rows ------------------------------------------------------------------


def log_row(node, *, workflow_id: str, task_type: str) -> dict:
    """The ``_.Log`` row one settled node produces."""

    target_type, target_name = _target_of(node)
    schema, name = _object_of(node)
    started, completed = _instants(node)
    return {
        "log_sk": uuid.uuid4().hex,
        "workflow_id": workflow_id,
        "task_type": task_type,
        "target_type": target_type or None,
        "target_name": target_name or None,
        "schema_name": schema,
        "object_name": name,
        "result": result_for(node),
        "started_datetime": started,
        "completed_datetime": completed,
        "duration_milliseconds": _duration(started, completed),
        "message": _message(node),
        "details": _details(node),
    }


def load_status_row(node, identity, *, workflow_id: str) -> dict:
    """The ``_.LoadStatus`` row one settled load leaves behind.

    Logical identity only. Where the object is physically installed is the
    Installation's to say, and duplicating it here would give a reader two places
    to disagree.
    """

    started, completed = _instants(node)
    return {
        **_identity(identity),
        "workflow_id": workflow_id,
        "result": result_for(node),
        "started_datetime": started,
        "completed_datetime": completed,
        "duration_milliseconds": _duration(started, completed),
    }


def load_statistic_row(node, identity, *, workflow_id: str) -> dict:
    """The ``_.LoadStatistic`` row one executed load appends.

    The counts describe the target rather than the source: ``rows_read`` is what
    the source produced, and the rest are what the load did with it.
    """

    started, completed = _instants(node)
    result = node.result
    return {
        "load_statistic_sk": uuid.uuid4().hex,
        "workflow_id": workflow_id,
        **_identity(identity),
        "started_datetime": started,
        "completed_datetime": completed,
        "duration_milliseconds": _duration(started, completed),
        "rows_read": _count(result, "rows_read"),
        "rows_inserted": _count(result, "rows_inserted"),
        "rows_updated": _count(result, "rows_updated"),
        "rows_deleted": _count(result, "rows_deleted"),
        "rows_rejected": _count(result, "rows_rejected"),
        # False until reload is available. Written rather than left null, so a
        # reader counting reloads gets zero rather than nothing.
        "is_reload": False,
        "is_static_skip": bool(getattr(result, "is_static_skip", False)),
    }


def test_status_row(node, identity, *, workflow_id: str) -> dict:
    """The ``_.TestStatus`` row one settled validation leaves behind.

    ``failure_count`` is how much disagreed, and it is meaningful only for a
    validation that was evaluated: one that could not run found nothing, and
    reporting zero discrepancies for it would be the one answer a validation must
    never give. The result says which of the two happened.
    """

    started, completed = _instants(node)
    return {
        **_identity(identity),
        "test_type": _test_type(node),
        "workflow_id": workflow_id,
        "result": result_for(node),
        "started_datetime": started,
        "completed_datetime": completed,
        "duration_milliseconds": _duration(started, completed),
        "failure_count": _failure_count(node),
    }


def _identity(identity) -> dict:
    """One installed object's four-part identity, as every table keys it."""

    from ..catalogue.claims import bookmark_row

    return bookmark_row(identity)


def _test_type(node) -> str | None:
    """Whether this validation is a Test or an Assumption, as stored."""

    from ..catalogue.tables import ROLE_ASSUMPTION, ROLE_TEST
    from ..declaration.metadata import ASSUMPTION, TEST

    return {TEST: ROLE_TEST, ASSUMPTION: ROLE_ASSUMPTION}.get(node.role)


def _failure_count(node) -> int | None:
    """How much a validation found, or None where it found nothing at all."""

    result = node.result
    if result is None or getattr(node, "raised", False):
        return None
    for name in ("failure_count", "violation_count"):
        found = getattr(result, name, None)
        if found is not None:
            return int(found)
    return None


def _count(result, name: str) -> int:
    return int(getattr(result, name, 0) or 0)


# --- what one run writes -------------------------------------------------------


@dataclass
class RunRecord:
    """One workflow's operational record, written through its catalogue.

    Downstream of the Runner by construction: a run is correct without one, and
    this is called by the operation that wants a durable record rather than by
    the thing doing the work. ``task_type`` is what the record says it was.

    One place, because it is one catalogue: the evidence that a node ran, the
    status it left and the bookmark a clean load moved are rows in the same ``_``
    schema, written through the same connection and made durable by the same
    flush.
    """

    workflow_id: str
    task_type: str
    catalogue: Any

    def settled(self, node) -> None:
        """Record one settled node: its evidence, and the state it left."""

        self.catalogue.submit(
            LOG,
            log_row(node, workflow_id=self.workflow_id, task_type=self.task_type),
        )
        identity = _installed(node)
        if identity is None:
            # An endpoint refresh is not an object, so it has no state to leave.
            return
        if self.task_type == TEST_TASK:
            self.catalogue.update(
                TEST_STATUS,
                test_status_row(node, identity, workflow_id=self.workflow_id),
            )
            return
        self.catalogue.update(
            LOAD_STATUS,
            load_status_row(node, identity, workflow_id=self.workflow_id),
        )
        if node.executed:
            # A statistic describes a load that ran. A blocked node did nothing,
            # and a row of zeroes for it would read as a load that moved nothing.
            self.catalogue.submit(
                LOAD_STATISTIC,
                load_statistic_row(node, identity, workflow_id=self.workflow_id),
            )
        self._bookmark(node, identity)

    def _bookmark(self, node, identity) -> None:
        """Advance the bookmark, for a clean load that established an instant.

        Two conditions, each ruling out a case the other does not: a clean
        success, so a rejecting or failed load keeps the bookmark it had; and an
        instant reported, so a Static skip moves nothing.
        """

        if node.status != "succeeded":
            return
        at = getattr(node.result, "bookmark_datetime", None)
        if at is None:
            return
        from ..catalogue.claims import bookmark_row

        self.catalogue.update(BOOKMARK, bookmark_row(identity, at))

    def flush(self) -> None:
        """Wait for what this run recorded, and say what did not land."""

        from ..catalogue.flusher import FlushError

        try:
            self.catalogue.flush()
        except FlushError as exc:
            raise RunError(
                f"the {self.task_type} ran but what it did was not recorded, so "
                "the estate's account of itself is behind what happened: "
                f"{exc}"
            ) from exc


def _installed(node):
    """The installed object this node was about, or None if it was about none."""

    from ..declaration.model import WeaverDocumentId, parse_installed_identity

    if not node.logical_id:
        return None
    identity = parse_installed_identity(node.logical_id)
    return identity if isinstance(identity, WeaverDocumentId) else None


def _target_of(node) -> tuple[str | None, str | None]:
    target_type = getattr(node, "target_type", None)
    target_name = getattr(node, "target_name", None)
    if target_type is None and target_name is None:
        target_type, _, target_name = str(node.physical_target).partition("/")
    return target_type, target_name


def _object_of(node) -> tuple[str | None, str | None]:
    schema = getattr(node, "schema_name", None)
    name = getattr(node, "object_name", None)
    if schema is None and name is None:
        return _object_parts(node.logical_id)
    return schema, name


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


def _instants(node) -> tuple[datetime | None, datetime | None]:
    return _instant(node.started_at), _instant(node.finished_at)


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


def open_run_record(
    catalogue, *, workspace=None, task_type: str, workflow_id=None, session=None
) -> RunRecord:
    """Where this run's operational record goes — the catalogue that owns ``_``.

    It builds rows and does not own a write stream: the catalogue does, and the
    runtime tables are its.
    """

    if workspace is not None and not workspace.catalogue:
        raise RunError("recording what a run did needs a Workspace with a catalogue")
    return RunRecord(
        workflow_id=workflow_id
        or (session.workflow_id if session is not None else None)
        or new_workflow_id(),
        task_type=task_type,
        catalogue=catalogue,
    )


__all__ = [
    "LOAD_TASK",
    "RESULT_FOR_STATUS",
    "RunRecord",
    "TEST_TASK",
    "load_statistic_row",
    "load_status_row",
    "log_row",
    "new_workflow_id",
    "open_run_record",
    "result_for",
    "test_status_row",
]
