"""Build and write catalogue evidence for settled runtime work.

Blocked nodes have status but no statistics. Reload state is reset durably before
execution, and row construction remains separate from writing and flushing.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from ..catalogue.claims import bookmark_row, catalogue_schema
from ..catalogue.tables import (
    BLOCKED,
    BOOKMARK,
    BOOKMARK_SENTINEL,
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

if TYPE_CHECKING:  # pragma: no cover - for type readers only
    from .result import RunNodeResult

LOAD_TASK = "load"
TEST_TASK = "test"

#: Every run and validation status, in the frozen public ``[Result]``
#: vocabulary. A status missing from here fails the run at its last step, so a
#: new one is added here.
RESULT_FOR_STATUS = {
    "succeeded": SUCCEEDED,
    "succeeded_with_rejects": REJECTED,
    # A validation that ran and found nothing.
    "passed": SUCCEEDED,
    "failed": FAILED,
    # Resolution failed before dispatch, so nothing was evaluated.
    "invalid": ERROR,
    "blocked": BLOCKED,
    "skipped": SKIPPED,
    # Never reached, because the run stopped before scheduling it.
    "pending": PENDING,
    # The dry-run outcomes. A dry run writes nothing, so neither reaches a row;
    # they are mapped so a change of mind about that is not a failure at the last
    # step.
    "validated": PENDING,
    "planned": PENDING,
}


def result_for(node, *, task_type: str = LOAD_TASK) -> str:
    """Map a node outcome to the catalogue's frozen Result vocabulary."""

    status = node.status
    try:
        result = RESULT_FOR_STATUS[status]
    except KeyError:
        raise RunError(
            f"Cannot record status {status!r}: it is not in the public Result "
            "vocabulary"
        ) from None
    if result != FAILED or not getattr(node, "raised", False):
        return result
    if task_type == TEST_TASK:
        return ERROR
    return FAILED if getattr(node, "refused", False) else ERROR


# --- the rows ------------------------------------------------------------------


def log_row(node, *, workflow_id: str, task_type: str) -> dict:

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
        "result": result_for(node, task_type=task_type),
        "started_datetime": started,
        "completed_datetime": completed,
        "duration_milliseconds": _duration(started, completed),
        "message": _message(node),
        "details": _details(node),
    }


def load_status_row(node, identity, *, workflow_id: str) -> dict:
    """Build a LoadStatus row keyed by logical identity."""

    started, completed = _instants(node)
    return {
        **_identity(identity),
        "workflow_id": workflow_id,
        "result": result_for(node, task_type=LOAD_TASK),
        "started_datetime": started,
        "completed_datetime": completed,
        "duration_milliseconds": _duration(started, completed),
    }


def load_statistic_row(
    node, identity, *, workflow_id: str, reload: bool = False
) -> dict:
    """Build statistics for an executed load.

    Counts describe the target, so ``rows_read`` need not equal their sum.
    ``reload`` comes from the request because a raised load may have no result.
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
        "is_reload": bool(reload),
        "is_static_skip": bool(getattr(result, "is_static_skip", False)),
    }


def reset_load_status_row(identity, *, workflow_id: str, started) -> dict:
    """Build the Pending status written before a reload clears its target."""

    return {
        **_identity(identity),
        "workflow_id": workflow_id,
        "result": PENDING,
        "started_datetime": started,
        "completed_datetime": None,
        "duration_milliseconds": None,
    }


def test_status_row(node, identity, *, workflow_id: str) -> dict:

    started, completed = _instants(node)
    return {
        **_identity(identity),
        "test_type": _test_type(node),
        "workflow_id": workflow_id,
        "result": result_for(node, task_type=TEST_TASK),
        "started_datetime": started,
        "completed_datetime": completed,
        "duration_milliseconds": _duration(started, completed),
        "failure_count": _failure_count(node),
    }


def _identity(identity) -> dict:
    return bookmark_key(identity)


def bookmark_key(identity) -> dict:

    return bookmark_row(identity)


def _test_type(node) -> str | None:

    from ..catalogue.tables import ROLE_ASSUMPTION, ROLE_TEST
    from ..declaration.metadata import ASSUMPTION, TEST

    return {TEST: ROLE_TEST, ASSUMPTION: ROLE_ASSUMPTION}.get(node.role)


def _failure_count(node) -> int | None:

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
    """One workflow's buffered operational record."""

    workflow_id: str
    task_type: str
    catalogue: Any
    #: The objects this record has reset for a reload. ``_.LoadStatistic`` writes
    #: ``Is reload`` from it, so a reload that raised is still recorded as one.
    reloaded: set = field(default_factory=set)

    def settled(self, node) -> None:

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
        # Every load attempt moves _.LoadStatus forward, a Static skip
        # included: it records Result=Skipped with this workflow's timestamps,
        # the same row the Warehouse ``_.Load`` procedure writes. A skip does
        # not advance the bookmark, because it consumed no source window; its
        # LoadStatus timestamp still takes part in health ancestry ordering.
        # See ``design/health.md``.
        self.catalogue.update(
            LOAD_STATUS,
            load_status_row(node, identity, workflow_id=self.workflow_id),
        )
        if node.executed:
            # A statistic describes a load that ran. A blocked node did nothing,
            # and a row of zeroes for it would read as a load that moved nothing.
            self.catalogue.submit(
                LOAD_STATISTIC,
                load_statistic_row(
                    node,
                    identity,
                    workflow_id=self.workflow_id,
                    reload=identity in self.reloaded,
                ),
            )
        self._bookmark(node, identity)

    def reset(self, identity) -> None:
        """Durably invalidate status and bookmark before reconstruction.

        The sentinel preserves one bookmark row while forcing the next load to
        read the complete source.
        """

        started = datetime.now(timezone.utc)
        # Before either write: a reset that did not land was still asked for as
        # a reload.
        self.reloaded.add(identity)
        self.catalogue.update(
            LOAD_STATUS,
            reset_load_status_row(
                identity, workflow_id=self.workflow_id, started=started
            ),
        )
        self.catalogue.update(BOOKMARK, bookmark_row(identity, BOOKMARK_SENTINEL))
        self.flush()

    def _bookmark(self, node, identity) -> None:
        """Advance only after a clean load establishes a new instant."""

        if node.status != "succeeded":
            return
        at = getattr(node.result, "bookmark_datetime", None)
        if at is None:
            return
        self.catalogue.update(BOOKMARK, bookmark_row(identity, at))

    def flush(self) -> None:

        from ..catalogue.flusher import FlushError

        try:
            self.catalogue.flush()
        except FlushError as exc:
            raise RunError(
                f"The {self.task_type} completed, but its catalogue record could "
                f"not be written: {exc}. Check catalogue connectivity before "
                "running it again."
            ) from exc


# --- a standalone call, presented as the settled unit of work it is ------------


def settled_load(
    identity,
    result,
    *,
    physical_target: str,
    started,
    completed,
    raised: bool = False,
    refused: bool = False,
) -> "RunNodeResult":
    """Represent a standalone load in the shared runtime-table vocabulary."""

    from .outcome import status_of
    from .result import FAILED, RunNodeResult

    return RunNodeResult(
        node_id=str(identity),
        physical_target=physical_target,
        primitive_kind="standalone",
        logical_id=str(identity),
        status=FAILED if raised else status_of(result),
        raised=raised,
        refused=refused or not raised,
        executed=True,
        result=result,
        started_at=_isoformat(started),
        finished_at=_isoformat(completed),
        target_type=physical_target.partition("/")[0] or None,
        target_name=physical_target.partition("/")[2] or None,
        schema_name=catalogue_schema(identity),
        object_name=identity.object_id.object,
    )


def settled_validation(
    identity,
    result,
    *,
    physical_target: str,
    kind: str,
    started,
    completed,
    raised: bool = False,
) -> "RunNodeResult":
    """Represent a standalone Test or Assumption for runtime recording."""

    from .outcome import status_of
    from .result import FAILED, RunNodeResult

    return RunNodeResult(
        node_id=str(identity),
        physical_target=physical_target,
        primitive_kind="standalone",
        logical_id=str(identity),
        role=kind,
        status=FAILED if raised else status_of(result),
        raised=raised,
        # It ran and reported, so what it says is Weaver's own judgement.
        refused=not raised,
        executed=True,
        result=result,
        started_at=_isoformat(started),
        finished_at=_isoformat(completed),
        target_type=physical_target.partition("/")[0] or None,
        target_name=physical_target.partition("/")[2] or None,
        schema_name=catalogue_schema(identity),
        object_name=identity.object_id.object,
    )


def _isoformat(at) -> str | None:
    return None if at is None else at.isoformat()


def _installed(node):
    """Recover the installed identity carried by a settled node."""

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
    """Return catalogue object keys, preserving area distinctions."""

    from ..catalogue.claims import catalogue_columns
    from ..declaration.model import WeaverDocumentId
    from ..errors import IdentityError

    if not logical_id:
        return None, None
    try:
        return catalogue_columns(WeaverDocumentId.parse(str(logical_id)))
    except IdentityError:
        pass
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
    """Prefer the run's message, then a standalone result's error."""

    for message in getattr(node, "messages", ()):
        text = getattr(message, "message", None) or str(message)
        if text:
            return text[:4000]
    carried = getattr(node.result, "error_message", None)
    return str(carried)[:4000] if carried else None


def _details(node) -> str | None:
    """Serialise the node without persisting validation diagnostics."""

    try:
        mapping = node.to_mapping()
    except Exception:  # noqa: BLE001 - evidence must not fail a run
        return None
    mapping.pop("diagnostics", None)
    text = json.dumps(mapping, default=str, sort_keys=True)
    return text[:4000]


def new_workflow_id() -> str:

    return uuid.uuid4().hex


def open_run_record(
    catalogue, *, workspace=None, task_type: str, workflow_id=None, session=None
) -> RunRecord:

    if workspace is not None and not workspace.catalogue:
        raise RunError("Recording this run needs a Workspace with a Weaver catalogue")
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
    "settled_load",
    "settled_validation",
    "test_status_row",
]
