"""Where a run's advanced bookmarks go.

Downstream of the Runner, exactly as ``_.Log`` is: the Runner decides what runs
and what its outcome was, and the operation that wants the estate's bookmarks
kept up to date opens this and feeds it each settled node. A Runner with no
bookmark writer still runs correctly; it just advances nothing.

Unlike the log, a failure here is not tolerated. A log row is a record of
something that already happened, so losing one loses evidence. A bookmark is
state the *next* load reads, so a lost advance is a load that will read a window
it has already read — recoverable, but the caller must not be told the operation
succeeded when part of what it was for did not.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from ..catalogue.tables import BOOKMARK
from ..declaration.model import WeaverDocumentId, parse_installed_identity
from .result import SUCCEEDED, RunError


class RunBookmarks:
    """The bookmarks one run advances, written through the catalogue's flusher."""

    def __init__(self, flusher: Any) -> None:
        self.flusher = flusher
        self._advanced: dict[WeaverDocumentId, datetime] = {}

    @property
    def advanced(self) -> dict:
        """What this run moved, for a report or a test to read."""

        return dict(self._advanced)

    def advance(self, result) -> None:
        """Queue this node's bookmark, if the node earned one.

        Three conditions, and each rules out a case the others do not. The node
        settled as a clean success, so a rejecting or failed load leaves the
        bookmark it had. It reported an instant, so a Static skip — which is a
        clean success — moves nothing. And it names a logical object, so an
        endpoint refresh, which is neither, is not one of these.
        """

        if result.status != SUCCEEDED or result.logical_id is None:
            return
        at = getattr(result.result, "bookmark_datetime", None)
        if at is None:
            return
        identity = parse_installed_identity(result.logical_id)
        if not isinstance(identity, WeaverDocumentId):
            return
        from ..catalogue.claims import bookmark_row

        at = at if at.tzinfo is not None else at.replace(tzinfo=timezone.utc)
        self._advanced[identity] = at
        self.flusher.update(bookmark_row(identity, at))

    def flush(self) -> None:
        """Wait for every queued bookmark to be written, and surface a failure."""

        from ..catalogue.flusher import FlushError

        try:
            self.flusher.flush()
        except FlushError as exc:
            raise RunError(
                "the load ran but its bookmarks were not recorded, so the next "
                f"load would read a window this one already read: {exc}"
            ) from exc


def open_run_bookmarks(session, *, workspace=None) -> RunBookmarks:
    """Where this run's advanced bookmarks go — the sink, opened at the boundary."""

    from ..targets import WarehouseTarget

    workspace = workspace if workspace is not None else session.workspace
    if workspace is None or not workspace.catalogue:
        raise RunError("advancing bookmarks needs a Workspace with a catalogue")
    catalogue = WarehouseTarget(warehouse=workspace.catalogue_item)
    return RunBookmarks(
        flusher=session.flusher(BOOKMARK, warehouse=catalogue, workspace=workspace)
    )


__all__ = ["RunBookmarks", "open_run_bookmarks"]
