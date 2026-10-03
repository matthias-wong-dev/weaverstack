"""Empty one Lakehouse area within the scope a Wipe plan authorised.

Shortcuts beneath the area are detached through the workspace first, and the
area is swept only after OneLake has released every removed path, so a sweep can
never delete through a shortcut into its source item. Enumeration happens here,
inside the frozen scope.
"""

from __future__ import annotations

import time
from typing import Any

from ...errors import InstallError
from ...locations import Location
from ...physical_wipe import (
    KEPT_SCHEMAS,
    NAME_RELEASE_POLL_INTERVAL,
    NAME_RELEASE_TIMEOUT,
    DetachedShortcuts,
    clear_area,
    detach_shortcuts,
    released,
    unreleased,
)
from ...targets import FILES_AREA, TABLES_AREA
from .base import InstallationContext, Waiting

DETACH_FILE_SHORTCUTS = "detach_file_shortcuts"
DETACH_TABLE_SHORTCUTS = "detach_table_shortcuts"
CLEAR_FILES = "clear_files"
CLEAR_TABLES = "clear_tables"

AREA = {
    DETACH_FILE_SHORTCUTS: FILES_AREA,
    DETACH_TABLE_SHORTCUTS: TABLES_AREA,
    CLEAR_FILES: FILES_AREA,
    CLEAR_TABLES: TABLES_AREA,
}


class LakehouseWipeExecutor:
    """Waiting state is plain data, because the invocation ledger records it."""

    name = "lakehouse_wipe"
    resumable = True

    def execute(
        self, action, payload, context: InstallationContext, state=None
    ) -> dict[str, Any] | Waiting:
        area = AREA.get(action.kind)
        if area is None:
            raise InstallError(f"wipe action {action.id!r} has unknown kind")
        lakehouse = context.target.lakehouse
        if action.kind in (CLEAR_FILES, CLEAR_TABLES):
            root = (
                context.resolver.files_root(lakehouse)
                if area == FILES_AREA
                else context.resolver.tables_root(lakehouse)
            )
            removed = clear_area(
                context.store,
                root,
                context.resolver.root,
                dry_run=False,
                keep=KEPT_SCHEMAS if area == TABLES_AREA else (),
            )
            return {"location": root.value, "removed": list(removed)}
        if state is None:
            detached = detach_shortcuts(context.resolver, lakehouse, prefix=area)
            state = {
                "removed": list(detached.removed),
                "locations": [location.value for location in detached.locations],
                "started": time.monotonic(),
            }
        locations = [Location(value) for value in state["locations"]]
        if released(context.store, locations):
            return {"removed": state["removed"]}
        if time.monotonic() - state["started"] >= NAME_RELEASE_TIMEOUT:
            raise unreleased(
                DetachedShortcuts(tuple(state["removed"]), tuple(locations))
            )
        return Waiting(state, NAME_RELEASE_POLL_INTERVAL)
