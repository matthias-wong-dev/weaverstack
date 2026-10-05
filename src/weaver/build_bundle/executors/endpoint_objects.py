"""Wait until a Warehouse can read objects through a Lakehouse's SQL endpoint.

A completed metadata refresh does not mean the endpoint lists a new shortcut
table yet. The Warehouse that reads it asks through its own connection, and
asks Fabric to refresh again while anything is still missing.
"""

from __future__ import annotations

import json
import time
from typing import Any

from ...errors import InstallError, WeaverError
from .base import InstallationContext, Waiting

EXECUTOR = "await_endpoint_objects"
TIMEOUT = 600.0
POLL_INTERVAL = 2.0
#: How often a fresh refresh is requested while objects are missing.
REFRESH_INTERVAL = 20.0


class EndpointObjectsExecutor:
    """Waiting state is plain data, because the invocation ledger records it."""

    name = EXECUTOR
    resumable = True

    def execute(
        self, action, payload, context: InstallationContext, state=None
    ) -> dict[str, Any] | Waiting:
        if state is None:
            if payload is None:
                raise InstallError(f"{action.id!r} names no objects to wait for")
            state = {
                "objects": json.loads(payload.decode("utf-8"))["objects"],
                "started": time.monotonic(),
                "refreshed": time.monotonic(),
            }
        missing = _missing(context.sql, state["objects"])
        elapsed = time.monotonic() - state["started"]
        if not missing:
            return {"visible_after_seconds": round(elapsed, 1)}
        if elapsed >= TIMEOUT:
            raise InstallError(
                "the SQL endpoint did not list "
                + ", ".join(".".join(each) for each in missing)
                + f" within {int(TIMEOUT)}s"
            )
        if time.monotonic() - state["refreshed"] >= REFRESH_INTERVAL:
            _refresh(context, {database for database, _s, _o in missing})
            state = {**state, "refreshed": time.monotonic()}
        return Waiting({**state, "objects": missing}, POLL_INTERVAL)


def _missing(sql, objects) -> list[list[str]]:
    from ...catalogue.tsql import identifier

    missing = []
    for database in sorted({each[0] for each in objects}):
        listed = {
            (str(row["table_schema"]).casefold(), str(row["table_name"]).casefold())
            for row in sql.query(
                "select table_schema, table_name from "
                f"{identifier(database)}.INFORMATION_SCHEMA.TABLES"
            )
        }
        missing.extend(
            each
            for each in objects
            if each[0] == database
            and (each[1].casefold(), each[2].casefold()) not in listed
        )
    return missing


def _refresh(context, databases) -> None:
    from ...targets import ItemRef

    begin = getattr(context.resolver, "start_sql_endpoint_refresh", None)
    if begin is None:
        return
    for database in sorted(databases):
        try:
            begin(ItemRef(database))
        except WeaverError:
            # One already running answers for this one.
            pass
