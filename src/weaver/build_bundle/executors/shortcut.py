"""Create, remove and await frozen OneLake shortcut addresses.

Bound sources resolve through another build target; direct sources carry the
workspace, item and path frozen during generation. Actions use the shortcut API
because deleting a shortcut through storage or Spark could reach source data.

Creation finishes when Fabric has accepted every definition. Readiness is a
separate action, because metadata can appear before a consumer surface can read
it. Every wait returns ``Waiting`` rather than holding a worker.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any

from ...errors import InstallError
from ...locations import Location
from ...targets import DeltaTarget, FolderTarget
from ..models import InstallAction
from ..targets import WAREHOUSE_TARGET
from .base import InstallationContext, ResolvedTarget, Waiting

FILES_AREA = "Files"
TABLES_SURFACE = "tables"
FILES_SURFACE = "files"

# Bound discovery reports a missing shortcut before its consumer runs.
ADDRESSABLE_TIMEOUT = 300.0
ADDRESSABLE_POLL_INTERVAL = 2.0

# OneLake may retain a removed shortcut's namespace after it stops listing it.
NAME_RELEASE_TIMEOUT = 300.0
NAME_RELEASE_POLL_INTERVAL = 1.0


class ShortcutExecutor:
    """Waiting states are plain data, because the invocation ledger records them."""

    name = "shortcut"
    resumable = True

    def __init__(self) -> None:
        # Sources are resolved once per action; a retried submission reuses them.
        self._requests: dict[str, list] = {}

    def execute(
        self,
        action: InstallAction,
        payload: bytes | None,
        context: InstallationContext,
        state=None,
    ) -> dict[str, Any] | Waiting | None:
        if state is not None:
            if state["phase"] == "release":
                return self._released(state, context)
            return self._submit(action, payload, state, context)
        if payload is None:
            raise InstallError(f"shortcut action {action.id!r} has no payload")
        manifest = json.loads(payload.decode("utf-8"))
        if "remove" in manifest:
            return self._remove(action, manifest["remove"], context)
        frozen = manifest["shortcuts"]
        if not frozen:
            return {"shortcuts": []}
        if getattr(context.resolver, "submit_onelake_shortcuts", None) is None:
            raise InstallError(
                f"shortcut action {action.id!r} cannot be materialised here: this "
                "environment offers no way to create a OneLake shortcut"
            )
        state = {
            "phase": "create",
            "pending": list(range(len(frozen))),
            "made": {},
            "deadline": None,
        }
        return self._submit(action, payload, state, context)

    def _submit(self, action, payload, state, context):
        from ...fabric.shortcuts import (
            SOURCE_POLL_INTERVAL,
            SOURCE_TIMEOUT,
            sources_not_published,
        )

        frozen = json.loads(payload.decode("utf-8"))["shortcuts"]
        requests = self._requests.get(action.id)
        if requests is None:
            requests = self._requests[action.id] = [
                self._request(each, context) for each in frozen
            ]
        pending = list(state["pending"])
        submitted = context.resolver.submit_onelake_shortcuts(
            context.target.lakehouse, [requests[i] for i in pending]
        )
        made = dict(state["made"])
        for position, detail in submitted.created.items():
            made[str(pending[position])] = detail
        pending = [pending[i] for i in submitted.waiting]
        if pending:
            # A Warehouse table can reach OneLake after its transaction settles.
            now = time.monotonic()
            deadline = state["deadline"] or now + SOURCE_TIMEOUT
            if now >= deadline:
                raise sources_not_published(
                    context.target.bound.name,
                    [frozen[i]["shortcut"] for i in pending],
                )
            return Waiting(
                {**state, "pending": pending, "made": made, "deadline": deadline},
                SOURCE_POLL_INTERVAL,
            )
        self._requests.pop(action.id, None)
        return {
            "shortcuts": [
                {
                    "shortcut": each["shortcut"],
                    "source": each["source"],
                    **(made.get(str(i)) or {}),
                }
                for i, each in enumerate(frozen)
            ]
        }

    def _remove(
        self, action: InstallAction, frozen: list, context: InstallationContext
    ) -> dict[str, Any] | Waiting:
        """Remove pointer roots through the API without touching source data."""

        remove = getattr(context.resolver, "remove_onelake_shortcut", None)
        if remove is None:
            raise InstallError(
                f"shortcut action {action.id!r} cannot be run here: this "
                "environment offers no way to remove a OneLake shortcut"
            )
        for each in frozen:
            remove(context.target.lakehouse, path=each["path"], name=each["name"])
        details: dict[str, Any] = {"removed": [each["shortcut"] for each in frozen]}
        if not action.awaits_name_release or getattr(context, "store", None) is None:
            return details
        release = {
            "phase": "release",
            "details": details,
            "locations": [
                _location(context.target, each, context, source=False).value
                for each in frozen
            ],
            "started": time.monotonic(),
        }
        return self._released(release, context)

    def _released(self, release, context) -> dict[str, Any] | Waiting:
        """Return once removed paths release their names, or the wait is spent.

        A spent wait returns so the following create can report the occupied
        name directly.
        """

        elapsed = time.monotonic() - release["started"]
        if elapsed < NAME_RELEASE_TIMEOUT and any(
            context.store.exists(Location(value)) for value in release["locations"]
        ):
            return Waiting(release, NAME_RELEASE_POLL_INTERVAL)
        return {**release["details"], "released_after_seconds": round(elapsed, 1)}

    def _request(self, frozen: dict, context) -> dict:
        if "source_target_id" in frozen:
            source = context.resolved(frozen["source_target_id"])
            source_item = source.lakehouse
            source_kind = source.bound.kind
            source_name = _physical_source_name(frozen, context, source)
            source_path = (
                f"{frozen['source_area']}/{frozen['source_schema']}/{source_name}"
            )
        else:
            # Direct addresses are already resolved and case-exact.
            source_item = ExternalItem(
                id=frozen["source_item_id"],
                name=frozen["source_item_name"],
                workspace_id=frozen["source_workspace_id"],
            )
            source_path = frozen["source_path"]
            source_kind = None
        return {
            "path": frozen["path"],
            "name": frozen["name"],
            "source": source_item,
            "source_kind": source_kind,
            "source_path": source_path,
        }


class ShortcutReadinessExecutor:
    """Wait until created shortcuts are readable from their consumer surface.

    Tables need both the named relation and the Delta path; Files need the
    storage path. Addresses resolve once, and each poll is one sweep over every
    shortcut not yet ready.
    """

    name = "shortcut_readiness"
    resumable = True

    def execute(
        self,
        action: InstallAction,
        payload: bytes | None,
        context: InstallationContext,
        state=None,
    ) -> dict[str, Any] | Waiting:
        if state is None:
            if payload is None:
                raise InstallError(f"readiness action {action.id!r} has no payload")
            manifest = json.loads(payload.decode("utf-8"))
            state = {
                "pending": readiness_checks(
                    manifest["surface"], manifest["shortcuts"], context=context
                ),
                "started": time.monotonic(),
                "failure": None,
            }
        pending, failure = readiness_sweep(state["pending"], context=context)
        failure = failure or state["failure"]
        elapsed = time.monotonic() - state["started"]
        if not pending:
            return {"ready_after_seconds": round(elapsed, 1)}
        if elapsed >= ADDRESSABLE_TIMEOUT:
            raise InstallError(
                f"shortcut(s) {', '.join(sorted(pending))} were created but did "
                f"not become readable within {int(ADDRESSABLE_TIMEOUT)}s: {failure}"
            )
        return Waiting(
            {**state, "pending": pending, "failure": failure},
            ADDRESSABLE_POLL_INTERVAL,
        )


def readiness_checks(surface: str, frozen, *, context) -> dict:
    """Resolve each shortcut's consumer checks once, keyed by shortcut."""

    if surface == FILES_SURFACE:
        return {
            each["shortcut"]: {
                "storage": _location(context.target, each, context, source=False).value
            }
            for each in frozen
        }
    if surface != TABLES_SURFACE:
        raise InstallError(f"unknown shortcut readiness surface {surface!r}")
    if context.spark_sql is None:
        raise InstallError(
            "a table shortcut was created but this context offers no way to "
            "ask Spark whether it is readable yet"
        )
    destination, location = context.target.destination, context.target.location
    if destination is None or location is None:
        raise InstallError(
            f"target {context.target.bound.id!r} resolved to no Spark destination, "
            "so a shortcut in it cannot be read"
        )
    checks = {}
    for each in frozen:
        schema = each["path"].split("/", 1)[1]
        relation = destination.qualify(schema, each["name"])
        path = location.table_path(schema, each["name"])
        checks[each["shortcut"]] = {
            "relation": f"SELECT * FROM {relation} LIMIT 0",
            "delta path": f"SELECT * FROM delta.`{path}` LIMIT 0",
        }
    return checks


def readiness_sweep(pending: dict, *, context) -> tuple[dict, str | None]:
    """Check every unready surface once; return those still unready."""

    remaining: dict = {}
    failure = None
    for shortcut, surfaces in pending.items():
        unready = {}
        for surface, check in surfaces.items():
            try:
                if surface == "storage":
                    ready = context.store.exists(Location(check))
                else:
                    context.spark_sql(check, exact_case=True)
                    ready = True
            except Exception as exc:  # noqa: BLE001 - not yet readable
                failure, ready = f"{type(exc).__name__}: {exc}", False
            if not ready:
                unready[surface] = check
        if unready:
            remaining[shortcut] = unready
    return remaining, failure


@dataclass(frozen=True)
class ExternalItem:
    """The shortcut API identity of an unbound item outside this build."""

    id: str
    name: str
    workspace_id: str


def _location(
    target: ResolvedTarget,
    frozen: dict,
    context: InstallationContext,
    *,
    source: bool,
):
    area = frozen["source_area"] if source else frozen["path"].split("/", 1)[0]
    schema = frozen["source_schema"] if source else frozen["path"].split("/", 1)[1]
    name = frozen["source_object"] if source else frozen["name"]
    if area == FILES_AREA:
        return context.resolver.folder_object(
            FolderTarget(lakehouse=target.lakehouse), schema, name
        )
    return context.resolver.delta_table(
        DeltaTarget(lakehouse=target.lakehouse), schema, name
    )


def _physical_source_name(frozen: dict, context: InstallationContext, source) -> str:
    """Return the source's case-exact storage spelling.

    Prefer authored spelling when present; otherwise require one
    case-insensitive match.
    """

    if source.bound.kind == WAREHOUSE_TARGET:
        # Warehouses publish declared spelling and expose no Lakehouse location
        # for storage inspection.
        return frozen["source_object"]

    producer = _location(source, frozen, context, source=True)
    if context.store.exists(producer):
        return producer.name

    parent = Location(producer.value.rsplit("/", 1)[0])
    try:
        matches = [
            entry.name
            for entry in context.store.list(parent)
            if entry.name.casefold() == producer.name.casefold()
        ]
    except Exception as exc:  # noqa: BLE001 - converted to an install diagnosis
        raise InstallError(
            f"shortcut {frozen['shortcut']} has no readable source parent at "
            f"{parent.value}: {type(exc).__name__}: {exc}"
        ) from exc
    if len(matches) == 1:
        return matches[0]
    if not matches:
        raise InstallError(
            f"shortcut {frozen['shortcut']} has no source to point at: "
            f"{producer.value} does not exist"
        )
    raise InstallError(
        f"shortcut {frozen['shortcut']} source {producer.value} is ambiguous on "
        "storage: " + ", ".join(sorted(matches))
    )
