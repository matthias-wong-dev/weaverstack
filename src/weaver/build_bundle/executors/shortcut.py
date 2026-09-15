"""Create and remove frozen OneLake shortcut addresses.

Bound sources resolve through another build target; direct sources carry the
workspace, item and path frozen during generation. Actions use the shortcut API
because deleting a shortcut through storage or Spark could reach source data.

Table creation completes only when both the named relation and Delta path are
readable. Schema shortcuts expose a changing source namespace and are not waited
on.
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
from .base import InstallationContext, ResolvedTarget

FILES_AREA = "Files"

# Bound discovery reports a missing shortcut before its consumer runs.
ADDRESSABLE_TIMEOUT = 300.0
ADDRESSABLE_POLL_INTERVAL = 5.0

# OneLake may retain a removed shortcut's namespace after it stops listing it.
NAME_RELEASE_TIMEOUT = 300.0
NAME_RELEASE_POLL_INTERVAL = 3.0


class ShortcutExecutor:
    name = "shortcut"

    def execute(
        self,
        action: InstallAction,
        payload: bytes | None,
        context: InstallationContext,
    ) -> dict[str, Any] | None:
        if payload is None:
            raise InstallError(f"shortcut action {action.id!r} has no payload")
        manifest = json.loads(payload.decode("utf-8"))
        if "remove" in manifest:
            return self._remove(action, manifest["remove"], context)
        frozen = manifest["shortcuts"]
        if not frozen:
            return {"shortcuts": []}

        create = getattr(context.resolver, "create_onelake_shortcuts", None)
        if create is None:
            raise InstallError(
                f"shortcut action {action.id!r} cannot be materialised here: this "
                "environment offers no way to create a OneLake shortcut"
            )

        # Resolve every case-exact source before the single bulk create request.
        requested = [self._request(each, context) for each in frozen]
        created = create(context.target.lakehouse, requested)
        made = [
            {"shortcut": each["shortcut"], "source": each["source"], **(detail or {})}
            for each, detail in zip(frozen, created)
        ]

        details: dict[str, Any] = {"shortcuts": made}
        # Wait for all created shortcuts in one discovery window.
        waited = self._await_addressable(context, frozen)
        if waited is not None:
            details["addressable_after_seconds"] = waited
        return details

    def _remove(
        self, action: InstallAction, frozen: list, context: InstallationContext
    ) -> dict[str, Any]:
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
        if action.awaits_name_release:
            waited = self._await_name_release(context, frozen)
            if waited is not None:
                details["released_after_seconds"] = waited
        return details

    def _await_name_release(
        self, context: InstallationContext, frozen: list
    ) -> float | None:
        """Wait one window for removed paths to release their names.

        A spent wait returns so the following create can report the occupied
        name directly.
        """

        store = getattr(context, "store", None)
        if store is None:
            return None
        locations = [
            _location(context.target, each, context, source=False) for each in frozen
        ]
        started = time.monotonic()
        deadline = started + NAME_RELEASE_TIMEOUT
        while time.monotonic() < deadline:
            if not any(store.exists(location) for location in locations):
                break
            time.sleep(NAME_RELEASE_POLL_INTERVAL)
        return round(time.monotonic() - started, 1)

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

    def _await_addressable(
        self, context: InstallationContext, frozen: list
    ) -> float | None:
        if context.spark_sql is None:
            # Skipping the wait would expose the discovery race this executor owns.
            raise InstallError(
                "a table shortcut was created but this context offers no way to "
                "ask Spark whether it is readable yet, so the discovery wait "
                "cannot run"
            )
        destination = context.target.destination
        if destination is None:
            raise InstallError(
                f"target {context.target.bound.id!r} resolved to no Spark "
                "destination, so a shortcut in it cannot be named"
            )
        location = context.target.location
        if location is None:
            raise InstallError(
                f"target {context.target.bound.id!r} resolved to no Spark "
                "location, so a shortcut's Delta path cannot be checked"
            )
        return await_addressable(
            frozen,
            destination=destination,
            location=location,
            spark_sql=context.spark_sql,
        )


def await_addressable(frozen, *, destination, location, spark_sql) -> float | None:
    """Wait until every table shortcut's relation and Delta path can be read.

    Metadata can appear before either consumer surface is ready. Mirror uses this
    same readiness contract.
    """

    tables = [each for each in frozen if each.get("type", "table") == "table"]
    if not tables:
        return None

    pending = {
        each["shortcut"]: {
            "relation": str(
                destination.qualify(each["path"].split("/", 1)[1], each["name"])
            ),
            "delta path": location.table_path(
                each["path"].split("/", 1)[1], each["name"]
            ),
        }
        for each in tables
    }

    started = time.monotonic()
    deadline = started + ADDRESSABLE_TIMEOUT
    failure: Exception | None = None
    while pending:
        for shortcut, surfaces in list(pending.items()):
            for surface, address in list(surfaces.items()):
                statement = (
                    f"SELECT * FROM {address} LIMIT 0"
                    if surface == "relation"
                    else f"SELECT * FROM delta.`{address}` LIMIT 0"
                )
                try:
                    spark_sql(statement, exact_case=True)
                    del surfaces[surface]
                except Exception as exc:
                    failure = exc
            if not surfaces:
                del pending[shortcut]
        if not pending:
            break
        if time.monotonic() >= deadline:
            raise InstallError(
                f"shortcut(s) {', '.join(sorted(pending))} were created but "
                f"did not become readable within {int(ADDRESSABLE_TIMEOUT)}s: "
                f"{failure}"
            ) from failure
        time.sleep(ADDRESSABLE_POLL_INTERVAL)
    return round(time.monotonic() - started, 1)


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
