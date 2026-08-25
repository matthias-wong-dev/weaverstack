"""Materialising one Lakehouse shortcut, in whatever form the host has.

The payload names two addresses and nothing else: where the shortcut appears in
this item, and what it points at. A **bound** source names another target of this
build, resolved through the environment the same way every other action's
destination is, so the bundle carries no path from the machine that wrote it. A
**direct** source carries the workspace, item and path resolved when the bundle
was generated, because it is not a target of this build and nothing here would
know where to look.

The pointer is a OneLake shortcut in the destination Lakehouse, created through
the workspace's own API.

Which shortcut, over what, is settled in the manifest; how a name is made to
point somewhere is the transport's business. A shortcut holds no data, so an
existing one is replaced rather than treated as a collision: a build has to run
twice.

**The action is not finished until a table shortcut can be read both ways.**
Fabric creates a shortcut synchronously and discovers its named relation and
physical Delta path asynchronously. Those surfaces may settle at different
times, and consumers use both. A schema shortcut is not waited on: what it
presents is the source item's, and its contents can change without a build.
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

#: How long a freshly created shortcut may take to become addressable, and how
#: often to ask. Discovery normally takes seconds; the bound exists so a
#: never-appearing shortcut fails naming itself rather than as an obscure error in
#: whatever statement reads it next.
ADDRESSABLE_TIMEOUT = 300.0
ADDRESSABLE_POLL_INTERVAL = 5.0


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
        frozen = json.loads(payload.decode("utf-8"))["shortcuts"]
        if not frozen:
            return {"shortcuts": []}

        shortcut = getattr(context.resolver, "create_onelake_shortcut", None)
        if shortcut is None:
            raise InstallError(
                f"shortcut action {action.id!r} cannot be materialised here: this "
                "environment offers no way to create a OneLake shortcut"
            )

        made = [self._shortcut(shortcut, each, context) for each in frozen]

        details: dict[str, Any] = {"shortcuts": made}
        # Every shortcut is created before anything waits, so the cost is one
        # discovery window rather than one per shortcut.
        waited = self._await_addressable(context, frozen)
        if waited is not None:
            details["addressable_after_seconds"] = waited
        return details

    def _shortcut(self, shortcut, frozen: dict, context) -> dict:
        """One shortcut, from whichever kind of source the plan froze."""

        if "source_target_id" in frozen:
            source = context.resolved(frozen["source_target_id"])
            source_item = source.lakehouse
            source_kind = source.bound.kind
            source_name = _physical_source_name(frozen, context, source)
            source_path = (
                f"{frozen['source_area']}/{frozen['source_schema']}/{source_name}"
            )
        else:
            # Already resolved, and case-exact: Fabric validates a shortcut's
            # target when it is created and its paths are case-sensitive.
            source_item = ExternalItem(
                id=frozen["source_item_id"],
                name=frozen["source_item_name"],
                workspace_id=frozen["source_workspace_id"],
            )
            source_path = frozen["source_path"]
            source_kind = None
        made = shortcut(
            context.target.lakehouse,
            path=frozen["path"],
            name=frozen["name"],
            source=source_item,
            source_kind=source_kind,
            source_path=source_path,
        )
        return {
            "shortcut": frozen["shortcut"],
            "source": frozen["source"],
            **(made or {}),
        }

    def _await_addressable(
        self, context: InstallationContext, frozen: list
    ) -> float | None:
        """Wait until every table shortcut's relation and Delta path can be read.

        Reads rather than catalogue or storage lookups, because Fabric can report
        the shortcut's metadata before either consumer surface is ready. All of
        them are waited on together, so several shortcuts cost one discovery
        window.
        """

        tables = [
            each for each in frozen if each.get("type", "table") == "table"
        ]
        if not tables:
            return None

        if context.spark_sql is None:
            # Loud rather than silent: every context the Installer builds has
            # this, so its absence means one was assembled by hand — and not
            # waiting is the race this exists to prevent.
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
                        # The probes cross; the waiting does not.
                        context.spark_sql(statement, exact_case=True)
                        del surfaces[surface]
                    except Exception as exc:  # not discovered yet — or never will be
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
    """A physical item outside this build, as the shortcut API addresses it.

    Enough of an item to be a shortcut's source and nothing more. It carries no
    binding, because a direct shortcut points at something Weaver does not
    manage.
    """

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
    """Return the source's storage spelling, which Fabric may have folded.

    Logical identity remains exact-case, but OneLake shortcut target paths are
    physical and case-sensitive. Some Fabric estates materialise an authored
    ``Customer`` table directory as ``customer``. Prefer the authored spelling
    when it exists; otherwise resolve one case-insensitive storage match.
    """

    if source.bound.kind == WAREHOUSE_TARGET:
        # A Warehouse publishes its table at the declared OneLake spelling, but
        # it has no Lakehouse Spark location for the store resolver to inspect.
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
