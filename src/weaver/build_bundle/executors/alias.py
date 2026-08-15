"""Materialising one Lakehouse alias — a pointer, in whatever form the host has.

The payload names two addresses and nothing else: this item's ``Schema.Object``,
and the object in another item it stands for. Both are resolved through the
environment, the same way every other action's destination is, so the bundle
carries no path from the machine that wrote it.

The pointer is a OneLake shortcut in the destination Lakehouse, created through
the workspace's own API.

Which alias, over what, is settled in the manifest; how a name is made to point
somewhere is the transport's business. An alias holds no data, so an existing
one is replaced rather than treated as a collision: a build has to run twice.

**The action is not finished until the alias can be read.** Fabric creates a
shortcut synchronously and discovers it asynchronously, so for a few seconds the
Lakehouse reports the name as neither a view nor a table. An action reporting
success there would push the failure into the next item's DDL.
"""

from __future__ import annotations

import json
import time
from typing import Any

from ...errors import InstallError
from ...locations import Location
from ...targets import DeltaTarget, FolderTarget
from ..models import InstallAction
from .base import InstallationContext, ResolvedTarget

FILES_AREA = "Files"

#: How long a freshly created shortcut may take to become addressable, and how
#: often to ask. Discovery normally takes seconds; the bound exists so a
#: never-appearing alias fails naming itself rather than as an obscure error in
#: whatever statement reads it next.
ADDRESSABLE_TIMEOUT = 300.0
ADDRESSABLE_POLL_INTERVAL = 5.0


class AliasExecutor:

    name = "alias"

    def execute(
        self,
        action: InstallAction,
        payload: bytes | None,
        context: InstallationContext,
    ) -> dict[str, Any] | None:
        if payload is None:
            raise InstallError(f"alias action {action.id!r} has no payload")
        frozen = json.loads(payload.decode("utf-8"))["aliases"]
        if not frozen:
            return {"aliases": []}

        shortcut = getattr(context.resolver, "create_onelake_shortcut", None)
        if shortcut is None:
            raise InstallError(
                f"alias action {action.id!r} cannot be materialised here: this "
                "environment offers no way to create a OneLake shortcut"
            )

        made = [
            self._shortcut(
                shortcut, each, context, context.resolved(each["source_target_id"])
            )
            for each in frozen
        ]

        details: dict[str, Any] = {"aliases": made}
        # Every shortcut is created before anything waits, so the cost is one
        # discovery window rather than one per alias.
        waited = self._await_addressable(context, frozen)
        if waited is not None:
            details["addressable_after_seconds"] = waited
        return details

    def _shortcut(self, shortcut, frozen: dict, context, source) -> dict:
        source_name = _physical_source_name(frozen, context, source)
        made = shortcut(
            context.target.lakehouse,
            path=f"{frozen['area']}/{frozen['schema']}",
            name=frozen["object"],
            source=source.lakehouse,
            source_path=(
                f"{frozen['source_area']}/{frozen['source_schema']}"
                f"/{source_name}"
            ),
        )
        return {"alias": frozen["alias"], "source": frozen["source"], **(made or {})}


    def _await_addressable(self, context: InstallationContext, frozen: list) -> float | None:
        """Wait until every table alias just created can actually be read.

        A read rather than a catalogue lookup, because the catalogue is the part
        that is briefly wrong: Fabric reports the shortcut's metadata while still
        refusing it as a relation. All of them are waited on together, so several
        aliases cost one discovery window.
        """

        if context.spark_sql is None:
            # Loud rather than silent: every context the Installer builds has
            # this, so its absence means one was assembled by hand — and not
            # waiting is the race this exists to prevent.
            raise InstallError(
                "a table alias was created but this context offers no way to ask "
                "Spark whether it is readable yet, so the discovery wait cannot "
                "run"
            )

        destination = context.target.destination
        if destination is None:
            raise InstallError(
                f"target {context.target.bound.id!r} resolved to no Spark "
                "destination, so an alias in it cannot be named"
            )
        pending = {
            each["alias"]: destination.qualify(each["schema"], each["object"])
            for each in frozen
            if each["area"] != FILES_AREA
        }
        if not pending:
            return None

        started = time.monotonic()
        deadline = started + ADDRESSABLE_TIMEOUT
        failure: Exception | None = None
        while pending:
            for alias, qualified in list(pending.items()):
                try:
                    # The probe crosses; the waiting does not.
                    context.spark_sql(
                        f"SELECT * FROM {qualified} LIMIT 0", exact_case=True
                    )
                    del pending[alias]
                except Exception as exc:  # not discovered yet — or never will be
                    failure = exc
            if not pending:
                break
            if time.monotonic() >= deadline:
                raise InstallError(
                    f"alias(es) {', '.join(sorted(pending))} were created but did "
                    f"not become readable within {int(ADDRESSABLE_TIMEOUT)}s: {failure}"
                ) from failure
            time.sleep(ADDRESSABLE_POLL_INTERVAL)
        return round(time.monotonic() - started, 1)


def _location(
    target: ResolvedTarget,
    frozen: dict,
    context: InstallationContext,
    *,
    source: bool,
):
    area = frozen["source_area"] if source else frozen["area"]
    schema = frozen["source_schema"] if source else frozen["schema"]
    name = frozen["source_object"] if source else frozen["object"]
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
            f"alias {frozen['alias']} has no readable source parent at "
            f"{parent.value}: {type(exc).__name__}: {exc}"
        ) from exc
    if len(matches) == 1:
        return matches[0]
    if not matches:
        raise InstallError(
            f"alias {frozen['alias']} has no source to point at: "
            f"{producer.value} does not exist"
        )
    raise InstallError(
        f"alias {frozen['alias']} source {producer.value} is ambiguous on storage: "
        + ", ".join(sorted(matches))
    )
