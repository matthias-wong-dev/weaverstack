"""Materialising one Lakehouse alias — a pointer, in whatever form the host has.

The payload names two addresses and nothing else: this item's ``Schema.Object``,
and the object in another item it stands for. Both are resolved through the
environment, the same way every other action's destination is, so the bundle
carries no path from the machine that wrote it.

The *form* the pointer takes is transport:

``Fabric``
    a OneLake shortcut in the destination Lakehouse, created through the
    workspace's own API.
``the local emulator``
    a filesystem link beside the destination's other tables, which is what makes
    the emulator's ``Tables/`` area keep mirroring what a shortcut looks like in
    OneLake — plus the catalogue registration Fabric performs for itself, because
    a link no statement could name would not be an alias at all.

That is the same split :mod:`weaver.build_bundle.executors.spark_schema`
documents: which alias, over what, is settled and in the manifest; how a name is
made to point somewhere is the environment's business. An alias holds no data, so
an existing one is replaced rather than treated as an unexpected collision — a
build has to be able to run twice.

**The action is not finished until the alias can be read.** Fabric creates a
shortcut synchronously and *discovers* it asynchronously, so the API call
returning is not the same thing as the alias existing: for a few seconds the
Lakehouse reports the name as "neither a view nor a table". An action that
reported success there would make the barrier the plan puts around it a lie, and
the failure would surface in the next item's DDL — which is exactly where it did
surface, before this waited.
"""

from __future__ import annotations

import json
import time
from typing import Any

from ...errors import InstallError
from ...targets import DeltaTarget, FolderTarget
from ..models import InstallAction
from .base import InstallationContext, ResolvedTarget
from .spark_case import exact_identifier_case

FILES_AREA = "Files"

#: How long a freshly created shortcut may take to become addressable, and how
#: often to ask. Discovery normally takes seconds; the bound exists so a
#: never-appearing alias fails naming itself rather than as an obscure error in
#: whatever statement reads it next.
ADDRESSABLE_TIMEOUT = 300.0
ADDRESSABLE_POLL_INTERVAL = 5.0


class AliasExecutor:

    #: This executor reaches Spark, so on a host without one the action
    #: crosses whole rather than the capability being faked underneath it.
    needs_spark = True
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
        link = getattr(context.store, "link", None)
        if shortcut is None and link is None:
            raise InstallError(
                f"alias action {action.id!r} cannot be materialised here: this "
                "environment offers neither a OneLake shortcut nor a store link"
            )

        made = []
        for each in frozen:
            source = context.resolved(each["source_target_id"])
            if shortcut is not None:
                made.append(self._shortcut(shortcut, each, context, source))
            else:
                made.append(self._link(link, each, context, source))

        details: dict[str, Any] = {"aliases": made}
        # Every shortcut is created before anything waits, so the cost is one
        # discovery window rather than one per alias.
        if shortcut is not None and context.spark is not None:
            waited = self._await_addressable(context, frozen)
            if waited is not None:
                details["addressable_after_seconds"] = waited
        return details

    def _shortcut(self, shortcut, frozen: dict, context, source) -> dict:
        made = shortcut(
            context.target.lakehouse,
            path=f"{frozen['area']}/{frozen['schema']}",
            name=frozen["object"],
            source=source.lakehouse,
            source_path=(
                f"{frozen['source_area']}/{frozen['source_schema']}"
                f"/{frozen['source_object']}"
            ),
        )
        return {"alias": frozen["alias"], "source": frozen["source"], **(made or {})}

    def _link(self, link, frozen: dict, context, source) -> dict:
        destination = _location(context.target, frozen, context, source=False)
        producer = _location(source, frozen, context, source=True)
        if not context.store.exists(producer):
            raise InstallError(
                f"alias {frozen['alias']} has no source to point at: "
                f"{producer.value} does not exist"
            )
        if context.store.exists(destination):
            context.store.delete(destination, recursive=True)
        link(producer, destination)
        made = {
            "alias": frozen["alias"],
            "source": frozen["source"],
            "linked": destination.value,
            "to": producer.value,
        }
        if frozen["area"] != FILES_AREA and context.spark is not None:
            # Fabric discovers a shortcut under Tables/ and the table appears in
            # the catalogue. Local Spark discovers nothing, so the emulator names
            # it — otherwise the link would exist and no statement could reach it.
            catalogue = context.catalogue
            with exact_identifier_case(
                context.spark,
                enabled=catalogue.destination.preserve_table_identifier_case,
            ):
                made["registered"] = catalogue.register_external_table(
                    frozen["schema"], frozen["object"], destination.value
                )
        return made

    def _await_addressable(self, context: InstallationContext, frozen: list) -> float | None:
        """Wait until every table alias just created can actually be read.

        The probe is a read, not a catalogue lookup, because the catalogue is the
        thing that is briefly wrong: Fabric reports the shortcut's metadata
        location while still refusing it as a relation. Only a read that succeeds
        proves the alias is usable, and a read is what the next action does.

        All of them are waited on together, so several aliases cost one discovery
        window instead of one each.
        """

        catalogue = context.catalogue
        pending = {
            each["alias"]: catalogue.qualify(each["schema"], each["object"])
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
                    with exact_identifier_case(
                        context.spark,
                        enabled=catalogue.destination.preserve_table_identifier_case,
                    ):
                        context.spark.sql(f"SELECT * FROM {qualified} LIMIT 0").collect()
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
