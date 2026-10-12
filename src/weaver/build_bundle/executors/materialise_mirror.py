"""Materialise protected borrowed objects with native Fabric operations."""

from __future__ import annotations

import json
import time

from ...errors import InstallError
from ...spark import FabricSparkTarget
from ...targets import validate_name
from .base import Waiting
from .shortcut import NAME_RELEASE_POLL_INTERVAL, NAME_RELEASE_TIMEOUT


class MaterialiseMirrorExecutor:
    name = "materialise_mirror"
    resumable = True

    def execute(self, action, payload, context, state=None):
        if payload is None:
            raise InstallError(f"materialisation {action.id!r} has no payload")
        instruction = json.loads(payload)
        kind = instruction["physical_type"]
        if kind not in ("folder", "table"):
            raise InstallError(f"unsupported protected mirror type {kind!r}")
        area = "Files" if kind == "folder" else "Tables"
        if instruction["area"] != area:
            raise InstallError("protected mirror type and area disagree")
        names = {
            key: validate_name(instruction[key], what=key)
            for key in ("schema", "name", "source_schema", "source_object")
        }
        resolver = context.resolver
        warehouse = context.target.bound.kind == "warehouse"
        if warehouse and kind != "table":
            raise InstallError("Warehouse materialisation requires a Table")
        producer = resolver.external_item(
            instruction["source_target"],
            item_type="Warehouse" if warehouse else "Lakehouse",
            workspace=instruction["source_workspace"],
        )
        destination_workspace_id = (
            context.target.bound.workspace_id or resolver.workspace.id
        )
        same_item = (producer.workspace_id, producer.id) == (
            destination_workspace_id,
            context.target.bound.item_id,
        )
        if same_item and (names["schema"], names["name"]) == (
            names["source_schema"],
            names["source_object"],
        ):
            raise InstallError("protected mirror source overlaps its destination")
        if warehouse:
            return self._warehouse(instruction, names, producer, context)
        source_path = f"{area}/{names['source_schema']}/{names['source_object']}"
        source = resolver.external_root(producer).join(*source_path.split("/"))
        destination = resolver.lakehouse(context.target.lakehouse).join(
            area, names["schema"], names["name"]
        )
        if (
            source == destination
            or source.value.startswith(destination.value + "/")
            or destination.value.startswith(source.value + "/")
        ):
            raise InstallError("protected mirror source overlaps its destination")
        path = f"{area}/{names['schema']}"
        shortcuts = [
            s
            for s in resolver.onelake_shortcuts(context.target.lakehouse)
            if (s.path, s.name) == (path, names["name"])
        ]
        if len(shortcuts) > 1:
            raise InstallError("protected mirror has multiple shortcut definitions")
        if shortcuts:
            pointer = shortcuts[0]
            if (
                pointer.target_workspace_id,
                pointer.target_item_id,
                pointer.target_path,
            ) != (producer.workspace_id, producer.id, source_path):
                raise InstallError(
                    "protected mirror shortcut no longer points at its recorded source"
                )
        if kind == "folder":
            copy = getattr(context.store, "copy_directory", None)
            if copy is None:
                raise InstallError(
                    "protected Folder materialisation requires native recursive copy"
                )
        elif context.spark_sql is None:
            raise InstallError("protected Delta materialisation requires Spark SQL")
        if shortcuts and state is None:
            resolver.remove_onelake_shortcut(
                context.target.lakehouse, path=path, name=names["name"]
            )
            state = {"started": time.monotonic()}
        if state is not None and context.store.exists(destination):
            if time.monotonic() - state["started"] >= NAME_RELEASE_TIMEOUT:
                raise InstallError(
                    f"removed shortcut {path}/{names['name']} still occupies its path"
                )
            return Waiting(state, NAME_RELEASE_POLL_INTERVAL)
        if kind == "folder":
            copy(source, destination)
            mechanism = "recursive_copy"
        else:
            origin = FabricSparkTarget(
                instruction["source_workspace"] or resolver.workspace.name,
                producer.name,
            ).qualify(names["source_schema"], names["source_object"])
            local = context.destination.qualify(names["schema"], names["name"])
            context.spark_sql(
                f"CREATE TABLE {local} SHALLOW CLONE {origin}", exact_case=True
            )
            mechanism = "shallow_clone"
        return {
            "object": instruction["identity"],
            "source": source.value,
            "destination": destination.value,
            "mechanism": mechanism,
        }

    @staticmethod
    def _warehouse(instruction, names, producer, context):
        if producer.workspace_id != (
            context.target.bound.workspace_id or context.resolver.workspace.id
        ):
            raise InstallError(
                "Warehouse materialisation requires a source in the destination workspace"
            )
        if context.sql is None:
            raise InstallError("protected Warehouse materialisation requires TDS")

        def quote(name):
            return "[" + name.replace("]", "]]") + "]"

        local = ".".join(quote(names[key]) for key in ("schema", "name"))
        origin = ".".join(
            quote(value)
            for value in (producer.name, names["source_schema"], names["source_object"])
        )
        context.sql.execute_script(f"DROP VIEW {local};")
        context.sql.execute_script(f"CREATE TABLE {local} AS SELECT * FROM {origin};")
        return {
            "object": instruction["identity"],
            "source": origin,
            "destination": local,
            "mechanism": "ctas",
        }
