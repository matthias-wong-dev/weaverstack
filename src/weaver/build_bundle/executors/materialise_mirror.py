"""Copy borrowed contents into private storage before removing their pointer."""

from __future__ import annotations

import hashlib
import importlib
import json
import tempfile
import time
from pathlib import Path
from urllib.parse import unquote, urlsplit
from uuid import uuid4

from ...errors import InstallError, OutcomeUnknown
from ...locations import Location
from ...targets import validate_name
from .base import Waiting


class MaterialiseMirrorExecutor:
    name = "materialise_mirror"
    resumable = True

    def execute(self, action, payload, context, state=None):
        if state is not None:
            return self._publish(state, context)
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
        resolver, store = context.resolver, context.store
        publish = getattr(store, "publish_directory", None)
        copy = getattr(store, "copy", None)
        if publish is None or copy is None:
            raise InstallError(
                "protected materialisation needs binary copy and non-overwriting directory publication"
            )
        path = f"{area}/{names['schema']}"
        destination = resolver.lakehouse(context.target.lakehouse).join(
            area, names["schema"], names["name"]
        )
        private = resolver.files_root(context.target.lakehouse).join(
            "_weaver_materialisation"
        )
        receipt = private.join(
            hashlib.sha256(instruction["identity"].encode()).hexdigest() + ".json"
        )
        shortcuts = [
            s
            for s in resolver.onelake_shortcuts(context.target.lakehouse)
            if (s.path, s.name) == (path, names["name"])
        ]
        if len(shortcuts) > 1:
            raise InstallError("protected mirror has multiple shortcut definitions")
        if not shortcuts and store.exists(destination):
            if not store.exists(receipt):
                raise InstallError(
                    "protected mirror has local contents without a verified copy"
                )
            recorded = json.loads(store.read(receipt))
            if (
                recorded.get("instruction") != instruction
                or recorded.get("destination") != destination.value
            ):
                raise InstallError(
                    "protected mirror verified copy belongs to another operation"
                )
            self._validate_tree(destination, kind, store)
            if self._snapshot(destination, store) != recorded["snapshot"]:
                raise InstallError(
                    "protected mirror local contents differ from its verified copy"
                )
            return {
                "object": instruction["identity"],
                "verified": True,
                "already_owned": True,
            }
        producer = resolver.external_item(
            instruction["source_target"],
            item_type="Lakehouse",
            workspace=instruction["source_workspace"],
        )
        source_path = f"{area}/{names['source_schema']}/{names['source_object']}"
        source = resolver.external_root(producer).join(*source_path.split("/"))
        if (
            source == destination
            or source.value.startswith(destination.value.rstrip("/") + "/")
            or destination.value.startswith(source.value.rstrip("/") + "/")
        ):
            raise InstallError("protected mirror source overlaps its destination")
        if shortcuts:
            shortcut = shortcuts[0]
            if (
                shortcut.target_workspace_id,
                shortcut.target_item_id,
                shortcut.target_path,
            ) != (producer.workspace_id, producer.id, source_path):
                raise InstallError(
                    "protected mirror shortcut no longer points at its recorded source"
                )
        self._validate_tree(source, kind, store)
        if kind == "table":
            self._validate_delta_references(source, store)
        before = self._snapshot(source, store)
        stage = private.join(uuid4().hex)
        store.make_directory(stage)
        try:
            for relative, entry in sorted(before.items()):
                local = stage.join(*relative.split("/"))
                if entry[0] == "directory":
                    store.make_directory(local)
                else:
                    store.make_directory(Location(local.value.rsplit("/", 1)[0]))
                    copy(source.join(*relative.split("/")), local)
            if (
                self._snapshot(stage, store) != before
                or self._snapshot(source, store) != before
            ):
                raise InstallError(
                    "protected mirrored contents changed or were not copied exactly"
                )
        except Exception:
            # Before cutover only our private staging tree may be removed.
            store.delete(stage, recursive=True)
            raise
        state = {
            "stage": stage.value,
            "destination": destination.value,
            "identity": instruction["identity"],
            "kind": kind,
            "snapshot": before,
            "deadline": time.monotonic() + 300.0,
        }
        store.write(
            receipt,
            json.dumps(
                {
                    "instruction": instruction,
                    "destination": destination.value,
                    "snapshot": before,
                },
                sort_keys=True,
            ).encode(),
        )
        if shortcuts:
            resolver.remove_onelake_shortcut(
                context.target.lakehouse, path=path, name=names["name"]
            )
        return self._publish(state, context)

    def _publish(self, state, context):
        store = context.store
        destination, stage = Location(state["destination"]), Location(state["stage"])
        if store.exists(destination):
            if time.monotonic() >= state["deadline"]:
                raise InstallError(
                    f"protected mirror name remains occupied; verified copy retained at {stage.value}"
                )
            return Waiting(state, 1.0)
        try:
            store.publish_directory(stage, destination)
            self._validate_tree(destination, state["kind"], store)
            if self._snapshot(destination, store) != state["snapshot"]:
                raise InstallError(
                    "materialised contents differ from the verified private copy"
                )
        except OutcomeUnknown:
            raise
        except Exception as exc:
            raise InstallError(
                f"protected mirror publication failed; inspect {stage.value} and {destination.value}: {exc}"
            ) from exc
        return {
            "object": state["identity"],
            "verified": True,
            "already_owned": False,
            "entries": len(state["snapshot"]),
        }

    @staticmethod
    def _validate_tree(root, kind, store):
        if not store.is_directory(root):
            raise InstallError(
                f"protected mirror contents are not a readable directory: {root.value}"
            )
        if kind == "table" and not store.is_directory(root.join("_delta_log")):
            raise InstallError(
                f"protected Table has no Delta transaction log: {root.value}"
            )

    @staticmethod
    def _snapshot(root, store):
        prefix = root.value.rstrip("/") + "/"
        result = {}
        for entry in store.list(root, recursive=True):
            if not entry.location.value.startswith(prefix):
                raise InstallError("protected copy listing escaped its root")
            relative = entry.location.value[len(prefix) :]
            if not relative or any(
                part in ("", ".", "..") for part in relative.split("/")
            ):
                raise InstallError("protected copy contains a noncanonical path")
            if entry.is_directory:
                result[relative] = ["directory"]
            else:
                data = store.read(entry.location)
                result[relative] = ["file", len(data), hashlib.sha256(data).hexdigest()]
        return result

    @staticmethod
    def _validate_delta_references(root, store):
        """Read Delta checkpoint paths and retained commits before copying."""
        log = root.join("_delta_log")
        prefix = log.value.rstrip("/") + "/"

        def relative_path(value):
            decoded = unquote(value)
            if (
                urlsplit(decoded).scheme
                or decoded.startswith("/")
                or "\\" in decoded
                or any(part in ("", ".", "..") for part in decoded.split("/"))
            ):
                raise InstallError(
                    f"protected Delta has an external file reference: {value!r}"
                )

        with tempfile.TemporaryDirectory() as directory:
            local = Path(directory)
            for entry in store.list(log, recursive=True):
                if entry.is_directory:
                    continue
                if not entry.location.value.startswith(prefix):
                    raise InstallError("protected Delta log listing escaped its root")
                relative = entry.location.value[len(prefix) :]
                relative_path(relative)
                target = local / "_delta_log" / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                data = store.read(entry.location)
                target.write_bytes(data)
                if relative.endswith(".json") and entry.name[:20].isdigit():
                    for line in data.splitlines():
                        record = json.loads(line)
                        addition = record.get("add") or record.get("remove")
                        if addition is not None and "path" in addition:
                            relative_path(addition["path"])
                            vector = addition.get("deletionVector") or {}
                            if vector.get("storageType") == "p":
                                relative_path(vector["pathOrInlineDv"])
            table = importlib.import_module("deltalake").DeltaTable(str(local))
            for path in table.get_add_actions().column("path").to_pylist():
                relative_path(path)
                if not store.exists(root.join(*unquote(path).split("/"))):
                    raise InstallError(
                        f"protected Delta has a missing active data file: {path!r}"
                    )
