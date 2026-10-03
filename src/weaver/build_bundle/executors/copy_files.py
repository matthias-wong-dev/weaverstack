"""Copy a frozen list of files from one Lakehouse's Files area into another's."""

from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from contextvars import copy_context
from typing import Any

from ...errors import InstallError
from ...targets import FolderTarget
from .base import InstallationContext

#: Copies are independent reads and writes, so several run at once.
COPIES = 16


class CopyFilesExecutor:
    name = "copy_files"

    def execute(self, action, payload, context: InstallationContext) -> dict[str, Any]:
        if payload is None:
            raise InstallError(f"copy action {action.id!r} has no payload")
        manifest = json.loads(payload.decode("utf-8"))
        source = context.resolved(manifest["source_target_id"])
        parts = manifest["path"].split("/")
        source_root = context.resolver.folder_root(
            FolderTarget(lakehouse=source.lakehouse)
        ).join(*parts)
        destination_root = context.resolver.folder_root(
            FolderTarget(lakehouse=context.target.lakehouse)
        ).join(*parts)
        files = list(manifest["files"])

        def copy(relative):
            components = relative.split("/")
            context.store.write(
                destination_root.join(*components),
                context.store.read(source_root.join(*components)),
            )

        if files:
            with ThreadPoolExecutor(max_workers=min(COPIES, len(files))) as pool:
                for done in [
                    pool.submit(copy_context().run, copy, each) for each in files
                ]:
                    done.result()
        return {"copied": len(files)}
