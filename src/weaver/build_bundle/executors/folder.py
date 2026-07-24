"""Folder execution — create a managed folder's directory.

Building a Folder is creating its directory in the Lakehouse Files area; there is
no data (staging files into it is *load*). The action carries no payload — the
executor resolves the folder's location from the resource id and the bound
target, and makes the directory.
"""

from __future__ import annotations

from typing import Any

from ...errors import InstallError
from ...targets import FolderTarget
from ..models import BuildAction
from .base import InstallationContext


class FolderExecutor:
    name = "folder"

    def execute(
        self,
        action: BuildAction,
        payload: bytes | None,
        context: InstallationContext,
    ) -> dict[str, Any] | None:
        if action.resource_node_id is None:
            raise InstallError(f"folder action {action.id!r} names no resource")
        schema, name = _schema_object(action.resource_node_id)
        location = context.resolver.folder_object(
            FolderTarget(lakehouse=context.target.lakehouse), schema, name
        )
        context.store.make_directory(location)
        return {"folder": location.value}


def _schema_object(node_id: str) -> tuple[str, str]:
    qualified = node_id.split(":", 1)[1]
    schema, name = qualified.split(".", 1)
    return schema, name
