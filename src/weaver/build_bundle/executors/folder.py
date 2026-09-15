"""Create or remove managed Lakehouse Files directories.

Build and drop require the expected prior state. Prune is tolerant of absence and
may remove either one folder object or an unmanaged schema directory.
"""

from __future__ import annotations

from typing import Any

from ...errors import InstallError
from ...targets import FolderTarget
from ..models import BUILD_FOLDER, DROP_FOLDER, PRUNE_FOLDER, InstallAction
from .base import InstallationContext


class FolderExecutor:
    name = "folder"

    def execute(
        self,
        action: InstallAction,
        payload: bytes | None,
        context: InstallationContext,
    ) -> dict[str, Any] | None:
        if action.resource_node_id is None:
            raise InstallError(f"folder action {action.id!r} names no resource")
        location = self._location(action.resource_node_id, context)
        if action.kind == BUILD_FOLDER:
            if context.store.exists(location):
                raise InstallError(f"managed folder already exists: {location.value}")
            # Folder build creates structure only; data arrives during load.
            context.store.make_directory(location)
            return {"created": location.value}
        if action.kind == DROP_FOLDER:
            if not context.store.exists(location):
                raise InstallError(f"managed folder does not exist: {location.value}")
            context.store.delete(location, recursive=True)
            return {"dropped": location.value}
        if action.kind == PRUNE_FOLDER:
            if context.store.exists(location):
                context.store.delete(location, recursive=True)
            return {"pruned": location.value}
        raise InstallError(
            f"folder action {action.id!r} has unknown kind {action.kind!r}"
        )

    def _location(self, node_id: str, context: InstallationContext):
        target = FolderTarget(lakehouse=context.target.lakehouse)
        if "/Files/" in node_id:
            # The batch supplies the physical binding; the item prefix is
            # identity only.
            qualified = node_id.split("/Files/", 1)[1]
        else:
            qualified = node_id.split(":", 1)[1]
        if "." in qualified:
            schema, name = qualified.split(".", 1)
            return context.resolver.folder_object(target, schema, name)
        # An unqualified prune names the whole unmanaged schema directory.
        return context.resolver.files_root(context.target.lakehouse).join(qualified)
