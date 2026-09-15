"""Write and remove files in a Lakehouse item's deployed runtime tree.

Write payloads are already complete and target-specific. Deletes do not remove
empty parents; folder pruning owns removal of the declared runtime tree.
"""

from __future__ import annotations

from typing import Any

from ...errors import InstallError
from ...targets import FolderTarget
from ..models import DELETE_FILE, WRITE_FILE, InstallAction
from .base import InstallationContext


class LoadFileExecutor:
    name = "load_file"

    def execute(
        self,
        action: InstallAction,
        payload: bytes | None,
        context: InstallationContext,
    ) -> dict[str, Any] | None:
        if action.resource_node_id is None:
            raise InstallError(f"load file action {action.id!r} names no resource")
        location = self._location(action.resource_node_id, context)
        if action.kind == WRITE_FILE:
            if payload is None:
                raise InstallError(f"load file action {action.id!r} has no payload")
            # The payload is the frozen authored or generated artefact; installation
            # performs no rendering.
            context.store.write(location, payload)
            return {"written": location.value, "bytes": len(payload)}
        if action.kind == DELETE_FILE:
            # Deletion reconciles toward absence, so prior removal is success.
            if context.store.exists(location):
                context.store.delete(location)
                return {"deleted": location.value}
            return {"absent": location.value}
        raise InstallError(
            f"load file action {action.id!r} has unknown kind {action.kind!r}"
        )

    def _location(self, node_id: str, context: InstallationContext):
        """Resolve the path below ``Files`` using the batch's physical target."""

        marker = "/file:"
        if marker not in node_id:
            raise InstallError(
                f"load file action names {node_id!r}, which is not a file identity"
            )
        relative = node_id.split(marker, 1)[1]
        target = FolderTarget(lakehouse=context.target.lakehouse)
        return context.resolver.folder_root(target).join(*relative.split("/"))
