"""Folder execution — create a managed folder, or remove a pruned one.

Building a Folder is creating its directory in the Lakehouse Files area; there is
no data (staging files into it is *load*). Pruning one is removing a directory
the build already decided, at freeze time, is unmanaged. Both resolve their path
from the action's resource id and the bound target and touch no catalog:

- ``build_folder``  make ``Files/<schema>/<object>``;
- ``prune_folder``  remove ``Files/<schema>/<object>`` (an object), or
  ``Files/<schema>`` when the whole schema is unmanaged (resource ``folder:<schema>``).
"""

from __future__ import annotations

from typing import Any

from ...errors import InstallError
from ...targets import FolderTarget
from ..models import BUILD_FOLDER, PRUNE_FOLDER, BuildAction
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
        location = self._location(action.resource_node_id, context)
        if action.kind == BUILD_FOLDER:
            context.store.make_directory(location)
            return {"created": location.value}
        if action.kind == PRUNE_FOLDER:
            if context.store.exists(location):
                context.store.delete(location, recursive=True)
            return {"pruned": location.value}
        raise InstallError(f"folder action {action.id!r} has unknown kind {action.kind!r}")

    def _location(self, node_id: str, context: InstallationContext):
        target = FolderTarget(lakehouse=context.target.lakehouse)
        qualified = node_id.split(":", 1)[1]
        if "." in qualified:  # a specific folder object
            schema, name = qualified.split(".", 1)
            return context.resolver.folder_object(target, schema, name)
        # a whole unmanaged folder schema: the schema directory itself
        return context.resolver.files_root(context.target.lakehouse).join(qualified)
