"""Folder execution: strictly create/drop a managed folder, or prune one.

Building a Folder is creating its directory in the Lakehouse Files area; there is
no data (staging files into it is load). Pruning one is removing a directory
the build already decided, at freeze time, is unmanaged. Both resolve their path
from the action's resource id and the bound target and touch no catalog:

- ``build_folder``  make ``Files/<schema>/<object>`` and fail on collision;
- ``drop_folder``   remove a selected managed object and fail if absent;
- ``prune_folder``  remove ``Files/<schema>/<object>`` (an object), or
  ``Files/<schema>`` when the whole schema is unmanaged (resource ``folder:<schema>``).
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
                raise InstallError(
                    f"cannot create managed folder because it already exists: "
                    f"{location.value}"
                )
            context.store.make_directory(location)
            return {"created": location.value}
        if action.kind == DROP_FOLDER:
            if not context.store.exists(location):
                raise InstallError(
                    f"cannot drop managed folder because it does not exist: "
                    f"{location.value}"
                )
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
            # Item-oriented canonical identity. The batch already carries the
            # physical Lakehouse binding, so the logical item prefix is only
            # identity and is not reinterpreted here.
            qualified = node_id.split("/Files/", 1)[1]
        else:
            qualified = node_id.split(":", 1)[1]
        if "." in qualified:  # a specific folder object
            schema, name = qualified.split(".", 1)
            return context.resolver.folder_object(target, schema, name)
        # a whole unmanaged folder schema: the schema directory itself
        return context.resolver.files_root(context.target.lakehouse).join(qualified)
