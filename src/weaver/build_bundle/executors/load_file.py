"""Writing and removing one file of a Lakehouse item's deployed runtime tree.

A ``write_file`` action carries the exact bytes to put down: an authored module
travels verbatim, and a generated one is complete when it is generated, because
a module reads its target's columns when it runs rather than when it is written.

Nothing happens on the way down. A generated module already names the Lakehouse
it reads, because the build knew the target when it rendered the module, so what
is written is what was frozen.

.. code-block:: text

    authored module     written exactly as it was authored
    generated module    object tokens resolved, then written

Installing a load therefore needs only the store. Nothing here needs Spark.

A ``delete_file`` action removes a file whose source has stopped claiming it.

Both derive their location the same way every other executor does: from the
action's resource id and the target the batch names. The identity says where the
file goes (``_/Load/lib/dates.py`` beneath ``Files``) and the bound target says
which Lakehouse, so nothing here decides placement — that was settled when the
artefact was claimed.

Directories are the store's business on the way down, and nobody's on the way
back up. The tree is owned by a declared folder, so when the last artefact goes
the folder stops being projected and ordinary folder prune removes the whole
subtree — an executor walking upward deleting empty parents would be a second,
quieter answer to a question already answered.
"""

from __future__ import annotations

from typing import Any

from ...declaration.load import SPARK_LOAD_EXTENSION as MODULE_EXTENSION
from ...declaration.spark_sql_module import GENERATED_MODULE_MARKER
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
            context.store.write(location, payload)
            return {"written": location.value, "bytes": len(payload)}
        if action.kind == DELETE_FILE:
            # Tolerant of absence, and only here. A delete is reconciliation
            # toward "this must not exist", and something else having already
            # removed it is that state reached — unlike a create, where a
            # collision means two things believe they own one name.
            if context.store.exists(location):
                context.store.delete(location)
                return {"deleted": location.value}
            return {"absent": location.value}
        raise InstallError(
            f"load file action {action.id!r} has unknown kind {action.kind!r}"
        )


    def _location(self, node_id: str, context: InstallationContext):
        """``Lakehouse/Sales/file:_/Load/lib/dates.py`` under this batch's target.

        The logical item prefix is identity only: the batch already carries the
        physical binding, so the path beneath ``Files`` is what is used and the
        item it names is not reinterpreted here.
        """

        marker = "/file:"
        if marker not in node_id:
            raise InstallError(
                f"load file action names {node_id!r}, which is not a file identity"
            )
        relative = node_id.split(marker, 1)[1]
        target = FolderTarget(lakehouse=context.target.lakehouse)
        return context.resolver.folder_root(target).join(*relative.split("/"))
