"""Writing and removing one file of a Lakehouse item's deployed runtime tree.

The load layer's file half. A ``write_file`` action carries the exact bytes to
put down — a deployed Python module, or a generated Spark SQL statement — and a
``delete_file`` action removes one whose source has stopped claiming it.

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

from ...declaration.load import SPARK_LOAD_EXTENSION
from ...errors import InstallError
from ...targets import FolderTarget
from ..models import DELETE_FILE, WRITE_FILE, BuildAction
from .base import InstallationContext


class LoadFileExecutor:
    name = "load_file"

    def execute(
        self,
        action: BuildAction,
        payload: bytes | None,
        context: InstallationContext,
    ) -> dict[str, Any] | None:
        if action.resource_node_id is None:
            raise InstallError(f"load file action {action.id!r} names no resource")
        location = self._location(action.resource_node_id, context)
        if action.kind == WRITE_FILE:
            if payload is None:
                raise InstallError(f"load file action {action.id!r} has no payload")
            payload = self._addressed(location.value, payload, context)
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

    def _addressed(
        self, path: str, payload: bytes, context: InstallationContext
    ) -> bytes:
        """Resolve a generated Spark SQL program's object tokens as it lands.

        The bundle stays destination-free — that is what lets one repository
        generate the same bytes everywhere and two bundles be diffed for what
        actually differs. The *installed file* cannot be: it has to be runnable
        by anyone who opens it, and by then the destination is known, so this is
        the moment the two requirements stop conflicting.

        Deployed Python is left exactly as authored. A module is source code, not
        a statement, and it addresses its target through the resolved Lakehouse
        it is constructed with.
        """

        if not path.endswith(SPARK_LOAD_EXTENSION):
            return payload
        text = payload.decode("utf-8")
        return context.catalogue.expand(text).encode("utf-8")

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
