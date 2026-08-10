"""Writing and removing one file of a Lakehouse item's deployed runtime tree.

The load layer's file half, and it is deliberately one phase. A ``write_file``
action carries the exact bytes to put down: an authored Python module travels
verbatim, and a Spark SQL table's *generated* module is complete when it is
generated — it needs no built table, because a module reads its target's columns
when it runs rather than when it is written.

One thing still happens on the way down. A generated module carries its object
references as ``{{object:…}}`` tokens, because a bundle must produce the same
bytes wherever it is built; the installed file cannot, because it has to be
runnable by whoever opens it. So the tokens are resolved here, which is the
first moment the destination is known.

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
from ...spark import tokens
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
        """Finish a generated load, and address it, on the way down.

        The bundle stays destination-free, which is what lets one repository
        generate the same bytes everywhere and two bundles be diffed for what
        actually differs. The *installed file* cannot be: it has to be runnable
        by anyone who opens it, and by then the destination is known, so this is
        the moment the two requirements stop conflicting.

        Deployed Python is left exactly as authored. A module is source code, not
        a statement, and it addresses its target through the resolved Lakehouse
        it is constructed with — so only a *generated* module has anything to
        resolve.

        **Decided by what the payload is, not by what it is called.** An authored
        module and a generated one are both ``.py`` in one tree, so the name
        cannot tell them apart; a generated module says what it is on its first
        line, which is a fact about the file. (Keying on a name was tried in the
        design this replaced, and shipped every installed program with its tokens
        intact.)
        """

        if not path.endswith(MODULE_EXTENSION):
            return payload
        text = payload.decode("utf-8")
        if not text.lstrip().startswith(GENERATED_MODULE_MARKER):
            return payload
        destination = context.target.destination
        if destination is None:
            raise InstallError(
                f"a generated load lands in {context.target.bound.id!r}, which "
                "resolved to no Spark destination, so its object names cannot be "
                "addressed"
            )
        # Resolved against the destination directly rather than through the
        # catalogue: writing a file needs no Spark session, and asking for one
        # would make installing a load depend on a capability it never uses.
        return tokens.expand(text, destination).encode("utf-8")

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
