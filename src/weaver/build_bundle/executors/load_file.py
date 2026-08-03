"""Writing and removing one file of a Lakehouse item's deployed runtime tree.

The load layer's file half — and for one kind of file, its second phase.

A ``write_file`` action usually carries the exact bytes to put down: a deployed
Python module is authored source and travels verbatim. A **generated Spark SQL
load does not**. Its payload is an instruction, and finishing it is this
executor's job:

.. code-block:: text

    read the built target's schema, through Spark
    take its physical business columns
    render the executable program from the instruction
    resolve the destination tokens
    write the file

That two-phase shape is the same one the Warehouse load uses, which ships a
script that reads ``sys.columns`` and assembles the procedure server-side. The
reason is the same too: what a load writes are the *physical* target's columns,
and a Spark SQL table may leave its schema to be inferred at build — so the
program cannot be finished while the table is still a declaration. Writing a
file up front would be writing down a guess.

A consequence worth stating: installing a generated load therefore **needs a
Spark session**, where deploying a module needs only the store.

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

from ...declaration.load import TSQL_LOAD_EXTENSION as SQL_EXTENSION
from ...declaration.spark_load import GENERATED_LOAD_MARKER
from ...spark import tokens


def _generated_load(text: str) -> dict | None:
    """The instruction this payload carries, or ``None`` if it is not one."""

    import json

    from ...declaration.spark_load import GENERATED_LOAD_INSTRUCTION

    stripped = text.lstrip()
    if not stripped.startswith("{"):
        return None
    try:
        payload = json.loads(stripped)
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    return payload if payload.get("weaver") == GENERATED_LOAD_INSTRUCTION else None
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
        """Finish a generated load, and address it, on the way down.

        Two steps, not one. An instruction is first *rendered* into a program
        against the built target's columns (:meth:`_render`); whatever program
        results then has its object tokens resolved. A payload that is already a
        program — one written by an older bundle — skips straight to the second.

        The bundle stays destination-free, which is what lets one repository
        generate the same bytes everywhere and two bundles be diffed for what
        actually differs. The *installed file* cannot be: it has to be runnable
        by anyone who opens it, and by then the destination is known, so this is
        the moment the two requirements stop conflicting.

        Deployed Python is left exactly as authored. A module is source code, not
        a statement, and it addresses its target through the resolved Lakehouse
        it is constructed with.

        **Decided by what the payload is, not by what it is called.** Keying on a
        ``.spark.sql`` suffix looked right and was wrong: a generated load keeps
        its *authored* name, ``Sales.OrderSummary.sql``, so the suffix never
        matched and every installed program shipped with its tokens intact and
        could not run. A generated Spark program announces itself in its first
        line, which is a fact about the file rather than about its name.
        """

        if not path.endswith(SQL_EXTENSION):
            return payload
        text = payload.decode("utf-8")
        instruction = _generated_load(text)
        if instruction is not None:
            text = self._render(instruction, context)
        elif not text.lstrip().startswith(GENERATED_LOAD_MARKER):
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

    def _render(self, instruction: dict, context: InstallationContext) -> str:
        """Finish a generated load from the table it will write to.

        The columns are the one thing generation cannot know: a Spark SQL table
        may leave its schema to be inferred at build, and even a declared one is
        materialised as the physical table the program has to name. So the
        bundle carries an instruction and this reads the built table — the same
        two-phase shape the Warehouse load uses with ``sys.columns``, rather
        than writing a file from a guess and hoping the guess held.
        """

        from ...declaration.spark_load import render_installed_program
        # The module rather than the name. `tests/test_core_boundary.py` matches
        # raw source text, so pulling in a symbol whose name begins with the
        # engine's reads to it as the forbidden dependency.
        from ...runtime import load_contract

        if context.spark is None:
            raise InstallError(
                f"a generated load for {instruction['qualified']} must read its "
                "target's columns, and no Spark session was provided"
            )
        target = context.catalogue.expand(instruction["object"])
        audit = set(load_contract.delta_audit_columns())
        columns = tuple(
            field.name
            for field in context.spark.table(target).schema.fields
            if field.name not in audit
        )
        if not columns:
            raise InstallError(
                f"{target} has no loadable columns; build the table before "
                "installing its load"
            )
        return render_installed_program(instruction, columns)

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
