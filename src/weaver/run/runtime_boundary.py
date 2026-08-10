"""A run's runtime scope, wherever the imports for it have to happen.

One concept, two implementations, and the Runner is told neither:

.. code-block:: text

    this process is where the data is    → RuntimeScope, in memory
    a desktop reaching into Fabric       → RemoteScope, naming one over there

Both answer ``close()``, and both are what ``dispatch`` asks to run a Python
primitive. That is what keeps "one scope per logical run, closed at the end of
it" a single rule rather than a rule and an exception — the guarantee the
decomposition most had to preserve, because a scope outliving its run would let
a rebuilt module go unnoticed until the Spark session was replaced.

The remote one holds nothing but a name. Everything it does is a statement
submitted through the Session, and its ``close()`` is the ``end_run`` that
releases the far side's imports.
"""

from __future__ import annotations

import uuid

from ..session.program import RemoteProgram


def open_runtime_scope(session, *, workspace=None):
    """The scope this run's Python primitives will be imported into.

    Created lazily by the Runner, once, and closed when the run finishes —
    including when it fails, because a scope left open is a scope the next run
    would inherit.
    """

    from ..errors import CommandError
    from ..runtime.python_context import RuntimeScope

    if session is None:
        return RuntimeScope.new()
    try:
        here = session.executes_here(workspace)
    except CommandError:
        # A Session with no workspace to place itself against has no remote to
        # reach into either, so the imports happen in this process. Positive
        # knowledge is required to go the other way: a scope opened remotely by
        # mistake would run the primitive somewhere the caller never named.
        here = True
    if here:
        return RuntimeScope.new()
    return RemoteScope.begin(session, workspace=workspace)


class RemoteScope:
    """One run's imports, living in the Fabric session that can perform them.

    Not a :class:`~weaver.runtime.python_context.RuntimeScope` and deliberately
    not pretending to be one: it has no ``context_for``, because a context is a
    set of loaded modules and those cannot cross a process boundary. What it has
    is the two things a caller on this side can actually use — dispatch, and
    close.
    """

    def __init__(self, session, workspace, run_id: str) -> None:
        self._session = session
        self._workspace = workspace
        self.run_id = run_id
        self._closed = False

    @classmethod
    def begin(cls, session, *, workspace=None) -> "RemoteScope":
        run_id = uuid.uuid4().hex
        scope = cls(session, workspace, run_id)
        scope._submit("begin_run", {"run_id": run_id}, addressed=False)
        return scope

    # --- what dispatch asks of it -------------------------------------------

    def dispatch_python(self, **arguments) -> dict:
        """One deployed Python primitive, run in this scope."""

        return self._submit(
            "dispatch_python",
            {"run_id": self.run_id, **arguments},
            detail=arguments.get("node_id"),
        )

    def dispatch_validation(self, *, installed: dict, collect: bool) -> dict:
        """One installed Lakehouse validation, run in this scope."""

        return self._submit(
            "dispatch_validation",
            {"run_id": self.run_id, "installed": installed, "collect": collect},
            detail=installed.get("logical"),
        )

    def close(self) -> None:
        """Release the far side's imports. Never fails a run that has finished.

        A run that has produced its result must not then fail because the
        cleanup call did — and if the Livy session is already gone, so is the
        scope, which is the outcome ``end_run`` exists to reach.
        """

        if self._closed:
            return
        self._closed = True
        try:
            self._submit("end_run", {"run_id": self.run_id}, addressed=False)
        except Exception:  # noqa: BLE001 - the scope dies with the interpreter
            pass

    # --- the crossing --------------------------------------------------------

    def _submit(self, name: str, arguments: dict, *, addressed=True, detail=None):
        """One call to :mod:`weaver.run.remote`, spelled for both sides.

        ``addressed`` says whether the call takes a workspace and a Session.
        ``begin_run`` and ``end_run`` name a scope and touch no estate; the
        dispatchers need to know which workspace they are reaching into.
        """

        from . import remote

        workspace = self._workspace
        here = getattr(remote, name)
        if addressed:
            # The Session is built in the submitted body, around the
            # interpreter's own ``spark`` global — the construction every other
            # crossing performs. One built inside the call would have to go
            # looking for an active Spark session rather than being handed the
            # one the statement is running in.
            preamble = (
                "from weaver.session import NotebookSession\n"
                "session = NotebookSession(workspace=workspace, spark=spark)\n"
            )
            passed = f"session=session, workspace=workspace, **{arguments!r}"

            def call():
                return here(session=self._session, workspace=workspace, **arguments)

        else:
            preamble = ""
            passed = f"**{arguments!r}"

            def call():
                return here(**arguments)

        source = (
            "from weaver.workspaces import FabricWorkspace\n"
            f"from weaver.run.remote import {name}\n"
            f"workspace = {_workspace_literal(workspace)}\n"
            f"{preamble}"
            f"emit({name}({passed}))\n"
        )
        return self._session.execute_python(
            RemoteProgram(name=name, call=call, source=source, detail=detail),
            workspace=workspace,
        )


def _workspace_literal(workspace) -> str:
    if workspace is None:
        return "None"
    return (
        f"FabricWorkspace(workspace={workspace.workspace!r}, "
        f"weaver_lakehouse={workspace.weaver_lakehouse!r}, "
        f"environment={workspace.environment!r})"
    )


__all__ = ["RemoteScope", "open_runtime_scope"]
