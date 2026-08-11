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


#: How a dead interpreter announces itself. The Livy states Weaver already
#: treats as "this session is finished" (``LivySession.active``), plus the
#: resource layer's own word for a capability it could not hand over.
_INTERPRETER_GONE = ("dead", "killed", "shutting_down", "error", "not usable")


def _interpreter_is_gone(exc: BaseException) -> bool:
    """Whether this failure means the scope was released by the session dying.

    Matched on the error type first, because that is the structural answer, and
    on the message only for :class:`~weaver.fabric.livy.LivyError`, which
    reports the session's state as text. A cleanup failure that is *not* one of
    these is a defect worth hearing about, so the default is False.
    """

    from ..fabric.livy import LivyError
    from ..session.resources import ResourceError

    if isinstance(exc, ResourceError):
        return True
    if isinstance(exc, LivyError):
        message = str(exc).casefold()
        return any(state in message for state in _INTERPRETER_GONE)
    return False


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

    # A Session with no workspace to place itself against has no remote to reach
    # into either, so the imports happen here. That is the *only* answer worth
    # guessing at, and it is asked as its own question rather than inferred from
    # whatever executes_here raises: catching every CommandError from that call
    # turned a bad configuration or a closed Session into a local RuntimeScope,
    # so a run that should have stopped instead started importing primitives in
    # the console — reporting success against nothing the caller had named.
    placed = getattr(session, "workspace_or_default", None)
    if placed is not None:
        try:
            placed(workspace)
        except CommandError:
            return RuntimeScope.new()

    if session.executes_here(workspace):
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

        Not failing is not the same as not noticing, and the two failures here
        mean opposite things:

        **The interpreter is gone** — a dead or killed Livy session. The scope
        went with it, which is the outcome ``end_run`` exists to reach, so there
        is nothing to report and nothing to fix. Silence is right.

        **The interpreter is alive and the call failed** — a serialisation
        problem, a signature that has drifted, a name that is not there in the
        published wheel. That is a defect in this crossing, and the scope it
        meant to release is *still open in a live session*, where the next run
        will inherit the modules a rebuild has replaced. Swallowed, it shows up
        later as a stale primitive nobody can explain.

        So the second is warned about and counted, and the run still succeeds:
        it produced its result, and a completed run must not be retracted by
        its own cleanup.
        """

        if self._closed:
            return
        self._closed = True
        try:
            self._submit("end_run", {"run_id": self.run_id}, addressed=False)
        except Exception as exc:  # noqa: BLE001 - never fails a finished run
            if _interpreter_is_gone(exc):
                return
            self._report_leak(exc)

    def _report_leak(self, exc: BaseException) -> None:
        """Say that a live session is still holding a scope, and count it."""

        telemetry = getattr(self._session, "telemetry", None)
        if telemetry is not None:
            telemetry.count("run.scope_not_released")
        warn = getattr(self._session, "warn", None)
        if warn is not None:
            warn(
                f"the runtime scope for run {self.run_id} was not released: "
                f"{type(exc).__name__}: {exc}. The Fabric session is still up, so "
                "it still holds this run's imported modules — restart it if a "
                "rebuilt primitive appears not to have taken effect."
            )

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
