"""A run's runtime scope, wherever the imports for it have to happen.

One declared interface, two implementations, and the Runner is told neither:

.. code-block:: text

    this process is where the data is    → DirectRunScope, importing here
    a desktop reaching into Fabric       → FabricRunScope, naming one over there

Both answer the whole of :class:`RunScope`, so a caller dispatches without
asking which it holds. That is what keeps "one scope per logical run, closed at
the end of it" a single rule rather than a rule and an exception: a scope
outliving its run would let a rebuilt module go unnoticed until the Spark
session was replaced.

:class:`DirectRunScope` wraps a :class:`~weaver.runtime.python_context.RuntimeScope`
rather than extending it. A `RuntimeScope` owns imported-module state; dispatching
a primitive is a different job, and giving it one would point `weaver.runtime` at
`weaver.run`, which imports it.

The Fabric one holds nothing but a name. Everything it does is a statement
submitted through the Session, and its ``close()`` releases the far side's
imports by closing the scope that holds them.
"""

from __future__ import annotations

import uuid
from typing import Any, Protocol

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


class RunScope(Protocol):
    """What a run asks of the place its deployed modules are imported.

    Three methods, because three is what a run needs: dispatch a load primitive,
    dispatch a validation, and release the imports at the end.
    """

    def dispatch_python(
        self, node, *, expected_class: str, fault_tolerant: bool
    ) -> dict:
        """Run one deployed module and answer with the row it reported.

        A row rather than a load result, because that is the representation both
        positions produce without either of them depending on a load's
        vocabulary. What the row *means* is settled in
        :mod:`weaver.run.dispatch`, which is where a load primitive is reached.
        """

    def dispatch_validation(self, installed, *, collect: bool) -> Any: ...

    def close(self) -> None: ...


class DirectRunScope:
    """Imports that happen in this process, against the Spark it is running in.

    Holds the `RuntimeScope` the modules land in, and the Session and workspace
    a primitive is constructed against, so that dispatching takes the same
    arguments here as it does across a boundary.
    """

    def __init__(self, runtime_scope, session=None, workspace=None) -> None:
        self.runtime_scope = runtime_scope
        self._session = session
        self._workspace = workspace

    def dispatch_python(self, node, *, expected_class: str, fault_tolerant: bool):
        from .dispatch import python_primitive

        return python_primitive(
            node_id=node.node_id,
            logical_item=node.logical_id.item,
            physical_target=node.physical_target,
            schema=node.primitive_object.schema,
            object=node.primitive_object.object,
            expected_class=expected_class,
            fault_tolerant=fault_tolerant,
            runtime_scope=self.runtime_scope,
            session=self._session,
            workspace=self._workspace,
        ).as_row()

    def dispatch_validation(self, installed, *, collect: bool):
        from ..test_execution import run_installed_validation

        return run_installed_validation(
            installed,
            session=self._session,
            workspace=self._workspace,
            runtime_scope=self.runtime_scope,
            collect_diagnostics=collect,
        )

    def close(self) -> None:
        self.runtime_scope.close()


def open_runtime_scope(session, *, workspace=None) -> RunScope:
    """The scope this run's Python primitives will be imported into.

    Created lazily by the Runner, once, and closed when the run finishes,
    including when it fails, because a scope left open is a scope the next run
    would inherit.
    """

    from ..runtime.python_context import RuntimeScope
    from ..session.base import ACROSS_BOUNDARY

    if session is None:
        return DirectRunScope(RuntimeScope.new())

    # The Session says where it is; this decides what to build from that. A
    # Session that cannot place itself against a workspace answers ``UNPLACED``
    # and the imports happen here, because there is nothing to reach into. That
    # judgement belongs to the Session: reading it out of whatever
    # ``executes_here`` raised turned a bad configuration, or a Session someone
    # had closed, into a local RuntimeScope — and the run then imported
    # primitives into the console and reported success against an estate it had
    # never reached.
    if session.position(workspace) == ACROSS_BOUNDARY:
        return FabricRunScope.begin(session, workspace=workspace)
    return DirectRunScope(RuntimeScope.new(), session, workspace)


class FabricRunScope:
    """One run's imports, living in the Fabric session that can perform them.

    It holds a name and no modules: a context is a set of loaded module objects
    and those cannot cross a process boundary. Everything it does is a statement
    submitted through the Session.
    """

    def __init__(self, session, workspace, run_id: str) -> None:
        self._session = session
        self._workspace = workspace
        self.run_id = run_id
        self._closed = False

    @classmethod
    def begin(cls, session, *, workspace=None) -> "FabricRunScope":
        from ..runtime.session_scopes import open_scope

        run_id = uuid.uuid4().hex
        scope = cls(session, workspace, run_id)
        scope._submit(open_scope, {"run_id": run_id}, addressed=False)
        return scope

    # --- what dispatch asks of it -------------------------------------------

    def dispatch_python(self, node, *, expected_class: str, fault_tolerant: bool):
        """One deployed Python primitive, run in this scope.

        The node is flattened here rather than serialised. What crosses is the
        handful of strings the import and the construction use, so the far side
        never has to be kept in step with the Runner's own model.
        """

        from .entry import run_python_primitive

        return self._submit(
            run_python_primitive,
            {
                "run_id": self.run_id,
                "node_id": node.node_id,
                "item": str(node.logical_id.item),
                "target": node.physical_target.name,
                "schema": node.primitive_object.schema,
                "object": node.primitive_object.object,
                "expected_class": expected_class,
                "fault_tolerant": fault_tolerant,
            },
            detail=node.node_id,
        )

    def dispatch_validation(self, installed, *, collect: bool):
        """One installed Lakehouse validation, run in this scope."""

        from .entry import run_validation_primitive

        carried = self._submit(
            run_validation_primitive,
            {
                "run_id": self.run_id,
                "installed": installed.to_mapping(),
                "collect": collect,
            },
            detail=str(getattr(installed, "logical", "")) or None,
        )
        return _carried(carried, installed)

    def close(self) -> None:
        """Release the far side's imports. Never fails a run that has finished.

        Not failing is not the same as not noticing, and the two failures here
        mean opposite things:

        **The interpreter is gone** — a dead or killed Livy session. The scope
        went with it, which is the outcome closing it exists to reach, so there
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

        from ..runtime.session_scopes import close_scope

        if self._closed:
            return
        self._closed = True
        try:
            self._submit(close_scope, {"run_id": self.run_id}, addressed=False)
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

    def _submit(self, here, arguments: dict, *, addressed=True, detail=None):
        """One named function, spelled for both sides.

        The function itself is passed rather than looked up, so the import the
        submitted body carries is written from the thing this side would have
        called. The two halves cannot name different functions, and a rename is
        an ordinary rename.

        ``addressed`` says whether the call takes a workspace and a Session.
        Opening and closing a scope names one and touches no estate; the
        dispatchers need to know which workspace they are reaching into.
        """

        workspace = self._workspace
        name = here.__name__
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
            f"from {here.__module__} import {name}\n"
            f"workspace = {_workspace_literal(workspace)}\n"
            f"{preamble}"
            f"emit({name}({passed}))\n"
        )
        return self._session.execute_python(
            RemoteProgram(name=name, call=call, source=source, detail=detail),
            workspace=workspace,
        )


def _carried(payload, installed):
    """A remote validation's judgement, rebuilt as the value a run settles on."""

    from ..declaration.metadata import ASSUMPTION
    from ..runtime.validation_result import AssumptionResult, TestResult
    from ..test_execution import _WithDiagnostics

    shape = AssumptionResult if installed.kind == ASSUMPTION else TestResult
    return _WithDiagnostics(
        shape.from_mapping(payload["result"]), tuple(payload.get("diagnostics") or ())
    )


def _workspace_literal(workspace) -> str:
    if workspace is None:
        return "None"
    return (
        f"FabricWorkspace(workspace={workspace.workspace!r}, "
        f"weaver_lakehouse={workspace.weaver_lakehouse!r}, "
        f"environment={workspace.environment!r})"
    )


class LazyRunScope:
    """A run's scope, opened only if something actually imports.

    One object with an explicit lifecycle, so a Warehouse-only run opening no
    scope is a property of this class rather than an invariant maintained
    across two methods on the Runner. ``close()`` never opens, and is safe to
    call whether or not ``get()` ever was.
    """

    def __init__(self, open_scope) -> None:
        self._open = open_scope
        self._scope: RunScope | None = None

    def get(self) -> RunScope:
        if self._scope is None:
            self._scope = self._open()
        return self._scope

    @property
    def opened(self) -> bool:
        return self._scope is not None

    def close(self) -> None:
        scope, self._scope = self._scope, None
        if scope is not None:
            scope.close()


__all__ = [
    "DirectRunScope",
    "FabricRunScope",
    "LazyRunScope",
    "RunScope",
    "open_runtime_scope",
]
