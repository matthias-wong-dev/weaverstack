"""Where a run's deployed Python primitives are imported.

One interface, :class:`RunScope`, and two implementations: :class:`DirectRunScope`
imports in this process, :class:`FabricRunScope` names a scope in a Fabric
session and submits to it. The Runner is told neither.

A scope belongs to one run and is closed with it. One that outlived its run
would let the next run import modules a rebuild had replaced.
"""

from __future__ import annotations

import uuid
from typing import Any, Protocol

from ..sessions.program import RemoteProgram

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
    from ..sessions.resources import ResourceError

    if isinstance(exc, ResourceError):
        return True
    if isinstance(exc, LivyError):
        message = str(exc).casefold()
        return any(state in message for state in _INTERPRETER_GONE)
    return False


class RunScope(Protocol):
    """Where a run's deployed modules are imported, and how it dispatches into them."""

    def dispatch_python(
        self, node, *, expected_class: str, fault_tolerant: bool
    ) -> dict:
        """Run one deployed module and answer with the row it reported.

        A row rather than a load result: it is what both positions produce
        without depending on a load's vocabulary. :mod:`weaver.run.dispatch`
        settles what it means.
        """

    def dispatch_validation(self, installed, *, collect: bool) -> Any: ...

    def close(self) -> None: ...


class DirectRunScope:
    """Imports in this process, against the Spark it is running in.

    Wraps a :class:`~weaver.runtime.python_context.RuntimeScope` rather than
    extending it: giving `runtime` a dispatch method would point it at `run`,
    which imports it.
    """

    def __init__(
        self, runtime_scope, session=None, workspace=None, *, catalogue=None
    ) -> None:
        self.runtime_scope = runtime_scope
        self._session = session
        self._workspace = workspace
        self._catalogue = catalogue

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
            catalogue=self._catalogue,
            node_identity=node.logical_id,
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


def open_runtime_scope(session, *, workspace=None, catalogue=None) -> RunScope:
    """The scope this run's Python primitives will be imported into.

    The run's catalogue travels with it, because it has the same lifetime and
    crosses for the same reason: read once for the run, and read from there by
    every object the run dispatches.
    """

    from ..runtime.python_context import RuntimeScope
    from ..sessions.base import ACROSS_BOUNDARY

    if session is None:
        return DirectRunScope(RuntimeScope.new(), catalogue=catalogue)

    # An unplaced Session has nothing to reach into, so the imports happen here.
    # That judgement is the Session's: inferring it from an error would turn a
    # bad configuration into a local scope, and the run would report success
    # against an estate it never reached.
    if session.position(workspace) == ACROSS_BOUNDARY:
        return FabricRunScope.begin(session, workspace=workspace, catalogue=catalogue)
    return DirectRunScope(RuntimeScope.new(), session, workspace, catalogue=catalogue)


def _as_data(catalogue) -> dict | None:
    """One run's catalogue as a submitted program can carry it.

    The catalogue's own serialisation, because a catalogue crossing a boundary is
    a solved problem and a second representation would be one more thing to keep
    in step.
    """

    return None if catalogue is None else catalogue.to_mapping()


class FabricRunScope:
    """One run's imports, in the Fabric session that can perform them.

    Holds a name and no modules — loaded module objects cannot cross a process
    boundary — so everything it does is a program submitted through the Session.
    """

    def __init__(self, session, workspace, run_id: str) -> None:
        self._session = session
        self._workspace = workspace
        self.run_id = run_id
        self._closed = False

    @classmethod
    def begin(cls, session, *, workspace=None, catalogue=None) -> "FabricRunScope":
        from ..runtime.session_scopes import open_scope

        run_id = uuid.uuid4().hex
        scope = cls(session, workspace, run_id)
        # The bookmarks cross once, with the scope that will outlive every node.
        # As text, because what crosses is a submitted program's arguments.
        scope._submit(
            open_scope,
            {"run_id": run_id, "catalogue": _as_data(catalogue)},
            addressed=False,
        )
        return scope

    # --- what dispatch asks of it -------------------------------------------

    def dispatch_python(self, node, *, expected_class: str, fault_tolerant: bool):
        """One deployed Python primitive, run in this scope.

        The node is flattened rather than serialised, so the far side never has
        to be kept in step with the Runner's model.
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
                "identity": str(node.logical_id) if node.logical_id else None,
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

        A dead interpreter took the scope with it, so there is nothing to
        report. Any other failure leaves a scope open in a live session, where
        the next run would inherit stale modules — so it is warned about and
        counted, and the run still succeeds.
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
        """Call one named function, on this side or the far one.

        The function is passed rather than looked up by name, so the submitted
        import is written from the thing this side would have called and the two
        halves cannot drift apart.

        ``addressed`` says whether the call takes a workspace and a Session:
        opening and closing a scope touches no estate, dispatching does.
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
                "from weaver.sessions import NotebookSession\n"
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
            "from weaver.workspaces import Workspace\n"
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
        f"Workspace(workspace={workspace.workspace!r}, "
        f"catalogue={workspace.catalogue!r}, "
        f"environment={workspace.environment!r})"
    )


class LazyRunScope:
    """A run's scope, opened only when something imports.

    ``close()`` never opens, so a Warehouse-only run opens no scope at all.
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
