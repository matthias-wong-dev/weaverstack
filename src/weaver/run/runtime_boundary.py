"""Hold deployed Python imports for exactly one run.

Closing each scope prevents a later run from reusing modules replaced by a
rebuild.
"""

from __future__ import annotations

import uuid
from typing import Any, Protocol

from ..sessions.program import RemoteProgram

#: Livy states and resource wording that mean the interpreter released the scope.
_INTERPRETER_GONE = ("dead", "killed", "shutting_down", "error", "not usable")


def _interpreter_is_gone(exc: BaseException) -> bool:
    """Return whether a dead interpreter already released the scope.

    Only Livy state lacks a typed signal and requires message matching.
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
    """A run-scoped importer and dispatcher for deployed modules."""

    def dispatch_python(
        self, node, *, expected_class: str, fault_tolerant: bool, reload: bool = False
    ) -> dict:
        """Run one deployed module and return its transport-neutral row."""

    def dispatch_validation(self, installed, *, collect: bool) -> Any: ...

    def close(self) -> None: ...


class DirectRunScope:
    """Dispatch through a runtime scope in this process."""

    def __init__(
        self, runtime_scope, session=None, workspace=None, *, catalogue=None
    ) -> None:
        self.runtime_scope = runtime_scope
        self._session = session
        self._workspace = workspace
        self._catalogue = catalogue

    def dispatch_python(
        self, node, *, expected_class: str, fault_tolerant: bool, reload: bool = False
    ):
        from .dispatch import python_primitive

        return python_primitive(
            node_id=node.node_id,
            logical_item=node.logical_id.item,
            physical_target=node.physical_target,
            schema=node.primitive_object.schema,
            object=node.primitive_object.object,
            expected_class=expected_class,
            fault_tolerant=fault_tolerant,
            reload=reload,
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
    """Open the scope that will import this run's Python primitives."""

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
    """Use the catalogue's canonical boundary representation."""

    return None if catalogue is None else catalogue.to_mapping()


class FabricRunScope:
    """A named import scope held by a Fabric session."""

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

    def dispatch_python(
        self, node, *, expected_class: str, fault_tolerant: bool, reload: bool = False
    ):
        """Dispatch a flattened node without serialising the Runner model."""

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
                "reload": reload,
                "identity": str(node.logical_id) if node.logical_id else None,
            },
            detail=node.node_id,
        )

    def dispatch_validation(self, installed, *, collect: bool):

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
        """Release imports without changing the outcome of a finished run.

        Ignore a dead interpreter; warn when a live session may retain modules.
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

        telemetry = getattr(self._session, "telemetry", None)
        if telemetry is not None:
            telemetry.count("run.scope_not_released")
        warn = getattr(self._session, "warn", None)
        if warn is not None:
            warn(
                f"Run {self.run_id} left imported modules in the Fabric session: "
                f"{type(exc).__name__}: {exc}. Restart the session before running "
                "rebuilt primitives."
            )

    # --- the crossing --------------------------------------------------------

    def _submit(self, here, arguments: dict, *, addressed=True, detail=None):
        """Submit the exact function used locally.

        ``addressed`` adds a Workspace and Session only for estate operations.
        """

        workspace = self._workspace
        name = here.__name__
        if addressed:
            # The Session is built in the submitted body, around the
            # interpreter's own ``spark`` global, the construction every other
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
    environment = None if workspace.environment is None else str(workspace.environment)
    return (
        f"Workspace(workspace={workspace.workspace!r}, "
        f"catalogue={workspace.catalogue!r}, "
        f"environment={environment!r})"
    )


class LazyRunScope:
    """A lazy scope whose ``close()`` never opens it."""

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
