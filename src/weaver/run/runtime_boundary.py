"""Hold deployed Python imports for exactly one run.

Deployed modules are imported where Spark is, so a run that executes one runs
in Fabric: a client sends such a run there whole. Closing each scope prevents a
later run from reusing modules replaced by a rebuild.
"""

from __future__ import annotations

import threading
from typing import Any, Protocol


class RunScope(Protocol):
    """A run-scoped importer and dispatcher for deployed modules.

    ``isolated`` runs a module in a Spark session of its own, for one that runs
    beside others.
    """

    def dispatch_python(
        self,
        node,
        *,
        expected_class: str,
        fault_tolerant: bool,
        reload: bool = False,
        ignore_stability_threshold: bool = False,
        isolated: bool = False,
    ) -> dict:
        """Run one deployed module and return its load result as a row."""

    def dispatch_validation(
        self, installed, *, collect: bool, isolated: bool = False
    ) -> Any: ...

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
        self._inherited: dict | None = None
        self._inheriting = threading.Lock()

    def _spark(self, isolated: bool):
        from .dispatch import inherited_settings, isolated_spark

        if not isolated or self._session is None:
            return None
        parent = self._session.spark(self._workspace)
        with self._inheriting:
            if self._inherited is None:
                self._inherited = inherited_settings(parent)
        return isolated_spark(parent, self._inherited)

    def dispatch_python(
        self,
        node,
        *,
        expected_class: str,
        fault_tolerant: bool,
        reload: bool = False,
        ignore_stability_threshold: bool = False,
        isolated: bool = False,
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
            ignore_stability_threshold=ignore_stability_threshold,
            runtime_scope=self.runtime_scope,
            session=self._session,
            workspace=self._workspace,
            catalogue=self._catalogue,
            node_identity=node.logical_id,
            spark=self._spark(isolated),
        ).as_row()

    def dispatch_validation(self, installed, *, collect: bool, isolated: bool = False):
        from ..test_execution import run_installed_validation

        return run_installed_validation(
            installed,
            session=self._session,
            workspace=self._workspace,
            runtime_scope=self.runtime_scope,
            collect_diagnostics=collect,
            spark=self._spark(isolated),
        )

    def close(self) -> None:
        self.runtime_scope.close()


def open_runtime_scope(session, *, workspace=None, catalogue=None) -> RunScope:
    """Open the scope that will import this run's Python primitives."""

    from ..runtime.python_context import RuntimeScope
    from ..sessions.base import CLIENT
    from .result import RunError

    if session is None:
        return DirectRunScope(RuntimeScope.new(), catalogue=catalogue)
    # An unplaced Session has nothing to reach into, so the imports happen here.
    # That judgement is the Session's: inferring it from an error would turn a
    # bad configuration into a local scope, and the run would report success
    # against an estate it never reached.
    if session.position(workspace) == CLIENT:
        raise RunError(
            "A client imports no deployed module. Run the load or test through "
            "the Session, which sends it to Fabric."
        )
    return DirectRunScope(RuntimeScope.new(), session, workspace, catalogue=catalogue)


class LazyRunScope:
    """A lazy scope whose ``close()`` never opens it."""

    def __init__(self, open_scope) -> None:
        import threading

        self._open = open_scope
        self._scope: RunScope | None = None
        # Concurrent nodes may be the first to need it at the same moment.
        self._lock = threading.Lock()

    def get(self) -> RunScope:
        with self._lock:
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
    "LazyRunScope",
    "RunScope",
    "open_runtime_scope",
]
