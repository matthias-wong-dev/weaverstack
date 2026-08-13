"""Session implementation for Weaver running inside a Fabric host.

The session uses notebook-provided Spark, storage, and resolution resources for
its attached Workspace. It exposes the same host-neutral capabilities as a
console Session.
"""

from __future__ import annotations

from typing import Any, Sequence

from ..errors import CommandError
from ..workspaces import Workspace
from .base import Session, WorkspaceScope, workspace_context
from .program import RemoteProgram
from .resources import Resource


class NotebookSession(Session):
    """A Session for Weaver running inside a Fabric notebook or Livy session."""

    def __init__(
        self,
        *,
        spark: Any = None,
        store: Any = None,
        resolver: Any = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        if self.workspace is None:
            raise CommandError("A NotebookSession requires its attached Workspace.")
        # Reuse resources supplied by the notebook host.
        self._spark = spark
        self._given_store = store
        self._given_resolver = resolver

    def _new_scope(self, workspace: Workspace) -> "NotebookScope":
        if workspace_context(workspace) != workspace_context(self.workspace):
            raise CommandError(
                f"this notebook is attached to {self.workspace.workspace}; it "
                f"cannot execute against {getattr(workspace, 'workspace', workspace)}"
            )
        return NotebookScope(
            workspace,
            telemetry=self.telemetry,
            executor=self._executor,
            spark=self._spark,
            store=self._given_store,
            resolver=self._given_resolver,
        )

    # --- position -----------------------------------------------------------

    def executes_here(self, workspace: Workspace | None = None) -> bool:
        return True

    def spark(self, workspace: Workspace | None = None):
        return self.scope(workspace).spark()

    # --- host-neutral capabilities ------------------------------------------

    def execute_python(
        self,
        program: RemoteProgram,
        *,
        workspace: Workspace | None = None,
        timeout: float | None = None,
    ) -> Any:
        # Framed by the caller, not here — see ConsoleSession.execute_python for
        # why. Both hosts have to agree about this or the same operation reads
        # differently depending on where it ran.
        self.scope(workspace)  # the attachment check, before any work happens
        with self.telemetry.timing(f"python.{program.name}"):
            return program.call()

    def execute_spark_sql(
        self,
        statement: str,
        *,
        workspace: Workspace | None = None,
        timeout: float | None = None,
    ) -> Any:
        with self.telemetry.timing("spark.sql"):
            frame = self.scope(workspace).spark().sql(statement)
            return [row.asDict() for row in frame.collect()]

    def execute_tsql(
        self,
        statement: str,
        *,
        target: Any,
        workspace: Workspace | None = None,
        parameters: Sequence[Any] | None = None,
    ) -> None:
        executor = self.scope(workspace).sql_for(target)
        with self.telemetry.timing("tds.execute"):
            executor.execute(statement, parameters or ())

    def query_tsql(
        self,
        statement: str,
        *,
        target: Any,
        workspace: Workspace | None = None,
        parameters: Sequence[Any] | None = None,
    ) -> Any:
        executor = self.scope(workspace).sql_for(target)
        with self.telemetry.timing("tds.query"):
            return executor.query(statement, parameters or ())

    def sql_executor(self, target: Any, *, workspace: Workspace | None = None):
        return self.scope(workspace).sql_for(target)


class NotebookScope(WorkspaceScope):
    """The attached notebook's own resources — mostly things it already has."""

    def __init__(self, workspace: Workspace, *, spark: Any = None, **kwargs) -> None:
        super().__init__(workspace, **kwargs)
        self._spark = spark
        self._sql: dict[str, Resource] = {}

    @property
    def executes_here(self) -> bool:
        return True

    def spark(self):
        if self._spark is None:
            from .host import active_spark

            self._spark = active_spark()
        return self._spark

    def sql_for(self, target: Any):
        """Warehouse SQL, authenticated Fabric-natively, once per Warehouse."""

        from ..targets import ItemRef, WarehouseTarget

        warehouse = target if isinstance(target, WarehouseTarget) else WarehouseTarget(
            target if isinstance(target, ItemRef) else ItemRef(str(target))
        )
        name = warehouse.warehouse.name
        with self._lock:
            resource = self._sql.get(name)
            if resource is None:
                resource = self._sql[name] = Resource(
                    f"tds.{name}",
                    lambda: self._acquire_sql(warehouse),
                    executor=self.executor,
                    telemetry=self.telemetry,
                    release=lambda executor: executor.close(),
                )
                self.track(resource)
        return resource.get()

    def _acquire_sql(self, warehouse):
        from ..fabric.sql import fabric_sql_executor

        return fabric_sql_executor(warehouse, self.workspace, resolver=self.resolver)


__all__ = ["NotebookScope", "NotebookSession"]
