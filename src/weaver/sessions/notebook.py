"""Session implementation for Weaver running inside Fabric.

The Session uses Spark, storage, and resolution resources supplied by the
notebook runtime.
"""

from __future__ import annotations

from typing import Any, Sequence

from ..errors import CommandError
from ..workspaces import Workspace
from .base import Session, WorkspaceScope, run_spark_statements
from .program import RemoteProgram
from .resources import Resource


class NotebookSession(Session):
    """A Session for Weaver running in a Fabric notebook or Livy session."""

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
        self._spark = spark
        self._given_store = store
        self._given_resolver = resolver

    def _new_scope(self, workspace: Workspace) -> "NotebookScope":
        if workspace.workspace != self.workspace.workspace:
            raise CommandError(
                f"This notebook is attached to {self.workspace.workspace} and cannot "
                f"execute against {getattr(workspace, 'workspace', workspace)}."
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

    # --- execution capabilities ---------------------------------------------

    def create_delta_table(
        self,
        qualified_name: str,
        columns: Sequence[Sequence[Any]],
        *,
        identity_column: str | None = None,
        column_mapping: bool = True,
        validate_only: bool = False,
        workspace: Workspace | None = None,
        timeout: float | None = None,
    ) -> Any:
        from .delta_table import create_delta_table_in_session

        spark = self.scope(workspace).spark()
        with self.telemetry.timing("spark.delta_table"):
            return create_delta_table_in_session(
                spark,
                qualified_name,
                columns,
                identity_column=identity_column,
                column_mapping=column_mapping,
                validate_only=validate_only,
            )

    def execute_python(
        self,
        program: RemoteProgram,
        *,
        workspace: Workspace | None = None,
        timeout: float | None = None,
    ) -> Any:
        # Callers frame execution so reporting is identical in both positions.
        self.scope(workspace)  # validate the attachment before running the program
        with self.telemetry.timing(f"python.{program.name}"):
            return program.call()

    def execute_spark_sql_batch(
        self,
        statements: Sequence[str],
        *,
        exact_case: bool = False,
        workspace: Workspace | None = None,
        timeout: float | None = None,
    ) -> Any:
        """Run statements in one identifier-case scope.

        Inside Fabric, batching keeps setup and query statements under the same
        identifier-case setting.
        """

        ordered = list(statements)
        if not ordered:
            return []
        from ..build_bundle.executors.spark_case import exact_identifier_case

        spark = self.scope(workspace).spark()
        with self.telemetry.timing("spark.sql"):
            with exact_identifier_case(spark, enabled=exact_case):
                return run_spark_statements(spark, ordered)

    def execute_tsql(
        self,
        statement: str,
        *,
        target: Any,
        workspace: Workspace | None = None,
        parameters: Sequence[Any] | None = None,
    ) -> None:
        executor = self.scope(workspace).sql_for(target)
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
        return executor.query(statement, parameters or ())

    def sql_executor(self, target: Any, *, workspace: Workspace | None = None):
        return self.scope(workspace).sql_for(target)


class NotebookScope(WorkspaceScope):
    """Resources supplied by the attached notebook runtime."""

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
        from ..targets import ItemRef, WarehouseTarget

        warehouse = (
            target
            if isinstance(target, WarehouseTarget)
            else WarehouseTarget(
                target if isinstance(target, ItemRef) else ItemRef(str(target))
            )
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
                    telemetry_resource="tds",
                )
                self.track(resource)
        return resource.get()

    def _acquire_sql(self, warehouse):
        from ..fabric.sql import fabric_sql_executor
        from .sql import SessionSqlExecutor

        return SessionSqlExecutor(
            fabric_sql_executor(warehouse, self.workspace, resolver=self.resolver),
            self.telemetry,
        )


__all__ = ["NotebookScope", "NotebookSession"]
