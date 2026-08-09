"""Weaver driven from a console process, against a local emulator or Fabric.

One console session, several possible workspaces. What the workspace *is*
decides how every capability is met:

.. code-block:: text

    ConsoleSession + LocalWorkspace     ConsoleSession + FabricWorkspace

    local Python/Spark, in process      Livy, in the workspace
    FilesystemStore                     OneLake
    LocalResolver                       FabricResolver over REST
    no SQL                              TDS per Warehouse

The expensive half is Fabric, and this class exists mostly to stop paying for it
repeatedly. One credential, so ``az`` is shelled out to once rather than per
operation. One REST client and one resolver, so a name resolved by ``build`` is
still resolved for the ``load`` that follows. One Livy session, because a small
capacity has exactly one slot and starting a second means queueing behind the
first. One TDS connection per Warehouse, because a connection is not a statement
and a failed statement leaves a healthy connection.

**Execution here stays coarse.** A console does not orchestrate a graph node by
node across Livy; it prepares what it can locally and crosses once, with a whole
:class:`~weaver.session.program.RemoteProgram`. Breaking those crossings into
host-driven steps is the later decomposition work — this class is the seam that
work will need, not the work itself.
"""

from __future__ import annotations

from typing import Any, Sequence

from ..errors import CommandError
from ..targets import ItemRef, WarehouseTarget
from ..workspaces import FabricWorkspace, LocalWorkspace, Workspace
from .base import Session, WorkspaceScope
from .program import RemoteProgram
from .protocol import check, guarded
from .resources import Resource


class ConsoleSession(Session):
    """A reusable console-process execution scope.

    ``workspace`` is a *default context* and nothing more: ``weaver session``
    starts without one, and every command may name its own.
    """

    def __init__(self, *, require_weaver: bool = True, **kwargs) -> None:
        super().__init__(**kwargs)
        self.require_weaver = require_weaver

    def _new_scope(self, workspace: Workspace) -> "ConsoleScope":
        return ConsoleScope(
            workspace,
            telemetry=self.telemetry,
            executor=self._executor,
            require_weaver=self.require_weaver,
        )

    # --- readiness ----------------------------------------------------------

    def warm(self, workspace: Workspace | None = None) -> None:
        """Begin acquiring this context's expensive resources, without waiting.

        The console prompt returns immediately and the first command that needs
        Spark waits on the startup already running rather than starting a second
        one. Nothing here fails a caller: a warm-up that cannot complete is
        reported by whichever command actually needs the resource.
        """

        self.scope(workspace).warm()

    def executes_here(self, workspace: Workspace | None = None) -> bool:
        """Whether this process is already where the data engineering happens."""

        return self.scope(workspace).executes_here

    def spark(self, workspace: Workspace | None = None):
        """The live Spark session, where the caller is executing where the data is.

        A boundary accessor for in-process work — the local emulator — and not a
        transport. A console reaching into Fabric has no Spark of its own and
        says so, because the answer there is to cross with a program instead.
        """

        return self.scope(workspace).spark()

    # --- host-neutral capabilities ------------------------------------------

    def execute_python(
        self,
        program: RemoteProgram,
        *,
        workspace: Workspace | None = None,
        timeout: float | None = None,
    ) -> Any:
        scope = self.scope(workspace)
        self.substep_started(program.name, program.detail)
        try:
            if scope.executes_here:
                with self.telemetry.timing(f"python.{program.name}"):
                    payload = program.call()
            else:
                payload = scope.livy_run(
                    guarded(program.source),
                    name=program.name,
                    timeout=timeout if timeout is not None else program.timeout,
                )
                payload = check(payload, workspace=scope.name)
        except BaseException as exc:
            self.substep_failed(program.name, exc)
            raise
        self.substep_completed(program.name)
        return payload

    def execute_spark_sql(
        self,
        statement: str,
        *,
        workspace: Workspace | None = None,
        timeout: float | None = None,
    ) -> Any:
        scope = self.scope(workspace)
        if scope.executes_here:
            with self.telemetry.timing("spark.sql"):
                frame = scope.spark().sql(statement)
                return [row.asDict() for row in frame.collect()]
        source = (
            f"_statement = {statement!r}\n"
            "emit([row.asDict() for row in spark.sql(_statement).collect()])\n"
        )
        payload = scope.livy_run(guarded(source), name="spark_sql", timeout=timeout)
        return check(payload, workspace=scope.name)

    def execute_tsql(
        self,
        statement: str,
        *,
        target: Any,
        workspace: Workspace | None = None,
        parameters: Sequence[Any] | None = None,
    ) -> Any:
        scope = self.scope(workspace)
        executor = scope.sql_for(target)
        with self.telemetry.timing("tds.execute"):
            return executor.execute(statement, parameters or ())

    def sql_executor(self, target: Any, *, workspace: Workspace | None = None):
        """The reused TDS capability for one Warehouse.

        Handed out because the SQL executor is itself the boundary object that
        readers, wipes and Warehouse primitives already take. What matters is
        that it is acquired once per Warehouse per Session, not that it is
        hidden.
        """

        return self.scope(workspace).sql_for(target)


class ConsoleScope(WorkspaceScope):
    """One workspace's resources, held for the life of a console session."""

    def __init__(self, workspace: Workspace, *, require_weaver: bool = True, **kwargs) -> None:
        super().__init__(workspace, **kwargs)
        self.require_weaver = require_weaver
        self.name = str(getattr(workspace, "workspace", workspace))
        self._sql: dict[str, Resource] = {}
        self._credential = None

        if isinstance(workspace, FabricWorkspace):
            self.auth: Resource | None = self.track(
                Resource(
                    "auth",
                    self._acquire_token_provider,
                    executor=self.executor,
                    telemetry=self.telemetry,
                )
            )
            self.livy: Resource | None = self.track(
                Resource(
                    "livy",
                    self._acquire_livy,
                    executor=self.executor,
                    telemetry=self.telemetry,
                    release=lambda session: session.close(),
                )
            )
            self.local_spark: Resource | None = None
        elif isinstance(workspace, LocalWorkspace):
            self.auth = None
            self.livy = None
            self.local_spark = self.track(
                Resource(
                    "spark",
                    self._acquire_local_spark,
                    executor=self.executor,
                    telemetry=self.telemetry,
                    release=lambda opened: opened[1](None, None, None),
                )
            )
        else:
            raise CommandError(
                f"a console session cannot address a {type(workspace).__name__}"
            )

    # --- position -----------------------------------------------------------

    @property
    def executes_here(self) -> bool:
        """Whether the data engineering happens in this process.

        True for the local emulator, where the console *is* the host. False for
        Fabric, where the console prepares and crosses.
        """

        return self.local_spark is not None

    def warm(self) -> None:
        """Start acquiring what the next command will probably want.

        Speculative throughout: a warm-up nobody asked for must not fail the
        command that follows, so a failure here leaves the resource unstarted
        and the real attempt reports in its own terms.

        Livy is warmed only where it *can* start. A workspace naming no
        Environment cannot have a session created against it, and warming one
        would replace that command's clear message with a stale warm-up failure.
        """

        if self.auth is not None:
            self.auth.start(speculative=True)
        if self.livy is not None and getattr(self.workspace, "environment", None):
            self.livy.start(speculative=True)
        if self.local_spark is not None:
            # The JVM is the largest fixed cost of every local command, so the
            # emulator gets the same treatment as Livy.
            self.local_spark.start(speculative=True)

    # --- resolution ---------------------------------------------------------

    @property
    def resolver(self):
        with self._lock:
            if self._resolver is None:
                if isinstance(self.workspace, FabricWorkspace):
                    from ..fabric.resolution import FabricResolver

                    self._resolver = FabricResolver(
                        self.workspace, client=self._fabric_client()
                    )
                else:
                    from ..resolution import resolver_for

                    self._resolver = resolver_for(self.workspace)
            return self._resolver

    def _fabric_client(self):
        from ..fabric.client import FabricClient

        # One client, carrying the Session's own renewing token source, so every
        # REST call in this Session shares one credential rather than shelling
        # out to the Azure CLI per operation.
        return FabricClient(token=self.token_provider())

    def token_provider(self):
        from ..fabric.auth import FABRIC_SCOPE, TokenProvider

        if self._credential is None:
            from ..fabric.auth import credential

            self._credential = credential()
        return TokenProvider(FABRIC_SCOPE, self._credential)

    def _acquire_token_provider(self):
        provider = self.token_provider()
        provider()  # pay the acquisition here, in the background, once
        return provider

    # --- Spark --------------------------------------------------------------

    def spark(self):
        if self.local_spark is None:
            raise CommandError(
                "a console reaching into Fabric has no Spark session of its "
                "own; cross with execute_python instead"
            )
        return self.local_spark.get()[0]

    def _acquire_local_spark(self):
        from ..spark import local_delta_session

        opened = local_delta_session(self.workspace)
        return (opened.__enter__(), opened.__exit__)

    # --- Livy ---------------------------------------------------------------

    def _acquire_livy(self):
        from ..fabric import LivySession

        if self.auth is not None:
            self.auth.get()
        return LivySession.for_workspace(
            self.workspace,
            resolver=self.resolver,
            require_weaver=self.require_weaver,
            token=self.token_provider(),
        )

    def livy_run(self, source: str, *, name: str, timeout: float | None = None):
        """Submit one statement to this scope's Livy session and return its payload.

        A statement that fails is the caller's failure, not the session's: the
        resource is left healthy, because a Spark error is what Weaver is here
        to report and throwing away the session would turn one failed command
        into a minute of startup for the next one.
        """

        from ..fabric import LivyError

        if self.livy is None:
            raise CommandError("this workspace has no Livy session")
        session = self.livy.get()
        with self.telemetry.timing(f"livy.{name}"):
            kwargs = {} if timeout is None else {"timeout": timeout}
            try:
                result = session.run(source, **kwargs)
            except LivyError:
                # The session itself is gone — a statement error would have come
                # back as a result, not an exception.
                self.livy.fail()
                raise
        if not result.returned:
            raise CommandError(
                f"{name} ran in Fabric but returned nothing; see the Livy "
                "session output"
            )
        return result.payload

    # --- SQL ----------------------------------------------------------------

    def sql_for(self, target: Any):
        """The TDS capability for one Warehouse, acquired once per Session."""

        if not self.workspace.supports_sql:
            raise CommandError(
                f"{type(self.workspace).__name__} has no SQL capability"
            )
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

    def _acquire_sql(self, warehouse: WarehouseTarget):
        from ..fabric import desktop_sql_executor

        if self._credential is None:
            self.token_provider()
        return desktop_sql_executor(
            warehouse,
            self.workspace,
            credential=self._credential,
            resolver=self.resolver,
        )


__all__ = ["ConsoleScope", "ConsoleSession"]
