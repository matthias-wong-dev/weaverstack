"""A Session for Weaver running on a desktop, reaching into Fabric.

Desktop execution uses Livy for Spark SQL and Python, OneLake for storage, TDS
for T-SQL, and REST for Fabric APIs. There is no local Spark session.

Resources are cached per workspace for reuse across commands.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Any, Sequence

from ..errors import CommandError
from ..targets import ItemRef, WarehouseTarget
from ..workspaces import Workspace
from .base import TASK, Session, WorkspaceScope
from .program import RemoteProgram
from .resources import Resource


def _duration(seconds: float | None) -> str:
    if seconds is None:
        return ""
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes, remainder = divmod(int(seconds), 60)
    return f"{minutes}m{remainder:02d}s"


def _environment_publish_command(workspace: Workspace) -> str:
    reference = workspace.environment
    if reference is None:
        return "`weaver fabric environment publish <environment>`"
    if reference.workspace:
        return f"`weaver fabric environment publish {reference}`"
    return (
        f"`weaver fabric environment publish {reference} "
        f'--workspace "{workspace.workspace}"`'
    )


@dataclass(frozen=True)
class WarmUp:
    """Resources started or skipped by a warm-up, with reasons."""

    started: tuple[str, ...] = ()
    skipped: tuple[tuple[str, str], ...] = ()

    @property
    def anything(self) -> bool:
        return bool(self.started or self.skipped)


class ConsoleSession(Session):
    """A reusable desktop execution scope.

    ``workspace`` is only a default context: ``weaver session``
    starts without one, and every command may name its own.
    """

    def __init__(
        self,
        *,
        livy: Any = None,
        store: Any = None,
        resolver: Any = None,
        progress: Any = None,
        credential: Any = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        from ..fabric.auth import checked_credential

        # Validate the supplied credential now; acquire its token lazily.
        self._given_credential = checked_credential(credential)
        self._given_livy = livy
        self._given_store = store
        self._given_resolver = resolver
        #: Progress goes to stderr so stdout remains valid command output.
        #: ``False`` disables progress.
        self._progress = progress
        #: Width and lock for the transient progress line.
        self._painted = 0
        self._progress_lock = threading.Lock()
        self._ticker = None
        self._ticking = False

    # --- progress -----------------------------------------------------------

    #: Minimum width of the progress name column.
    PROGRESS_WIDTH = 52

    #: Kept back from the terminal's own width so the duration never wraps.
    DURATION_WIDTH = 8

    #: Seconds between redraws of the live progress line.
    PROGRESS_TICK = 1.0

    def present(self, frame, event: str, error: BaseException | None = None) -> None:
        """Display completed frames and the innermost active frame.

        Completed frames are permanent. Children precede their parent's total:

        .. code-block:: text

            Build

              Read physical state                         8.4s
                Sales.Customer                            3.2s
                Sales.Order                               4.1s
              Install Lakehouse/Sales                    18.6s
            ✓ Build                                      40.7s

        A transient line shows the innermost active frame:

        .. code-block:: text

            ⋯ Unbind catalogue claims                     1m47s

        The transient line is available only on a terminal and is erased before
        permanent output.
        """

        stream = self._progress_stream()
        if stream is None:
            return
        with self._progress_lock:
            self._erase(stream)
            if event == "started":
                if frame.kind == TASK:
                    print(f"\n{frame.name}\n", file=stream)
            else:
                if event == "failed":
                    mark = "✗"
                elif frame.kind == TASK:
                    mark = "✓"
                else:
                    mark = " "
                print(
                    f"{mark} {self._label(frame):<{self._width() - 2}}"
                    f"{_duration(frame.elapsed):>{self.DURATION_WIDTH}}",
                    file=stream,
                )
                if frame.kind == TASK:
                    print(file=stream)
            self._paint(stream)
        self._start_ticking()

    def _label(self, frame) -> str:
        return "  " * max(frame.depth - 1, 0) + frame.name

    def _width(self) -> int:
        """Return a name-column width that follows terminal resizing.

        Reading per line makes mid-run resizing take effect. Without a terminal,
        ``get_terminal_size`` returns 80 columns.
        """

        import shutil

        columns = shutil.get_terminal_size().columns
        return max(self.PROGRESS_WIDTH, columns - self.DURATION_WIDTH - 1)

    # --- the live line ------------------------------------------------------

    def _paint(self, stream) -> None:
        if not self._live(stream):
            return
        frame = self._innermost()
        if frame is None:
            return
        text = (
            f"⋯ {self._label(frame):<{self._width() - 2}}"
            f"{_duration(frame.age):>{self.DURATION_WIDTH}}"
        )
        stream.write("\r" + text)
        stream.flush()
        self._painted = len(text)

    def _erase(self, stream) -> None:
        if not self._painted:
            return
        stream.write("\r" + " " * self._painted + "\r")
        stream.flush()
        self._painted = 0

    def _innermost(self):
        frames = self.frames
        return frames[-1] if frames else None

    def _live(self, stream) -> bool:
        try:
            return bool(stream.isatty())
        except (AttributeError, ValueError):
            return False

    def _start_ticking(self) -> None:
        """Redraw elapsed time in a daemon thread while work is active."""

        stream = self._progress_stream()
        if stream is None or not self._live(stream) or self._ticker is not None:
            return
        import threading

        self._ticking = True
        self._ticker = threading.Thread(
            target=self._tick, name="weaver-progress", daemon=True
        )
        self._ticker.start()

    def _tick(self) -> None:
        import time

        while self._ticking:
            time.sleep(self.PROGRESS_TICK)
            stream = self._progress_stream()
            if stream is None:
                return
            with self._progress_lock:
                if not self._ticking:
                    return
                self._erase(stream)
                self._paint(stream)

    def warn(self, message: str) -> None:
        """Write a warning without colliding with the transient progress line."""

        stream = self._progress_stream()
        if stream is not None:
            with self._progress_lock:
                self._erase(stream)
                print(file=stream)
        super().warn(message)
        if stream is not None:
            with self._progress_lock:
                self._paint(stream)

    def stop_presenting(self) -> None:
        self._ticking = False
        stream = self._progress_stream()
        if stream is not None:
            with self._progress_lock:
                self._erase(stream)
        self._ticker = None

    def _progress_stream(self):
        if self._progress is False:
            return None
        if self._progress is not None:
            return self._progress
        import sys

        return sys.stderr

    def _new_scope(self, workspace: Workspace) -> "ConsoleScope":
        return ConsoleScope(
            workspace,
            telemetry=self.telemetry,
            executor=self._executor,
            livy=self._given_livy,
            store=self._given_store,
            resolver=self._given_resolver,
            credential=self._given_credential,
        )

    # --- readiness ----------------------------------------------------------

    def warm(self, workspace: Workspace | None = None) -> "WarmUp":
        """Begin acquiring the credential without waiting.

        Command-specific resources are not acquired until requested. Speculative
        failures are reported by the operation that uses the resource.
        """

        return self.scope(workspace).warm()

    def prepare(self, required, *, workspace: Workspace | None = None) -> "WarmUp":
        """Begin acquiring the declared resources without waiting.

        Preparation starts acquisitions but does not count as use. An unused
        Livy requirement opens no Spark session.
        """

        return self.scope(workspace).warm(required)

    def executes_here(self, workspace: Workspace | None = None) -> bool:
        return False

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
        from .delta_table import remote_delta_table_program

        source = remote_delta_table_program(
            qualified_name,
            columns,
            identity_column=identity_column,
            column_mapping=column_mapping,
            validate_only=validate_only,
        )
        scope = self.scope(workspace)
        livy = self._foreground_livy(scope)
        return scope.livy_run(
            source,
            name="delta_table",
            timeout=timeout,
            livy=livy,
        )

    def execute_python(
        self,
        program: RemoteProgram,
        *,
        workspace: Workspace | None = None,
        timeout: float | None = None,
    ) -> Any:
        # The caller owns the reporting frame; telemetry records the Livy cost.
        scope = self.scope(workspace)
        # Remote Python imports Weaver, so it requires a published Environment.
        # Spark SQL and TDS do not.
        scope._check_weaver_available()
        livy = self._foreground_livy(scope)
        scope.ensure_weaver(livy=livy)
        scope.check_published_version(self.warn, livy=livy)
        return scope.livy_run(
            program.source,
            name=program.name,
            timeout=timeout if timeout is not None else program.timeout,
            livy=livy,
        )

    def execute_spark_sql_batch(
        self,
        statements: Sequence[str],
        *,
        exact_case: bool = False,
        workspace: Workspace | None = None,
        timeout: float | None = None,
    ) -> Any:
        """Ordered Spark SQL statements, wherever this host's Spark is.

        One Livy submission when they have to cross, so statements belonging to
        one action share a trip and a session. See
        :meth:`weaver.sessions.base.Session.execute_spark_sql_batch`.
        """

        ordered = list(statements)
        if not ordered:
            return []
        scope = self.scope(workspace)
        # Keep the submitted program independent of build-package internals.
        source = (
            f"_statements = {ordered!r}\n"
            f"_exact = {bool(exact_case)!r}\n"
            "_key = 'spark.sql.caseSensitive'\n"
            "_previous = spark.conf.get(_key) if _exact else None\n"
            "_restore = _exact and str(_previous).lower() != 'true'\n"
            "if _restore:\n"
            "    spark.conf.set(_key, 'true')\n"
            "try:\n"
            "    for _statement in _statements[:-1]:\n"
            "        spark.sql(_statement)\n"
            "    _rows = [row.asDict() for row in spark.sql(_statements[-1]).collect()]\n"
            "finally:\n"
            "    if _restore:\n"
            "        spark.conf.set(_key, _previous)\n"
            "emit(_rows)\n"
        )
        livy = self._foreground_livy(scope)
        return scope.livy_run(source, name="spark_sql", timeout=timeout, livy=livy)

    def _foreground_livy(self, scope: "ConsoleScope"):
        if scope.livy is None:
            raise CommandError("No Livy session is available for this workspace.")
        if scope.livy.ready:
            return scope.livy.get()
        with self.substep("Wait for Spark session"):
            return scope.livy.get()

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
        """Return the Session-owned TDS executor for one Warehouse.

        Readers, wipes, and Warehouse primitives accept this executor directly.
        """

        return self.scope(workspace).sql_for(target)


class ConsoleScope(WorkspaceScope):
    """One workspace's resources for a desktop Session."""

    def __init__(
        self,
        workspace: Workspace,
        *,
        livy: Any = None,
        credential: Any = None,
        **kwargs,
    ) -> None:
        super().__init__(workspace, **kwargs)
        self.name = str(getattr(workspace, "workspace", workspace))
        self._sql: dict[str, Resource] = {}
        #: None defers credential selection and token acquisition until use.
        self._credential = credential
        self._transport_store = None
        self._version_checked = False

        self.auth: Resource = self.track(
            Resource(
                "auth",
                self._acquire_token_provider,
                executor=self.executor,
                telemetry=self.telemetry,
            )
        )
        self.livy: Resource = self.track(
            self._given_or_acquired(
                "livy",
                livy,
                self._acquire_livy,
                release=lambda session: session.close(),
            )
        )

    def _given_or_acquired(self, name, given, acquire, *, release) -> Resource:
        """Wrap an owned resource or a borrowed value.

        A Session closes what it opened, not borrowed values. This lets callers
        share a constrained Livy session safely.
        """

        if given is None:
            return Resource(
                name,
                acquire,
                executor=self.executor,
                telemetry=self.telemetry,
                release=release,
                telemetry_resource="livy" if name == "livy" else None,
            )
        return Resource(
            name, lambda: given, executor=self.executor, telemetry=self.telemetry
        )

    def warm(self, required=None) -> "WarmUp":
        """Start acquiring what the next command will probably want, and say what.

        Speculative throughout: a failure here leaves the resource unstarted and
        the real attempt reports in its own terms.

        No declaration warms only the credential. Livy is warmed only when named;
        a Warehouse-only command must not consume a Spark session.

        Livy is warmed only where it can start: a workspace naming no
        Environment cannot have a session created against it, and Fabric needs a
        Lakehouse to attach one to. A skipped resource comes back with the
        reason.
        """

        from .requirements import AUTH, LIVY

        started: list[str] = []
        skipped: list[tuple[str, str]] = []
        # No declaration warms only resources shared by every command.
        wanted = {AUTH} if required is None else set(required)

        if self.auth is not None and AUTH in wanted:
            self.auth.start(speculative=True)
            started.append("Fabric credential")
        if self.livy is not None and LIVY in wanted:
            reason = self._livy_cannot_start()
            if reason is None:
                self.livy.start(speculative=True)
                started.append("Spark session (Livy)")
            else:
                skipped.append(("Spark session (Livy)", reason))
        return WarmUp(started=tuple(started), skipped=tuple(skipped))

    def _livy_cannot_start(self) -> str | None:
        """Why a Spark session could not be started yet, or None when it can."""

        if self.spark_home is None and not getattr(
            self.workspace, "configured_lakehouses", None
        ):
            return (
                "Fabric attaches a Spark session to a Lakehouse, and none has "
                "been named. Give the command a Lakehouse target."
            )
        return None

    # --- resolution ---------------------------------------------------------

    @property
    def resolver(self):
        with self._lock:
            if self._resolver is None:
                from ..fabric.resolution import FabricResolver

                self._resolver = FabricResolver(
                    self.workspace, client=self._fabric_client()
                )
            return self._resolver

    @property
    def store(self):
        """The supplied store or this Session's OneLake DFS client.

        A store the Session was given wins outright; the caller owns it and is
        holding it open.

        ``FabricStore`` is unavailable on a desktop because it requires
        NotebookUtils.
        """

        if self._store is not None:
            return self._store
        return self.transport_store

    @property
    def transport_store(self):
        with self._lock:
            if self._transport_store is None:
                from ..fabric import OneLakeDfsClient

                self._transport_store = OneLakeDfsClient(telemetry=self.telemetry)
            return self._transport_store

    def _fabric_client(self):
        from ..fabric.client import FabricClient

        # Share one renewing token source across this Session's REST calls.
        return FabricClient(token=self.token_provider(), telemetry=self.telemetry)

    def token_provider(self):
        from ..fabric.auth import FABRIC_SCOPE, TokenProvider

        if self._credential is None:
            from ..fabric.auth import credential

            self._credential = credential()
        return TokenProvider(FABRIC_SCOPE, self._credential)

    def _acquire_token_provider(self):
        provider = self.token_provider()
        provider()  # acquire once during background warm-up
        return provider

    # --- Livy ---------------------------------------------------------------

    def _acquire_livy(self):
        """Acquire a running, bootstrapped Livy session.

        ``start`` waits for Fabric to report the session idle and completes its
        bootstrap before the shared resource becomes ready.
        """

        from ..fabric import LivySession

        if self.auth is not None:
            self.auth.get()
        session = LivySession.for_workspace(
            self.workspace,
            resolver=self.resolver,
            token=self.token_provider(),
            lakehouse=self.spark_home,
        )
        session.start()
        return session

    def _check_weaver_available(self) -> None:
        if self.livy is None:
            raise CommandError("No Livy session is available for this workspace.")
        if not self.workspace.environment:
            # Fail before starting a Spark session the program cannot use.
            from ..fabric.livy import missing_environment

            raise CommandError(missing_environment(self.workspace))

    def ensure_weaver(self, *, livy=None) -> None:
        self._check_weaver_available()
        if livy is None:
            livy = self.livy.get()
        with self.telemetry.external("livy", "ensure_weaver"):
            livy.ensure_weaver()

    def check_published_version(self, warn, *, livy=None) -> None:
        """Warn once when local and published Weaver versions differ.

        Version drift and version-check failures do not block work.
        """

        with self._lock:
            if self._version_checked:
                return
            self._version_checked = True

        from .. import __version__ as local

        try:
            published = self.livy_run(
                "import weaver\nemit(weaver.__version__)\n",
                name="version",
                livy=livy,
            )
        except Exception:  # noqa: BLE001 - a version check must never fail work
            return
        if published and published != local:
            warn(
                f"Local weaverstack is {local}; {self.name} has {published}. "
                f"To publish the local version, run "
                f"{_environment_publish_command(self.workspace)}."
            )

    def livy_run(
        self,
        source: str,
        *,
        name: str,
        timeout: float | None = None,
        livy=None,
    ):
        """Submit one statement to this scope's Livy session and return its payload.

        A statement that fails is the caller's failure, not the session's: the
        exception is re-raised and the resource left as it was, because the
        session is still up and costs a minute to replace. Only a session that
        has died is marked failed.
        """

        from ..fabric import LivyError, LivyStatementError

        if self.livy is None:
            raise CommandError("No Livy session is available for this workspace.")
        if livy is None:
            livy = self.livy.get()
        with self.telemetry.external("livy", name):
            kwargs = {} if timeout is None else {"timeout": timeout}
            try:
                result = livy.run(source, **kwargs)
            except LivyStatementError as exc:
                raise self._statement_failure(exc, name) from exc
            except LivyError:
                # Only a transport failure invalidates the shared Livy resource.
                self.livy.fail()
                raise
        if not result.returned:
            raise CommandError(
                f"{name} returned no result from Fabric. See the Livy session output."
            )
        return result.payload

    def _statement_failure(self, exc, name: str):
        """Add publishing guidance when remote Weaver cannot be imported.

        A missing Weaver import means the published Environment lacks code the
        submitted program needs.
        Other statement failures pass through unchanged.
        """

        missing = (exc.ename or "") in ("ModuleNotFoundError", "ImportError")
        if missing and "weaver" in (exc.evalue or ""):
            from .. import __version__

            return CommandError(
                f"{name} could not run in {self.name}: {exc.evalue}. "
                f"Publish weaverstack {__version__} with "
                f"{_environment_publish_command(self.workspace)}."
            )
        return exc

    # --- SQL ----------------------------------------------------------------

    def sql_for(self, target: Any):
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

    def _acquire_sql(self, warehouse: WarehouseTarget):
        from ..fabric import desktop_sql_executor
        from .sql import SessionSqlExecutor

        if self._credential is None:
            self.token_provider()
        return SessionSqlExecutor(
            desktop_sql_executor(
                warehouse,
                self.workspace,
                credential=self._credential,
                resolver=self.resolver,
            ),
            self.telemetry,
        )


__all__ = ["ConsoleScope", "ConsoleSession", "WarmUp"]
