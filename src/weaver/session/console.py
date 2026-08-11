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

import threading
from dataclasses import dataclass
from typing import Any, Sequence

from ..errors import CommandError
from ..targets import ItemRef, WarehouseTarget
from ..workspaces import FabricWorkspace, LocalWorkspace, Workspace
from .base import TASK, Session, WorkspaceScope
from .program import RemoteProgram
from .resources import Resource


def _duration(seconds: float | None) -> str:
    """A duration a person reads, not one a machine parses.

    Two significant figures is all anybody acts on: the difference between 8.4s
    and 8.43s changes no decision, and the extra digit costs a column that a
    nested name needs more.
    """

    if seconds is None:
        return ""
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes, remainder = divmod(int(seconds), 60)
    return f"{minutes}m{remainder:02d}s"


@dataclass(frozen=True)
class WarmUp:
    """What a warm-up started, and what it declined to start and why.

    The reason is the useful half. "Starting resources in the background" while
    silently skipping the expensive one tells a reader nothing they can act on;
    naming the Environment they did not pass tells them exactly what to do.
    """

    started: tuple[str, ...] = ()
    skipped: tuple[tuple[str, str], ...] = ()

    @property
    def anything(self) -> bool:
        return bool(self.started or self.skipped)


class ConsoleSession(Session):
    """A reusable console-process execution scope.

    ``workspace`` is a *default context* and nothing more: ``weaver session``
    starts without one, and every command may name its own.
    """

    def __init__(
        self,
        *,
        require_weaver: bool = True,
        livy: Any = None,
        spark: Any = None,
        store: Any = None,
        resolver: Any = None,
        progress: Any = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.require_weaver = require_weaver
        self._given_livy = livy
        self._given_spark = spark
        self._given_store = store
        self._given_resolver = resolver
        #: Where the timing tree is written. stderr by default, because stdout
        #: is a command's answer and several commands emit JSON on it — progress
        #: interleaved into that would make the answer unparseable. ``False``
        #: silences it.
        self._progress = progress
        #: How many characters of transient line are currently on screen, and
        #: the lock that keeps the ticker thread from drawing over a completion.
        self._painted = 0
        self._progress_lock = threading.Lock()
        self._ticker = None
        self._ticking = False

    # --- progress -----------------------------------------------------------

    #: How wide the name column is before the duration. Wide enough for a
    #: qualified object name at Sub-step depth, narrow enough to sit in an
    #: eighty-column terminal with the duration still on the same line.
    PROGRESS_WIDTH = 52

    #: How often the live line redraws while work is in flight. Slow enough to
    #: cost nothing, fast enough that the elapsed figure is visibly moving —
    #: which is the half of it that says the wait is alive rather than hung.
    PROGRESS_TICK = 1.0

    def present(self, frame, event: str, error: BaseException | None = None) -> None:
        """Keep the open work visible, and record each frame as it closes.

        Two things a reader needs, and they are not the same thing.

        **What finished, and what it cost** is written as a permanent line when
        a frame closes, because the duration is only known then. Children appear
        above their parent with the parent's total underneath — a roll-up, the
        way ``du`` reads:

        .. code-block:: text

            Build

              Read physical state                         8.4s
                Sales.Customer                            3.2s
                Sales.Order                               4.1s
              Install Lakehouse/Sales                    18.6s
            ✓ Build                                      40.7s

        **What is happening now** is a transient line below that, rewritten in
        place, naming the innermost frame still open and how long it has been
        running:

        .. code-block:: text

            ⋯ Unbind catalogue claims                     1m47s

        It is erased before anything permanent is written, so it never lands in
        the transcript. Recording completions alone was the earlier design and
        it was wrong for this feature: a Step that takes two minutes showed the
        Task heading and then nothing at all, and silence is exactly what a tool
        whose purpose is responsiveness must not answer a long wait with. The
        thing not wanted was *duplication* — a name printed on the way in and
        again on the way out — and a line that is overwritten rather than
        appended avoids that without going quiet.

        Live output needs a terminal to rewrite. Piped, redirected or captured,
        this degrades to exactly the completed lines it always wrote, so a log
        file and a test transcript are unchanged.
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
                    f"{mark} {self._label(frame):<{max(self.PROGRESS_WIDTH - 2, 1)}}"
                    f"{_duration(frame.elapsed):>8}",
                    file=stream,
                )
                if frame.kind == TASK:
                    print(file=stream)
            self._paint(stream)
        self._start_ticking()

    def _label(self, frame) -> str:
        """A Task heads its own block, so its Steps are the first indent level."""

        return "  " * max(frame.depth - 1, 0) + frame.name

    # --- the live line ------------------------------------------------------

    def _paint(self, stream) -> None:
        """Draw the innermost open frame, without ending the line."""

        if not self._live(stream):
            return
        frame = self._innermost()
        if frame is None:
            return
        text = (
            f"⋯ {self._label(frame):<{max(self.PROGRESS_WIDTH - 2, 1)}}"
            f"{_duration(frame.age):>8}"
        )
        stream.write("\r" + text)
        stream.flush()
        self._painted = len(text)

    def _erase(self, stream) -> None:
        """Take the transient line back before anything permanent is written."""

        if not self._painted:
            return
        stream.write("\r" + " " * self._painted + "\r")
        stream.flush()
        self._painted = 0

    def _innermost(self):
        """The deepest frame still open — what the wait is actually for.

        A Task names the command, which the heading already said; the useful
        answer to "what is it doing" is the smallest thing currently in flight.
        """

        frames = self.frames
        return frames[-1] if frames else None

    def _live(self, stream) -> bool:
        """Whether this stream can have a line rewritten in it."""

        try:
            return bool(stream.isatty())
        except (AttributeError, ValueError):
            return False

    def _start_ticking(self) -> None:
        """Keep the elapsed figure moving while a frame is open.

        Without this the line is painted only when some other frame opens or
        closes — which for the long waits that most need it is never. A daemon
        thread, so it can never hold the process open.
        """

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

    def stop_presenting(self) -> None:
        """Stop the ticker and leave no half-drawn line behind."""

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
            require_weaver=self.require_weaver,
            livy=self._given_livy,
            spark=self._given_spark,
            store=self._given_store,
            resolver=self._given_resolver,
        )

    # --- readiness ----------------------------------------------------------

    def warm(self, workspace: Workspace | None = None) -> "WarmUp":
        """Begin acquiring everything this context could want, without waiting.

        The console prompt returns immediately and the first command that needs
        Spark waits on the startup already running rather than starting a second
        one. Nothing here fails a caller: a warm-up that cannot complete is
        reported by whichever command actually needs the resource.
        """

        return self.scope(workspace).warm()

    def prepare(
        self, required, *, workspace: Workspace | None = None
    ) -> "WarmUp":
        """Begin acquiring exactly what a caller said it would need.

        The Session is told; it does not infer. It has no idea what a build is
        and must not acquire one — a Session that decided which resources an
        operation wanted would be a second place deciding what the operation
        does.

        **Preparing is not using.** This gives a head start to acquisitions that
        are coming anyway; it never makes one happen that would not otherwise.
        A command that declares Livy and then turns out to need none opens no
        Spark session, because the acquisition still belongs where the need is
        discovered.
        """

        return self.scope(workspace).warm(required)

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
                scope.check_published_version(self.warn)
                payload = scope.livy_run(
                    program.source,
                    name=program.name,
                    timeout=timeout if timeout is not None else program.timeout,
                )
        except BaseException as exc:
            # Reported and re-raised, whatever it was: an interrupt still ends
            # the sub-step it interrupted, and still travels on.
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
        return scope.livy_run(source, name="spark_sql", timeout=timeout)

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
        """The reused TDS capability for one Warehouse.

        Handed out because the SQL executor is itself the boundary object that
        readers, wipes and Warehouse primitives already take. What matters is
        that it is acquired once per Warehouse per Session, not that it is
        hidden.
        """

        return self.scope(workspace).sql_for(target)


class ConsoleScope(WorkspaceScope):
    """One workspace's resources, held for the life of a console session."""

    def __init__(
        self,
        workspace: Workspace,
        *,
        require_weaver: bool = True,
        livy: Any = None,
        spark: Any = None,
        **kwargs,
    ) -> None:
        super().__init__(workspace, **kwargs)
        self.require_weaver = require_weaver
        self.name = str(getattr(workspace, "workspace", workspace))
        self._sql: dict[str, Resource] = {}
        self._credential = None
        self._transport_store = None
        self._version_checked = False

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
                self._given_or_acquired(
                    "livy",
                    livy,
                    self._acquire_livy,
                    release=lambda session: session.close(),
                )
            )
            self.local_spark: Resource | None = None
        elif isinstance(workspace, LocalWorkspace):
            self.auth = None
            self.livy = None
            self.local_spark = self.track(
                self._given_or_acquired(
                    "spark",
                    None if spark is None else (spark, lambda *exc: None),
                    self._acquire_local_spark,
                    release=lambda opened: opened[1](None, None, None),
                )
            )
        else:
            raise CommandError(
                f"a console session cannot address a {type(workspace).__name__}"
            )

    def _given_or_acquired(self, name, given, acquire, *, release) -> Resource:
        """A resource this scope acquires, or one it was handed and must not close.

        A Session closes what it opened and nothing it was given. That is what
        lets a pytest suite start one Livy session with its own preflight and
        skip semantics and then hand it to a Session, on a capacity that permits
        exactly one — and it is the same rule that lets a notebook hand in the
        Spark session it is already running inside.
        """

        if given is None:
            return Resource(
                name,
                acquire,
                executor=self.executor,
                telemetry=self.telemetry,
                release=release,
            )
        return Resource(
            name, lambda: given, executor=self.executor, telemetry=self.telemetry
        )

    # --- position -----------------------------------------------------------

    @property
    def executes_here(self) -> bool:
        """Whether the data engineering happens in this process.

        True for the local emulator, where the console *is* the host. False for
        Fabric, where the console prepares and crosses.
        """

        return self.local_spark is not None

    def warm(self, required=None) -> "WarmUp":
        """Start acquiring what the next command will probably want, and say what.

        Speculative throughout: a warm-up nobody asked for must not fail the
        command that follows, so a failure here leaves the resource unstarted
        and the real attempt reports in its own terms.

        What it returns matters as much as what it starts. Livy is warmed only
        where it *can* start — a workspace naming no Environment cannot have a
        session created against it — and a prompt that announced "starting
        resources" and then quietly declined would be worse than one that says
        nothing. So a skipped resource comes back with the reason it was
        skipped, for the caller to show.
        """

        from .requirements import AUTH, LIVY

        started: list[str] = []
        skipped: list[tuple[str, str]] = []
        # No declaration means "whatever this context has", which is what
        # `weaver session` wants when it starts before any command is typed.
        wanted = None if required is None else set(required)

        def asked(name: str) -> bool:
            return wanted is None or name in wanted

        if self.auth is not None and asked(AUTH):
            self.auth.start(speculative=True)
            started.append("Fabric credential")
        if self.livy is not None and asked(LIVY):
            if getattr(self.workspace, "environment", None):
                self.livy.start(speculative=True)
                started.append("Spark session (Livy)")
            else:
                skipped.append(
                    (
                        "Spark session (Livy)",
                        "this workspace names no Environment — pass "
                        "--environment, or set one in workspace configuration",
                    )
                )
        if self.local_spark is not None and asked(LIVY):
            # The JVM is the largest fixed cost of every local command, so the
            # emulator gets the same treatment as Livy. Declared as Livy too:
            # what a command needs is *Spark*, and which one it gets is the
            # host's business rather than the caller's.
            self.local_spark.start(speculative=True)
            started.append("local Spark session")

        return WarmUp(started=tuple(started), skipped=tuple(skipped))

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

    @property
    def store(self):
        """The store a console reaches this workspace with.

        A store the Session was *given* wins outright: the caller owns it, is
        holding it open, and handing back a different one would silently ignore
        what it asked for.

        Otherwise: for the emulator that is the filesystem, which is both the
        within-workspace store and the way in. A console addressing Fabric has
        no within-workspace store at all — ``FabricStore`` is NotebookUtils, and
        NotebookUtils exists only inside a session — so its store is the DFS
        transport, and the two are the same object here.
        """

        if self._store is not None:
            return self._store
        if self.executes_here:
            return super().store
        return self.transport_store

    @property
    def transport_store(self):
        """How a console writes into a workspace it is not running inside."""

        if self.executes_here:
            return super().store
        with self._lock:
            if self._transport_store is None:
                from ..fabric import OneLakeDfsClient

                self._transport_store = OneLakeDfsClient()
            return self._transport_store

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
        """A Livy session that is *running*, not merely constructed.

        ``for_workspace`` builds the object; ``start`` is what asks Fabric for
        the session, waits for it to reach idle and runs the bootstrap. A
        resource that returned the unstarted object would look acquired to
        everything above it — the state would say ready, a second caller would
        share it, and the first statement would fail with "the Livy session has
        not been started" while no session ever appeared in the workspace.

        Acquiring means the expensive part is over. That is the whole contract a
        waiting caller is relying on.
        """

        from ..fabric import LivySession

        if self.auth is not None:
            self.auth.get()
        session = LivySession.for_workspace(
            self.workspace,
            resolver=self.resolver,
            require_weaver=self.require_weaver,
            token=self.token_provider(),
        )
        session.start()
        return session

    def check_published_version(self, warn) -> None:
        """Compare this checkout's Weaver with the one published in the workspace.

        A console prepares work locally and runs it against the *published*
        wheel, so the two are independently versioned halves of one deployment
        and they drift the moment either moves. When they differ the useful
        thing is to say so and name the fix — not to refuse, because a version
        difference is usually harmless and refusing would put a publish in front
        of every experiment during rapid development.

        Asked once per workspace context, on the first crossing, and never
        again: it costs one statement, and a warning repeated per command is a
        warning nobody reads.
        """

        with self._lock:
            if self._version_checked:
                return
            self._version_checked = True

        from .. import __version__ as local

        try:
            published = self.livy_run(
                "import weaver\nemit(weaver.__version__)\n", name="version"
            )
        except Exception:  # noqa: BLE001 - a version check must never fail work
            return
        if published and published != local:
            warn(
                f"this console runs weaverstack {local}; {self.name} has "
                f"{published} published — run `weaver install` if the difference "
                "matters"
            )

    def livy_run(self, source: str, *, name: str, timeout: float | None = None):
        """Submit one statement to this scope's Livy session and return its payload.

        A statement that fails is the caller's failure, not the session's. The
        Spark session is still up, still costs a minute to replace, and is what
        the next command is relying on — so a remote exception is re-raised and
        the resource is left exactly as it was. Only a session that has actually
        died is marked failed.
        """

        from ..fabric import LivyError, LivyStatementError

        if self.livy is None:
            raise CommandError("this workspace has no Livy session")
        session = self.livy.get()
        with self.telemetry.timing(f"livy.{name}"):
            kwargs = {} if timeout is None else {"timeout": timeout}
            try:
                result = session.run(source, **kwargs)
            except LivyStatementError as exc:
                raise self._statement_failure(exc, name) from exc
            except LivyError:
                # The session itself is gone, not the statement.
                self.livy.fail()
                raise
        if not result.returned:
            raise CommandError(
                f"{name} ran in Fabric but returned nothing; see the Livy "
                "session output"
            )
        return result.payload

    def _statement_failure(self, exc, name: str):
        """The remote failure, said in terms the reader can act on.

        A program that could not import Weaver at all is almost always a wheel
        older than the console that submitted it, and the raw
        ``ModuleNotFoundError`` sends the reader to look for a missing package
        rather than to publish. Everything else is passed through as it came:
        guessing at causes would bury the real ones.
        """

        missing = (exc.ename or "") in ("ModuleNotFoundError", "ImportError")
        if missing and "weaver" in (exc.evalue or ""):
            from .. import __version__

            return CommandError(
                f"{name} could not run: the Weaver published in {self.name} is "
                f"older than this console ({__version__}) and does not carry "
                f"{exc.evalue}. Publish the current wheel with `weaver install "
                f"--workspace \"{self.name}\" --environment "
                f"{getattr(self.workspace, 'environment', '<environment>')}`"
            )
        return exc

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


__all__ = ["ConsoleScope", "ConsoleSession", "WarmUp"]
