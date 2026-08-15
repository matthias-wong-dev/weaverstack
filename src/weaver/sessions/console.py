"""A Session for Weaver running on a desktop, reaching into Fabric.

Every capability crosses: Spark SQL and Python over Livy, storage over OneLake,
T-SQL over TDS, everything else over REST. There is no Spark session here — see
:mod:`weaver.sessions.notebook` for the position that has one.

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
    """A duration a person reads: two significant figures, which is all anybody
    acts on, and a column a nested name needs more."""

    if seconds is None:
        return ""
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes, remainder = divmod(int(seconds), 60)
    return f"{minutes}m{remainder:02d}s"


@dataclass(frozen=True)
class WarmUp:
    """What a warm-up started, and what it declined to start and why.

    The reason is the useful half: naming the Environment a caller did not pass
    tells them what to do.
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
        livy: Any = None,
        store: Any = None,
        resolver: Any = None,
        progress: Any = None,
        credential: Any = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        from ..fabric.auth import checked_credential

        # Checked here, where it was supplied. A Session acquires its token
        # lazily, so a wrong object would otherwise surface during whichever
        # operation first reached Fabric.
        self._given_credential = checked_credential(credential)
        self._given_livy = livy
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

    #: The narrowest the name column may be; the real width comes from the
    #: terminal (:meth:`_width`). A fixed column is only ever right for one
    #: estate — a name like ``Warehouse/Reporting/Reporting.CustomerRevenuePresent``
    #: runs past it and pushes its own duration out of alignment.
    PROGRESS_WIDTH = 52

    #: Kept back from the terminal's own width so the duration never wraps.
    DURATION_WIDTH = 8

    #: How often the live line redraws while work is in flight. Slow enough to
    #: cost nothing, fast enough that the elapsed figure is visibly moving —
    #: which is the half of it that says the wait is alive rather than hung.
    PROGRESS_TICK = 1.0

    def present(self, frame, event: str, error: BaseException | None = None) -> None:
        """Keep the open work visible, and record each frame as it closes.

        A closed frame is written as a permanent line, since its duration is
        only known then. Children appear above their parent with the parent's
        total underneath, the way ``du`` reads:

        .. code-block:: text

            Build

              Read physical state                         8.4s
                Sales.Customer                            3.2s
                Sales.Order                               4.1s
              Install Lakehouse/Sales                    18.6s
            ✓ Build                                      40.7s

        Below that sits a transient line, rewritten in place, naming the
        innermost frame still open and how long it has been running:

        .. code-block:: text

            ⋯ Unbind catalogue claims                     1m47s

        It is erased before anything permanent is written, so it never lands in
        the transcript. Live output needs a terminal to rewrite: piped or
        captured, this degrades to the completed lines alone.
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
        """A Task heads its own block, so its Steps are the first indent level."""

        return "  " * max(frame.depth - 1, 0) + frame.name

    def _width(self) -> int:
        """The name column, from the terminal, never below :attr:`PROGRESS_WIDTH`.

        Read per line rather than cached, because a terminal can be resized
        mid-run and an ``ioctl`` is free against work measured in seconds. With
        no terminal, ``get_terminal_size`` answers 80 columns.
        """

        import shutil

        columns = shutil.get_terminal_size().columns
        return max(self.PROGRESS_WIDTH, columns - self.DURATION_WIDTH - 1)

    # --- the live line ------------------------------------------------------

    def _paint(self, stream) -> None:
        """Draw the innermost open frame, without ending the line."""

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

        Otherwise the line is painted only when another frame opens or closes,
        which during a long wait is never. A daemon thread, so it cannot hold
        the process open.
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

    def warn(self, message: str) -> None:
        """A warning, on a line of its own, with the live line taken back first.

        The transient line is mid-write when a warning arrives, so without the
        erase the two collide::

            ⋯   Read target inventories  40.0swarning: this console runs ...
        """

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
            livy=self._given_livy,
            store=self._given_store,
            resolver=self._given_resolver,
            credential=self._given_credential,
        )

    # --- readiness ----------------------------------------------------------

    def warm(self, workspace: Workspace | None = None) -> "WarmUp":
        """Begin acquiring everything this context could want, without waiting.

        The prompt returns at once and the first command needing Spark waits on
        the startup already running. Nothing here fails a caller: a warm-up that
        cannot complete is reported by whichever command needs the resource.
        """

        return self.scope(workspace).warm()

    def prepare(
        self, required, *, workspace: Workspace | None = None
    ) -> "WarmUp":
        """Begin acquiring exactly what a caller said it would need.

        The Session is told rather than inferring, so it is not a second place
        deciding what an operation does.

        Preparing is not using: this gives a head start to acquisitions that are
        coming anyway. A command that declares Livy and needs none opens no
        Spark session.
        """

        return self.scope(workspace).warm(required)

    def executes_here(self, workspace: Workspace | None = None) -> bool:
        """A console is never where the data engineering happens; it crosses."""

        return False

    # --- host-neutral capabilities ------------------------------------------

    def execute_python(
        self,
        program: RemoteProgram,
        *,
        workspace: Workspace | None = None,
        timeout: float | None = None,
    ) -> Any:
        # No frame is opened here: a crossing is how the caller's work happens
        # rather than a second thing that happened, and framing it printed the
        # program's name above the frame that asked for it — two lines, one
        # duration. The cost is still recorded in telemetry.
        scope = self.scope(workspace)
        # A program is Python that imports Weaver where Spark is, so this is the
        # one crossing that waits on `weaver install`. Spark SQL and TDS reach
        # the same workspace without it.
        scope.ensure_weaver()
        scope.check_published_version(self.warn)
        return scope.livy_run(
            program.source,
            name=program.name,
            timeout=timeout if timeout is not None else program.timeout,
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
        # Spelled out rather than imported on the far side: this is a Session
        # capability, and reaching into the build package for a context manager
        # would point the dependency the wrong way for a two-line conf dance.
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

        Handed out rather than hidden, because the SQL executor is itself the
        boundary object readers, wipes and Warehouse primitives take. It is
        acquired once per Warehouse per Session.
        """

        return self.scope(workspace).sql_for(target)


class ConsoleScope(WorkspaceScope):
    """One workspace's resources, held for the life of a console session."""

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
        #: The credential this scope authenticates with. None means the library
        #: default, chosen when it is first needed rather than now: acquiring a
        #: token is a network call, and opening a Session must not make one.
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
        """A resource this scope acquires, or one it was handed and must not close.

        A Session closes what it opened and nothing it was given, which is what
        lets a suite hand in the one Livy session a capacity permits, and a
        notebook hand in the Spark session it is running inside.
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

    def warm(self, required=None) -> "WarmUp":
        """Start acquiring what the next command will probably want, and say what.

        Speculative throughout: a failure here leaves the resource unstarted and
        the real attempt reports in its own terms.

        Livy is warmed only where it can start — a workspace naming no
        Environment cannot have a session created against it — and a skipped
        resource comes back with the reason.
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
        return WarmUp(started=tuple(started), skipped=tuple(skipped))

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
        """The store a console reaches this workspace with.

        A store the Session was given wins outright; the caller owns it and is
        holding it open.

        Otherwise it is the DFS transport. A console has no within-workspace
        store at all — ``FabricStore`` goes through NotebookUtils, which exists
        only inside a session.
        """

        if self._store is not None:
            return self._store
        return self.transport_store

    @property
    def transport_store(self):
        """How a console writes into a workspace it is not running inside."""

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

    # --- Livy ---------------------------------------------------------------

    def _acquire_livy(self):
        """A Livy session that is running, rather than constructed.

        ``for_workspace`` builds the object; ``start`` asks Fabric for the
        session, waits for idle and runs the bootstrap. Returning the unstarted
        object would look acquired to everything above: the state would say
        ready, a second caller would share it, and the first statement would
        fail with no session ever appearing in the workspace.
        """

        from ..fabric import LivySession

        if self.auth is not None:
            self.auth.get()
        session = LivySession.for_workspace(
            self.workspace,
            resolver=self.resolver,
            token=self.token_provider(),
        )
        session.start()
        return session

    def ensure_weaver(self) -> None:
        """Assert the Livy session can import the published Weaver."""

        if self.livy is None:
            raise CommandError("this workspace has no Livy session")
        with self.telemetry.timing("livy.ensure_weaver"):
            self.livy.get().ensure_weaver()

    def check_published_version(self, warn) -> None:
        """Compare this checkout's Weaver with the one published in the workspace.

        The two are independently versioned halves of one deployment and drift
        the moment either moves. A difference warns and names the fix rather
        than refusing, because it is usually harmless.

        Asked once per workspace context, on the first crossing: a warning
        repeated per command is one nobody reads.
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

        A statement that fails is the caller's failure, not the session's: the
        exception is re-raised and the resource left as it was, because the
        session is still up and costs a minute to replace. Only a session that
        has died is marked failed.
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

        A program that could not import Weaver is almost always a wheel older
        than the console that submitted it, and the raw ``ModuleNotFoundError``
        sends the reader after a missing package rather than to publish.
        Everything else is passed through as it came.
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
