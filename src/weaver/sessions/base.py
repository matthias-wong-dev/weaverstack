"""Host-neutral access to workspace resources and execution capabilities.

A Session scopes cached resources to a workspace context and exposes operations
such as ``execute_python`` without exposing the underlying transport. Builder,
Installer, and Runner retain planning and orchestration responsibilities. See
``design/code-architecture.md`` for the layer boundaries.
"""

from __future__ import annotations

import threading
import time
from abc import ABC, abstractmethod
from concurrent.futures import Executor, ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Iterator, Sequence

from ..errors import CommandError
from ..targets import ItemRef
from ..workspaces import Workspace
from .resources import Resource
from .telemetry import SessionTelemetry


def workspace_context(workspace: Workspace) -> tuple:
    """The identity a Session caches resources under.

    Not the ``Workspace`` itself: it carries target declarations, so it is
    unhashable, and two configurations differing only in those still address the
    same workspace, items and Livy session.
    """

    return (
        str(workspace.workspace),
        workspace.catalogue,
        workspace.environment,
    )


#: Where a Session's data engineering happens, relative to this process. One
#: consumer: choosing which run scope to open, because a run's deployed modules
#: are imported where Spark is. Everything else asks for a capability instead.

#: Weaver is running where the data is: a Fabric notebook.
IN_SESSION = "in_session"
#: This process reaches into a workspace it is not running inside.
ACROSS_BOUNDARY = "across_boundary"
#: No workspace is named, so there is nothing to reach into.
UNPLACED = "unplaced"

#: The reporting hierarchy: task, step, then physical sub-step. Failures attach
#: to the reporting frame that failed.
TASK = "task"
STEP = "step"
SUBSTEP = "substep"


@dataclass
class ReportingFrame:
    """One Task, Step or Sub-step, and what it cost.

    ``elapsed`` is None while the frame is open and a duration once it closes; a
    frame still running has an age rather than an elapsed time.
    """

    kind: str
    name: str
    detail: str | None = None
    #: How deep this sits in the frames open when it started, so a reader can
    #: indent without reconstructing the stack.
    depth: int = 0
    started: float = field(default_factory=time.monotonic)
    elapsed: float | None = None
    failed: bool = False

    @property
    def age(self) -> float:
        """Seconds since this frame opened, whether or not it has closed."""

        return (
            self.elapsed
            if self.elapsed is not None
            else time.monotonic() - self.started
        )

    def to_mapping(self) -> dict:
        mapping: dict[str, Any] = {
            "kind": self.kind,
            "name": self.name,
            "depth": self.depth,
            "seconds": None if self.elapsed is None else round(self.elapsed, 3),
        }
        if self.detail is not None:
            mapping["detail"] = self.detail
        if self.failed:
            mapping["failed"] = True
        return mapping


class Session(ABC):
    """A reusable execution scope: resolution, resources and host capabilities.

    Concrete hosts are :class:`~weaver.sessions.console.ConsoleSession` — Weaver
    on a desktop, reaching into Fabric — and
    :class:`~weaver.sessions.notebook.NotebookSession`, where Weaver is itself
    executing inside Fabric.
    """

    def __init__(
        self,
        *,
        workspace: Workspace | None = None,
        telemetry: SessionTelemetry | None = None,
        executor: Executor | None = None,
    ) -> None:
        self._default_workspace = workspace
        self.telemetry = telemetry or SessionTelemetry()
        self._executor = executor or ThreadPoolExecutor(
            max_workers=4, thread_name_prefix="weaver-session"
        )
        self._owns_executor = executor is None
        self._scopes: dict[tuple, "WorkspaceScope"] = {}
        self._scope_lock = threading.Lock()
        self._frames: list[ReportingFrame] = []
        #: Every frame that has closed, in the order it closed. The logical
        #: ledger, beside :attr:`telemetry`'s transport one: this says a Step
        #: took eight seconds, that says the eight seconds were four Livy
        #: submissions, and neither can be derived from the other.
        self.timings: list[ReportingFrame] = []
        #: Everything this Session has warned about, in order.
        self.warnings: list[str] = []
        #: Warehouse flushers handed out, by write stream. Created on demand:
        #: opening a Session must not start a worker or a TDS connection, and
        #: most Sessions never append a row.
        self._flushers: dict = {}
        self._closed = False
        #: Between the start of `close` and the Session actually closing. The
        #: flushers are still writing through it, so it is not closed, but it
        #: hands out no new stream.
        self._draining = False

    # --- context ------------------------------------------------------------

    def __enter__(self) -> "Session":
        return self

    def __exit__(self, *exc) -> bool:
        self.close()
        return False

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def workspace(self) -> Workspace | None:
        """The default context, where the caller supplied one. Not an identity."""

        return self._default_workspace

    def workspace_or_default(self, workspace: Workspace | None) -> Workspace:
        """The workspace this command means, or a failure naming what is missing."""

        resolved = workspace if workspace is not None else self._default_workspace
        if resolved is None:
            raise CommandError(
                "A Workspace is required for this command. Pass --workspace or configure "
                "one in a workspace configuration file."
            )
        return resolved

    def position(self, workspace: Workspace | None = None) -> str:
        """Where this Session's data engineering happens, as a named value.

        Derived from ``executes_here`` rather than declared twice. A Session
        that cannot place itself against a workspace has nothing to reach into,
        so that is an answer here rather than an error for a caller to read.
        """

        try:
            self.workspace_or_default(workspace)
        except CommandError:
            return UNPLACED
        return IN_SESSION if self.executes_here(workspace) else ACROSS_BOUNDARY

    @abstractmethod
    def executes_here(self, workspace: Workspace | None = None) -> bool:
        """Whether this process is already where the data engineering happens."""

    def scope(self, workspace: Workspace | None = None) -> "WorkspaceScope":
        """The cached resources for one workspace context, created on demand."""

        resolved = self.workspace_or_default(workspace)
        key = workspace_context(resolved)
        with self._scope_lock:
            if self._closed:
                raise CommandError("The Session is closed.")
            scope = self._scopes.get(key)
            if scope is None:
                scope = self._scopes[key] = self._new_scope(resolved)
                self.telemetry.count("session.scopes")
            return scope

    @abstractmethod
    def _new_scope(self, workspace: Workspace) -> "WorkspaceScope":
        """The resources this host holds for one workspace context."""

    # --- resolution ---------------------------------------------------------

    def resolver(self, workspace: Workspace | None = None):
        """The one resolver for this workspace context, with its own cache.

        One per Session lifetime: rebuilt per call, its cache would always be
        empty and every operation would re-ask what the same names mean.
        """

        return self.scope(workspace).resolver

    def store(self, workspace: Workspace | None = None):
        """The within-workspace store for this context."""

        return self.scope(workspace).store

    def transport_store(self, workspace: Workspace | None = None):
        """The store this host writes *across* the boundary with.

        The same as :meth:`store` wherever Weaver is already inside the
        workspace. A console reaching into Fabric has no within-workspace store,
        so this is where a bundle archive crosses.
        """

        return self.scope(workspace).transport_store

    def resolve_workspace(self, workspace: Workspace | None = None):
        """The physical workspace this context names."""

        return self.scope(workspace).resolve_workspace()

    def resolve_item(
        self,
        item: ItemRef | str,
        *,
        item_type: str,
        workspace: Workspace | None = None,
    ):
        """One physical item, by ``workspace + type + name``. Cached and counted.

        The type is required and always will be: a Lakehouse and a Warehouse may
        share a display name, and a Lakehouse's SQL endpoint certainly does.
        """

        reference = item if isinstance(item, ItemRef) else ItemRef(item)
        return self.scope(workspace).resolve_item(reference, item_type=item_type)

    # --- host-neutral capabilities ------------------------------------------

    @abstractmethod
    def execute_python(
        self,
        program: str,
        *,
        workspace: Workspace | None = None,
        timeout: float | None = None,
    ) -> Any:
        """Run a Python program where this host's data engineering happens.

        The program returns a value by calling ``emit(...)``. A console reaching
        into Fabric crosses Livy here; a notebook is already there.
        """

    @abstractmethod
    def execute_spark_sql_batch(
        self,
        statements: Sequence[str],
        *,
        exact_case: bool = False,
        workspace: Workspace | None = None,
        timeout: float | None = None,
    ) -> Any:
        """Run ordered Spark SQL statements together, and return the last one's rows.

        One submission wherever they cross, because statements belonging to one
        action are one piece of work: a setup that registers a temporary view
        and the query reading it only mean anything in the same session.

        ``exact_case`` travels with the statements because a desktop caller has
        no Spark to set a conf on, and a statement analysed under the host's
        default case is a different statement.

        The other statements are run for their effect, as they are in a session.
        """

    def execute_spark_sql(
        self,
        statement: str,
        *,
        exact_case: bool = False,
        workspace: Workspace | None = None,
        timeout: float | None = None,
    ) -> Any:
        """Run one Spark SQL statement and return its rows."""

        return self.execute_spark_sql_batch(
            [statement],
            exact_case=exact_case,
            workspace=workspace,
            timeout=timeout,
        )

    @abstractmethod
    def execute_tsql(
        self,
        statement: str,
        *,
        target: Any,
        workspace: Workspace | None = None,
        parameters: Sequence[Any] | None = None,
    ) -> None:
        """Run one T-SQL statement against a named Warehouse, over TDS.

        A statement, not a question: nothing comes back. Asking is
        :meth:`query_tsql`, and a batch that answers more than once needs
        ``query_result_sets`` — reading only the first result set of several
        answers with whichever came back first, which is how a failing check
        reports as passing.
        """

    @abstractmethod
    def query_tsql(
        self,
        statement: str,
        *,
        target: Any,
        workspace: Workspace | None = None,
        parameters: Sequence[Any] | None = None,
    ) -> Any:
        """Ask one T-SQL question of a named Warehouse, and return its rows."""

    # --- asynchronous appends -------------------------------------------------

    def flusher(self, table, *, warehouse, workspace: Workspace | None = None):
        """The flusher for one Warehouse write stream, created on first use.

        One flusher per stream, so two callers appending to the same table
        share a worker and a connection rather than racing. Identity carries
        enough to keep unrelated streams apart: the same table in two
        Warehouses, or reached through two workspaces, is not one stream.
        """

        from ..catalogue.flusher import FlusherKey, WarehouseFlusher

        workspace = self.workspace_or_default(workspace)
        target = warehouse if hasattr(warehouse, "warehouse") else None
        name = target.warehouse.name if target is not None else str(warehouse)
        key = FlusherKey(
            workspace=workspace.workspace,
            warehouse=name,
            schema=table.qualified.split(".", 1)[0],
            table=table.name,
        )
        with self._scope_lock:
            if self._closed:
                raise CommandError("this Session is closed and appends nothing")
            if self._draining:
                raise CommandError("this Session is closing and appends nothing")
            existing = self._flushers.get(key)
            if existing is not None:
                return existing
            created = WarehouseFlusher(
                table,
                key=key,
                execute=lambda statement: self.execute_tsql(
                    statement, target=warehouse, workspace=workspace
                ),
            )
            self._flushers[key] = created
            return created

    def flush(self) -> None:
        """Wait for every flusher this Session handed out."""

        with self._scope_lock:
            flushers = list(self._flushers.values())
        for flusher in flushers:
            flusher.flush()

    # --- reporting context --------------------------------------------------
    #
    # What is currently being presented and timed, and nothing more. A Session
    # knows a Step is running; it does not know what the Step decided, which
    # node it belonged to, or whether the run as a whole succeeded.

    @property
    def frames(self) -> tuple[ReportingFrame, ...]:
        return tuple(self._frames)

    def task_started(self, name: str, detail: str | None = None) -> None:
        self._enter("task", name, detail)

    def task_completed(self, name: str | None = None) -> None:
        self._exit("task", name)

    def task_failed(
        self, name: str | None = None, error: BaseException | None = None
    ) -> None:
        self._exit("task", name, error=error)

    def step_started(self, name: str, detail: str | None = None) -> None:
        self._enter("step", name, detail)

    def step_completed(self, name: str | None = None) -> None:
        self._exit("step", name)

    def step_failed(
        self, name: str | None = None, error: BaseException | None = None
    ) -> None:
        self._exit("step", name, error=error)

    def substep_started(self, name: str, detail: str | None = None) -> None:
        self._enter("substep", name, detail)

    def substep_completed(self, name: str | None = None) -> None:
        self._exit("substep", name)

    def substep_failed(
        self, name: str | None = None, error: BaseException | None = None
    ) -> None:
        self._exit("substep", name, error=error)

    # --- the paired form, which is the one to use ----------------------------
    #
    # An unclosed frame swallows everything nested under it and reports a
    # duration for work that stopped, so instrumentation is written as a `with`.
    # The explicit pairs above remain for callers that cannot bracket their work
    # in one place.

    @contextmanager
    def task(self, name: str, detail: str | None = None) -> Iterator[ReportingFrame]:
        """One thing a person asked for, timed whatever happens to it."""

        yield from self._framed(TASK, name, detail)

    @contextmanager
    def step(self, name: str, detail: str | None = None) -> Iterator[ReportingFrame]:
        """One boundary within a Task that is worth waiting at."""

        yield from self._framed(STEP, name, detail)

    @contextmanager
    def substep(self, name: str, detail: str | None = None) -> Iterator[ReportingFrame]:
        """One physical unit within a Step."""

        yield from self._framed(SUBSTEP, name, detail)

    def _framed(self, kind: str, name: str, detail: str | None):
        if kind == TASK:
            # The one safe moment to replace a dead resource. A Livy session
            # that dies mid-Task takes its RuntimeScopes with it, so a run
            # continuing on a replacement would dispatch against scopes that no
            # longer exist. The Task fails; the next one acquires afresh.
            self.recover()
        frame = self._enter(kind, name, detail)
        try:
            yield frame
        except BaseException as exc:
            # An interrupt closes its frame and travels on, exactly as a failure
            # does: the timing of work that was cancelled is still the timing of
            # work that happened.
            self._close(frame, error=exc)
            raise
        self._close(frame)

    def _enter(self, kind: str, name: str, detail: str | None) -> ReportingFrame:
        frame = ReportingFrame(
            kind=kind, name=name, detail=detail, depth=len(self._frames)
        )
        self._frames.append(frame)
        self.present(frame, "started")
        return frame

    def _close(self, frame: ReportingFrame, error: BaseException | None = None) -> None:
        if frame.elapsed is not None:
            return  # already closed, by an inner unwind or an explicit pair
        if frame in self._frames:
            # Everything still open beneath it goes too, closed in the order a
            # reader would expect rather than left dangling.
            index = self._frames.index(frame)
            for orphan in reversed(self._frames[index + 1 :]):
                self._close(orphan, error=error)
            del self._frames[index:]
        frame.elapsed = time.monotonic() - frame.started
        # Not an overwrite: a caller may have marked the frame failed from
        # inside it, which is how work whose failure is *data* — a run node that
        # reports a failure rather than raising one — still reads as failed.
        frame.failed = frame.failed or error is not None
        self.timings.append(frame)
        self.present(frame, "failed" if frame.failed else "completed", error)

    def _exit(
        self, kind: str, name: str | None, error: BaseException | None = None
    ) -> None:
        for index in range(len(self._frames) - 1, -1, -1):
            frame = self._frames[index]
            if frame.kind == kind and (name is None or frame.name == name):
                self._close(frame, error=error)
                return

    def recover(self) -> None:
        """At a Task boundary, let anything that died be acquired once more."""

        with self._scope_lock:
            scopes = list(self._scopes.values())
        for scope in scopes:
            scope.recover()

    def present(
        self, frame: ReportingFrame, event: str, error: BaseException | None = None
    ) -> None:
        """Show one reporting event. Silent by default; hosts specialise it."""

    def stop_presenting(self) -> None:
        """Take down anything a host is drawing. Nothing to do by default."""

    def warn(self, message: str) -> None:
        """Tell the operator something they should know but need not act on now.

        Overridable, so a notebook can render it and a test can collect it. The
        default reaches stderr: a mismatched deployment that warned about
        nothing stays invisible until it fails somewhere confusing.
        """

        import sys

        self.warnings.append(message)
        print(f"warning: {message}", file=sys.stderr)

    # --- teardown -----------------------------------------------------------

    def close(self) -> None:
        """Release every resource this Session acquired, and nothing it was given.

        Closing is the durability barrier for asynchronous logging, so the order
        here is the guarantee. A flusher writes *through this Session*, and a
        closed Session refuses to hand out a scope — so the flushers drain while
        the Session is still open, and only then is it marked closed and its
        resources released. Marking it closed first would fail exactly the
        writes this barrier exists to complete, and only under enough load for
        the worker to still be behind.
        """

        self.stop_presenting()
        with self._scope_lock:
            if self._closed or self._draining:
                return
            # No new stream from here on. The Session is still open, because the
            # flushers below write through it, but one handed out after this
            # point would hold rows nobody waits for.
            self._draining = True
            flushers = list(self._flushers.values())
            self._flushers.clear()
        failures = []
        for flusher in flushers:
            try:
                flusher.close()
            except Exception as exc:  # noqa: BLE001 - re-raised once, below
                failures.append(exc)

        with self._scope_lock:
            self._closed = True
            scopes = list(self._scopes.values())
            self._scopes.clear()
        for scope in scopes:
            scope.close()
        if self._owns_executor:
            self._executor.shutdown(wait=False)
        if failures:
            raise failures[0]


class WorkspaceScope:
    """One workspace's resolver, store and expensive resources, for a Session.

    It holds no execution semantics, only what is expensive to acquire and safe
    to share for as long as the Session lives.
    """

    def __init__(
        self,
        workspace: Workspace,
        *,
        telemetry: SessionTelemetry,
        executor: Executor,
        resolver: Any = None,
        store: Any = None,
    ) -> None:
        self.workspace = workspace
        self.telemetry = telemetry
        self.executor = executor
        self._resolver = resolver
        self._store = store
        self._resources: list[Resource] = []
        # Reentrant: acquiring a resource asks this scope for the resolver that
        # names it, and a scope that deadlocked on its own bookkeeping would do
        # so only under the concurrency this Session exists to allow.
        self._lock = threading.RLock()

    # --- resolution ---------------------------------------------------------

    @property
    def resolver(self):
        with self._lock:
            if self._resolver is None:
                from ..resolution import resolver_for

                self._resolver = resolver_for(self.workspace)
            return self._resolver

    @property
    def store(self):
        with self._lock:
            if self._store is None:
                from ..resolution import store_for

                self._store = store_for(self.workspace)
            return self._store

    @property
    def transport_store(self):
        """Where Weaver is already inside the workspace, its own store."""

        return self.store

    def resolve_workspace(self):
        resolver = self.resolver
        physical = getattr(resolver, "workspace", None)
        if physical is None:
            raise CommandError(
                f"{type(resolver).__name__} resolves no physical workspace"
            )
        return physical

    def resolve_item(self, item: ItemRef, *, item_type: str):
        resolver = self.resolver
        resolve = getattr(resolver, "resolve", None)
        if resolve is None:
            raise CommandError(
                f"{type(resolver).__name__} does not resolve items by type"
            )
        # The resolver owns the cache, so it owns the hit count too; this reads
        # the count rather than reimplementing the lookup, because a hit is the
        # absence of a call and only the resolver can see one.
        before = getattr(resolver, "cache_hits", 0)
        with self.telemetry.timing("resolve.item"):
            resolved = resolve(item, item_type=item_type)
        if getattr(resolver, "cache_hits", 0) > before:
            self.telemetry.count("resolve.item.cache_hits")
        return resolved

    # --- resources ----------------------------------------------------------

    def recover(self) -> None:
        """Permit one further acquisition of anything that failed here.

        Bounded by the resource's own allowance, and quiet when it is spent: a
        resource that will not come back says so to whoever uses it, rather than
        failing a Task before it has begun.
        """

        from .resources import ResourceError, ResourceState

        for resource in list(self._resources):
            if resource.state is not ResourceState.FAILED:
                continue
            try:
                resource.reacquire()
            except ResourceError:
                pass

    def track(self, resource: Resource) -> Resource:
        """Own a resource for this scope's lifetime, closing it with the scope."""

        with self._lock:
            self._resources.append(resource)
        return resource

    def close(self) -> None:
        with self._lock:
            resources, self._resources = list(self._resources), []
        for resource in reversed(resources):
            resource.close()


def run_spark_statements(spark: Any, statements: Sequence[str]) -> list[dict]:
    """Run each statement in order against a live session; collect only the last.

    Spark executes a command when it is asked for, so collecting the earlier
    results would materialise what nothing reads.
    """

    for statement in statements[:-1]:
        spark.sql(statement)
    return [row.asDict() for row in spark.sql(statements[-1]).collect()]


__all__ = [
    "ACROSS_BOUNDARY",
    "IN_SESSION",
    "STEP",
    "SUBSTEP",
    "TASK",
    "UNPLACED",
    "ReportingFrame",
    "Session",
    "WorkspaceScope",
    "run_spark_statements",
    "workspace_context",
]
