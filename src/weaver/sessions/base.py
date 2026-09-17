"""Session resource ownership and execution capabilities.

A Session scopes cached resources to a workspace context and exposes operations
such as ``execute_python`` without exposing the transport. Builder, Installer,
and Runner retain planning and orchestration responsibilities.
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


#: Where execution happens relative to this process. Run scope uses this because
#: deployed modules are imported where Spark is.

IN_SESSION = "in_session"
ACROSS_BOUNDARY = "across_boundary"
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
    #: Depth in the frame stack when this frame started.
    depth: int = 0
    started: float = field(default_factory=time.monotonic)
    elapsed: float | None = None
    failed: bool = False

    @property
    def age(self) -> float:
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
    """A reusable execution scope for resolution, resources, and execution.

    Concrete hosts are :class:`~weaver.sessions.console.ConsoleSession`, Weaver on
    a desktop reaching into Fabric, and
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
        #: Closed reporting frames, in closing order. These record logical work;
        #: telemetry separately records physical operations.
        self.timings: list[ReportingFrame] = []
        self.warnings: list[str] = []
        #: Warehouse flushers by write stream. Creating a Session must not start
        #: a worker or TDS connection.
        self._flushers: dict = {}
        self._workflow_id: str | None = None
        #: Machine-readable CLI commands suppress human progress and styling.
        self.machine_output = False
        self._closed = False
        #: True while flushers drain through an otherwise open Session.
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
        return self._default_workspace

    def workspace_or_default(self, workspace: Workspace | None) -> Workspace:
        resolved = workspace if workspace is not None else self._default_workspace
        if resolved is None:
            raise CommandError(
                "A Workspace is required for this command. Pass --workspace or configure "
                "one in a workspace configuration file."
            )
        return resolved

    def offer_spark_home(
        self, lakehouses, *, workspace: Workspace | None = None
    ) -> None:
        """Name Lakehouses a Spark session may attach to for this work.

        Notebook execution ignores the offer. Desktop execution needs it because
        Fabric creates a Livy session against a Lakehouse.
        """

        self.scope(workspace).offer_spark_home(lakehouses)

    @property
    def workflow_id(self) -> str | None:
        return self._workflow_id

    @contextmanager
    def workflow(self, workflow_id: str) -> Iterator[str]:
        previous = self._workflow_id
        self._workflow_id = workflow_id
        try:
            yield workflow_id
        finally:
            self._workflow_id = previous

    def position(self, workspace: Workspace | None = None) -> str:
        """Return whether execution is in-session, across-boundary, or unplaced.

        A Session with no workspace is unplaced.
        """

        try:
            self.workspace_or_default(workspace)
        except CommandError:
            return UNPLACED
        return IN_SESSION if self.executes_here(workspace) else ACROSS_BOUNDARY

    @abstractmethod
    def executes_here(self, workspace: Workspace | None = None) -> bool:
        """Whether execution uses this process's Fabric session."""

    def scope(self, workspace: Workspace | None = None) -> "WorkspaceScope":
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
        pass

    # --- resolution ---------------------------------------------------------

    def resolver(self, workspace: Workspace | None = None):
        """Return the Session-owned resolver and its persistent item cache."""

        return self.scope(workspace).resolver

    def store(self, workspace: Workspace | None = None):
        return self.scope(workspace).store

    def transport_store(self, workspace: Workspace | None = None):
        """The store used to transfer files into the workspace.

        This is :meth:`store` in Fabric. A ConsoleSession uses its OneLake DFS
        client because NotebookUtils is unavailable on a desktop.
        """

        return self.scope(workspace).transport_store

    def resolve_workspace(self, workspace: Workspace | None = None):
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

    # --- execution capabilities ---------------------------------------------

    @abstractmethod
    def create_delta_table(
        self,
        qualified_name: str,
        columns: Sequence[Sequence[Any]],
        *,
        identity_column: str | None = None,
        column_mapping: bool = True,
        workspace: Workspace | None = None,
        timeout: float | None = None,
    ) -> Any:
        """Create one Delta table through this host's active Spark session."""

    @abstractmethod
    def execute_python(
        self,
        program: str,
        *,
        workspace: Workspace | None = None,
        timeout: float | None = None,
    ) -> Any:
        """Run a Python program in Fabric and return its result.

        A ConsoleSession runs remote source through Livy, where ``emit(...)``
        returns the result. A NotebookSession calls the in-process form directly.
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
        """Run ordered Spark SQL statements and return the last one's rows.

        Desktop execution uses one Livy submission, so setup statements and the
        final query share temporary views and session state.

        ``exact_case`` applies to the whole batch because a desktop caller cannot
        set the remote Spark configuration directly.

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
        """Run a T-SQL statement against a Warehouse and discard its results.

        Use :meth:`query_tsql` for one result set and ``query_result_sets`` for
        multiple result sets.
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
        """Run a T-SQL query against a Warehouse and return its rows."""

    # --- asynchronous appends -------------------------------------------------

    def flusher(self, table, *, warehouse, workspace: Workspace | None = None):
        """The flusher for one Warehouse write stream, created on first use.

        One flusher per stream, so two callers appending to the same table
        share a worker and connection. Workspace, Warehouse, schema, and table
        identity keep unrelated streams separate.
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
                raise CommandError("Cannot append: the Session is closed.")
            if self._draining:
                raise CommandError("Cannot append: the Session is closing.")
            existing = self._flushers.get(key)
            if existing is not None:
                return existing
            created = WarehouseFlusher(
                table,
                key=key,
                execute=lambda statement: self.execute_tsql(
                    statement, target=warehouse, workspace=workspace
                ),
                capture_context=self.telemetry.capture_context,
                use_context=self.telemetry.use_context,
            )
            self._flushers[key] = created
            return created

    def flush(self) -> None:
        with self._scope_lock:
            flushers = list(self._flushers.values())
        for flusher in flushers:
            flusher.flush()

    # --- reporting context --------------------------------------------------
    #
    # Reporting records elapsed work, not planning decisions or run state.

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

    # --- paired reporting ----------------------------------------------------
    #
    # Prefer these context managers so failures always close their frames. The
    # explicit pairs remain for callers that cannot bracket work in one place.

    @contextmanager
    def task(self, name: str, detail: str | None = None) -> Iterator[ReportingFrame]:
        yield from self._framed(TASK, name, detail)

    @contextmanager
    def step(self, name: str, detail: str | None = None) -> Iterator[ReportingFrame]:
        yield from self._framed(STEP, name, detail)

    @contextmanager
    def substep(self, name: str, detail: str | None = None) -> Iterator[ReportingFrame]:
        yield from self._framed(SUBSTEP, name, detail)

    def _framed(self, kind: str, name: str, detail: str | None):
        if kind == TASK:
            # A Livy replacement invalidates its RuntimeScopes. Recover only
            # between Tasks so a run never continues with stale scopes.
            self.recover()
        frame = self._enter(kind, name, detail)
        try:
            yield frame
        except BaseException as exc:
            # Interrupted work still contributes a closed, failed timing.
            self._close(frame, error=exc)
            raise
        self._close(frame)

    def _enter(self, kind: str, name: str, detail: str | None) -> ReportingFrame:
        frame = ReportingFrame(
            kind=kind, name=name, detail=detail, depth=len(self._frames)
        )
        self._frames.append(frame)
        self.telemetry.set_frames(self._frames)
        self.present(frame, "started")
        return frame

    def _close(self, frame: ReportingFrame, error: BaseException | None = None) -> None:
        if frame.elapsed is not None:
            return  # closed by an inner unwind or explicit pair
        if frame in self._frames:
            # Closing an outer frame also closes every nested frame.
            index = self._frames.index(frame)
            for orphan in reversed(self._frames[index + 1 :]):
                self._close(orphan, error=error)
            del self._frames[index:]
            self.telemetry.set_frames(self._frames)
        frame.elapsed = time.monotonic() - frame.started
        # Keep failures reported as data when the frame closes normally.
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
        with self._scope_lock:
            scopes = list(self._scopes.values())
        for scope in scopes:
            scope.recover()

    def present(
        self, frame: ReportingFrame, event: str, error: BaseException | None = None
    ) -> None:
        """Present a reporting event. Silent by default."""

    def report(self, lines: Sequence[str]) -> None:
        """Present untimed operator information. Silent by default."""

    def stop_presenting(self) -> None:
        pass

    def warn(self, message: str) -> None:
        """Tell the operator something they should know but need not act on now.

        Subclasses may render the warning differently. The default writes to
        stderr.
        """

        import sys

        self.warnings.append(message)
        if self.machine_output:
            return
        print(f"warning: {message}", file=sys.stderr)

    # --- teardown -----------------------------------------------------------

    def close(self) -> None:
        """Release every resource this Session acquired, and nothing it was given.

        Closing is the durability barrier for asynchronous logging. Flushers
        drain while the Session remains open; then the Session closes and
        releases its resources.
        """

        self.stop_presenting()
        with self._scope_lock:
            if self._closed or self._draining:
                return
            # Reject new streams while existing flushers drain through the
            # still-open Session.
            self._draining = True
            flushers = list(self._flushers.values())
        failures = []
        for flusher in flushers:
            try:
                flusher.close()
            except Exception as exc:  # noqa: BLE001 - re-raised once, below
                failures.append(exc)

        if failures:
            with self._scope_lock:
                self._draining = False
            raise failures[0]

        with self._scope_lock:
            self._flushers.clear()
            self._closed = True
            scopes = list(self._scopes.values())
            self._scopes.clear()
        for scope in scopes:
            scope.close()
        if self._owns_executor:
            self._executor.shutdown(wait=False)


class WorkspaceScope:
    """Resources shared for one workspace during a Session.

    It owns the reusable resolver, store, and resource handles. Operations retain
    execution semantics.
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
        #: Candidate Lakehouses for Livy attachment, not execution destinations.
        self._offered_spark_homes: set[str] = set()
        # Acquisition may re-enter the scope to resolve the resource.
        self._lock = threading.RLock()

    def offer_spark_home(self, lakehouses) -> None:
        """Note Lakehouses a Spark session may attach to, if this host needs one.

        Desktop execution needs a Lakehouse id because Fabric attaches Livy to a
        Lakehouse. This does not select an execution destination: every generated
        statement names its target in full.
        """

        names = {str(name) for name in lakehouses or () if name}
        if not names:
            return
        with self._lock:
            self._offered_spark_homes |= names

    @property
    def spark_home(self) -> str | None:
        """Return a stable Livy attachment from the offered Lakehouses.

        The first name in sorted order keeps attachment stable. None lets the
        caller fall back to the workspace configuration.
        """

        with self._lock:
            offered = sorted(self._offered_spark_homes)
        return offered[0] if offered else None

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
        return self.store

    def resolve_workspace(self):
        resolver = self.resolver
        physical = getattr(resolver, "workspace", None)
        if physical is None:
            raise CommandError(
                f"{type(resolver).__name__} cannot resolve a physical workspace."
            )
        return physical

    def resolve_item(self, item: ItemRef, *, item_type: str):
        resolver = self.resolver
        resolve = getattr(resolver, "resolve", None)
        if resolve is None:
            raise CommandError(
                f"{type(resolver).__name__} cannot resolve items by type."
            )
        # Only the resolver can identify a cache hit without repeating lookup
        # logic.
        before = getattr(resolver, "cache_hits", 0)
        with self.telemetry.timing("resolve.item"):
            resolved = resolve(item, item_type=item_type)
        if getattr(resolver, "cache_hits", 0) > before:
            self.telemetry.count("resolve.item.cache_hits")
        return resolved

    # --- resources ----------------------------------------------------------

    def recover(self) -> None:
        """Permit each failed resource one bounded reacquisition.

        Exhaustion is reported when the resource is next used, not before the
        Task starts.
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
        with self._lock:
            self._resources.append(resource)
        return resource

    def close(self) -> None:
        with self._lock:
            resources, self._resources = list(self._resources), []
        for resource in reversed(resources):
            resource.close()


def run_spark_statements(spark: Any, statements: Sequence[str]) -> list[dict]:
    """Run statements in order and materialise only the final result."""

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
