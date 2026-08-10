"""The Session contract — where Weaver is running and how it reaches physical things.

A Session answers exactly one family of questions:

.. code-block:: text

    where am I running
    what resources do I already have
    what physical thing does this logical Fabric reference mean
    how do I execute work in this host

and deliberately answers none of these:

.. code-block:: text

    what should be built          → Builder
    install this decision         → Installer
    what runs next, and what happened → Runner

That boundary is the whole point. A Session that learned what a DAG was would
become a second Runner, and the two would then disagree about which of them owns
a node's status.

**A Session is a process scope, not a workspace binding.** ``weaver session``
starts before any workspace has been named, and one console can address several
— a build into one workspace and a load out of another are two commands in one
shell. So the workspace arrives with the *command*, and this object caches a
:class:`WorkspaceScope` per workspace context: one resolver, one item cache, one
Livy session, one TDS connection per Warehouse, for as long as the Session lives.
A default workspace, where one was configured, is a default context only —
never the Session's identity.

**Capabilities are host-neutral.** Domain code asks for ``execute_python``, not
for Livy; the Session decides that a console reaching into Fabric means Livy and
that a notebook means the current process. Transport-specific methods stay
private, because the day a domain caller writes ``session.livy`` is the day
Weaver stops being able to run anywhere else.
"""

from __future__ import annotations

import threading
import time
from abc import ABC, abstractmethod
from concurrent.futures import Executor, ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Iterator, Mapping, Sequence

from ..errors import CommandError
from ..targets import ItemRef
from ..workspaces import FabricWorkspace, LocalWorkspace, Workspace
from .resources import Resource
from .telemetry import SessionTelemetry


def workspace_context(workspace: Workspace) -> tuple:
    """The identity a Session caches resources under.

    Not the ``Workspace`` itself: it carries mappings of target declarations, so
    it is unhashable, and two configurations that differ only in which targets
    they declare still address the same Fabric workspace with the same items and
    the same Livy session. What matters for resource identity is the place, the
    control Lakehouse and the Environment the code will run in.
    """

    return (
        workspace.workspace_type,
        str(workspace.workspace),
        workspace.weaver_lakehouse,
        workspace.environment,
    )


#: The execution hierarchy, outermost first. A Task is one thing a person asked
#: for; a Step is a boundary within it worth waiting at; a Sub-step is one
#: physical unit. There is deliberately no fourth level: an error is *content*
#: attached to whichever of these failed, not a depth of its own.
TASK = "task"
STEP = "step"
SUBSTEP = "substep"


@dataclass
class ReportingFrame:
    """One Task, Step or Sub-step, and what it cost.

    ``elapsed`` is None while the frame is open and a duration once it closes —
    which is the only honest way to hold it, because a frame that is still
    running has no elapsed time, it has an age.
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

        return self.elapsed if self.elapsed is not None else time.monotonic() - self.started

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

    Concrete hosts are :class:`~weaver.session.console.ConsoleSession` — Weaver
    driven from a console process, against either a local emulator or a Fabric
    workspace — and :class:`~weaver.session.notebook.NotebookSession`, where
    Weaver is itself executing inside the Fabric host.
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
        self._closed = False

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
                "this command needs a workspace: pass --workspace, or configure "
                "one in a workspace configuration file"
            )
        return resolved

    def scope(self, workspace: Workspace | None = None) -> "WorkspaceScope":
        """The cached resources for one workspace context, created on demand."""

        resolved = self.workspace_or_default(workspace)
        key = workspace_context(resolved)
        with self._scope_lock:
            if self._closed:
                raise CommandError("this session is closed")
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

        One per Session lifetime rather than one per operation. A resolver
        rebuilt for each call carries a cache that is always empty, so every
        operation re-asks the workspace what the same names mean.
        """

        return self.scope(workspace).resolver

    def store(self, workspace: Workspace | None = None):
        """The within-workspace store for this context."""

        return self.scope(workspace).store

    def transport_store(self, workspace: Workspace | None = None):
        """The store this host writes *across* the boundary with.

        The same thing as :meth:`store` wherever Weaver is already inside the
        workspace. A console reaching into Fabric has no within-workspace store
        at all, so this is where a bundle archive crosses — which is a Session
        concern, not something a Builder should know how to do.
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

        The program returns a value by calling ``emit(...)``; what comes back is
        that payload. A console reaching into Fabric crosses Livy here; a
        notebook is already there.
        """

    @abstractmethod
    def execute_spark_sql(
        self,
        statement: str,
        *,
        workspace: Workspace | None = None,
        timeout: float | None = None,
    ) -> Any:
        """Run one Spark SQL statement and return its rows."""

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
        :meth:`query_tsql`, and the two are separate because the SQL layer
        separates them — reading a result set from a statement that produces
        several answers with whichever came first, which is how a failing check
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

    def task_failed(self, name: str | None = None, error: BaseException | None = None) -> None:
        self._exit("task", name, error=error)

    def step_started(self, name: str, detail: str | None = None) -> None:
        self._enter("step", name, detail)

    def step_completed(self, name: str | None = None) -> None:
        self._exit("step", name)

    def step_failed(self, name: str | None = None, error: BaseException | None = None) -> None:
        self._exit("step", name, error=error)

    def substep_started(self, name: str, detail: str | None = None) -> None:
        self._enter("substep", name, detail)

    def substep_completed(self, name: str | None = None) -> None:
        self._exit("substep", name)

    def substep_failed(self, name: str | None = None, error: BaseException | None = None) -> None:
        self._exit("substep", name, error=error)

    # --- the paired form, which is the one to use ----------------------------
    #
    # A frame that is not closed is worse than one that was never opened: it
    # swallows everything nested under it and reports a duration for work that
    # stopped. So instrumentation is written as a `with`, and the explicit
    # started/completed pairs above remain for the few callers that genuinely
    # cannot bracket their work in one place.

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

    def _exit(self, kind: str, name: str | None, error: BaseException | None = None) -> None:
        for index in range(len(self._frames) - 1, -1, -1):
            frame = self._frames[index]
            if frame.kind == kind and (name is None or frame.name == name):
                self._close(frame, error=error)
                return

    def present(
        self, frame: ReportingFrame, event: str, error: BaseException | None = None
    ) -> None:
        """Show one reporting event. Silent by default; hosts specialise it."""

    def stop_presenting(self) -> None:
        """Take down anything a host is drawing. Nothing to do by default."""

    def warn(self, message: str) -> None:
        """Tell the operator something they should know but need not act on now.

        Overridable, so a notebook can render it and a test can collect it. The
        default reaches stderr because the alternative — a library that notices a
        problem and says nothing — is how a mismatched deployment stays invisible
        until it produces a confusing failure somewhere else.
        """

        import sys

        self.warnings.append(message)
        print(f"warning: {message}", file=sys.stderr)

    # --- teardown -----------------------------------------------------------

    def close(self) -> None:
        """Release every resource this Session acquired, and nothing it was given."""

        self.stop_presenting()
        with self._scope_lock:
            if self._closed:
                return
            self._closed = True
            scopes = list(self._scopes.values())
            self._scopes.clear()
        for scope in scopes:
            scope.close()
        if self._owns_executor:
            self._executor.shutdown(wait=False)


class WorkspaceScope:
    """One workspace's resolver, store and expensive resources, for a Session.

    Not a doer and not a second Session: it holds no execution semantics, only
    the things that are expensive to acquire and safe to share for as long as
    the Session lives.
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


def is_fabric(workspace: Workspace) -> bool:
    return isinstance(workspace, FabricWorkspace)


def is_local(workspace: Workspace) -> bool:
    return isinstance(workspace, LocalWorkspace)


__all__ = [
    "STEP",
    "SUBSTEP",
    "TASK",
    "ReportingFrame",
    "Session",
    "WorkspaceScope",
    "is_fabric",
    "is_local",
    "workspace_context",
]
