"""Plan and execute a RunGraph through an injected primitive dispatcher."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Sequence

from .graph import RunGraph, graph_for
from .result import (
    BLOCKED,
    FAILED,
    INVALID,
    PENDING,
    SKIPPED,
    SUCCEEDED,
    SUCCEEDED_WITH_REJECTS,
    VALIDATED,
    RunNodeResult,
    RunResult,
    run_status,
)
from .state import RunState


def node_label(node) -> str:
    """Return a display label that includes the target and logical object."""

    from .resolution import ENDPOINT_REFRESH

    target = getattr(node, "physical_target", None)
    if node.primitive_kind == ENDPOINT_REFRESH:
        return f"Refresh {target} SQL endpoint"

    what = node.logical_id
    if what is None:
        # Barriers and directly constructed nodes have no logical label.
        return node.node_id
    name = getattr(getattr(what, "object_id", None), "qualified", None) or str(what)
    # Validations use their Test or Assumption kind in the display label.
    verb = "Load" if node.role == LOAD else "Test"
    return f"{verb} {target}/{name}" if target is not None else f"{verb} {name}"


@contextmanager
def _node_substep(session, node):
    """Return a node timing frame when the Runner has a Session."""

    if session is None or not hasattr(session, "substep"):
        yield None
        return
    with session.substep(node_label(node)) as frame:
        yield frame


#: Run every loadable object installed in the requested targets.
LOAD = "load"
#: Run the installed Tests and Assumptions in the requested targets.
TEST = "test"


@dataclass(frozen=True)
class RunRequest:
    """The requested scope and the policy the run is executed under."""

    kind: str
    targets: tuple
    #: One installed node by name, where the caller asked for exactly one.
    name: str | None = None
    #: Exact installed loadables by ``Schema.Object``. ``load`` only.
    names: tuple[str, ...] = ()
    #: A source file compiled and run without being installed. ``test`` only.
    file: str | None = None
    #: Continue independent branches after a node fails, and report.
    fault_tolerant: bool = False
    #: Plan, resolve and report without dispatching anything.
    dry_run: bool = False

    def __post_init__(self) -> None:
        from ..errors import CommandError

        if not self.targets:
            raise CommandError(f"{self.kind} needs at least one target")
        if self.name is not None and self.file is not None:
            raise CommandError(
                "a run selects name= or file=, not both — one names something "
                "the estate has and the other something it may not"
            )
        if self.kind == LOAD and (self.name is not None or self.file is not None):
            raise CommandError("a load selects installed objects with names=")
        if self.kind == TEST and self.names:
            raise CommandError("a test selects one installed validation with name=")

    @classmethod
    def load(cls, targets: Sequence, **policy) -> "RunRequest":
        policy["names"] = tuple(policy.get("names") or ())
        return cls(kind=LOAD, targets=tuple(targets), **policy)

    @classmethod
    def test(cls, targets: Sequence, **policy) -> "RunRequest":
        return cls(kind=TEST, targets=tuple(targets), **policy)

    @property
    def selection(self) -> str | tuple[str, ...] | None:
        """What was selected within the targets, for a report to record."""

        if self.file is not None:
            return self.file
        if self.name is not None:
            return self.name
        return self.names or None

    def to_mapping(self) -> dict:
        return {
            "kind": self.kind,
            "targets": [str(target) for target in self.targets],
            "name": self.name,
            "names": list(self.names),
            "file": self.file,
            "fault_tolerant": self.fault_tolerant,
            "dry_run": self.dry_run,
        }


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _blocked_by(node, upstream, *, validated: bool = False):
    """Return the dependency-blocked message for a node."""

    from .result import DEPENDENCY_BLOCKED, error

    what = "did not validate" if validated else "did not succeed"
    return error(
        DEPENDENCY_BLOCKED,
        f"{node.node_id} cannot run: " + ", ".join(sorted(upstream)) + f" {what}",
        source="run.runner",
    )


class Runner:
    """Execute one run graph and collect its results."""

    def __init__(
        self,
        state: RunState,
        request: RunRequest,
        *,
        workspace: object | None = None,
        can_refresh: bool = True,
    ) -> None:
        self.state = state
        self.request = request
        self.workspace = workspace
        #: Whether this host has a SQL analytics endpoint.
        self.can_refresh = can_refresh
        self._graph: RunGraph | None = None
        self._events: list[dict] = []
        self._runtime_scope = None

    @property
    def graph(self) -> RunGraph:
        """Return the graph implied by this request."""

        if self._graph is None:
            self._graph = graph_for(self.request, self.state)
        return self._graph

    def plan(self) -> RunGraph:
        return self.graph

    @property
    def events(self) -> tuple[dict, ...]:
        """Return the settled node events in order."""

        return tuple(self._events)

    def resolve(self, node):
        """Derive the node's dispatch address from the graph."""

        from .resolution import resolve

        return resolve(node, can_refresh=self.can_refresh)

    def runtime_scope(self, session=None):
        """This run's deployed-module scope, held unopened until something imports.

        A Warehouse-only run never calls ``get()``, so it opens no scope and,
        on a desktop, submits nothing to open one.
        """

        if self._runtime_scope is None:
            from .runtime_boundary import LazyRunScope, open_runtime_scope

            self._runtime_scope = LazyRunScope(
                lambda: open_runtime_scope(
                    session,
                    workspace=self.workspace,
                    # Read once for the run, handed to whatever imports a module.
                    catalogue=self.state.catalogue,
                )
            )
        return self._runtime_scope

    def _close_runtime(self) -> None:
        """Close the run's deployed-module scope."""

        holder, self._runtime_scope = self._runtime_scope, None
        if holder is not None:
            holder.close()

    def run(
        self,
        *,
        session: object | None = None,
        dispatch: Callable | None = None,
        on_node: Callable | None = None,
    ) -> RunResult:
        """Execute the graph and return a result for every planned node.

        ``on_node`` receives each result when its status settles.
        """

        started = _now()
        graph = self.graph
        ordered = graph.order()

        if self.request.dry_run:
            return self._result(self._dry_run(ordered), started=started)

        if dispatch is None:
            from .dispatch import dispatch_primitive

            dispatch = dispatch_primitive

        statuses: dict[str, str] = {node.node_id: PENDING for node in ordered}
        results: dict[str, RunNodeResult] = {}
        stopped = False
        # Always close imported modules before another run can reuse them.

        def settle(result: RunNodeResult, status: str | None = None) -> None:
            results[result.node_id] = result
            if status is not None:
                statuses[result.node_id] = status
            self._events.append({"node": result.node_id, "status": result.status})
            if on_node is not None:
                on_node(result)

        def _execute() -> None:
            nonlocal stopped

            for node in ordered:
                blocking = self._blocking(node, statuses)
                if blocking:
                    settle(
                        self._settled(
                            node, BLOCKED, messages=(_blocked_by(node, blocking),)
                        ),
                        BLOCKED,
                    )
                    continue
                if stopped:
                    # Fail-fast leaves otherwise-ready nodes pending.
                    settle(self._settled(node, PENDING))
                    continue

                resolved = self.resolve(node)
                if not resolved.valid:
                    # Invalid means resolution failed before dispatch.
                    settle(
                        self._settled(
                            node,
                            INVALID,
                            messages=resolved.messages,
                            location=resolved.dispatch_location,
                        ),
                        FAILED,
                    )
                    if not self.request.fault_tolerant:
                        stopped = True
                    continue
                if resolved.unsupported:
                    # Skip nodes unsupported by the current host.
                    settle(
                        self._settled(
                            node,
                            SKIPPED,
                            messages=resolved.messages,
                            location=resolved.dispatch_location,
                        ),
                        SKIPPED,
                    )
                    continue

                settle(
                    self._dispatched(
                        node, dispatch=dispatch, session=session, resolved=resolved
                    ),
                    None,
                )
                status = results[node.node_id].status
                statuses[node.node_id] = status
                if status == FAILED and not self.request.fault_tolerant:
                    stopped = True

        try:
            _execute()
        finally:
            self._close_runtime()

        nodes = tuple(results[node.node_id] for node in ordered)
        return self._result(nodes, started=started)

    def _dry_run(self, ordered) -> tuple:
        """Resolve and classify every node without dispatching it."""

        resolutions = {node.node_id: self.resolve(node) for node in ordered}
        invalid = {node_id for node_id, one in resolutions.items() if not one.valid}
        blocked: dict[str, set[str]] = {}
        for node_id in invalid:
            for downstream in self.graph.descendants(node_id):
                blocked.setdefault(downstream, set()).add(node_id)

        settled = []
        for node in ordered:
            resolved = resolutions[node.node_id]
            causes = sorted(blocked.get(node.node_id, ()))
            if resolved.valid and causes:
                settled.append(
                    self._settled(
                        node,
                        BLOCKED,
                        messages=(_blocked_by(node, causes, validated=True),),
                        location=resolved.dispatch_location,
                    )
                )
            elif resolved.valid:
                # Unsupported nodes are valid but would be skipped.
                settled.append(
                    self._settled(
                        node,
                        VALIDATED,
                        messages=resolved.messages,
                        location=resolved.dispatch_location,
                    )
                )
            else:
                settled.append(
                    self._settled(
                        node,
                        INVALID,
                        messages=resolved.messages,
                        location=resolved.dispatch_location,
                    )
                )
        return tuple(settled)

    def _dispatched(self, node, *, dispatch, session, resolved=None) -> RunNodeResult:
        """Dispatch one node and record failures as node results."""

        from .outcome import settle

        started = _now()
        location = getattr(resolved, "dispatch_location", None)
        # Record one timing frame for each dispatched node.
        with _node_substep(session, node) as frame:
            try:
                returned = dispatch(
                    node,
                    session=session,
                    state=self.state,
                    resolved=resolved,
                    fault_tolerant=self.request.fault_tolerant,
                    open_runtime=self.runtime_scope(session),
                    workspace=self.workspace,
                )
            except Exception as exc:  # noqa: BLE001 - failures become node results
                # Do not intercept process-control exceptions.
                outcome = settle(node, raised=exc)
            else:
                outcome = settle(node, returned=returned)
            if frame is not None and outcome.status == FAILED:
                frame.failed = True
        return self._settled(
            node,
            outcome.status,
            executed=True,
            location=location,
            result=outcome.result,
            messages=outcome.messages,
            started_at=started,
            raised=outcome.raised,
            refused=outcome.refused,
        )

    #: Upstream outcomes that permit downstream work.
    _SATISFIED = (SUCCEEDED, SUCCEEDED_WITH_REJECTS, SKIPPED, VALIDATED)

    def _blocking(self, node, statuses) -> tuple[str, ...]:
        return tuple(
            sorted(
                upstream
                for upstream in self.graph.upstream(node.node_id)
                if statuses.get(upstream) not in self._SATISFIED
            )
        )

    def _settled(
        self,
        node,
        status: str,
        *,
        executed: bool = False,
        messages: tuple = (),
        result: object = None,
        started_at: str | None = None,
        location: str | None = None,
        raised: bool = False,
        refused: bool = False,
    ) -> RunNodeResult:
        target_type = getattr(node.physical_target, "kind", None)
        if target_type:
            target_type = str(target_type).title()
        target_name = getattr(node.physical_target, "name", None)
        object_id = getattr(node.logical_id, "object_id", None)
        return RunNodeResult(
            node_id=node.node_id,
            physical_target=str(node.physical_target),
            primitive_kind=node.primitive_kind,
            dispatch_location=location,
            role=node.role,
            raised=raised,
            logical_id=str(node.logical_id) if node.logical_id else None,
            status=status,
            refused=refused,
            executed=executed,
            messages=messages,
            result=result,
            started_at=started_at,
            finished_at=_now() if executed else None,
            target_type=target_type,
            target_name=target_name,
            schema_name=getattr(object_id, "schema", None),
            object_name=getattr(object_id, "object", None),
        )

    def _result(self, nodes, *, started: str) -> RunResult:
        graph = self.graph
        return RunResult(
            kind=self.request.kind,
            requested=tuple(str(target) for target in self.request.targets),
            status=run_status(nodes, dry_run=self.request.dry_run),
            dry_run=self.request.dry_run,
            fault_tolerant=self.request.fault_tolerant,
            nodes=nodes,
            edges=graph.edges,
            order=tuple(node.node_id for node in graph.order()),
            messages=graph.messages,
            selection=self.request.selection,
            started_at=started,
            finished_at=_now(),
            workspace=(
                None
                if self.workspace is None
                else str(getattr(self.workspace, "workspace", self.workspace))
            ),
        )


__all__ = ["Runner"]
