"""Plan and execute a runtime graph."""

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
    findings,
    rows_moved,
    run_status,
)
from .state import RunState


def node_label(node) -> str:
    """Label a node with its action and canonical logical identity."""

    from .resolution import ENDPOINT_REFRESH, ONELAKE_PUBLICATION

    target = getattr(node, "physical_target", None)
    if node.primitive_kind == ENDPOINT_REFRESH:
        return f"Refresh {target} SQL endpoint"
    if node.primitive_kind == ONELAKE_PUBLICATION:
        consumers = ", ".join(
            sorted({str(one.target) for one in node.publication_targets})
        )
        waiting = f"{target} to {consumers}" if consumers else str(target)
        return f"Wait for OneLake publication: {waiting}"

    what = node.logical_id
    if what is None:
        return node.node_id
    # Validations use their Test or Assumption kind in the display label.
    verb = "Load" if node.role == LOAD else "Test"
    return f"{verb} {what}"


@contextmanager
def _node_substep(session, node, *, concurrent: bool = False):

    if session is None or not hasattr(session, "substep"):
        yield None
        return
    opened = session.concurrent_substep if concurrent else session.substep
    with opened(node_label(node)) as frame:
        yield frame


def _conclude(frame, node, outcome) -> None:
    """Mark a node's frame with its outcome before the frame closes."""

    from .resolution import ENDPOINT_REFRESH, ONELAKE_PUBLICATION

    if frame is None:
        return
    if outcome.status == FAILED:
        frame.failed = True
    if outcome.status == SKIPPED:
        frame.skipped = True
        frame.note = "skipped"
        return
    if getattr(node, "installed", None) is not None:
        frame.note = findings(outcome.result) if frame.failed else None
    elif not frame.failed and node.primitive_kind not in (
        ENDPOINT_REFRESH,
        ONELAKE_PUBLICATION,
    ):
        frame.note = rows_moved(outcome.result)


#: Run every loadable object installed in the requested logical items.
LOAD = "load"
#: Run the installed Tests and Assumptions in the requested logical items.
TEST = "test"


@dataclass(frozen=True)
class RunRequest:
    """A run selection and its execution policy.

    Item selection remains logical even when items share a physical target.
    """

    kind: str
    items: tuple
    #: Regular expressions over installed names; only what they match runs.
    names: tuple[str, ...] = ()
    #: The installed loadables this run may execute, by logical identity.
    #: ``None`` runs every loadable the requested items own. ``load`` only.
    selected: tuple | None = None
    #: Continue through settled dependency failures, and report each outcome.
    fault_tolerant: bool = False
    #: Plan, resolve and report without dispatching anything.
    dry_run: bool = False
    #: Reconstruct each selected table from zero. ``load`` only, and local to
    #: what the request selected.
    reload: bool = False
    #: Waive the declared delete and update stability limits for this run only.
    #: ``load`` only. It waives nothing else: null and unique key checks, fault
    #: tolerance, selection and bookmarks are untouched.
    ignore_stability_threshold: bool = False

    def __post_init__(self) -> None:
        from ..errors import CommandError

        if not self.items:
            raise CommandError(f"{self.kind} needs at least one item")
        if self.selected is not None and self.kind != LOAD:
            raise CommandError("selected= applies only to loads")
        if self.reload and self.kind != LOAD:
            raise CommandError("reload applies only to loads")
        if self.ignore_stability_threshold and self.kind != LOAD:
            raise CommandError("ignore_stability_threshold applies only to loads")

    @classmethod
    def load(cls, items: Sequence, **policy) -> "RunRequest":
        policy["names"] = tuple(policy.get("names") or ())
        selected = policy.get("selected")
        policy["selected"] = None if selected is None else tuple(selected)
        return cls(kind=LOAD, items=tuple(items), **policy)

    @classmethod
    def test(cls, items: Sequence, **policy) -> "RunRequest":
        policy["names"] = tuple(policy.get("names") or ())
        return cls(kind=TEST, items=tuple(items), **policy)

    @property
    def selection(self) -> tuple[str, ...] | None:
        return self.names or None

    @classmethod
    def from_mapping(cls, payload) -> "RunRequest":
        from ..declaration.model import WeaverDocumentId, WeaverItemId

        selected = payload.get("selected")
        return cls(
            kind=payload["kind"],
            items=tuple(WeaverItemId.parse(one) for one in payload["items"]),
            names=tuple(payload.get("names") or ()),
            selected=None
            if selected is None
            else tuple(WeaverDocumentId.parse(one) for one in selected),
            fault_tolerant=bool(payload.get("fault_tolerant")),
            dry_run=bool(payload.get("dry_run")),
            reload=bool(payload.get("reload")),
            ignore_stability_threshold=bool(payload.get("ignore_stability_threshold")),
        )

    def to_mapping(self) -> dict:
        return {
            "kind": self.kind,
            "items": [str(item) for item in self.items],
            "names": list(self.names),
            "selected": None
            if self.selected is None
            else [str(one) for one in self.selected],
            "fault_tolerant": self.fault_tolerant,
            "dry_run": self.dry_run,
            "reload": self.reload,
            "ignore_stability_threshold": self.ignore_stability_threshold,
        }


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class Lanes:
    """How many nodes of each kind a run keeps going at once.

    A Python primitive, a load or a Lakehouse validation, that starts beside
    others runs in a Spark session of its own, in the host's one Spark
    application. A Warehouse procedure holds a connection of its own Warehouse.
    Anything else is a short wait.
    """

    spark: int = 4
    warehouse: int = 4
    other: int = 4

    @classmethod
    def configured(cls, workspace) -> "Lanes":
        """The lanes a Workspace's ``execution.run`` sets."""

        if workspace is None:
            return cls()
        run = workspace.execution.run
        return cls(spark=run.spark_concurrency, warehouse=run.warehouse_concurrency)

    def of(self, node) -> tuple:
        from .resolution import SPARK_KINDS, WAREHOUSE_PROCEDURE

        if node.primitive_kind in SPARK_KINDS:
            return ("spark",)
        if node.primitive_kind == WAREHOUSE_PROCEDURE:
            return ("warehouse", getattr(node.physical_target, "name", ""))
        return ("other",)

    def limit(self, lane: tuple) -> int:
        return {"spark": self.spark, "warehouse": self.warehouse}.get(
            lane[0], self.other
        )


def needs_spark(graph) -> bool:
    """Whether a run graph executes anything on Spark."""

    from .resolution import SPARK_KINDS

    return any(node.primitive_kind in SPARK_KINDS for node in graph.nodes)


def _blocked_by(node, upstream, *, validated: bool = False):

    from .result import DEPENDENCY_BLOCKED, error

    what = "did not validate" if validated else "did not succeed"
    return error(
        DEPENDENCY_BLOCKED,
        f"Cannot run {node.node_id}: " + ", ".join(sorted(upstream)) + f" {what}",
        source="run.runner",
    )


class Runner:
    """Execute a runtime graph and collect its results."""

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
        self._publication = None

    @property
    def graph(self) -> RunGraph:

        if self._graph is None:
            self._graph = graph_for(self.request, self.state)
        return self._graph

    def plan(self) -> RunGraph:
        return self.graph

    @property
    def events(self) -> tuple[dict, ...]:

        return tuple(self._events)

    def resolve(self, node):

        from .resolution import resolve

        return resolve(node, can_refresh=self.can_refresh)

    def runtime_scope(self, session=None):
        """Open the deployed-module scope only when a node imports from it."""

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

    @property
    def publication(self):

        if self._publication is None:
            from .publication import PublicationLedger
            from .resolution import ONELAKE_PUBLICATION

            self._publication = PublicationLedger(
                frozenset(
                    node.produced_by
                    for node in self.graph.nodes
                    if node.primitive_kind == ONELAKE_PUBLICATION and node.produced_by
                )
            )
        return self._publication

    def _close_runtime(self) -> None:

        holder, self._runtime_scope = self._runtime_scope, None
        if holder is not None:
            holder.close()

    def run(
        self,
        *,
        session: object | None = None,
        dispatch: Callable | None = None,
        on_node: Callable | None = None,
        before_node: Callable | None = None,
        lanes: Lanes | None = None,
    ) -> RunResult:
        """Execute the graph and return every planned node's result.

        ``before_node`` runs only before dispatch. ``on_node`` runs whenever a
        node settles, including blocked, skipped and unresolved nodes.

        ``lanes`` runs independent nodes at once, within those limits. Without
        them nodes run one at a time in graph order. Either way a node settles,
        and ``on_node`` sees it, in this thread.
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
                        INVALID,
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
                        node,
                        dispatch=dispatch,
                        session=session,
                        resolved=resolved,
                        before=before_node,
                    ),
                    None,
                )
                status = results[node.node_id].status
                statuses[node.node_id] = status
                if status == FAILED and not self.request.fault_tolerant:
                    stopped = True

        try:
            if lanes is None:
                _execute()
            else:
                self._execute_concurrently(
                    ordered,
                    statuses=statuses,
                    settle=settle,
                    session=session,
                    dispatch=dispatch,
                    before=before_node,
                    lanes=lanes,
                )
        finally:
            self._close_runtime()

        nodes = tuple(results[node.node_id] for node in ordered)
        return self._result(nodes, started=started)

    def _execute_concurrently(
        self,
        ordered,
        *,
        statuses,
        settle,
        session,
        dispatch,
        before,
        lanes: Lanes,
    ) -> None:
        """Dispatch every node whose upstream has settled, within ``lanes``.

        Each node is decided as the serial run decides it, once its upstream has
        settled. Of the nodes ready at once, the one with the longest chain still
        beneath it starts first, so a barrier such as an endpoint refresh is
        reached early and the work behind it overlaps the rest; ties keep graph
        order. A failure without fault tolerance starts nothing more: what is
        running finishes and settles, and what had not started is left pending.
        """

        from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait

        # Made here, once, rather than by whichever worker first asks.
        self.publication
        self.runtime_scope(session)
        heights = self.graph.topology.heights()
        waiting = sorted(ordered, key=lambda node: -heights[node.node_id])
        decided: set[str] = set()
        running: dict = {}
        occupied: dict = {}
        stopped = False

        def record(result: RunNodeResult, status: str | None = None) -> None:
            nonlocal stopped
            settle(result, status)
            decided.add(result.node_id)
            statuses[result.node_id] = result.status
            if result.status in (FAILED, INVALID) and not self.request.fault_tolerant:
                stopped = True

        def runnable() -> list:
            found = []
            for node in list(waiting):
                if not self.graph.upstream(node.node_id) <= decided:
                    continue
                blocking = self._blocking(node, statuses)
                if blocking:
                    waiting.remove(node)
                    record(
                        self._settled(
                            node, BLOCKED, messages=(_blocked_by(node, blocking),)
                        )
                    )
                    continue
                if stopped:
                    waiting.remove(node)
                    record(self._settled(node, PENDING))
                    continue
                resolved = self.resolve(node)
                if not resolved.valid:
                    waiting.remove(node)
                    record(
                        self._settled(
                            node,
                            INVALID,
                            messages=resolved.messages,
                            location=resolved.dispatch_location,
                        )
                    )
                    continue
                if resolved.unsupported:
                    waiting.remove(node)
                    record(
                        self._settled(
                            node,
                            SKIPPED,
                            messages=resolved.messages,
                            location=resolved.dispatch_location,
                        )
                    )
                    continue
                found.append((node, resolved))
            return found

        def prepared(node) -> RunNodeResult | None:
            """Run ``before`` here, where the run's record is written."""

            if before is None:
                return None
            from .outcome import settle as outcome

            try:
                before(node)
            except Exception as exc:  # noqa: BLE001 - failures become node results
                raised = outcome(node, raised=exc)
                return self._settled(
                    node,
                    raised.status,
                    executed=True,
                    result=raised.result,
                    messages=raised.messages,
                    started_at=_now(),
                    raised=raised.raised,
                    refused=raised.refused,
                )
            return None

        def admit(pool) -> None:
            for node, resolved in runnable():
                lane = lanes.of(node)
                if occupied.get(lane, 0) >= lanes.limit(lane):
                    continue
                waiting.remove(node)
                failed = prepared(node)
                if failed is not None:
                    record(failed)
                    continue
                # Alone on the host's Spark, a node needs no session of its own.
                isolated = lane == ("spark",) and occupied.get(lane, 0) > 0
                occupied[lane] = occupied.get(lane, 0) + 1
                future = pool.submit(
                    self._dispatched,
                    node,
                    dispatch=dispatch,
                    session=session,
                    resolved=resolved,
                    concurrent=True,
                    isolated=isolated,
                )
                running[future] = lane

        workers = lanes.spark + lanes.warehouse + lanes.other
        with ThreadPoolExecutor(max_workers=workers) as pool:
            while True:
                admit(pool)
                if not running:
                    break
                done, _pending = wait(running, return_when=FIRST_COMPLETED)
                for future in done:
                    occupied[running.pop(future)] -= 1
                    record(future.result())

    def _dry_run(self, ordered) -> tuple:

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

    def _dispatched(
        self,
        node,
        *,
        dispatch,
        session,
        resolved=None,
        before=None,
        concurrent=False,
        isolated=False,
    ) -> RunNodeResult:
        """Dispatch one node, treating ``before`` failures as node failures."""

        from .outcome import settle

        started = _now()
        location = getattr(resolved, "dispatch_location", None)
        # Record one timing frame for each dispatched node.
        with _node_substep(session, node, concurrent=concurrent) as frame:
            try:
                if before is not None:
                    before(node)
                returned = dispatch(
                    node,
                    session=session,
                    state=self.state,
                    resolved=resolved,
                    fault_tolerant=self.request.fault_tolerant,
                    reload=self.request.reload,
                    ignore_stability_threshold=(
                        self.request.ignore_stability_threshold
                    ),
                    open_runtime=self.runtime_scope(session),
                    workspace=self.workspace,
                    publication=self.publication,
                    isolated=isolated,
                )
            except Exception as exc:  # noqa: BLE001 - failures become node results
                # Do not intercept process-control exceptions.
                outcome = settle(node, raised=exc)
            else:
                outcome = settle(node, returned=returned)
            _conclude(frame, node, outcome)
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
        satisfied = (
            (*self._SATISFIED, FAILED)
            if self.request.fault_tolerant
            else self._SATISFIED
        )
        return tuple(
            sorted(
                upstream
                for upstream in self.graph.upstream(node.node_id)
                if statuses.get(upstream) not in satisfied
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
            target_type = str(node.physical_target).partition("/")[0]
        target_name = getattr(node.physical_target, "name", None)
        from ..catalogue.claims import catalogue_columns

        # Keyed as every catalogue table keys it, area and all, because these
        # two columns are what the ``_.Log`` row is written from.
        stored = (
            catalogue_columns(node.logical_id)
            if getattr(node.logical_id, "object_id", None) is not None
            else (None, None)
        )
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
            schema_name=stored[0],
            object_name=stored[1],
        )

    def _result(self, nodes, *, started: str) -> RunResult:
        graph = self.graph
        return RunResult(
            kind=self.request.kind,
            requested=tuple(str(item) for item in self.request.items),
            status=run_status(nodes, dry_run=self.request.dry_run),
            dry_run=self.request.dry_run,
            fault_tolerant=self.request.fault_tolerant,
            reload=self.request.reload,
            ignore_stability_threshold=self.request.ignore_stability_threshold,
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


__all__ = ["Lanes", "Runner"]
