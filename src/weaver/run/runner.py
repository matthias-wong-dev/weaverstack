"""Runner — what runs next, and what happened.

The fourth doer, and the owner of every piece of state that changes during a
run:

.. code-block:: text

    RunState + RunRequest
             ↓
          Runner
        ┌────┴────┐
    RunGraph   node state
        └────┬────┘
       dispatch one primitive
             ↓
         RunResult

**Planning needs no Session.** A Runner is constructed from a Catalogue and some
observed inventories — ordinary Python — so the whole of what a run *decides*
can be proven without Fabric, without Spark and without an estate:

.. code-block:: python

    runner = Runner(state, RunRequest.load(targets))
    assert [node.node_id for node in runner.graph.order()] == [...]

**Execution crosses outward at exactly one point.** ``dispatch`` is a callable,
not another doer: give it the real one and nodes run against the installed
estate; give it a controlled one and the whole state machine — readiness,
blocking, fail-fast, aggregation — is provable in milliseconds.

.. code-block:: python

    result = runner.run(session=session, dispatch=dispatch_primitive)
    result = runner.run(dispatch=controlled)          # no session at all

The Runner never learns which it got. That is what makes a fixture runtime
artefact indistinguishable from a production one, and it is why there is no
``test_mode`` anywhere in this file: the Registry indirection already points
nodes wherever the estate says, so a trivial fixture artefact is simply an
installed artefact that happens to be trivial.

One Runner serves load, test and whatever runtime work comes next. What differs
between them is which nodes are selected and which primitive runs — not how a
run behaves when one of them fails.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Callable

from .graph import RunGraph, graph_for
from .request import RunRequest
from .result import (
    BLOCKED,
    SUCCEEDED_WITH_REJECTS,
    FAILED,
    INVALID,
    PENDING,
    SKIPPED,
    SUCCEEDED,
    VALIDATED,
    RunNodeResult,
    RunResult,
    run_status,
)
from .state import RunState


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _blocked_by(node, upstream, *, validated: bool = False):
    """Why this node may not run, said with the code a reader can filter on."""

    from .messages import DEPENDENCY_BLOCKED, error

    what = "did not validate" if validated else "did not succeed"
    return error(
        DEPENDENCY_BLOCKED,
        f"{node.node_id} cannot run: " + ", ".join(sorted(upstream)) + f" {what}",
        source="run.runner",
    )


class Runner:
    """One runtime execution: its graph, its node state, and its result."""

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
        #: Whether this host has a SQL analytics endpoint to refresh at all. The
        #: emulator has none, which is an honest absence rather than a fault, so
        #: a refresh node is skipped there rather than failed.
        self.can_refresh = can_refresh
        self._graph: RunGraph | None = None
        self._events: list[dict] = []
        self._runtime_scope = None

    # --- planning -----------------------------------------------------------

    @property
    def graph(self) -> RunGraph:
        """The graph this request implies. Planned once, then inspectable."""

        if self._graph is None:
            self._graph = graph_for(self.request, self.state)
        return self._graph

    def plan(self) -> RunGraph:
        return self.graph

    @property
    def events(self) -> tuple[dict, ...]:
        """What the run did, in order. The source a log sink is written from."""

        return tuple(self._events)

    # --- resolution ---------------------------------------------------------

    def resolve(self, node):
        """Whether this node's target and primitive are actually there.

        Kept ahead of dispatch because the two fail for entirely different
        reasons and a reader deserves to be told which: a wrong graph is a
        planning fault, and a right graph pointing at an estate that is not
        there is a missing installation. Answered from the observed snapshot,
        never from a live connection — the reading happened once, above.
        """

        from .resolution import Resolved, resolve

        if not self.request.verifies_estate:
            # This run reads rather than writes, so an absent thing is its own
            # dispatch's answer to give. Claiming absence from an inventory
            # nobody read would be inventing a finding.
            return Resolved(node=node, target_present=True, primitive_present=True)
        return resolve(node, self.state, can_refresh=self.can_refresh)

    # --- the run's own runtime ----------------------------------------------

    @property
    def runtime_scope(self):
        """Where this run's deployed Python modules live, and how long they live.

        One scope per run, because a Fabric session outlives a build and a build
        rewrites deployed Python in place — so a module kept past the run that
        imported it is a module the next load would use instead of the one now
        on disk.
        """

        if self._runtime_scope is None:
            from ..runtime.python_context import RuntimeScope

            self._runtime_scope = RuntimeScope.new()
        return self._runtime_scope

    def _close_runtime(self) -> None:
        """Every module this run imported goes with it."""

        scope, self._runtime_scope = self._runtime_scope, None
        if scope is not None:
            scope.close()

    # --- execution ----------------------------------------------------------

    def run(
        self,
        *,
        session: object | None = None,
        dispatch: Callable | None = None,
        on_node: Callable | None = None,
    ) -> RunResult:
        """Execute the graph and return the whole result.

        ``on_node`` receives **every** planned node's result at the moment its
        status settles — executed, blocked, skipped or pending alike — which is
        how durable evidence reaches a sink without this class knowing what a
        file is. One record per planned node, in graph order: the alternative is
        a log that says which nodes ran and leaves a reader to infer what became
        of the rest, exactly when inference is least safe.
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

        def settle(result: RunNodeResult, status: str | None = None) -> None:
            results[result.node_id] = result
            if status is not None:
                statuses[result.node_id] = status
            self._events.append({"node": result.node_id, "status": result.status})
            if on_node is not None:
                on_node(result)

        for node in ordered:
            blocking = self._blocking(node, statuses)
            if blocking:
                settle(
                    self._settled(node, BLOCKED, messages=(_blocked_by(node, blocking),)),
                    BLOCKED,
                )
                continue
            if stopped:
                # Fail-fast stops *scheduling*, not reporting. A node whose own
                # dependencies were fine is not blocked — it simply never
                # started, and saying so beats inventing a failure for it.
                settle(self._settled(node, PENDING))
                continue

            resolved = self.resolve(node)
            if not resolved.valid:
                # Invalid, not failed: nothing ran. A reader distinguishing "the
                # primitive reported failure" from "there was nothing to run" is
                # asking a question the status should answer.
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
                # A capability this host does not have. The node is omitted
                # rather than failed, exactly as the build's own executor skips.
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

        nodes = tuple(results[node.node_id] for node in ordered)
        self._close_runtime()
        return self._result(nodes, started=started)

    def _dry_run(self, ordered) -> tuple:
        """The whole run, resolved and classified, with nothing executed.

        A validation status is never an execution status: a node that resolved
        is ``validated``, not ``succeeded``. And a dry run still says what could
        not run — a node whose upstream did not resolve is reported blocked,
        because "everything validated except the four things that depend on the
        one that did not" is the answer a dry run exists to give.
        """

        resolutions = {node.node_id: self.resolve(node) for node in ordered}
        invalid = {
            node_id for node_id, one in resolutions.items() if not one.valid
        }
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
                # Validated even where the host cannot perform it: a dry run
                # says what *would* happen, and "this host would skip it" is a
                # warning on a node that resolved, not an outcome of its own.
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
        """One node, run. A failure is data here, not an exception.

        Unconditionally so, whatever fault tolerance says: the run records what
        happened before it decides what to do about it, and deciding belongs to
        the operation once the whole graph is recorded.
        """

        from .outcome import settle

        started = _now()
        location = getattr(resolved, "dispatch_location", None)
        try:
            returned = dispatch(
                node,
                session=session,
                state=self.state,
                resolved=resolved,
                fault_tolerant=self.request.fault_tolerant,
                runtime_scope=self.runtime_scope,
                workspace=self.workspace,
            )
        except Exception as exc:  # noqa: BLE001 - a failed node is a result
            # Deliberately not BaseException. A KeyboardInterrupt or a
            # SystemExit is the operator or the process saying stop, not a
            # primitive reporting failure — recording one as a failed node
            # would swallow Ctrl-C at the prompt and leave a run that looks
            # like it decided something.
            outcome = settle(node, raised=exc)
        else:
            outcome = settle(node, returned=returned)
        return self._settled(
            node,
            outcome.status,
            executed=True,
            location=location,
            result=outcome.result,
            messages=outcome.messages,
            started_at=started,
            raised=outcome.raised,
        )

    #: An upstream in one of these did not stop the work downstream of it.
    #: Rejects belong here: the primitive wrote the valid rows and reported the
    #: refusal, so what a consumer reads is there — blocking on it would stop a
    #: run that partially succeeded from finishing the parts that were fine.
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
    ) -> RunNodeResult:
        return RunNodeResult(
            node_id=node.node_id,
            physical_target=str(node.physical_target),
            primitive_kind=node.primitive_kind,
            dispatch_location=location,
            role=node.role,
            raised=raised,
            logical_id=str(node.logical_id) if node.logical_id else None,
            status=status,
            executed=executed,
            messages=messages,
            result=result,
            started_at=started_at,
            finished_at=_now() if executed else None,
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
