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

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Sequence

from .graph import RunGraph, graph_for
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


def node_label(node) -> str:
    """What to call this node on screen: a verb and the thing it acts on.

    ``node_id`` is an identifier and reads like one — ``load:Lakehouse/Sales/
    Sales.Customer`` — which is right for a report that has to be matched up
    later and wrong for a line someone is watching go by. The id is unchanged
    and still what results carry; this is only what the frame is called.

    The target comes along because two Lakehouses can hold the same object
    name, and without it a reader watching a two-target run cannot tell which
    ``Sales.Customer`` is in flight.
    """

    from .resolution import ENDPOINT_REFRESH

    target = getattr(node, "physical_target", None)
    if node.primitive_kind == ENDPOINT_REFRESH:
        return f"Refresh the SQL endpoint for {target}"

    what = node.logical_id
    if what is None:
        # Nothing logical to name — a barrier, or a node built straight from an
        # id. The id is the only true answer, and inventing "Load None" would
        # be worse than the identifier this is trying to improve on.
        return node.node_id
    name = getattr(getattr(what, "object_id", None), "qualified", None) or str(what)
    # A load node's role is "load"; a validation carries its own kind — "Test",
    # "Assumption" — so anything that is not a load is something being checked.
    verb = "Load" if node.role == LOAD else "Check"
    return f"{verb} {name} in {target}" if target is not None else f"{verb} {name}"


@contextmanager
def _node_substep(session, node):
    """One node's timing frame, where there is a Session to record it on.

    A run-cycle test constructs a Runner with no Session at all — that is the
    whole point of the dispatch seam — so this yields ``None`` rather than
    making the Runner's timing depend on having crossed anything.
    """

    if session is None or not hasattr(session, "substep"):
        yield None
        return
    with session.substep(node_label(node)) as frame:
        yield frame


# --- what a run was asked for -------------------------------------------------
#
# Here rather than in a module of its own: the Runner is what a request is
# for, and reading one meant opening the other anyway.

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
    #: A source file compiled and run without being installed. ``test`` only.
    file: str | None = None
    #: Continue independent branches after a node fails, and report.
    fault_tolerant: bool = False
    #: Plan, resolve and report without dispatching anything.
    dry_run: bool = False
    #: Whether resolution should require the estate to be there before running.
    #:
    #: A load is about to *write*, so a missing target or an uninstalled
    #: artefact is a reason not to start — and saying which of the two it was is
    #: the point of resolving ahead of dispatching. A validation *reads*: if
    #: what it reads is not there, its own dispatch fails with a message about
    #: the thing that was missing, which is more precise than anything an
    #: inventory could say ahead of time.
    verifies_estate: bool = True

    def __post_init__(self) -> None:
        from ..errors import CommandError

        if not self.targets:
            raise CommandError(f"{self.kind} needs at least one target")
        if self.name is not None and self.file is not None:
            raise CommandError(
                "a run selects name= or file=, not both — one names something "
                "the estate has and the other something it may not"
            )

    @classmethod
    def load(cls, targets: Sequence, **policy) -> "RunRequest":
        return cls(kind=LOAD, targets=tuple(targets), **policy)

    @classmethod
    def test(cls, targets: Sequence, **policy) -> "RunRequest":
        policy.setdefault("verifies_estate", False)
        return cls(kind=TEST, targets=tuple(targets), **policy)

    @property
    def selection(self) -> str | None:
        """What was selected within the targets, for a report to record."""

        return self.file if self.file is not None else self.name

    def to_mapping(self) -> dict:
        return {
            "kind": self.kind,
            "targets": [str(target) for target in self.targets],
            "name": self.name,
            "file": self.file,
            "fault_tolerant": self.fault_tolerant,
            "dry_run": self.dry_run,
            # Behaviourally significant, so it is in the handover. A request
            # that crossed a boundary without it would arrive meaning something
            # else — preflighting an estate the caller said not to.
            "verifies_estate": self.verifies_estate,
        }



def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _blocked_by(node, upstream, *, validated: bool = False):
    """Why this node may not run, said with the code a reader can filter on."""

    from .result import DEPENDENCY_BLOCKED, error

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

    def runtime_scope(self, session=None):
        """Where this run's deployed Python modules live, and how long they live.

        One scope per run, because a Fabric session outlives a build and a build
        rewrites deployed Python in place — so a module kept past the run that
        imported it is a module the next load would use instead of the one now
        on disk.

        *Where* it lives depends on the host: in this process where this process
        is where the data is, and otherwise in the Fabric session that can
        perform the imports, named from here. The Runner is told neither — one
        scope per run, closed at the end of it, is one rule in both cases.
        """

        if self._runtime_scope is None:
            from .runtime_boundary import open_runtime_scope

            self._runtime_scope = open_runtime_scope(session, workspace=self.workspace)
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
        # In a `finally`, because a scope that outlived its run is a scope the
        # next run would inherit — along with the modules a rebuild has since
        # replaced. A failed node is data and never reaches here, but an
        # interrupt does, and a leak is exactly what an interrupted run must
        # not leave behind.

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


        try:
            _execute()
        finally:
            self._close_runtime()

        nodes = tuple(results[node.node_id] for node in ordered)
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
        # One Sub-step per node, which is where a run's per-object timing comes
        # from. The frame is marked failed from inside rather than by an
        # exception, because a failed node is a *result* here — the run records
        # what happened before it decides what to do about it.
        with _node_substep(session, node) as frame:
            try:
                returned = dispatch(
                    node,
                    session=session,
                    state=self.state,
                    resolved=resolved,
                    fault_tolerant=self.request.fault_tolerant,
                    open_runtime=lambda: self.runtime_scope(session),
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
