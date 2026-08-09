"""The Runner's state machine, exhaustively and in milliseconds.

Every claim here is about orchestration — readiness, blocking, fail-fast,
aggregation, dry run — and none of it needs an engine. The graph is built from a
Python Catalogue, the estate is a Python inventory, and dispatch returns whatever
the test says it returns.

That is the point of the seam. These claims used to be reached by authoring a
repository, running a real build, publishing a physical catalogue, installing
runtime artefacts and running a real load — nine estate builds to ask nine
questions about ordering, none of which the estate answered.
"""

from __future__ import annotations

import pytest

from weaver.run import RunRequest, Runner, RunState
from weaver.run.graph import RunGraph, RunNode
from weaver.run.result import (
    BLOCKED,
    FAILED,
    PENDING,
    RUN_FAILED,
    RUN_PARTIALLY_SUCCEEDED,
    RUN_SUCCEEDED,
    RUN_SUCCEEDED_WITH_REJECTS,
    SKIPPED,
    SUCCEEDED,
    SUCCEEDED_WITH_REJECTS,
    VALIDATED,
)


class Target:
    """A physical target, spelled the way a report prints one."""

    def __init__(self, name: str, kind: str = "Lakehouse") -> None:
        self.kind = kind
        self.name = name

    def __str__(self) -> str:
        return f"{self.kind}/{self.name}"


SALES = Target("Sales_LH")


class Outcome:
    """What a controlled dispatch hands back, shaped like a real result."""

    def __init__(self, status: str = SUCCEEDED, messages=(), rows=None) -> None:
        self.status = status
        self.messages = messages
        self.rows = rows or {}

    def as_row(self) -> dict:
        return dict(self.rows)


def node(node_id: str, **kwargs) -> RunNode:
    """One node, naming the deployed module it would run.

    Faithful rather than convenient: resolution asks whether the artefact a node
    names is installed, so a node that named none would be asserting against a
    check that never fired.
    """

    from weaver.load_plan import PhysicalObjectRef

    return RunNode(
        node_id=node_id,
        physical_target=kwargs.pop("target", SALES),
        primitive_kind=kwargs.pop("primitive_kind", "python_table"),
        logical_id=kwargs.pop("logical_id", None),
        primitive_object=kwargs.pop(
            "primitive_object",
            PhysicalObjectRef(
                target_id=str(kwargs.get("target", SALES)),
                target_kind="lakehouse",
                schema="_/Load",
                object=f"{node_id}.py",
                object_type="file",
            ),
        ),
        role=kwargs.pop("role", "load"),
    )


def runner(*, nodes, edges=(), present=(str(SALES),), installed=True, **policy) -> Runner:
    """A Runner over a stated graph and a stated estate. No engine anywhere.

    The inventory holds exactly what the nodes name, so resolution passes for a
    reason rather than by not looking. ``installed=False`` states the other
    case: a graph that is right about an estate that is not.
    """

    from weaver.build_bundle.prune import TargetInventory
    from weaver.catalogue.state import Catalogue

    files = tuple(
        f"{one.primitive_object.schema}/{one.primitive_object.object}"
        for one in nodes
        if one.primitive_object is not None
    ) if installed else ()
    state = RunState(
        catalogue=Catalogue(rows={}),
        target_inventories={
            name: TargetInventory(
                target_id=name, kind="lakehouse", target_name=name, files=files
            )
            for name in present
        },
    )
    made = Runner(state, RunRequest.load([SALES], **policy))
    made._graph = RunGraph(nodes=tuple(nodes), edges=tuple(edges), requested=(SALES,))
    return made


def controlled(outcomes):
    """Dispatch that answers from a table, and records what it was asked for."""

    seen = []

    def dispatch(node, **asked):
        seen.append(node.node_id)
        outcome = outcomes.get(node.node_id, Outcome())
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    dispatch.seen = seen
    return dispatch


# --- planning -----------------------------------------------------------------


def test_planning_needs_no_session_and_no_dispatch():
    made = runner(nodes=[node("a"), node("b")], edges=[("a", "b")])

    assert [one.node_id for one in made.graph.order()] == ["a", "b"]


def test_the_order_is_deterministic_rather_than_incidental():
    made = runner(
        nodes=[node("c", logical_id="C"), node("a", logical_id="A"), node("b", logical_id="B")]
    )

    assert [one.node_id for one in made.graph.order()] == ["a", "b", "c"]


def test_a_cycle_is_refused_rather_than_ordered():
    from weaver.errors import LoadError

    made = runner(nodes=[node("a"), node("b")], edges=[("a", "b"), ("b", "a")])

    with pytest.raises(LoadError, match="cycle"):
        made.graph.order()


# --- the ordinary path --------------------------------------------------------


def test_every_node_runs_and_is_reported(recwarn):
    dispatch = controlled({})
    result = runner(nodes=[node("a"), node("b")], edges=[("a", "b")]).run(
        dispatch=dispatch
    )

    assert dispatch.seen == ["a", "b"]
    assert result.status == RUN_SUCCEEDED
    assert [one.status for one in result.nodes] == [SUCCEEDED, SUCCEEDED]
    assert all(one.executed for one in result.nodes)


def test_a_run_with_no_session_still_runs_with_its_own_dispatch():
    result = runner(nodes=[node("a")]).run(session=None, dispatch=controlled({}))

    assert result.succeeded


def test_row_counts_come_back_on_the_node_that_produced_them():
    dispatch = controlled({"a": Outcome(rows={"inserted": 3})})

    result = runner(nodes=[node("a")]).run(dispatch=dispatch)

    assert result.by_node["a"].result.as_row() == {"inserted": 3}


# --- failure ------------------------------------------------------------------


def test_a_reported_failure_stops_what_depends_on_it():
    dispatch = controlled({"a": Outcome(status=FAILED)})

    result = runner(nodes=[node("a"), node("b")], edges=[("a", "b")]).run(
        dispatch=dispatch
    )

    assert result.by_node["a"].status == FAILED
    assert result.by_node["b"].status == BLOCKED
    assert dispatch.seen == ["a"], "the blocked node was never dispatched"


def test_an_exception_from_dispatch_is_a_failed_node_not_a_crash():
    dispatch = controlled({"a": RuntimeError("the engine said no")})

    result = runner(nodes=[node("a")]).run(dispatch=dispatch)

    assert result.by_node["a"].status == FAILED
    assert "the engine said no" in result.by_node["a"].messages[0]


def test_fail_fast_stops_scheduling_but_still_reports_every_node():
    dispatch = controlled({"a": Outcome(status=FAILED)})

    result = runner(nodes=[node("a"), node("b")]).run(dispatch=dispatch)

    assert dispatch.seen == ["a"]
    # Independent of the failure, so not blocked — it simply never started.
    assert result.by_node["b"].status == PENDING
    assert len(result.nodes) == 2, "every planned node has an outcome"


def test_fault_tolerance_lets_an_independent_branch_finish():
    dispatch = controlled({"a": Outcome(status=FAILED)})

    result = runner(nodes=[node("a"), node("b")], fault_tolerant=True).run(
        dispatch=dispatch
    )

    assert dispatch.seen == ["a", "b"]
    assert result.by_node["b"].status == SUCCEEDED
    assert result.status == RUN_PARTIALLY_SUCCEEDED


def test_fault_tolerance_does_not_run_what_the_failure_blocked():
    dispatch = controlled({"a": Outcome(status=FAILED)})

    result = runner(
        nodes=[node("a"), node("b"), node("c")],
        edges=[("a", "b")],
        fault_tolerant=True,
    ).run(dispatch=dispatch)

    assert result.by_node["b"].status == BLOCKED
    assert result.by_node["c"].status == SUCCEEDED


# --- the estate the graph was planned against ---------------------------------


def test_a_target_that_is_not_there_fails_the_node_it_would_have_run():
    """A right graph against a missing estate is a different fault from a wrong one."""

    made = runner(nodes=[node("a", target=Target("Gone_LH"))], present=())

    result = made.run(dispatch=controlled({}))

    assert result.by_node["a"].status == FAILED
    assert "not present" in result.by_node["a"].messages[0]
    assert not result.by_node["a"].executed


def test_an_artefact_that_was_never_installed_fails_before_it_is_dispatched():
    """The other half of the distinction: the target is there, the module is not."""

    dispatch = controlled({})

    result = runner(nodes=[node("a")], installed=False).run(dispatch=dispatch)

    assert result.by_node["a"].status == FAILED
    assert "not installed in" in " ".join(result.by_node["a"].messages)
    assert dispatch.seen == [], "nothing was dispatched at a missing artefact"


def test_a_refresh_this_host_cannot_do_is_skipped_rather_than_failed():
    """The emulator has no SQL analytics endpoint. That is an absence, not a fault."""

    made = runner(
        nodes=[node("refresh", primitive_kind="endpoint_refresh", primitive_object=None)]
    )
    made.can_refresh = False

    result = made.run(dispatch=controlled({}))

    assert result.by_node["refresh"].status == SKIPPED
    assert result.succeeded


def test_resolution_reads_the_snapshot_and_never_the_estate():
    """No session is given, so anything reaching for one would fail loudly."""

    made = runner(nodes=[node("a")])

    resolved = made.resolve(made.graph.nodes[0])

    assert resolved.valid
    assert resolved.target_present
    assert resolved.primitive_present
    assert resolved.expected_class == "a", "derived from the filename, not by importing"


# --- dry run ------------------------------------------------------------------


def test_a_dry_run_dispatches_nothing_and_says_so():
    dispatch = controlled({})

    result = runner(nodes=[node("a"), node("b")], dry_run=True).run(dispatch=dispatch)

    assert dispatch.seen == []
    assert [one.status for one in result.nodes] == [VALIDATED, VALIDATED]
    assert not any(one.executed for one in result.nodes)
    assert result.dry_run is True


def test_a_dry_run_reports_the_graph_the_real_run_would_execute():
    nodes = [node("a"), node("b")]
    edges = [("a", "b")]

    dry = runner(nodes=nodes, edges=edges, dry_run=True).run(dispatch=controlled({}))
    wet = runner(nodes=nodes, edges=edges).run(dispatch=controlled({}))

    assert dry.order == wet.order
    assert dry.edges == wet.edges


# --- aggregation --------------------------------------------------------------


def test_the_worst_node_decides_the_run_status():
    dispatch = controlled({"b": Outcome(status=FAILED)})

    result = runner(nodes=[node("a"), node("b")], fault_tolerant=True).run(
        dispatch=dispatch
    )

    assert result.by_node["a"].status == SUCCEEDED
    assert result.status == RUN_PARTIALLY_SUCCEEDED


def test_a_run_where_everything_failed_is_failed_not_partial():
    dispatch = controlled(
        {"a": Outcome(status=FAILED), "b": Outcome(status=FAILED)}
    )

    result = runner(nodes=[node("a"), node("b")], fault_tolerant=True).run(
        dispatch=dispatch
    )

    assert result.status == RUN_FAILED


def test_rejects_are_neither_success_nor_failure():
    dispatch = controlled({"a": Outcome(status=SUCCEEDED_WITH_REJECTS)})

    result = runner(nodes=[node("a")]).run(dispatch=dispatch)

    assert result.status == RUN_SUCCEEDED_WITH_REJECTS
    assert result.succeeded


# --- what a log sink is written from ------------------------------------------


def test_every_planned_node_reaches_the_sink_once_and_in_order():
    seen = []
    dispatch = controlled({"a": Outcome(status=FAILED)})
    made = runner(nodes=[node("a"), node("b"), node("c")], edges=[("a", "b")])

    result = made.run(dispatch=dispatch, on_node=seen.append)

    # Graph order, not scheduling order: a blocked node is reported where it was
    # planned, so a reader can follow the record against the plan.
    assert [one.node_id for one in seen] == list(result.order)
    assert len(seen) == len({one.node_id for one in seen})


def test_the_order_does_not_depend_on_how_the_graph_was_built():
    forwards = runner(nodes=[node("a"), node("b"), node("c")]).graph.order()
    backwards = runner(nodes=[node("c"), node("b"), node("a")]).graph.order()

    assert [one.node_id for one in forwards] == [one.node_id for one in backwards]


def test_a_run_needs_no_storage_to_be_correct():
    """Nothing here has a store, a log or a task id, and the run is still whole."""

    result = runner(nodes=[node("a")]).run(dispatch=controlled({}))

    assert result.task_log is None
    assert result.succeeded
