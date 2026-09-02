"""The Runner state machine without an execution engine."""

from __future__ import annotations

import pytest
from support.weaver_test import weaver_test

from weaver.run import Runner, RunRequest, RunState
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


def Outcome(status: str = SUCCEEDED, rows=None, rejected: int = 0, message=None):
    """What a controlled dispatch hands back, a real result, not a stand-in.

    The result contract is part of what the Runner enforces, so a fixture that
    handed back a convenient shape would be asserting against a check that never
    fired. These are the outcomes the plan asks a controlled dispatch to be able
    to produce; ``Malformed`` is the one that is not.
    """

    from weaver.runtime.load_result import LoadResult

    if status == FAILED:
        return LoadResult.failure(message or "the primitive reported failure")
    if status == SUCCEEDED_WITH_REJECTS:
        return LoadResult(
            succeeded=False,
            rows_rejected=rejected or 1,
            error_message=message or "some rows were refused",
        )
    return LoadResult(succeeded=True, **(rows or {}))


class Malformed:
    """A primitive that returned something that is not a load result at all."""

    def __repr__(self) -> str:
        return "Malformed()"


def node(node_id: str, **kwargs) -> RunNode:
    """One node, naming the deployed module it would run.

    Faithful rather than convenient: resolution asks whether the artefact a node
    names is installed, so a node that named none would be asserting against a
    check that never fired.
    """

    from weaver.targets import PhysicalObjectRef

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


def runner(*, nodes, edges=(), **policy) -> Runner:
    """A Runner over a stated graph. No engine anywhere."""

    from weaver.catalogue.state import Catalogue

    state = RunState(catalogue=Catalogue(rows={}))
    made = Runner(state, RunRequest.load([SALES], **policy))
    made._graph = RunGraph(nodes=tuple(nodes), edges=tuple(edges), items=(SALES,))
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


@weaver_test()
def test_planning_needs_no_session_and_no_dispatch():
    made = runner(nodes=[node("a"), node("b")], edges=[("a", "b")])

    assert [one.node_id for one in made.graph.order()] == ["a", "b"]


@weaver_test()
def test_the_order_is_deterministic_rather_than_incidental():
    made = runner(
        nodes=[
            node("c", logical_id="C"),
            node("a", logical_id="A"),
            node("b", logical_id="B"),
        ]
    )

    assert [one.node_id for one in made.graph.order()] == ["a", "b", "c"]


@weaver_test()
def test_a_cycle_is_refused_rather_than_ordered():
    from weaver.run.result import RunError

    made = runner(nodes=[node("a"), node("b")], edges=[("a", "b"), ("b", "a")])

    with pytest.raises(RunError, match="cycle"):
        made.graph.order()


# --- the ordinary path --------------------------------------------------------


@weaver_test()
def test_every_node_runs_and_is_reported(recwarn):
    dispatch = controlled({})
    result = runner(nodes=[node("a"), node("b")], edges=[("a", "b")]).run(
        dispatch=dispatch
    )

    assert dispatch.seen == ["a", "b"]
    assert result.status == RUN_SUCCEEDED
    assert [one.status for one in result.nodes] == [SUCCEEDED, SUCCEEDED]
    assert all(one.executed for one in result.nodes)


@weaver_test()
def test_a_run_with_no_session_still_runs_with_its_own_dispatch():
    result = runner(nodes=[node("a")]).run(session=None, dispatch=controlled({}))

    assert result.succeeded


@weaver_test()
def test_row_counts_come_back_on_the_node_that_produced_them():
    dispatch = controlled({"a": Outcome(rows={"rows_inserted": 3})})

    result = runner(nodes=[node("a")]).run(dispatch=dispatch)

    assert result.by_node["a"].result.rows_inserted == 3


# --- failure ------------------------------------------------------------------


@weaver_test()
def test_a_reported_failure_stops_what_depends_on_it():
    dispatch = controlled({"a": Outcome(status=FAILED)})

    result = runner(nodes=[node("a"), node("b")], edges=[("a", "b")]).run(
        dispatch=dispatch
    )

    assert result.by_node["a"].status == FAILED
    assert result.by_node["b"].status == BLOCKED
    assert dispatch.seen == ["a"], "the blocked node was never dispatched"


@weaver_test()
def test_an_exception_from_dispatch_is_a_failed_node_not_a_crash():
    dispatch = controlled({"a": RuntimeError("the engine said no")})

    result = runner(nodes=[node("a")]).run(dispatch=dispatch)

    assert result.by_node["a"].status == FAILED
    assert "the engine said no" in result.by_node["a"].messages[0].message
    assert result.by_node["a"].messages[0].code == "dispatch_exception"


@weaver_test()
def test_fail_fast_stops_scheduling_but_still_reports_every_node():
    dispatch = controlled({"a": Outcome(status=FAILED)})

    result = runner(nodes=[node("a"), node("b")]).run(dispatch=dispatch)

    assert dispatch.seen == ["a"]
    # Independent of the failure, so not blocked. It simply never started.
    assert result.by_node["b"].status == PENDING
    assert len(result.nodes) == 2, "every planned node has an outcome"


@weaver_test()
def test_fault_tolerance_lets_an_independent_branch_finish():
    dispatch = controlled({"a": Outcome(status=FAILED)})

    result = runner(nodes=[node("a"), node("b")], fault_tolerant=True).run(
        dispatch=dispatch
    )

    assert dispatch.seen == ["a", "b"]
    assert result.by_node["b"].status == SUCCEEDED
    assert result.status == RUN_PARTIALLY_SUCCEEDED


@weaver_test()
def test_fault_tolerance_does_not_run_what_the_failure_blocked():
    dispatch = controlled({"a": Outcome(status=FAILED)})

    result = runner(
        nodes=[node("a"), node("b"), node("c")],
        edges=[("a", "b")],
        fault_tolerant=True,
    ).run(dispatch=dispatch)

    assert result.by_node["b"].status == BLOCKED
    assert result.by_node["c"].status == SUCCEEDED


# --- resolution and dispatch --------------------------------------------------


@weaver_test()
def test_physical_failure_is_reported_by_dispatch_not_preflight():
    dispatch = controlled({"a": FileNotFoundError("deployed module is missing")})

    result = runner(nodes=[node("a", target=Target("Gone_LH"))]).run(dispatch=dispatch)

    assert dispatch.seen == ["a"]
    assert result.by_node["a"].status == FAILED
    assert result.by_node["a"].executed


@weaver_test()
def test_a_refresh_this_host_cannot_do_is_skipped_rather_than_failed():
    """A target with no SQL analytics endpoint is an absence, not a fault."""

    made = runner(
        nodes=[
            node("refresh", primitive_kind="endpoint_refresh", primitive_object=None)
        ]
    )
    made.can_refresh = False

    result = made.run(dispatch=controlled({}))

    assert result.by_node["refresh"].status == SKIPPED
    assert result.succeeded


@weaver_test()
def test_resolution_derives_dispatch_metadata_without_physical_state():

    made = runner(nodes=[node("a")])

    resolved = made.resolve(made.graph.nodes[0])

    assert resolved.valid
    assert resolved.expected_class == "a", "derived from the filename, not by importing"


# --- dry run ------------------------------------------------------------------


@weaver_test()
def test_a_dry_run_dispatches_nothing_and_says_so():
    dispatch = controlled({})

    result = runner(nodes=[node("a"), node("b")], dry_run=True).run(dispatch=dispatch)

    assert dispatch.seen == []
    assert [one.status for one in result.nodes] == [VALIDATED, VALIDATED]
    assert not any(one.executed for one in result.nodes)
    assert result.dry_run is True


@weaver_test()
def test_a_dry_run_reports_the_graph_the_real_run_would_execute():
    nodes = [node("a"), node("b")]
    edges = [("a", "b")]

    dry = runner(nodes=nodes, edges=edges, dry_run=True).run(dispatch=controlled({}))
    wet = runner(nodes=nodes, edges=edges).run(dispatch=controlled({}))

    assert dry.order == wet.order
    assert dry.edges == wet.edges


# --- aggregation --------------------------------------------------------------


@weaver_test()
def test_the_worst_node_decides_the_run_status():
    dispatch = controlled({"b": Outcome(status=FAILED)})

    result = runner(nodes=[node("a"), node("b")], fault_tolerant=True).run(
        dispatch=dispatch
    )

    assert result.by_node["a"].status == SUCCEEDED
    assert result.status == RUN_PARTIALLY_SUCCEEDED


@weaver_test()
def test_a_run_where_everything_failed_is_failed_not_partial():
    dispatch = controlled({"a": Outcome(status=FAILED), "b": Outcome(status=FAILED)})

    result = runner(nodes=[node("a"), node("b")], fault_tolerant=True).run(
        dispatch=dispatch
    )

    assert result.status == RUN_FAILED


@weaver_test()
def test_rejects_are_neither_success_nor_failure():
    dispatch = controlled({"a": Outcome(status=SUCCEEDED_WITH_REJECTS)})

    result = runner(nodes=[node("a")]).run(dispatch=dispatch)

    assert result.status == RUN_SUCCEEDED_WITH_REJECTS
    assert result.succeeded


# --- what a log sink is written from ------------------------------------------


@weaver_test()
def test_every_planned_node_reaches_the_sink_once_and_in_order():
    seen = []
    dispatch = controlled({"a": Outcome(status=FAILED)})
    made = runner(nodes=[node("a"), node("b"), node("c")], edges=[("a", "b")])

    result = made.run(dispatch=dispatch, on_node=seen.append)

    # Graph order, not scheduling order: a blocked node is reported where it was
    # planned, so the record follows the plan.
    assert [one.node_id for one in seen] == list(result.order)
    assert len(seen) == len({one.node_id for one in seen})


@weaver_test()
def test_the_order_does_not_depend_on_how_the_graph_was_built():
    forwards = runner(nodes=[node("a"), node("b"), node("c")]).graph.order()
    backwards = runner(nodes=[node("c"), node("b"), node("a")]).graph.order()

    assert [one.node_id for one in forwards] == [one.node_id for one in backwards]


@weaver_test()
def test_a_run_needs_no_storage_to_be_correct():
    """Nothing here has a store or a log, and the run is still whole."""

    result = runner(nodes=[node("a")]).run(dispatch=controlled({}))

    assert result.succeeded


@weaver_test()
def test_a_dry_run_validates_the_catalogue_graph_without_physical_checks():
    made = runner(
        nodes=[node("a", target=Target("Gone_LH")), node("b")],
        edges=[("a", "b")],
        dry_run=True,
    )

    result = made.run(dispatch=controlled({}))

    assert result.by_node["a"].status == VALIDATED
    assert result.by_node["b"].status == VALIDATED
    assert not any(one.executed for one in result.nodes)


@weaver_test()
def test_a_primitive_that_returned_the_wrong_shape_is_a_failed_node():
    """Not an exception, but not a dispatch either.

    Nothing ran to completion, so it must never read as a tolerated rejection,
    which is what inferring the status from the counts would produce.
    """

    dispatch = controlled({"a": Malformed()})

    result = runner(nodes=[node("a")]).run(dispatch=dispatch)

    assert result.by_node["a"].status == FAILED
    assert result.by_node["a"].messages[0].code == "result_contract_invalid"


@weaver_test()
def test_a_tolerated_rejection_is_not_a_failure_and_a_raised_one_is():
    """The distinction the whole outcome layer exists for.

    Both carry rejected rows and neither "succeeded", but one wrote the valid
    rows and the other wrote nothing.
    """

    from weaver.errors import LoadError
    from weaver.runtime.load_result import LoadResult

    tolerated = runner(nodes=[node("a")], fault_tolerant=True).run(
        dispatch=controlled({"a": Outcome(SUCCEEDED_WITH_REJECTS, rejected=2)})
    )
    refused = LoadError("the load refused 2 rows")
    refused.result = LoadResult(succeeded=False, rows_rejected=2)
    raised = runner(nodes=[node("a")], fault_tolerant=True).run(
        dispatch=controlled({"a": refused})
    )

    assert tolerated.by_node["a"].status == SUCCEEDED_WITH_REJECTS
    assert raised.by_node["a"].status == FAILED
    assert raised.by_node["a"].result.rows_rejected == 2, "the counts survive"


@weaver_test()
def test_rejects_do_not_block_what_comes_after_them():
    """The valid rows were written, so what a consumer reads is there.

    Blocking on a rejection would stop a run that partially succeeded from
    finishing the parts that were fine.
    """

    dispatch = controlled({"a": Outcome(SUCCEEDED_WITH_REJECTS, rejected=2)})

    result = runner(nodes=[node("a"), node("b")], edges=[("a", "b")]).run(
        dispatch=dispatch
    )

    assert dispatch.seen == ["a", "b"]
    assert result.by_node["b"].status == SUCCEEDED
    assert result.status == RUN_SUCCEEDED_WITH_REJECTS


# --- what a representation hands over ----------------------------------------


@weaver_test()
def test_a_request_hands_over_every_field_that_changes_behaviour():
    """A partial mapping arrives meaning something else than it left as."""

    from dataclasses import fields

    request = RunRequest.test([SALES], name="One", dry_run=True)
    handed = request.to_mapping()

    assert set(handed) == {field.name for field in fields(RunRequest)}


@weaver_test()
def test_a_node_result_hands_over_what_a_reader_needs_to_tell_outcomes_apart():
    dispatch = controlled({"a": RuntimeError("the engine said no")})

    result = runner(nodes=[node("a")]).run(dispatch=dispatch)
    handed = result.by_node["a"].to_mapping()

    # Nothing was evaluated, and the mapping has to say so, otherwise this
    # reads as a primitive that ran and reported failure.
    assert handed["raised"] is True
    assert handed["role"] == "load"


@weaver_test()
def test_a_result_does_not_claim_to_know_where_evidence_was_written():
    """A run is correct without a log; where one went belongs to the sink."""

    result = runner(nodes=[node("a")]).run(dispatch=controlled({}))

    assert "workflow_id" not in result.to_mapping()
    assert not hasattr(result, "workflow_id")


@weaver_test()
def test_a_run_state_round_trips_through_its_mapping():
    from weaver.catalogue.state import Catalogue

    state = RunState(catalogue=Catalogue(rows={}))

    returned = RunState.from_mapping(state.to_mapping())

    assert returned == state


# --- what a run must not swallow ---------------------------------------------


@weaver_test()
def test_an_interrupt_escapes_rather_than_becoming_a_failed_node():
    """Ctrl-C is the operator saying stop, not a primitive reporting failure.

    A run that recorded it as a node failure would swallow the interrupt at the
    prompt and hand back a report that looks like it decided something.
    """

    def dispatch(node, **asked):
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        runner(nodes=[node("a")]).run(dispatch=dispatch)


@weaver_test()
def test_a_process_exit_escapes_too():
    def dispatch(node, **asked):
        raise SystemExit(2)

    with pytest.raises(SystemExit):
        runner(nodes=[node("a")]).run(dispatch=dispatch)


@weaver_test()
def test_a_result_that_is_not_row_shaped_still_serializes():
    """The Runner's contract is "it reports whether it succeeded", and its
    serialization has to be exactly as narrow, or a future runtime result would
    execute perfectly and then fail while writing itself down."""

    class SemanticModelRefresh:
        """Something a future runtime operation might legitimately return."""

        succeeded = True

        def to_mapping(self) -> dict:
            return {"succeeded": True, "model": "Sales", "refreshed": True}

    result = runner(nodes=[node("a")]).run(
        dispatch=controlled({"a": SemanticModelRefresh()})
    )

    assert result.by_node["a"].status == SUCCEEDED
    assert result.to_mapping()["nodes"][0]["rows"] == {
        "succeeded": True,
        "model": "Sales",
        "refreshed": True,
    }


@weaver_test()
def test_a_result_that_describes_itself_no_further_still_serializes():
    """Neither to_mapping nor as_row: it answers what every result must."""

    class BareOutcome:
        succeeded = False
        error_message = "the model would not refresh"

    result = runner(nodes=[node("a")], fault_tolerant=True).run(
        dispatch=controlled({"a": BareOutcome()})
    )

    assert result.by_node["a"].status == FAILED
    assert result.to_mapping()["nodes"][0]["rows"] == {
        "succeeded": False,
        "error_message": "the model would not refresh",
    }


# --- what a run costs, per node -----------------------------------------------
#
# One Sub-step per dispatched node, which is where a run's per-object timing
# comes from. Recorded on the Session, so a Runner given none still runs. That
# is what the dispatch seam is for, and timing must not become a
# reason to need a Session.


@weaver_test()
def test_each_dispatched_node_is_timed_as_a_substep():
    from weaver.sessions import ConsoleSession

    with ConsoleSession(progress=False) as session:
        runner(nodes=[node("a"), node("b")], edges=[("a", "b")]).run(
            session=session, dispatch=controlled({})
        )

        assert [frame.name for frame in session.timings] == ["a", "b"]
        assert all(frame.kind == "substep" for frame in session.timings)
        assert all(frame.elapsed is not None for frame in session.timings)


@weaver_test()
def test_a_failed_node_is_a_failed_frame_though_nothing_was_raised():
    """A failed node is a result here, the run records what happened before
    it decides what to do about it, and the timing has to agree."""

    from weaver.sessions import ConsoleSession

    with ConsoleSession(progress=False) as session:
        runner(nodes=[node("a"), node("b")], fault_tolerant=True).run(
            session=session, dispatch=controlled({"a": Outcome(status=FAILED)})
        )

        failed = {frame.name: frame.failed for frame in session.timings}
        assert failed == {"a": True, "b": False}


@weaver_test()
def test_a_node_that_was_never_dispatched_is_never_timed():
    """Blocked, skipped and pending nodes waited on nothing of their own."""

    from weaver.sessions import ConsoleSession

    with ConsoleSession(progress=False) as session:
        runner(nodes=[node("a"), node("b")], edges=[("a", "b")]).run(
            session=session, dispatch=controlled({"a": Outcome(status=FAILED)})
        )

        assert [frame.name for frame in session.timings] == ["a"]


@weaver_test()
def test_a_runner_with_no_session_still_runs():
    result = runner(nodes=[node("a")]).run(session=None, dispatch=controlled({}))

    assert result.succeeded


@weaver_test()
def test_an_interrupted_run_still_closes_its_runtime_scope():
    """A scope that outlived its run is one the next run would inherit: along
    with the modules a rebuild has since replaced.

    A failed node is data and never raises out of the loop, so the case that
    needs a `finally` is the one nothing else covers: an interrupt.
    """

    closed = []

    class Scope:
        def context_for(self, **_kwargs):
            raise AssertionError("nothing should have been imported")

        def close(self):
            closed.append(True)

    made = runner(nodes=[node("a"), node("b")])
    made._runtime_scope = Scope()

    def interrupted(node, **asked):
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        made.run(dispatch=interrupted)

    assert closed == [True]


# --- what a node is called on screen ------------------------------------------


@weaver_test()
def test_a_node_is_named_by_what_it_does_to_which_object():
    """``node_id`` is an identifier and reads like one. This is the line
    somebody watches go past, so it is a verb and a physical id."""

    from weaver.run.resolution import ENDPOINT_REFRESH
    from weaver.run.runner import node_label

    load = RunNode(
        node_id="load:Lakehouse/Sales/Sales.Customer",
        physical_target="Lakehouse/Sales",
        primitive_kind="python_module",
        logical_id=_Logical("Sales.Customer"),
        role="load",
    )
    refresh = RunNode(
        node_id="refresh:Lakehouse/Sales",
        physical_target="Lakehouse/Sales",
        primitive_kind=ENDPOINT_REFRESH,
    )
    check = RunNode(
        node_id="Warehouse/Reporting/Reporting.CustomerRevenuePresent",
        physical_target="Warehouse/Reporting",
        primitive_kind="warehouse_procedure",
        logical_id=_Logical("Reporting.CustomerRevenuePresent"),
        role="Assumption",
    )

    folder = RunNode(
        node_id="load:Lakehouse/Sales/Files/Sales.Customer",
        physical_target="Lakehouse/Sales",
        primitive_kind="python_folder",
        logical_id=_Logical("Sales.Customer", is_files=True),
        role="load",
    )

    assert node_label(load) == "Load Lakehouse/Sales/Sales.Customer"
    assert node_label(refresh) == "Refresh Lakehouse/Sales SQL endpoint"
    assert (
        node_label(check) == "Test Warehouse/Reporting/Reporting.CustomerRevenuePresent"
    )
    # A Folder and a table of one name are two lines, not one repeated.
    assert node_label(folder) == "Load Lakehouse/Sales/Files/Sales.Customer"
    assert node_label(folder) != node_label(load)


@weaver_test()
def test_a_node_with_nothing_logical_to_name_keeps_its_id():
    """Inventing "Load None" would be worse than the identifier this improves on."""

    from weaver.run.runner import node_label

    node = RunNode(node_id="a", physical_target="Lakehouse/Sales", primitive_kind="x")

    assert node_label(node) == "a"


@weaver_test()
def test_run_evidence_uses_the_nodes_structured_identity():
    from dataclasses import replace

    from weaver.run.record import log_row

    result = runner(nodes=[node("a", logical_id=_Logical("Sales.Customer"))]).run(
        dispatch=controlled({"a": Outcome()})
    )
    settled = replace(
        result.nodes[0],
        physical_target="display text is not identity",
        logical_id="neither is this",
    )

    row = log_row(settled, workflow_id="workflow", task_type="load")

    assert row["target_type"] == "Lakehouse"
    assert row["target_name"] == "Sales_LH"
    assert row["schema_name"] == "Sales"
    assert row["object_name"] == "Customer"


class _Logical:
    def __init__(self, qualified, is_files=False):
        schema, object_name = qualified.split(".", 1)
        self.object_id = type(
            "ObjectId",
            (),
            {"qualified": qualified, "schema": schema, "object": object_name},
        )()
        #: A Folder is stored beneath ``Files/``, and its label says so.
        self.is_files = is_files

    def __str__(self):
        return self.object_id.qualified


# --- what runs before a node is dispatched ------------------------------------
#
# A reload ends an object's load state while the target it describes is still
# there, and that is where. The Runner names no mode: it offers the seam, and
# ``weaver.operations.load`` fills it.


@weaver_test()
def test_each_dispatched_node_reaches_before_node_ahead_of_its_dispatch():
    order = []
    dispatch = controlled({})
    made = runner(nodes=[node("a"), node("b")], edges=[("a", "b")])

    made.run(
        dispatch=lambda one, **asked: (
            (order.append(f"dispatch {one.node_id}")) or dispatch(one, **asked)
        ),
        before_node=lambda one: order.append(f"before {one.node_id}"),
    )

    assert order == ["before a", "dispatch a", "before b", "dispatch b"]


@weaver_test()
def test_a_node_that_never_dispatches_never_reaches_before_node():
    """Nothing was cleared for it, so nothing about it may be ended."""

    seen = []
    made = runner(nodes=[node("a"), node("b")], edges=[("a", "b")])

    made.run(
        dispatch=controlled({"a": Outcome(status=FAILED)}),
        before_node=lambda one: seen.append(one.node_id),
    )

    assert seen == ["a"]


@weaver_test()
def test_a_before_node_failure_is_that_nodes_failure_and_nothing_dispatches():
    """A reload whose state reset did not land must not go on to clear a target."""

    dispatch = controlled({})
    made = runner(nodes=[node("a"), node("b")], fault_tolerant=True)

    result = made.run(
        dispatch=dispatch,
        before_node=_failing_on("a", RuntimeError("the catalogue went away")),
    )

    assert result.by_node["a"].status == FAILED
    assert dispatch.seen == ["b"]


def _failing_on(node_id: str, error: Exception):
    def before(one):
        if one.node_id == node_id:
            raise error

    return before


@weaver_test()
def test_the_reload_mode_reaches_every_dispatch():
    asked = []
    made = runner(nodes=[node("a")], reload=True)

    made.run(dispatch=lambda one, **policy: asked.append(policy) or Outcome())

    assert asked[0]["reload"] is True


@weaver_test()
def test_an_ordinary_run_dispatches_without_reload():
    asked = []
    made = runner(nodes=[node("a")])

    made.run(dispatch=lambda one, **policy: asked.append(policy) or Outcome())

    assert asked[0]["reload"] is False


@weaver_test()
def test_reload_is_a_load_mode():
    from weaver.errors import CommandError

    with pytest.raises(CommandError, match="reload is a load mode"):
        RunRequest.test([SALES], reload=True)
