"""A run with lanes: independent nodes overlap and settle as a serial run's do.

The doubles hold a node open until the test lets it go, so what overlaps and
what waits is decided here rather than by thread timing.
"""

from __future__ import annotations

import threading

from support.weaver_test import weaver_test
from test_run_cycle import Outcome, Target, node, runner

from weaver.run.result import BLOCKED, FAILED, PENDING, SUCCEEDED
from weaver.run.runner import Lanes

WAIT = 10.0
WAREHOUSE = Target("Sales_WH", kind="Warehouse")


def procedure(node_id: str):
    return node(node_id, target=WAREHOUSE, primitive_kind="warehouse_procedure")


class Gate:
    """Dispatch that records who is in flight and holds each node until opened."""

    def __init__(self, outcomes=None, hold=()):
        self.outcomes = outcomes or {}
        self.hold = {name: threading.Event() for name in hold}
        self.lock = threading.Lock()
        self.in_flight: set[str] = set()
        self.overlapped: set[frozenset] = set()
        self.started: list[str] = []
        self.batches: list[tuple[str, ...]] = []

    def dispatch(self, node, **_asked):
        with self.lock:
            self.started.append(node.node_id)
            for other in self.in_flight:
                self.overlapped.add(frozenset((other, node.node_id)))
            self.in_flight.add(node.node_id)
        try:
            gate = self.hold.get(node.node_id)
            if gate is not None:
                assert gate.wait(WAIT), f"{node.node_id} was never released"
            outcome = self.outcomes.get(node.node_id, Outcome())
            if isinstance(outcome, Exception):
                raise outcome
            return outcome
        finally:
            with self.lock:
                self.in_flight.discard(node.node_id)

    def dispatch_many(self, nodes, **asked):
        asked.pop("resolved", None)
        with self.lock:
            self.batches.append(tuple(one.node_id for one in nodes))
        results = [None] * len(nodes)

        def one(index, node):
            try:
                results[index] = self.dispatch(node, **asked)
            except Exception as exc:  # noqa: BLE001 - the node's outcome
                results[index] = exc

        threads = [
            threading.Thread(target=one, args=(index, node))
            for index, node in enumerate(nodes)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        return results


def run(made, gate, *, lanes=Lanes(), on_node=None, together=True):
    return made.run(
        session=object(),
        dispatch=gate.dispatch,
        dispatch_many=gate.dispatch_many if together else None,
        lanes=lanes,
        on_node=on_node,
    )


def statuses(result) -> dict:
    return {one.node_id: one.status for one in result.nodes}


@weaver_test()
def test_independent_warehouse_loads_run_at_once():
    gate = Gate(hold=("a", "c"))
    made = runner(nodes=[procedure("a"), procedure("c")])

    def release_when_both_started():
        while len(gate.started) < 2:
            pass
        for event in gate.hold.values():
            event.set()

    releaser = threading.Thread(target=release_when_both_started)
    releaser.start()
    result = run(made, gate)
    releaser.join()

    assert frozenset(("a", "c")) in gate.overlapped
    assert statuses(result) == {"a": SUCCEEDED, "c": SUCCEEDED}


@weaver_test()
def test_python_loads_that_become_ready_together_are_dispatched_together():
    gate = Gate()
    made = runner(nodes=[node("a"), node("c"), node("e")])

    result = run(made, gate)

    assert gate.batches == [("a", "c", "e")]
    assert set(statuses(result).values()) == {SUCCEEDED}


@weaver_test()
def test_a_lane_holds_no_more_than_its_limit():
    gate = Gate()
    made = runner(nodes=[node(name) for name in "abcde"])

    run(made, gate, lanes=Lanes(spark=2))

    assert gate.batches[0] == ("a", "b")
    assert all(len(batch) <= 2 for batch in gate.batches)
    assert sorted(name for batch in gate.batches for name in batch) == list("abcde")


@weaver_test()
def test_a_node_starts_only_after_its_upstream_settled():
    gate = Gate()
    made = runner(
        nodes=[node("a"), node("b"), node("c"), node("d"), node("e")],
        edges=[("a", "b"), ("c", "d")],
    )

    result = run(made, gate)

    started = gate.started
    assert started.index("b") > started.index("a")
    assert started.index("d") > started.index("c")
    assert gate.batches[0] == ("a", "c", "e")
    assert set(statuses(result).values()) == {SUCCEEDED}


@weaver_test()
def test_a_failure_starts_nothing_more_and_lets_running_work_settle():
    """Fail-fast: the failure's dependants are blocked, what was running finishes,
    and what had not started is pending, exactly as a serial run leaves it."""

    gate = Gate(outcomes={"a": RuntimeError("a broke")}, hold=("c",))
    made = runner(
        nodes=[procedure("a"), procedure("b"), procedure("c"), procedure("d")],
        edges=[("a", "b"), ("c", "d")],
    )

    def settled(result):
        if result.node_id == "a":
            gate.hold["c"].set()

    result = run(made, gate, on_node=settled)

    assert statuses(result) == {
        "a": FAILED,
        "b": BLOCKED,
        "c": SUCCEEDED,
        "d": PENDING,
    }
    assert "d" not in gate.started


@weaver_test()
def test_a_tolerant_run_continues_every_branch_after_a_failure():
    gate = Gate(outcomes={"a": RuntimeError("a broke")})
    made = runner(
        nodes=[procedure("a"), procedure("b"), procedure("c")],
        edges=[("c", "b")],
        fault_tolerant=True,
    )

    result = run(made, gate)

    assert statuses(result) == {"a": FAILED, "b": SUCCEEDED, "c": SUCCEEDED}


@weaver_test()
def test_every_node_settles_in_the_thread_that_ran_the_run():
    """The run's record is written where it was opened, one settlement at a time."""

    gate = Gate()
    made = runner(
        nodes=[node("a"), node("b"), procedure("c"), procedure("d")],
        edges=[("a", "b")],
    )
    threads = set()

    run(made, gate, on_node=lambda _result: threads.add(threading.get_ident()))

    assert threads == {threading.get_ident()}


@weaver_test()
def test_without_a_together_seam_python_loads_dispatch_one_by_one():
    gate = Gate()
    made = runner(nodes=[node("a"), node("c")])

    result = run(made, gate, together=False)

    assert gate.batches == []
    assert sorted(gate.started) == ["a", "c"]
    assert set(statuses(result).values()) == {SUCCEEDED}


@weaver_test()
def test_the_report_keeps_the_graph_order_whatever_finished_first():
    gate = Gate(hold=("a",))
    made = runner(nodes=[procedure("a"), procedure("b")])

    def release_after_b(result):
        if result.node_id == "b":
            gate.hold["a"].set()

    result = run(made, gate, on_node=release_after_b)

    assert [one.node_id for one in result.nodes] == ["a", "b"]
