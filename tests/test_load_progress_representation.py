"""What an operator sees while a load runs.

Every Load node gets one line when it starts and one when its outcome is known,
whether it ran alone or beside others. How nodes were grouped to reach their
host is not shown. The final report adds totals and the nodes that need
attention, not a second line for every success.

The Runner and the ConsoleSession are the real ones. Dispatch is a double that
holds nodes open where completion order is the subject.
"""

from __future__ import annotations

import io
import re
import threading
from datetime import datetime, timezone

from support.weaver_test import weaver_test
from test_concurrent_run_cycle import WAIT, Gate
from test_run_cycle import FAILED, Outcome, Target, _Logical, node, runner

from weaver.run.runner import Lanes
from weaver.sessions import ConsoleSession

WAREHOUSE = Target("Reporting", kind="Warehouse")
STARTED = datetime(2026, 10, 5, tzinfo=timezone.utc)
MACHINERY = ("batch", "lane", "SparkSession", "Livy", "concurrent", "running")


def lakehouse(name: str, **kwargs):
    return node(name, logical_id=_Logical(f"Sales.{name}"), **kwargs)


def warehouse(name: str, **kwargs):
    return node(
        name,
        target=WAREHOUSE,
        primitive_kind="warehouse_procedure",
        logical_id=_Logical(f"Sales.{name}", item="Warehouse/Reporting"),
        **kwargs,
    )


def LH(name: str) -> str:
    return f"Load Lakehouse/Sales/Tables/Sales.{name}"


def WH(name: str) -> str:
    return f"Load Warehouse/Reporting/Sales.{name}"


def present(made, gate, *, machine_output=False, **run):
    """Run a load's Execute Step and return the progress lines it printed."""

    out = io.StringIO()
    with ConsoleSession(progress=out) as session:
        session.machine_output = machine_output
        with session.task("Load"):
            with session.step("Execute"):
                result = made.run(
                    session=session,
                    dispatch=gate.dispatch,
                    dispatch_many=gate.dispatch_many,
                    lanes=Lanes(),
                    **run,
                )
    return out.getvalue().splitlines(), result


def node_lines(lines) -> list[tuple[str, str]]:
    """``(mark, name)`` for every node line, in printed order."""

    found = []
    for line in lines:
        match = re.match(r"^([→✓✗]) +(Load \S+)", line)
        if match:
            found.append(match.groups())
    return found


def assert_one_start_and_one_end(lines, names) -> None:
    seen = node_lines(lines)
    for name in names:
        marks = [mark for mark, said in seen if said == name]
        assert len(marks) == 2, (name, marks)
        assert marks[0] == "→" and marks[1] in "✓✗", (name, marks)


@weaver_test()
def test_one_lakehouse_node_reads_as_an_ordinary_load():
    gate = Gate(outcomes={"Customer": Outcome(rows={"rows_read": 5})})

    lines, _ = present(runner(nodes=[lakehouse("Customer")]), gate)

    assert node_lines(lines) == [("→", LH("Customer")), ("✓", LH("Customer"))]
    finished = next(line for line in lines if line.startswith("✓   Load"))
    assert "(read 5, +0 ~0 -0 !0)" in finished
    assert re.search(r"\d+\.\ds$", finished)
    assert not any(word in "\n".join(lines) for word in MACHINERY)


@weaver_test()
def test_one_warehouse_node_reads_the_same_way():
    gate = Gate()

    lines, _ = present(runner(nodes=[warehouse("Revenue")]), gate)

    assert node_lines(lines) == [("→", WH("Revenue")), ("✓", WH("Revenue"))]


@weaver_test()
def test_concurrent_warehouse_nodes_each_start_and_finish_once():
    """The first to start finishes last, and the lines say so."""

    gate = Gate(hold=("Customer",))
    made = runner(nodes=[warehouse("Customer"), warehouse("Order")])

    def release_after_order():
        while "Order" not in {node_id for _mark, node_id in finished()}:
            pass
        gate.hold["Customer"].set()

    out = io.StringIO()

    def finished():
        return [
            (mark, said.rsplit(".", 1)[-1])
            for mark, said in node_lines(out.getvalue().splitlines())
            if mark == "✓"
        ]

    releaser = threading.Thread(target=release_after_order, daemon=True)
    with ConsoleSession(progress=out) as session:
        with session.task("Load"), session.step("Execute"):
            releaser.start()
            made.run(
                session=session,
                dispatch=gate.dispatch,
                dispatch_many=gate.dispatch_many,
                lanes=Lanes(),
            )
    releaser.join(WAIT)

    lines = out.getvalue().splitlines()
    assert_one_start_and_one_end(lines, [WH("Customer"), WH("Order")])
    assert [name for _mark, name in finished()] == ["Order", "Customer"]
    # Every line is whole: a mark, a name, and a duration on the finish.
    for line in lines:
        if line.startswith(("✓   Load", "✗   Load")):
            assert re.match(r"^[✓✗] {3}Load \S+.*\d+\.\ds$", line), line


@weaver_test()
def test_lakehouse_nodes_sent_together_each_get_their_own_lines():
    gate = Gate()
    names = ["Customer", "Order", "Product"]

    lines, _ = present(runner(nodes=[lakehouse(name) for name in names]), gate)

    assert gate.batches == [tuple(names)]
    assert node_lines(lines) == [
        *(("→", LH(name)) for name in names),
        *(("✓", LH(name)) for name in names),
    ]
    assert not any(word in "\n".join(lines) for word in MACHINERY)


@weaver_test()
def test_lakehouse_and_warehouse_nodes_share_one_vocabulary():
    gate = Gate()
    made = runner(nodes=[lakehouse("Customer"), warehouse("Revenue")])

    lines, _ = present(made, gate)

    assert_one_start_and_one_end(lines, [LH("Customer"), WH("Revenue")])


@weaver_test()
def test_a_failure_is_marked_where_it_finishes_and_detailed_in_the_report(capsys):
    from test_cli_load_representation import _cli_module

    from weaver.operations.load import _as_load_report

    gate = Gate(outcomes={"Order": Outcome(FAILED, message="Order id is null")})
    made = runner(nodes=[warehouse("Customer"), warehouse("Order")])

    lines, result = present(made, gate)

    assert ("→", WH("Order")) in node_lines(lines)
    assert ("✗", WH("Order")) in node_lines(lines)
    assert ("✓", WH("Customer")) in node_lines(lines)

    _cli_module()._print_load(_as_load_report(result, started=STARTED, record=None))
    printed = capsys.readouterr().out
    assert "Order id is null" in printed
    assert "  1 succeeded" in printed
    assert "  1 failed" in printed
    assert "Sales.Customer" not in printed


@weaver_test()
def test_machine_output_prints_no_progress():
    gate = Gate()
    made = runner(nodes=[lakehouse("Customer"), warehouse("Revenue")])

    lines, result = present(made, gate, machine_output=True)

    assert lines == []
    assert len(result.nodes) == 2


@weaver_test()
def test_a_large_successful_run_ends_with_its_summary_alone(capsys):
    from test_cli_load_representation import _cli_module

    from weaver.operations.load import _as_load_report

    names = [f"Table{index:02d}" for index in range(12)]
    gate = Gate(outcomes={name: Outcome(rows={"rows_read": 10}) for name in names})

    lines, result = present(runner(nodes=[lakehouse(name) for name in names]), gate)
    _cli_module()._print_load(_as_load_report(result, started=STARTED, record=None))
    printed = capsys.readouterr().out

    assert len([mark for mark, _name in node_lines(lines) if mark == "✓"]) == 12
    assert not any(name in printed for name in names)
    assert " 12 succeeded" in printed
    assert "read                 120" in printed


@weaver_test()
def test_a_refresh_finishes_without_row_counts():
    """Waiting for an endpoint moves no rows, so it says none."""

    from weaver.run.resolution import ENDPOINT_REFRESH

    refresh = node("refresh", primitive_kind=ENDPOINT_REFRESH)
    lines, _ = present(runner(nodes=[refresh]), Gate())

    finished = next(line for line in lines if line.startswith("✓   Refresh"))
    assert "read" not in finished


@weaver_test()
def test_lakehouse_nodes_sent_together_each_report_their_own_times():
    """A batch returns when its slowest node does; each node keeps its own time."""

    from weaver.run.result import Timed

    gate = Gate()
    together = gate.dispatch_many
    names = ["Customer", "Order"]
    took = {"Customer": (1.0, "00:00:01"), "Order": (3.0, "00:00:03")}

    def dispatch_many(nodes, **asked):
        return [
            Timed(
                value,
                started_at="2026-10-05T00:00:00+00:00",
                finished_at=f"2026-10-05T{took[node.node_id][1]}+00:00",
                seconds=took[node.node_id][0],
            )
            for node, value in zip(nodes, together(nodes, **asked))
        ]

    gate.dispatch_many = dispatch_many
    lines, result = present(runner(nodes=[lakehouse(name) for name in names]), gate)

    for name, (seconds, finished) in took.items():
        line = next(one for one in lines if one.startswith(f"✓   {LH(name)}"))
        assert line.endswith(f"{seconds:.1f}s"), line
        node = next(one for one in result.nodes if one.node_id == name)
        assert node.started_at == "2026-10-05T00:00:00+00:00"
        assert node.finished_at == f"2026-10-05T{finished}+00:00"
