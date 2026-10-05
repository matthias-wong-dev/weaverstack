"""A test run with lanes: validations are independent, so they run at once.

Warehouse validations are procedures and take Warehouse lanes. Lakehouse
validations that start together go to the host together. A finding never stops
the rest, and the line where a validation finishes says what it found.
"""

from __future__ import annotations

import io
import threading

from support.weaver_test import weaver_test
from test_concurrent_run_cycle import WAIT, Gate
from test_run_cycle import Target, _Logical, node, runner

from weaver.run.resolution import PYTHON_VALIDATION, WAREHOUSE_PROCEDURE
from weaver.run.result import FAILED, SUCCEEDED
from weaver.run.runner import Lanes
from weaver.runtime.validation_result import AssumptionResult, TestResult
from weaver.sessions import ConsoleSession

WAREHOUSE = Target("Reporting", kind="Warehouse")


def lakehouse_check(name: str):
    return node(
        name,
        primitive_kind=PYTHON_VALIDATION,
        installed=name,
        role="test",
        logical_id=_Logical(f"Sales.{name}"),
    )


def warehouse_check(name: str):
    return node(
        name,
        target=WAREHOUSE,
        primitive_kind=WAREHOUSE_PROCEDURE,
        installed=name,
        role="test",
        logical_id=_Logical(f"Sales.{name}", item="Warehouse/Reporting"),
    )


def run(made, gate, session=None):
    return made.run(
        session=session or object(),
        dispatch=gate.dispatch,
        dispatch_many=gate.dispatch_many,
        lanes=Lanes(),
    )


def statuses(result) -> dict:
    return {one.node_id: one.status for one in result.nodes}


@weaver_test()
def test_lakehouse_validations_that_start_together_go_to_the_host_together():
    gate = Gate(outcomes={name: TestResult() for name in "abc"})
    made = runner(nodes=[lakehouse_check(name) for name in "abc"], fault_tolerant=True)

    result = run(made, gate)

    assert gate.batches == [("a", "b", "c")]
    assert set(statuses(result).values()) == {SUCCEEDED}


@weaver_test()
def test_warehouse_validations_overlap():
    gate = Gate(outcomes={"a": TestResult(), "b": TestResult()}, hold=("a", "b"))
    made = runner(
        nodes=[warehouse_check("a"), warehouse_check("b")], fault_tolerant=True
    )

    def release_when_both_started():
        while len(gate.started) < 2:
            pass
        for event in gate.hold.values():
            event.set()

    releaser = threading.Thread(target=release_when_both_started)
    releaser.start()
    run(made, gate)
    releaser.join(WAIT)

    assert frozenset(("a", "b")) in gate.overlapped


@weaver_test()
def test_a_finding_stops_nothing_and_is_said_where_it_finishes():
    gate = Gate(
        outcomes={
            "a": AssumptionResult(violation_count=2),
            "b": TestResult(),
            "c": TestResult(),
        }
    )
    made = runner(
        nodes=[warehouse_check("a"), warehouse_check("b"), lakehouse_check("c")],
        fault_tolerant=True,
    )
    out = io.StringIO()

    with ConsoleSession(progress=out) as session:
        with session.task("Test"), session.step("Execute"):
            result = run(made, gate, session)

    assert statuses(result) == {"a": FAILED, "b": SUCCEEDED, "c": SUCCEEDED}
    lines = out.getvalue().splitlines()
    failed = next(line for line in lines if line.startswith("✗   Test"))
    assert "(2 violations)" in failed
    passed = [line for line in lines if line.startswith("✓   Test")]
    assert len(passed) == 2
    assert all("(" not in line for line in passed)
