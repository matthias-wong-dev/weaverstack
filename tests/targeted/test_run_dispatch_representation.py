"""What one node's dispatch actually asks the engine for.

The Runner decides *when* a node runs; this is the translation between a node and
the engine that runs it, and it is the only place a run crosses outward. What
matters here is the shape of the request — which procedure, with which inputs,
read back how — not what the engine does with it, which is a Primitive's claim
and is proven against a real Warehouse.

The orchestration claims that used to live alongside these — ordering, fault
tolerance, blocking, result normalisation, the observer — are in
``tests/test_run_cycle.py`` now, where they need no engine at all and run in
milliseconds.
"""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from support.weaver_test import weaver_test

from weaver.load_plan import PhysicalTargetRef
from weaver.run.dispatch import dispatch_primitive
from weaver.run.resolution import WAREHOUSE_PROCEDURE
from weaver.runtime.load_result import LoadResult

REPORTING = PhysicalTargetRef("warehouse", "Reporting_WH")


class RecordingSql:
    """A Warehouse connection that remembers what it was asked for."""

    def __init__(self, row) -> None:
        self.row = row
        self.calls: list = []

    def call_procedure(self, procedure, *, inputs=(), outputs=()):
        self.calls.append((procedure, tuple(inputs), tuple(outputs)))
        return self.row

    def query(self, statement, parameters=None):  # pragma: no cover - a trap
        raise AssertionError(
            "the load result is read from named outputs, never from a result set"
        )


def _dispatch(fault_tolerant: bool, row):
    """One Warehouse node, dispatched through the Session that owns the connection."""

    from weaver.declaration.metadata import ObjectId

    sql = RecordingSql(row)
    node = SimpleNamespace(
        node_id="Sales.Customer",
        primitive_kind=WAREHOUSE_PROCEDURE,
        physical_target=REPORTING,
        logical_id=SimpleNamespace(object_id=ObjectId("Sales", "Customer")),
    )
    session = SimpleNamespace(sql_executor=lambda target, workspace=None: sql)
    result = dispatch_primitive(node, session=session, fault_tolerant=fault_tolerant)
    return result, sql


#: The instant the procedure reported having begun. A clean load reports one;
#: the run advances the object's bookmark to it.
BEGAN = datetime(2026, 8, 22, 3, 4, 5, tzinfo=timezone.utc)

ROW = {
    "succeeded": True,
    "rows_read": 4,
    "rows_inserted": 1,
    "rows_updated": 2,
    "rows_deleted": 0,
    "rows_rejected": 0,
    "error_message": None,
    "bookmark_datetime": BEGAN,
}


@weaver_test()
def test_a_warehouse_load_asks_for_its_result_by_name():
    """Never by reading a result set, which authored setup may also produce."""

    from weaver.declaration.tsql_load import RESULT_PARAMETERS

    result, sql = _dispatch(False, ROW)

    procedure, inputs, outputs = sql.calls[0]
    assert procedure == "[_].[Load Sales.Customer]"
    assert inputs == (("fault_tolerant", 0), ("update_catalogue", 0))
    assert outputs == RESULT_PARAMETERS
    assert result == LoadResult(
        succeeded=True,
        rows_read=4,
        rows_inserted=1,
        rows_updated=2,
        bookmark_datetime=BEGAN,
    )


@weaver_test()
def test_an_orchestrated_warehouse_load_does_not_maintain_its_own_bookmark():
    """The run advances it, with the same record that says the load happened.

    Two writers for one row would be two decisions about when it moved, and the
    run's is the one that also knows whether the node it belongs to settled.
    """

    _result, sql = _dispatch(False, ROW)

    assert ("update_catalogue", 0) in sql.calls[0][1]


@weaver_test()
def test_fault_tolerance_reaches_the_procedure_as_an_input():
    _result, sql = _dispatch(
        True, dict(ROW, rows_read=0, rows_inserted=0, rows_updated=0)
    )

    assert sql.calls[0][1] == (("fault_tolerant", 1), ("update_catalogue", 0))


@weaver_test()
def test_a_run_with_no_session_says_what_it_needed():
    """The one crossing a run makes is through a Session. There is no other."""

    from weaver.run.result import RunError

    node = SimpleNamespace(
        node_id="Sales.Customer",
        primitive_kind=WAREHOUSE_PROCEDURE,
        physical_target=REPORTING,
    )

    with pytest.raises(RunError, match="needs a Session"):
        dispatch_primitive(node, session=None)


@weaver_test()
def test_an_unknown_primitive_kind_is_refused_rather_than_guessed_at():
    node = SimpleNamespace(
        node_id="Sales.Customer",
        primitive_kind="something_new",
        physical_target=REPORTING,
    )

    from weaver.run.result import RunError

    with pytest.raises(RunError, match="unknown primitive kind"):
        dispatch_primitive(node, session=SimpleNamespace())
