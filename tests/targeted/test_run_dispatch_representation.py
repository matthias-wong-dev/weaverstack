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
    from weaver.declaration.tsql_load import (
        PROCEDURE_RESULT_PARAMETERS,
        RESULT_PARAMETERS,
    )

    physical_row = {
        physical_name: row[logical_name]
        for (logical_name, _logical_type), (physical_name, _physical_type) in zip(
            RESULT_PARAMETERS, PROCEDURE_RESULT_PARAMETERS, strict=True
        )
    }
    sql = RecordingSql(physical_row)
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
    "is_static_skip": False,
}


@weaver_test()
def test_a_warehouse_load_asks_for_its_result_by_name():
    """Never by reading a result set, which authored setup may also produce."""

    from weaver.declaration.tsql_load import PROCEDURE_RESULT_PARAMETERS

    result, sql = _dispatch(False, ROW)

    procedure, inputs, outputs = sql.calls[0]
    assert procedure == "[_].[Load Sales.Customer]"
    assert inputs == (("fault_tolerant", 0),)
    assert outputs == PROCEDURE_RESULT_PARAMETERS
    assert result == LoadResult(
        succeeded=True,
        rows_read=4,
        rows_inserted=1,
        rows_updated=2,
        bookmark_datetime=BEGAN,
    )


def _publication_estate(tmp_path, row):
    """A Warehouse load, the barrier behind it, and a Session for both."""

    from weaver.declaration.metadata import ObjectId
    from weaver.declaration.model import WeaverDocumentId, WeaverItemId
    from weaver.load_plan import OneLakeReadiness, PhysicalObjectRef
    from weaver.locations import Location
    from weaver.run.graph import RunNode
    from weaver.run.resolution import ONELAKE_PUBLICATION
    from weaver.store import FilesystemStore

    root = Location(str(tmp_path / "warehouse"))
    log = root.join("Tables", "Sales", "Customer", "_delta_log")
    store = FilesystemStore()
    store.make_directory(log)
    store.write(
        log / "00000000000000000000.json", b'{"add":{"path":"settled.parquet"}}'
    )
    events = []

    class Sql:
        def call_procedure(self, _name, *, inputs, outputs):
            events.append("procedure")
            store.write(
                log / "00000000000000000001.json", b'{"add":{"path":"new.parquet"}}'
            )
            return row

    class Resolver:
        def resolve(self, item, *, item_type):
            return SimpleNamespace(id="warehouse-id", name=item.name)

        def external_root(self, _item):
            return root

        def lakehouse_spark_location(self, item):
            return SimpleNamespace(
                table_path=lambda schema, object: (
                    f"abfss://workspace/{item.name}/Tables/{schema}/{object}"
                )
            )

    class Session:
        def resolver(self, _workspace=None):
            return Resolver()

        def transport_store(self, _workspace=None):
            return store

        def sql_executor(self, _target, workspace=None):
            return Sql()

        def execute_spark_sql_batch(self, batch, *, workspace=None):
            events.append(("spark", tuple(batch)))
            return [{"rows": 1}]

    identity = WeaverDocumentId(
        WeaverItemId("Warehouse", "Reporting"), ObjectId("Sales", "Customer")
    )
    producer = RunNode(
        node_id="load:Warehouse/Reporting_WH/Sales.Customer",
        physical_target=REPORTING,
        primitive_kind=WAREHOUSE_PROCEDURE,
        logical_id=identity,
        physical_object=PhysicalObjectRef(
            target_id="Reporting_WH",
            target_kind="warehouse",
            schema="Sales",
            object="Customer",
            object_type="table",
        ),
    )
    barrier = RunNode(
        node_id="publish:Warehouse/Reporting_WH/Sales.Customer",
        physical_target=REPORTING,
        primitive_kind=ONELAKE_PUBLICATION,
        publication_of=identity,
        publication_targets=(
            OneLakeReadiness(
                target=PhysicalTargetRef("lakehouse", "Published_LH"),
                schema="WH",
                object="Reporting",
            ),
        ),
        produced_by=producer.node_id,
    )
    return producer, barrier, Session(), events


@weaver_test()
def test_a_warehouse_load_leaves_the_waiting_to_its_publication_barrier(tmp_path):
    """The load runs its procedure and records what OneLake had already published.

    The wait itself belongs to the barrier behind it, so a load that committed
    cleanly settles as a load even when the publication never arrives.
    """

    from weaver.declaration.tsql_load import (
        PROCEDURE_RESULT_PARAMETERS,
        RESULT_PARAMETERS,
    )
    from weaver.run.publication import PublicationLedger

    row = {
        physical_name: ROW[logical_name]
        for (logical_name, _logical_type), (physical_name, _physical_type) in zip(
            RESULT_PARAMETERS, PROCEDURE_RESULT_PARAMETERS, strict=True
        )
    }
    producer, barrier, session, events = _publication_estate(tmp_path, row)
    ledger = PublicationLedger(frozenset({producer.node_id}))

    result = dispatch_primitive(producer, session=session, publication=ledger)

    assert result.rows_inserted == 1
    # The baseline was read before the procedure, and nothing was probed.
    assert events == ["procedure"]
    assert ledger.baseline(producer.node_id) == frozenset({"00000000000000000000.json"})
    assert ledger.moved(producer.node_id)

    dispatch_primitive(barrier, session=session, publication=ledger)

    assert events[1:] == [
        (
            "spark",
            (
                "select * from parquet."
                "`abfss://workspace/Published_LH/Tables/WH/Reporting"
                "/new.parquet` limit 1",
            ),
        )
    ]


@weaver_test()
def test_a_barrier_behind_a_load_that_moved_nothing_reaches_no_spark(tmp_path):
    """Nothing was written, so nothing is published and there is nothing to open."""

    from weaver.declaration.tsql_load import (
        PROCEDURE_RESULT_PARAMETERS,
        RESULT_PARAMETERS,
    )
    from weaver.run.publication import PublicationLedger

    unchanged = dict(ROW, rows_inserted=0, rows_updated=0, rows_deleted=0)
    row = {
        physical_name: unchanged[logical_name]
        for (logical_name, _logical_type), (physical_name, _physical_type) in zip(
            RESULT_PARAMETERS, PROCEDURE_RESULT_PARAMETERS, strict=True
        )
    }
    producer, barrier, session, events = _publication_estate(tmp_path, row)
    ledger = PublicationLedger(frozenset({producer.node_id}))

    dispatch_primitive(producer, session=session, publication=ledger)
    result = dispatch_primitive(barrier, session=session, publication=ledger)

    assert result.succeeded
    assert not ledger.moved(producer.node_id)
    assert events == ["procedure"]


@weaver_test()
def test_a_run_calls_the_objects_own_procedure_and_not_the_entry_point():
    """``_.Load`` records; the object's procedure does not.

    Two writers for one row would be two decisions about when it moved, and the
    run's is the one that also knows whether the node it belongs to settled. So
    the run calls the primitive, and which interface it called is what decides
    who records.
    """

    _result, sql = _dispatch(False, ROW)
    procedure, inputs, _outputs = sql.calls[0]

    assert procedure == "[_].[Load Sales.Customer]"
    assert procedure != "[_].[Load]"
    assert not [name for name, _value in inputs if "catalogue" in name]


@weaver_test()
def test_fault_tolerance_reaches_the_procedure_as_an_input():
    _result, sql = _dispatch(
        True, dict(ROW, rows_read=0, rows_inserted=0, rows_updated=0)
    )

    assert sql.calls[0][1] == (("fault_tolerant", 1),)


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
