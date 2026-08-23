"""What a run tells the catalogue about itself, row by row.

The Runner dispatches primitives and settles nodes; this is the record that
settling leaves behind. Five tables, and which of them a node writes to depends
on what the node was:

.. code-block:: text

    every settled node       _.Log
    a load about an object   _.LoadStatus
    a load that executed     _.LoadStatistic
    a clean load             _.Bookmark
    a validation             _.TestStatus

Pure Python. A settled node is a value, and the catalogue records into a writer
that keeps what it was given, so what a run *would* write is exactly what is
asserted here.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from support.catalogues import Recording
from support.weaver_test import weaver_test

from weaver.catalogue.state import Catalogue
from weaver.catalogue.tables import (
    BOOKMARK,
    LOAD_STATISTIC,
    LOAD_STATUS,
    LOG,
    TEST_STATUS,
)
from weaver.run.record import LOAD_TASK, TEST_TASK, RunRecord, result_for
from weaver.run.result import RunNodeResult
from weaver.runtime.load_result import LoadResult
from weaver.runtime.validation_result import AssumptionResult, TestResult

BEGAN = datetime(2026, 8, 22, 3, 4, 5, tzinfo=timezone.utc)
STARTED = "2026-08-22T03:04:05+00:00"
FINISHED = "2026-08-22T03:04:07+00:00"

CUSTOMER = "Lakehouse/Sales/DWG.Customer"
RECONCILE = "Lakehouse/Sales/DWG.CustomerReconcile"


def _node(**overrides) -> RunNodeResult:
    """One settled load node, in whatever state a case wants it."""

    values = {
        "node_id": "DWG.Customer",
        "physical_target": "Lakehouse/Sales_LH",
        "primitive_kind": "python_table",
        "logical_id": CUSTOMER,
        "status": "succeeded",
        "executed": True,
        "result": LoadResult(
            succeeded=True,
            rows_read=10,
            rows_inserted=3,
            rows_updated=2,
            rows_deleted=1,
            bookmark_datetime=BEGAN,
        ),
        "started_at": STARTED,
        "finished_at": FINISHED,
        "target_type": "Lakehouse",
        "target_name": "Sales_LH",
        "schema_name": "DWG",
        "object_name": "Customer",
    }
    values.update(overrides)
    return RunNodeResult(**values)


def _validation(**overrides) -> RunNodeResult:
    """One settled validation node."""

    values = {
        "node_id": "DWG.CustomerReconcile",
        "physical_target": "Lakehouse/Sales_LH",
        "primitive_kind": "python_validation",
        "logical_id": RECONCILE,
        "role": "Test",
        "status": "succeeded",
        "executed": True,
        "result": TestResult(),
        "started_at": STARTED,
        "finished_at": FINISHED,
        "target_type": "Lakehouse",
        "target_name": "Sales_LH",
        "schema_name": "DWG",
        "object_name": "CustomerReconcile",
    }
    values.update(overrides)
    return RunNodeResult(**values)


def _recorded(node, *, task_type: str = LOAD_TASK):
    """What one settled node writes, by table."""

    writer = Recording()
    catalogue = Catalogue({}, writer=writer)
    RunRecord(
        workflow_id="workflow-1", task_type=task_type, catalogue=catalogue
    ).settled(node)
    return writer


def _one(writer, table) -> dict:
    (row,) = writer.rows(table.name)
    return row


# --- a load ---------------------------------------------------------------------


@weaver_test()
def test_a_clean_load_writes_every_table_it_touches():
    """Evidence, current status, statistics, and the bookmark it advanced."""

    writer = _recorded(_node())

    assert [name for name, _row in writer.submitted] == [LOG.name, LOAD_STATISTIC.name]
    assert [name for name, _row in writer.updated] == [LOAD_STATUS.name, BOOKMARK.name]


@weaver_test()
def test_the_status_row_carries_the_objects_logical_identity_and_nothing_physical():
    """Where the object lives is the Installation's to say, not a status row's."""

    row = _one(_recorded(_node()), LOAD_STATUS)

    assert row["item_type"] == "Lakehouse"
    assert row["item_name"] == "Sales"
    assert row["schema_name"] == "DWG"
    assert row["object_name"] == "Customer"
    assert "target_name" not in row
    assert "target_type" not in row


@weaver_test()
def test_the_status_row_carries_the_result_and_the_workflow_that_produced_it():
    row = _one(_recorded(_node()), LOAD_STATUS)

    assert row["result"] == "succeeded"
    assert row["workflow_id"] == "workflow-1"
    assert row["duration_milliseconds"] == 2000


@weaver_test()
def test_the_statistic_row_carries_what_the_load_did():
    row = _one(_recorded(_node()), LOAD_STATISTIC)

    assert row["rows_read"] == 10
    assert row["rows_inserted"] == 3
    assert row["rows_updated"] == 2
    assert row["rows_deleted"] == 1
    assert row["rows_rejected"] == 0
    assert row["is_reload"] is False
    assert row["is_static_skip"] is False


@weaver_test()
def test_a_static_skip_says_so_rather_than_reading_as_a_load_of_nothing():
    """A skip and a load that read an empty window both move no rows.

    Only the engine that ran it knows which happened, so the result reports it.
    """

    node = _node(result=LoadResult(succeeded=True, is_static_skip=True))
    writer = _recorded(node)

    assert _one(writer, LOAD_STATISTIC)["is_static_skip"] is True
    # And nothing moved the bookmark: the skip established no instant.
    assert writer.rows(BOOKMARK.name) == []


@weaver_test()
def test_a_load_with_rejected_rows_is_rejected_and_keeps_its_bookmark():
    """Valid rows landed and some did not, which is neither of the other two."""

    node = _node(
        status="succeeded_with_rejects",
        result=LoadResult(
            succeeded=False, rows_read=10, rows_rejected=2, error_message="2 rejected"
        ),
    )
    writer = _recorded(node)

    assert _one(writer, LOAD_STATUS)["result"] == "rejected"
    assert _one(writer, LOAD_STATISTIC)["rows_rejected"] == 2
    assert writer.rows(BOOKMARK.name) == []


@weaver_test()
def test_a_load_that_refused_a_change_it_was_declared_not_to_make_failed():
    """Ran under Weaver's control and produced an unacceptable result."""

    node = _node(
        status="failed",
        result=LoadResult.failure("delete threshold exceeded"),
    )

    assert _one(_recorded(node), LOAD_STATUS)["result"] == "failed"


@weaver_test()
def test_a_load_whose_dispatch_threw_is_an_error_and_not_a_failure():
    """Nothing ran to completion, so nothing was established about the data."""

    node = _node(status="failed", raised=True, result=LoadResult.failure("boom"))

    assert _one(_recorded(node), LOAD_STATUS)["result"] == "error"
    assert _one(_recorded(node), LOG)["result"] == "error"


@weaver_test()
def test_a_blocked_node_records_evidence_and_a_status_but_no_statistic():
    """It did nothing, and a row of zeroes would read as a load that moved none."""

    node = _node(status="blocked", executed=False, result=None, finished_at=None)
    writer = _recorded(node)

    assert _one(writer, LOG)["result"] == "blocked"
    assert _one(writer, LOAD_STATUS)["result"] == "blocked"
    assert writer.rows(LOAD_STATISTIC.name) == []


@weaver_test()
def test_a_node_the_run_never_reached_is_pending():
    """Nothing decided not to run it, so it is not Skipped."""

    node = _node(status="pending", executed=False, result=None, finished_at=None)

    assert _one(_recorded(node), LOG)["result"] == "pending"


@weaver_test()
def test_an_endpoint_refresh_records_evidence_and_no_state():
    """It is not an object, so it has no operational state to leave."""

    node = _node(
        node_id="refresh",
        primitive_kind="endpoint_refresh",
        logical_id=None,
        schema_name=None,
        object_name=None,
        result=LoadResult(succeeded=True),
    )
    writer = _recorded(node)

    assert [name for name, _row in writer.submitted] == [LOG.name]
    assert writer.updated == []


# --- a validation ---------------------------------------------------------------


@weaver_test()
def test_a_validation_that_found_nothing_writes_its_evidence_and_its_status():
    writer = _recorded(_validation(), task_type=TEST_TASK)

    assert [name for name, _row in writer.submitted] == [LOG.name]
    assert [name for name, _row in writer.updated] == [TEST_STATUS.name]


@weaver_test()
def test_a_test_status_row_says_which_kind_of_validation_it_was():
    """A Test and an Assumption are reported apart, as the estate stores them."""

    assert (
        _one(_recorded(_validation(), task_type=TEST_TASK), TEST_STATUS)["test_type"]
        == "test"
    )
    assumption = _validation(role="Assumption", result=AssumptionResult())
    assert (
        _one(_recorded(assumption, task_type=TEST_TASK), TEST_STATUS)["test_type"]
        == "assumption"
    )


@weaver_test()
def test_a_failed_test_records_how_much_disagreed():
    node = _validation(
        status="failed", result=TestResult(missing_count=3, unexpected_count=1)
    )

    row = _one(_recorded(node, task_type=TEST_TASK), TEST_STATUS)

    assert row["result"] == "failed"
    assert row["failure_count"] == 4


@weaver_test()
def test_a_failed_assumption_records_its_violations():
    node = _validation(
        role="Assumption", status="failed", result=AssumptionResult(violation_count=7)
    )

    row = _one(_recorded(node, task_type=TEST_TASK), TEST_STATUS)

    assert row["failure_count"] == 7


@weaver_test()
def test_a_validation_that_could_not_be_evaluated_reports_no_failure_count():
    """It found nothing, and zero discrepancies is the answer it must not give."""

    node = _validation(
        status="failed", raised=True, result=TestResult.failed_to_run("procedure threw")
    )

    row = _one(_recorded(node, task_type=TEST_TASK), TEST_STATUS)

    assert row["result"] == "error"
    assert row["failure_count"] is None


@weaver_test()
def test_a_validation_records_no_load_state():
    """Two populations, and a validation belongs to one of them."""

    writer = _recorded(_validation(), task_type=TEST_TASK)

    assert writer.rows(LOAD_STATUS.name) == []
    assert writer.rows(LOAD_STATISTIC.name) == []
    assert writer.rows(BOOKMARK.name) == []


# --- the vocabulary itself ------------------------------------------------------


@weaver_test()
def test_an_unknown_status_is_refused_rather_than_written():
    """A status with no place in the public vocabulary fails here, not in Fabric."""

    from weaver.run.result import RunError

    with pytest.raises(RunError, match="public Result vocabulary"):
        result_for(_node(status="reticulating"))


@weaver_test()
def test_the_record_is_flushed_separately_from_being_built():
    """Rows are queued as work settles; the flush is the durability barrier."""

    writer = Recording()
    catalogue = Catalogue({}, writer=writer)
    record = RunRecord(
        workflow_id="workflow-1", task_type=LOAD_TASK, catalogue=catalogue
    )

    record.settled(_node())
    assert writer.flushes == 0

    record.flush()
    assert writer.flushes == 1


@weaver_test()
def test_a_write_that_did_not_land_says_what_it_cost():
    """A caller told the run finished must be able to rely on the record."""

    from weaver.catalogue.flusher import FlushError
    from weaver.run.result import RunError

    writer = Recording(failing=FlushError("refused"))
    record = RunRecord(
        workflow_id="workflow-1",
        task_type=LOAD_TASK,
        catalogue=Catalogue({}, writer=writer),
    )
    record.settled(_node())

    with pytest.raises(RunError, match="was not recorded"):
        record.flush()


__all__: tuple = ()
