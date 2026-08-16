"""A load report, through JSON and back.

A load runs where the data is, so a desktop asking for a Fabric workspace gets
its report as JSON from a session it reached into. What it renders has to be the
same object an in-session caller holds — otherwise the CLI becomes a second
place that knows what a load report means, and the two drift.

So the claim is round-trip fidelity, and it is asserted over the *whole* value
rather than field by field: a mapping that loses a message, a count or a
node's timing is a report that reads differently on the two sides of a boundary
nobody can see.
"""

from __future__ import annotations

import json

from weaver.load_report import (
    BLOCKED,
    DEPENDENCY_BLOCKED,
    PRIMITIVE_REJECTS,
    SUCCEEDED_WITH_REJECTS,
    TASK_PARTIALLY_SUCCEEDED,
    LoadMessage,
    LoadNodeReport,
    LoadResult,
    LoadRunReport,
    error,
    warning,
)


def _report() -> LoadRunReport:
    """A report with every optional part populated.

    Deliberately not the simple case: what a round trip loses is whatever was
    absent from the example it was tested with.
    """

    return LoadRunReport(
        requested=("Lakehouse/Sales", "Warehouse/Reporting"),
        status=TASK_PARTIALLY_SUCCEEDED,
        dry_run=False,
        fault_tolerant=True,
        nodes=(
            LoadNodeReport(
                node_id="load:Lakehouse/Sales/Sales.Customer",
                logical_id="Lakehouse/Sales/Sales.Customer",
                physical_target="Lakehouse/Sales",
                primitive_kind="python_table",
                dispatch_location="/lh/Files/_/Load/Sales__Customer.py",
                status=SUCCEEDED_WITH_REJECTS,
                executed=True,
                messages=(
                    warning(
                        PRIMITIVE_REJECTS,
                        "2 row(s) rejected",
                        source="python_table",
                        detail="blank_primary_key",
                    ),
                ),
                result=LoadResult(
                    succeeded=False,
                    rows_read=9,
                    rows_inserted=5,
                    rows_updated=2,
                    rows_deleted=1,
                    rows_rejected=2,
                    error_message="2 rows were rejected",
                ),
                started_at="2026-08-07T09:15:22.123456+00:00",
                finished_at="2026-08-07T09:15:24.987654+00:00",
            ),
            LoadNodeReport(
                node_id="load:Warehouse/Reporting/Sales.Summary",
                logical_id=None,
                physical_target="Warehouse/Reporting",
                primitive_kind="warehouse_procedure",
                dispatch_location=None,
                status=BLOCKED,
                executed=False,
                messages=(
                    error(
                        DEPENDENCY_BLOCKED,
                        "upstream did not complete",
                        source="load_execution",
                    ),
                ),
            ),
        ),
        edges=(
            (
                "load:Lakehouse/Sales/Sales.Customer",
                "load:Warehouse/Reporting/Sales.Summary",
            ),
        ),
        order=(
            "load:Lakehouse/Sales/Sales.Customer",
            "load:Warehouse/Reporting/Sales.Summary",
        ),
        messages=(LoadMessage("info", "planning", "two nodes", source="load_plan"),),
        workflow_id="abc123",
        started_at="2026-08-07T09:15:22.000000+00:00",
        finished_at="2026-08-07T09:15:25.000000+00:00",
        workspace="My Workspace",
    )


def _crossed(report: LoadRunReport) -> LoadRunReport:
    """The report as it arrives on the far side of a Livy call."""

    return LoadRunReport.from_mapping(json.loads(json.dumps(report.to_mapping())))


def test_a_report_survives_the_crossing_whole():
    report = _report()

    assert _crossed(report) == report


def test_the_counts_a_node_reported_survive():
    node = _crossed(_report()).by_node["load:Lakehouse/Sales/Sales.Customer"]

    assert node.result == LoadResult(
        succeeded=False,
        rows_read=9,
        rows_inserted=5,
        rows_updated=2,
        rows_deleted=1,
        rows_rejected=2,
        error_message="2 rows were rejected",
    )


def test_a_nodes_messages_survive_with_their_severity_and_detail():
    node = _crossed(_report()).by_node["load:Lakehouse/Sales/Sales.Customer"]
    (message,) = node.messages

    assert (message.severity, message.code) == ("warning", PRIMITIVE_REJECTS)
    assert message.detail == "blank_primary_key"
    assert message.source == "python_table"


def test_a_node_that_never_ran_crosses_as_one():
    node = _crossed(_report()).by_node["load:Warehouse/Reporting/Sales.Summary"]

    assert node.status == BLOCKED
    assert not node.executed
    assert node.result is None
    assert node.logical_id is None
    assert node.dispatch_location is None


def test_the_graph_survives_as_edges_rather_than_lists():
    crossed = _crossed(_report())

    assert crossed.edges == (
        (
            "load:Lakehouse/Sales/Sales.Customer",
            "load:Warehouse/Reporting/Sales.Summary",
        ),
    )
    assert isinstance(crossed.order, tuple)


def test_the_workflow_identity_survives():
    """What correlates a run's `_.Log` rows has to reach the caller intact."""

    crossed = _crossed(_report())

    assert crossed.workflow_id == "abc123"


def test_a_dry_run_report_crosses_with_its_absences_intact():
    """A dry run writes no evidence, and the report says so by carrying none."""

    dry = LoadRunReport(
        requested=("Lakehouse/Sales",),
        status="succeeded",
        dry_run=True,
        fault_tolerant=False,
    )

    crossed = _crossed(dry)

    assert crossed == dry
    assert crossed.workflow_id is None
    assert crossed.workflow_id is None


def test_a_report_of_nothing_crosses_as_nothing():
    empty = LoadRunReport(
        requested=(), status="succeeded", dry_run=False, fault_tolerant=False
    )

    assert _crossed(empty) == empty
