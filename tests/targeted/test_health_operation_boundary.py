"""What ``weaver.health`` reads, and what it does not.

Health answers from the catalogue, one bounded window of its history tables, and
each selected target's physical state. It runs no authored code, so it opens no
Livy session, and these hold it to that against a `TestSession`, which records
every crossing a host was asked to make.

The bounded reads are their own claim: ``_.Log`` and ``_.LoadStatistic`` grow
with the estate's age, so what health asks of them is one workflow's worth.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from factories import document_id, installation_row, registry_row
from support.weaver_test import weaver_test
from support.workspaces import given_workspace

from weaver.catalogue.history import (
    DEFAULT_LIMIT,
    LOAD_TASK,
    latest_load_workflow,
    load_statistics,
)
from weaver.catalogue.state import Catalogue
from weaver.catalogue.tables import INSTALLATION, LOAD_STATISTIC, LOG, REGISTRY
from weaver.declaration.model import WeaverItemId
from weaver.errors import CommandError
from weaver.health import latest_load, load_activity
from weaver.operations.health import HEALTH_TABLES, _as_of, _selected, run_health
from weaver.sessions.testing import TestSession
from weaver.targets import PhysicalTargetRef

RAW = "Lakehouse/Raw"
REPORTING = "Warehouse/Reporting"
RAW_LH = PhysicalTargetRef("lakehouse", "Raw_LH")
REPORTING_WH = PhysicalTargetRef("warehouse", "Reporting_WH")

NOW = datetime(2026, 4, 23, 12, 0, tzinfo=timezone.utc)


# --- the instant a report measures against ------------------------------------


@weaver_test()
def test_as_of_defaults_to_a_day_before_the_operation_started():
    assert _as_of(None, started=NOW) == NOW - timedelta(hours=24)


@weaver_test()
def test_an_aware_datetime_is_normalised_to_utc():
    from datetime import timezone as tz

    melbourne = timezone(timedelta(hours=10))
    given = datetime(2026, 4, 23, 22, 0, tzinfo=melbourne)

    assert _as_of(given, started=NOW) == datetime(2026, 4, 23, 12, 0, tzinfo=tz.utc)


@pytest.mark.parametrize(
    "written", ["2026-04-22T00:00:00Z", "2026-04-22T10:00:00+10:00"]
)
@weaver_test()
def test_an_iso_string_with_a_zone_is_accepted(written):
    assert _as_of(written, started=NOW) == datetime(2026, 4, 22, tzinfo=timezone.utc)


@weaver_test()
def test_a_naive_datetime_is_refused():
    """A report that named an instant without a zone would mean two moments."""

    with pytest.raises(CommandError, match="must carry a timezone"):
        _as_of(datetime(2026, 4, 22), started=NOW)


@weaver_test()
def test_a_string_that_is_not_an_instant_is_refused():
    with pytest.raises(CommandError, match="ISO-8601"):
        _as_of("yesterday", started=NOW)


# --- what a request may name --------------------------------------------------


def _installed_catalogue() -> Catalogue:
    return Catalogue(
        rows={
            WeaverItemId.parse(RAW): {
                INSTALLATION.name: (installation_row(RAW, "Raw_LH"),),
                REGISTRY.name: (registry_row(document_id(f"{RAW}/Sales.Order")),),
            }
        }
    )


@weaver_test()
def test_no_target_means_every_target_the_catalogue_binds():
    assert _selected(_installed_catalogue(), ()) == (RAW_LH,)


@weaver_test()
def test_an_unknown_target_is_an_ordinary_command_error():
    with pytest.raises(CommandError, match="no installed estate in"):
        _selected(_installed_catalogue(), (REPORTING_WH,))


@weaver_test()
def test_the_refusal_lists_what_is_installed():
    with pytest.raises(CommandError, match="Installed: Lakehouse/Raw_LH"):
        _selected(_installed_catalogue(), (REPORTING_WH,))


# --- bounded history ----------------------------------------------------------


class _Connection:
    """A catalogue connection that answers from configured rows.

    It records every statement, so a claim about what health asked for is a
    claim about the query rather than about a result somebody arranged.
    """

    def __init__(self, *, shape=(), rows=None) -> None:
        self.shape = {table.name.casefold(): {} for table in shape}
        self.answers = rows or []
        self.statements: list[str] = []

    def columns_of(self, table):
        return self.shape.get(table.name.casefold())

    def rows(self, statement: str):
        self.statements.append(statement)
        for match, answer in self.answers:
            if match in statement:
                return answer
        return []


def _log_row(result: str, workflow: str = "workflow-1", **at):
    return {"result": result, "row_count": 1, **at}


@weaver_test()
def test_an_absent_log_table_reads_as_no_activity():
    """Bootstrap: nothing has ever run, so there is nothing to report."""

    assert latest_load_workflow(_Connection()) is None


@weaver_test()
def test_the_latest_workflow_is_chosen_by_its_completion_instant():
    connection = _Connection(
        shape=(LOG,),
        rows=[
            ("SELECT TOP 1", [{"workflow_id": "workflow-2"}]),
            (
                "GROUP BY",
                [
                    _log_row(
                        "Succeeded",
                        started_datetime=NOW - timedelta(minutes=6),
                        completed_datetime=NOW,
                    )
                ],
            ),
        ],
    )

    found = latest_load_workflow(connection)

    assert found["workflow_id"] == "workflow-2"
    assert "ORDER BY" in connection.statements[0]
    assert LOAD_TASK in connection.statements[0]


@weaver_test()
def test_the_window_spans_the_workflow_and_counts_its_results():
    connection = _Connection(
        shape=(LOG,),
        rows=[
            ("SELECT TOP 1", [{"workflow_id": "workflow-1"}]),
            (
                "GROUP BY",
                [
                    {
                        "result": "Succeeded",
                        "row_count": 18,
                        "started_datetime": NOW - timedelta(minutes=6),
                        "completed_datetime": NOW - timedelta(minutes=1),
                    },
                    {
                        "result": "Rejected",
                        "row_count": 2,
                        "started_datetime": NOW - timedelta(minutes=4),
                        "completed_datetime": NOW,
                    },
                ],
            ),
        ],
    )

    window = latest_load(latest_load_workflow(connection))

    assert window.workflow_id == "workflow-1"
    assert window.counts == {"succeeded": 18, "rejected": 2}
    assert window.started_at == NOW - timedelta(minutes=6)
    assert window.completed_at == NOW


@weaver_test()
def test_statistics_are_scoped_to_one_workflow():
    connection = _Connection(
        shape=(LOAD_STATISTIC,),
        rows=[
            (
                LOAD_STATISTIC.name,
                [
                    _statistic("Sales", "Order", workflow="workflow-1", read=10),
                    _statistic("Sales", "Daily", workflow="workflow-0", read=99),
                ],
            )
        ],
    )

    found = load_statistics(connection, workflow_id="workflow-1")

    assert [row["object_name"] for row in found] == ["Order"]


@weaver_test()
def test_statistics_come_back_in_identity_order():
    connection = _Connection(
        shape=(LOAD_STATISTIC,),
        rows=[
            (
                LOAD_STATISTIC.name,
                [
                    _statistic("Sales", "Order"),
                    _statistic("Sales", "Customer"),
                ],
            )
        ],
    )

    found = load_statistics(connection, workflow_id="workflow-1")

    assert [row["object_name"] for row in found] == ["Customer", "Order"]


@weaver_test()
def test_the_window_is_capped():
    connection = _Connection(
        shape=(LOAD_STATISTIC,),
        rows=[
            (
                LOAD_STATISTIC.name,
                [_statistic("Sales", f"Object{index:04d}") for index in range(20)],
            )
        ],
    )

    assert len(load_statistics(connection, workflow_id="workflow-1", limit=5)) == 5
    assert DEFAULT_LIMIT > 5


@weaver_test()
def test_activity_names_the_target_each_object_is_installed_in():
    """A statistic row holds logical identity; where it lives is Installation's."""

    rows = [_statistic("Sales", "Order")]
    bound = {WeaverItemId.parse(RAW): RAW_LH}

    found = load_activity(rows, targets=bound)

    assert found[0].object_id == f"{RAW}/Sales.Order"
    assert found[0].target == "Lakehouse/Raw_LH"


@weaver_test()
def test_a_null_duration_survives_the_conversion():
    found = load_activity([_statistic("Sales", "Order", duration=None)])

    assert found[0].duration_ms is None
    assert found[0].rows_read == 0


def _statistic(schema, name, *, workflow="workflow-1", read=0, duration=1200):
    return {
        "load_statistic_sk": f"{schema}.{name}",
        "workflow_id": workflow,
        "item_type": "Lakehouse",
        "item_name": "Raw",
        "schema_name": schema,
        "object_name": name,
        "started_datetime": None,
        "completed_datetime": None,
        "duration_milliseconds": duration,
        "rows_read": read,
        "rows_inserted": None,
        "rows_updated": None,
        "rows_deleted": None,
        "rows_rejected": None,
        "is_reload": False,
        "is_static_skip": False,
    }


# --- what health reaches for --------------------------------------------------


@weaver_test()
def test_health_reads_the_two_current_status_tables_a_run_never_asks_about():
    from weaver.catalogue.tables import LOAD_STATUS, READABLE_TABLES, TEST_STATUS

    assert set(HEALTH_TABLES) == set(READABLE_TABLES) | {LOAD_STATUS, TEST_STATUS}


@weaver_test()
def test_health_reads_no_history_table_whole():
    assert LOG not in HEALTH_TABLES
    assert LOAD_STATISTIC not in HEALTH_TABLES


@weaver_test()
def test_a_report_starts_no_spark_session():
    """Health runs no authored code, so nothing crosses to Livy."""

    workspace = given_workspace()
    session = TestSession(workspace=workspace)

    report = run_health(
        session,
        workspace=workspace,
        as_of=NOW - timedelta(hours=24),
        generated_at=NOW,
        inventories=False,
    )

    assert report.status == "green"
    assert {call.kind for call in session.calls} == {"tsql"}


@weaver_test()
def test_the_declared_requirements_name_no_livy():
    from weaver_cli.main import _requires_health

    class _Args:
        targets = ("Lakehouse/Sales_LH", "Warehouse/Reporting_WH")

    assert "livy" not in _requires_health(_Args())
    assert "tds" in _requires_health(_Args())
