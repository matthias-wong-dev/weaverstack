"""What ``weaver.health`` reads, and what it does not.

Health answers from the catalogue, one bounded window of its history tables, and
each selected target's physical state. It runs no authored code, so it opens no
Livy session, and these hold it to that against a `TestSession`, which records
every crossing a host was asked to make.

The bounded reads are their own claim: ``_.LoadStatistic`` grows with the
estate's age, so what health asks of it is the rows behind current
``_.LoadStatus`` state and no more.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from factories import document_id, installation_row, registry_row
from support.weaver_test import weaver_test
from support.workspaces import given_workspace

from weaver.catalogue.history import DEFAULT_LIMIT, read_load_history
from weaver.catalogue.state import Catalogue
from weaver.catalogue.tables import (
    INSTALLATION,
    LOAD_STATISTIC,
    LOAD_STATUS,
    LOG,
    REGISTRY,
)
from weaver.declaration.model import WeaverItemId
from weaver.errors import CommandError
from weaver.health import current_load, load_activity
from weaver.operations.health import HEALTH_TABLES, _as_of, run_health
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
                REGISTRY.name: (
                    registry_row(document_id(f"{RAW}/Tables/Sales.Order")),
                ),
            }
        }
    )


@weaver_test()
def test_no_item_means_every_target_the_catalogue_binds():
    report = _health(_installed_catalogue())

    assert report.targets == (str(RAW_LH),)


@weaver_test()
def test_one_item_reports_on_the_target_it_is_installed_in():
    """Resolved through `_.Installation`, as a load resolves it."""

    report = _health(_installed_catalogue(), items=(WeaverItemId.parse(RAW),))

    assert report.targets == (str(RAW_LH),)


@weaver_test()
def test_an_item_with_no_installation_is_an_ordinary_command_error():
    with pytest.raises(CommandError, match="has no installation"):
        _health(_installed_catalogue(), items=(WeaverItemId.parse(REPORTING),))


@weaver_test()
def test_the_refusal_lists_what_is_installed():
    with pytest.raises(CommandError, match=f"Installed: {RAW}"):
        _health(_installed_catalogue(), items=(WeaverItemId.parse(REPORTING),))


def _health(catalogue, *, items=()):
    """One health report over a catalogue this test wrote, reading no estate."""

    from unittest.mock import patch

    workspace = given_workspace(catalogue="Warehouse/Weaver")
    with patch(
        "weaver.catalogue.state.read_installed_catalogue", return_value=catalogue
    ):
        return run_health(
            TestSession(workspace=workspace),
            workspace=workspace,
            items=items,
            as_of=NOW,
            generated_at=NOW,
            inventories=False,
        )


# --- bounded history ----------------------------------------------------------


class _Connection:
    """A catalogue connection that answers from configured rows.

    It records every statement, so a claim about what health asked the Warehouse
    for is a claim about the query rather than about a result somebody arranged
    in Python.
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

    def asked_for(self, fragment: str) -> str:
        """The one statement carrying ``fragment``, or a failure naming them all."""

        found = [each for each in self.statements if fragment in each]
        assert len(found) == 1, (fragment, self.statements)
        return found[0]


def _history_connection(*, statistics=(), workflow="workflow-1", results=()):
    """A Warehouse holding current load state and the statistics behind it."""

    return _Connection(
        shape=(LOAD_STATUS, LOAD_STATISTIC),
        rows=[
            ("GROUP BY", list(results) or [{"result": "Succeeded", "row_count": 1}]),
            (
                LOAD_STATISTIC.name,
                list(statistics) or [_statistic("Sales", "Order", workflow=workflow)],
            ),
        ],
    )


@weaver_test()
def test_an_absent_status_table_reads_as_no_activity():
    """Bootstrap: nothing has settled a load, so there is nothing to report."""

    assert read_load_history(_Connection()) is None


@weaver_test()
def test_current_state_spanning_two_workflows_carries_both():
    """A later partial load explains its own objects and not the rest."""

    connection = _history_connection(
        statistics=[
            _statistic("Sales", "Order", workflow="workflow-1"),
            _statistic("Sales", "Customer", workflow="workflow-2"),
        ]
    )

    history = read_load_history(connection)

    assert history.workflow_ids == ("workflow-1", "workflow-2")


@weaver_test()
def test_the_window_spans_its_loads_and_counts_current_results():
    connection = _history_connection(
        results=[
            {"result": "Succeeded", "row_count": 18},
            {"result": "Rejected", "row_count": 2},
        ],
        statistics=[
            _statistic(
                "Sales",
                "Order",
                started=NOW - timedelta(minutes=6),
                completed=NOW - timedelta(minutes=1),
            ),
            _statistic(
                "Sales",
                "Customer",
                started=NOW - timedelta(minutes=4),
                completed=NOW,
            ),
        ],
    )

    window = current_load(read_load_history(connection))

    assert window.workflow_ids == ("workflow-1",)
    assert window.counts == {"succeeded": 18, "rejected": 2}
    assert window.started_at == NOW - timedelta(minutes=6)
    assert window.completed_at == NOW


@weaver_test()
def test_the_engine_matches_the_statistics_against_current_state():
    """The five-column match is in the statement: nothing reads the table whole."""

    connection = _history_connection()

    read_load_history(connection)

    statement = connection.asked_for(LOAD_STATISTIC.name)
    assert "WHERE EXISTS" in statement
    for name in ("workflow_id", "item_type", "item_name", "schema_name", "object_name"):
        assert f"[{LOAD_STATUS.public_name_of(name)}]" in statement


@weaver_test()
def test_the_engine_bounds_and_orders_the_statistics():
    connection = _history_connection()

    read_load_history(connection, limit=5)

    statement = connection.asked_for(LOAD_STATISTIC.name)
    assert "TOP 5" in statement
    assert "ORDER BY" in statement
    assert statement.index("ORDER BY") > statement.index("WHERE")


@weaver_test()
def test_a_bounded_read_needs_an_order_to_be_the_same_prefix_twice():
    from weaver.catalogue.reader import read_table

    with pytest.raises(ValueError, match="needs an order"):
        read_table(_Connection(shape=(LOAD_STATISTIC,)), LOAD_STATISTIC, top=5)


@weaver_test()
def test_a_full_window_says_it_is_a_prefix():
    connection = _history_connection(
        statistics=[_statistic("Sales", f"Object{index:04d}") for index in range(5)]
    )

    history = read_load_history(connection, limit=5)

    assert history.is_truncated
    assert DEFAULT_LIMIT > 5


@weaver_test()
def test_a_window_within_its_limit_is_whole():
    connection = _history_connection(statistics=[_statistic("Sales", "Order")])

    assert not read_load_history(connection, limit=5).is_truncated


@weaver_test()
def test_activity_names_the_target_each_object_is_installed_in():
    """A statistic row holds logical identity; where it lives is Installation's."""

    history = read_load_history(
        _history_connection(statistics=[_statistic("Sales", "Order")])
    )
    bound = {WeaverItemId.parse(RAW): RAW_LH}

    found = load_activity(history, targets=bound)

    assert found[0].object_id == f"{RAW}/Tables/Sales.Order"
    assert found[0].target == "Lakehouse/Raw_LH"


@weaver_test()
def test_a_null_duration_survives_the_conversion():
    history = read_load_history(
        _history_connection(statistics=[_statistic("Sales", "Order", duration=None)])
    )

    found = load_activity(history)

    assert found[0].duration_ms is None
    assert found[0].rows_read == 0


@weaver_test()
def test_a_catalogue_read_with_no_window_reports_no_activity():
    assert current_load(None) is None
    assert load_activity(None) == ()


def _statistic(
    schema,
    name,
    *,
    workflow="workflow-1",
    read=0,
    duration=1200,
    started=None,
    completed=None,
):
    """One ``_.LoadStatistic`` row, keyed as a Lakehouse table is keyed."""

    schema = f"Tables/{schema}"
    return {
        "load_statistic_sk": f"{schema}.{name}",
        "workflow_id": workflow,
        "item_type": "Lakehouse",
        "item_name": "Raw",
        "schema_name": schema,
        "object_name": name,
        "started_datetime": started,
        "completed_datetime": completed,
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
def test_health_materialises_the_tables_it_consults_and_no_others():
    """Each table is a round trip, so one nothing consults is one nobody needed."""

    assert [table.name for table in HEALTH_TABLES] == [
        "Installation",
        "Registry",
        "TableDictionary",
        "FolderDictionary",
        "TestDictionary",
        "Dependency",
        "Shortcut",
        "Bookmark",
        "LoadStatus",
        "TestStatus",
    ]


@weaver_test()
def test_health_reads_no_dictionary_of_columns_or_keys():
    """Nothing health decides asks what an object's columns or keys are."""

    from weaver.catalogue.tables import (
        COLUMN_DICTIONARY,
        FOREIGN_KEY_DICTIONARY,
        KEY_DICTIONARY,
        SCHEMA_DICTIONARY,
    )

    unread = {
        SCHEMA_DICTIONARY,
        COLUMN_DICTIONARY,
        KEY_DICTIONARY,
        FOREIGN_KEY_DICTIONARY,
    }
    assert unread.isdisjoint(HEALTH_TABLES)


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
