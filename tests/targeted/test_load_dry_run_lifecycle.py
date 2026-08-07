"""The whole orchestration path, with dispatch removed.

A dry run is the composition test the load layer could not otherwise have: it
exercises the *real* planning and resolution seams end to end — catalogue read,
reverse binding, dependency resolution, alias resolution, physical DAG,
endpoint-refresh insertion, dispatch-location resolution, deterministic order —
and stops at the boundary where a target would be touched.

So the dispatchers here are not fakes that answer plausibly. They are callables
that fail the test if anything reaches them, which is the only way to assert
"nothing ran" rather than "nothing appeared to run".

The session is prepared rather than acquired. Workspace resolution, Spark and TDS
are what differ between the emulator, a desktop and a Fabric session, and none of
them changes anything about the orchestration this module is about.
"""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from weaver.load import run_load
from weaver.load_plan import ENDPOINT_REFRESH, PhysicalTargetRef
from weaver.load_report import (
    BLOCKED,
    INVALID,
    TASK_INVALID,
    TASK_SUCCEEDED,
    VALIDATED,
)
from weaver.load_resolution import LoadEnvironment
from weaver.resolution import LocalResolver
from weaver.store import FilesystemStore
from weaver.workspaces import LocalWorkspace

from factories import (
    installed_catalogue,
    installed_inventories,
    load_estate,
    load_estate_bindings,
)

RAW = PhysicalTargetRef("lakehouse", "Raw_LH")
REPORTING = PhysicalTargetRef("warehouse", "Reporting_WH")

ORDER = "load:Lakehouse/Raw_LH/Sales.Order"
DAILY = "load:Lakehouse/Raw_LH/Sales.Daily"
EXPORT = "load:Lakehouse/Raw_LH/Sales.Export"
REFRESH = "refresh:Lakehouse/Raw_LH"
SUMMARY = "load:Warehouse/Reporting_WH/Sales.Summary"


class Forbidden:
    """Anything reaching this is a dry run that was not one."""

    def __init__(self, what: str) -> None:
        self.what = what

    def __call__(self, *args, **kwargs):
        raise AssertionError(f"a dry run must not {self.what}")

    # A Spark session, a store and a SQL executor all in one, so a single object
    # can stand in wherever dispatch would have reached.
    sql = query = execute = execute_script = read = write = __call__

    def refresh_sql_endpoint(self, item):
        raise AssertionError("a dry run must not refresh an endpoint")


@dataclass
class PreparedSession:
    """A load session whose state is prepared rather than acquired."""

    catalogue: object
    inventories: dict
    workspace: object
    resolver: object
    log_opened: list = None

    def __post_init__(self):
        if self.log_opened is None:
            self.log_opened = []

    def read_catalogue(self):
        return self.catalogue

    def environment(self, dag, requested=()):
        return LoadEnvironment(
            resolver=self.resolver,
            inventories=self.inventories,
            store=Forbidden("read or write a target"),
            spark=Forbidden("run Spark SQL"),
            sql={"Reporting_WH": Forbidden("execute a Warehouse procedure")},
            workspace=self.workspace,
        )

    def open_log(self):
        self.log_opened.append(True)
        raise AssertionError("a dry run must not open a task log")


class Refreshing(LocalResolver):
    def refresh_sql_endpoint(self, item):
        raise AssertionError("a dry run must not refresh an endpoint")


@pytest.fixture
def session(tmp_path):
    repository = load_estate(tmp_path / "repository")
    bindings = load_estate_bindings()
    return PreparedSession(
        catalogue=installed_catalogue(repository, bindings),
        inventories=installed_inventories(repository, bindings),
        workspace=LocalWorkspace(
            workspace=str(tmp_path / "estate"), weaver_lakehouse="Weaver_LH"
        ),
        resolver=Refreshing(
            LocalWorkspace(
                workspace=str(tmp_path / "estate"), weaver_lakehouse="Weaver_LH"
            )
        ),
    )


def dry_run(session, *targets, fault_tolerant=False):
    return run_load(
        session,
        requested=targets or (RAW, REPORTING),
        fault_tolerant=fault_tolerant,
        dry_run=True,
    )


# --- the complete physical DAG, resolved --------------------------------------


def test_load_dry_run_resolves_the_complete_physical_dag(session):
    report = dry_run(session)

    assert report.order == (EXPORT, ORDER, DAILY, REFRESH, SUMMARY)
    assert report.edges == (
        (DAILY, REFRESH),
        (EXPORT, REFRESH),
        (ORDER, DAILY),
        (ORDER, REFRESH),
        (REFRESH, SUMMARY),
    )


def test_load_dry_run_validates_every_primitive_without_executing_it(session):
    report = dry_run(session)

    assert {node.node_id: node.status for node in report.nodes} == {
        EXPORT: VALIDATED,
        ORDER: VALIDATED,
        DAILY: VALIDATED,
        REFRESH: VALIDATED,
        SUMMARY: VALIDATED,
    }
    assert all(not node.executed for node in report.nodes)
    assert all(node.result is None for node in report.nodes)


def test_load_dry_run_resolves_the_exact_dispatch_location_of_every_node(
    session, tmp_path
):
    root = str(tmp_path / "estate")
    report = dry_run(session)

    assert {node.node_id: node.dispatch_location for node in report.nodes} == {
        EXPORT: f"{root}/Raw_LH/Files/_/Load/Files/Sales__Export.py",
        ORDER: f"{root}/Raw_LH/Files/_/Load/Sales__Order.py",
        DAILY: f"{root}/Raw_LH/Files/_/Load/Sales__Daily.py",
        REFRESH: "Lakehouse/Raw_LH/sql_endpoint",
        SUMMARY: "Warehouse/Reporting_WH/[_].[Load Sales.Summary]",
    }


def test_load_dry_run_emits_the_normal_run_report_shape(session):
    report = dry_run(session)

    assert report.dry_run
    assert report.status == TASK_SUCCEEDED
    assert report.requested == ("Lakehouse/Raw_LH", "Warehouse/Reporting_WH")
    assert report.started_at and report.finished_at
    # The shape a real run returns, minus the two things only a real run has.
    assert report.task_id is None
    assert report.task_log is None
    assert set(report.to_mapping()) == {
        "requested",
        "status",
        "dry_run",
        "fault_tolerant",
        "workspace",
        "task_id",
        "task_log",
        "started_at",
        "finished_at",
        "order",
        "edges",
        "nodes",
        "messages",
    }


# --- what a dry run must not do -----------------------------------------------


def test_load_dry_run_does_not_execute_warehouse_procedures(session):
    """The Warehouse node resolves; the SQL capability is never asked anything."""

    report = dry_run(session, REPORTING)

    assert report.by_node[SUMMARY].status == VALIDATED
    assert report.by_node[SUMMARY].dispatch_location == (
        "Warehouse/Reporting_WH/[_].[Load Sales.Summary]"
    )


def test_load_dry_run_does_not_execute_spark_sql(session):
    report = dry_run(session, RAW)

    assert report.by_node[DAILY].status == VALIDATED
    assert not report.by_node[DAILY].executed


def test_load_dry_run_does_not_import_and_run_python_loads(session):
    report = dry_run(session, RAW)

    assert report.by_node[ORDER].status == VALIDATED
    assert report.by_node[EXPORT].status == VALIDATED


def test_load_dry_run_does_not_refresh_endpoints(session):
    report = dry_run(session)

    assert report.by_node[REFRESH].primitive_kind == ENDPOINT_REFRESH
    assert report.by_node[REFRESH].status == VALIDATED
    assert not report.by_node[REFRESH].executed


def test_load_dry_run_writes_no_task_log(session):
    report = dry_run(session)

    assert session.log_opened == []
    assert report.task_log is None


def test_load_dry_run_creates_no_task_log_folder(session, tmp_path):
    dry_run(session)

    assert not (tmp_path / "estate" / "Weaver_LH" / "Files" / "_" / "Log").exists()


# --- and what it reports when the estate is wrong -----------------------------


def test_load_dry_run_reports_missing_primitives(session):
    from dataclasses import replace

    session.inventories = {
        **session.inventories,
        "Warehouse/Reporting_WH": replace(
            session.inventories["Warehouse/Reporting_WH"], procedures=()
        ),
    }

    report = dry_run(session)

    assert report.by_node[SUMMARY].status == INVALID
    assert report.status == TASK_INVALID
    assert [message.code for message in report.by_node[SUMMARY].messages] == [
        "dispatch_location_missing"
    ]


def test_a_dry_run_never_reports_an_execution_status(session):
    """A validated node has not run, and no word for a thing that ran fits it."""

    from dataclasses import replace

    from weaver.load_report import SUCCEEDED, VALIDATION_STATUSES

    raw = session.inventories["Lakehouse/Raw_LH"]
    session.inventories = {
        **session.inventories,
        "Lakehouse/Raw_LH": replace(
            raw,
            files=tuple(
                name for name in raw.files if not name.endswith("Sales__Order.py")
            ),
        ),
    }

    report = dry_run(session)

    statuses = {node.status for node in report.nodes}
    assert statuses <= set(VALIDATION_STATUSES)
    assert SUCCEEDED not in statuses
    assert len(statuses) > 1


def test_load_dry_run_blocks_descendants_of_invalid_nodes(session):
    from dataclasses import replace

    raw = session.inventories["Lakehouse/Raw_LH"]
    session.inventories = {
        **session.inventories,
        "Lakehouse/Raw_LH": replace(
            raw,
            files=tuple(
                name for name in raw.files if not name.endswith("Sales__Order.py")
            ),
        ),
    }

    report = dry_run(session)

    assert report.by_node[ORDER].status == INVALID
    assert report.by_node[DAILY].status == BLOCKED
    assert report.by_node[SUMMARY].status == BLOCKED
    assert report.by_node[EXPORT].status == VALIDATED
    assert report.status == TASK_INVALID
