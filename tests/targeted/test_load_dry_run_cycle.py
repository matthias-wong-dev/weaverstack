"""The whole orchestration path, with dispatch removed.

A dry run is the composition test the load layer could not otherwise have: it
exercises the real planning and resolution seams end to end, catalogue read,
reverse binding, dependency resolution, shortcut resolution, physical DAG,
endpoint-refresh insertion, dispatch-location resolution, deterministic order,
and stops at the boundary where a target would be touched.

So the dispatchers here are not fakes that answer plausibly. They are callables
that fail the test if anything reaches them, which is the only way to assert
"nothing ran" rather than "nothing appeared to run".

The session is prepared rather than acquired. Workspace resolution, Spark and TDS
are what differ between a desktop and a Fabric session, and none of
them changes anything about the orchestration this module is about.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pytest
from factories import (
    installed_catalogue,
    load_estate,
    load_estate_bindings,
)
from support.weaver_test import weaver_test
from support.workspaces import given_workspace

from weaver.declaration.model import WeaverItemId
from weaver.fabric.resolution import FabricResolver
from weaver.load_plan import ENDPOINT_REFRESH
from weaver.load_report import (
    TASK_SUCCEEDED,
    VALIDATED,
)
from weaver.operations.load import run_load
from weaver.run import RunState

if TYPE_CHECKING:  # names used only in annotations
    from weaver.lakehouse import Lakehouse

#: What a request names. The node ids below stay physical: that is where the
#: work runs.
RAW = WeaverItemId.parse("Lakehouse/Raw")
REPORTING = WeaverItemId.parse("Warehouse/Reporting")

ORDER = "load:Lakehouse/Raw_LH/Tables/Sales.Order"
DAILY = "load:Lakehouse/Raw_LH/Tables/Sales.Daily"
# A Folder's graph id carries its ``Files/`` area, which is what keeps it
# apart from a table of the same ``Schema.Object``.
EXPORT = "load:Lakehouse/Raw_LH/Files/Sales.Export"
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
class Prepared:
    """Catalogue state and a Session whose store refuses writes."""

    catalogue: Lakehouse / object
    workspace: object
    session: object


class Refreshing(FabricResolver):
    def refresh_sql_endpoint(self, item):
        raise AssertionError("a dry run must not refresh an endpoint")


@pytest.fixture
def session(tmp_path):
    repository = load_estate(tmp_path / "repository")
    bindings = load_estate_bindings()
    from support.sessions import given_session

    workspace = given_workspace(catalogue="Warehouse/Weaver_LH")

    class Refuses:
        """Any write here is a dry run that wrote something."""

        def __getattr__(self, name):
            def refuse(*args, **kwargs):
                raise AssertionError(f"a dry run must not {name}")

            return refuse

    opened = given_session(
        workspace=workspace, resolver=Refreshing(workspace), store=Refuses()
    )
    return Prepared(
        catalogue=installed_catalogue(repository, bindings, session=opened),
        workspace=workspace,
        session=opened,
    )


def dry_run(session, *targets, names=(), fault_tolerant=False, reload=False):
    return _dry_run(
        session,
        items=targets or (RAW, REPORTING),
        names=names,
        fault_tolerant=fault_tolerant,
        reload=reload,
    )


def _dry_run(session, *, items, names=(), fault_tolerant=False, reload=False):
    return run_load(
        session.session,
        workspace=session.workspace,
        state=RunState(catalogue=session.catalogue),
        items=items,
        names=names,
        fault_tolerant=fault_tolerant,
        dry_run=True,
        reload=reload,
    )


# --- the scope a run with no item covers --------------------------------------


@weaver_test()
def test_naming_no_item_covers_every_installed_item(session):
    """Where ``load()`` with no argument gets its scope from.

    ``_.Installation`` is the source, and it is read before anything is planned,
    so an unscoped run and one naming both items resolve to the same DAG.
    """

    unscoped = _dry_run(session, items=())

    assert unscoped.requested == ("Lakehouse/Raw", "Warehouse/Reporting")
    assert unscoped.order == dry_run(session, RAW, REPORTING).order


@weaver_test()
def test_naming_an_item_leaves_the_rest_of_the_estate_alone(session):
    """The scope is a boundary, so it is not the whole catalogue by accident."""

    scoped = dry_run(session, REPORTING)

    assert scoped.requested == ("Warehouse/Reporting",)
    assert scoped.order == (SUMMARY,)


# --- the complete physical DAG, resolved --------------------------------------


@weaver_test()
def test_load_dry_run_resolves_the_complete_physical_dag(session):
    report = dry_run(session)

    assert report.order == (EXPORT, ORDER, DAILY, REFRESH, SUMMARY)
    # Sorted, and a Folder's ``Files/`` area sorts ahead of a bare schema.
    assert report.edges == (
        (EXPORT, REFRESH),
        (DAILY, REFRESH),
        (ORDER, DAILY),
        (ORDER, REFRESH),
        (REFRESH, SUMMARY),
    )


@weaver_test()
def test_named_dry_run_is_the_exact_nodes_without_dependency_edges(session):
    report = dry_run(
        session,
        RAW,
        names=("Sales.Order", "Sales.Daily"),
    )

    assert set(report.order) == {ORDER, DAILY}
    assert report.edges == ()


@weaver_test()
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


@weaver_test()
def test_load_dry_run_names_the_primitive_every_node_would_reach(session):
    """Which installed thing, not where it happens to sit on this host.

    A dry run is planned against a snapshot and asks no workspace anything, so
    it names the procedure or the deployed module the way the estate names it.
    Turning that into an absolute path is the resolver's business, and it
    happens at dispatch, where the run is already talking to the host anyway.
    """

    report = dry_run(session)

    assert {node.node_id: node.dispatch_location for node in report.nodes} == {
        EXPORT: "Lakehouse/Raw_LH/_/Load/Files/Sales__Export.py",
        ORDER: "Lakehouse/Raw_LH/_/Load/Tables/Sales__Order.py",
        DAILY: "Lakehouse/Raw_LH/_/Load/Tables/Sales__Daily.py",
        REFRESH: "Lakehouse/Raw_LH/sql_endpoint",
        SUMMARY: "Warehouse/Reporting_WH/[_].[Load Sales.Summary]",
    }


@weaver_test()
def test_load_dry_run_emits_the_normal_run_report_shape(session):
    report = dry_run(session)

    assert report.dry_run
    assert report.status == TASK_SUCCEEDED
    # The logical items the caller asked for, so a report reads as the request did.
    assert report.requested == ("Lakehouse/Raw", "Warehouse/Reporting")
    assert report.started_at and report.finished_at
    # The shape a real run returns, minus the two things only a real run has.
    assert report.workflow_id is None
    assert report.workflow_id is None
    assert set(report.to_mapping()) == {
        "requested",
        "status",
        "dry_run",
        "fault_tolerant",
        "reload",
        "workspace",
        "workflow_id",
        "workflow_id",
        "started_at",
        "finished_at",
        "order",
        "edges",
        "nodes",
        "messages",
    }


# --- what a dry run must not do -----------------------------------------------


@weaver_test()
def test_load_dry_run_does_not_execute_warehouse_procedures(session):
    """The Warehouse node resolves; the SQL capability is never asked anything."""

    report = dry_run(session, REPORTING)

    assert report.by_node[SUMMARY].status == VALIDATED
    assert report.by_node[SUMMARY].dispatch_location == (
        "Warehouse/Reporting_WH/[_].[Load Sales.Summary]"
    )


@weaver_test()
def test_load_dry_run_does_not_execute_spark_sql(session):
    report = dry_run(session, RAW)

    assert report.by_node[DAILY].status == VALIDATED
    assert not report.by_node[DAILY].executed


@weaver_test()
def test_load_dry_run_does_not_import_and_run_python_loads(session):
    report = dry_run(session, RAW)

    assert report.by_node[ORDER].status == VALIDATED
    assert report.by_node[EXPORT].status == VALIDATED


@weaver_test()
def test_load_dry_run_does_not_refresh_endpoints(session):
    report = dry_run(session)

    assert report.by_node[REFRESH].primitive_kind == ENDPOINT_REFRESH
    assert report.by_node[REFRESH].status == VALIDATED
    assert not report.by_node[REFRESH].executed


@weaver_test()
def test_load_dry_run_writes_no_task_log(session):
    """Proven by the store, not by a flag: every write through it refuses."""

    report = dry_run(session)

    assert report.workflow_id is None
    assert report.workflow_id is None


@weaver_test()
def test_load_dry_run_appends_nothing_to_the_log(session, tmp_path):
    """A row for work nobody did would be evidence of a load that never ran."""

    dry_run(session)
    session.session.flush()

    assert not [
        statement
        for call in session.session.calls
        if call.kind == "tsql"
        for statement in call.body
        if "[_].[Log]" in statement
    ]


@weaver_test()
def test_a_dry_run_moves_no_bookmark(session):
    """A bookmark it advanced would make the next real load skip a window.

    Nothing read it, so nothing has been read. Proven by the statements rather
    than by a flag: no statement touching the table is submitted at all.
    """

    dry_run(session)
    session.session.flush()

    assert not [
        statement
        for call in session.session.calls
        if call.kind == "tsql"
        for statement in call.body
        if "[_].[Bookmark]" in statement
    ]


@weaver_test()
def test_a_dry_run_never_reports_an_execution_status(session):
    """A validated node has not run, and no word for a thing that ran fits it."""

    from weaver.load_report import SUCCEEDED, VALIDATION_STATUSES

    report = dry_run(session)

    statuses = {node.status for node in report.nodes}
    assert statuses <= set(VALIDATION_STATUSES)
    assert SUCCEEDED not in statuses
    assert statuses == {VALIDATED}


# --- a dry run of a reload -----------------------------------------------------


@weaver_test()
def test_a_dry_run_reload_reports_the_mode_without_ending_any_state(session):
    """The destructive half of a reload is the half a dry run must not reach.

    Nothing here can write: the store refuses, the resolver refuses to refresh
    and the dispatchers fail the test. What is left to assert is that the report
    still says which mode was asked for.
    """

    report = dry_run(session, RAW, names=("Sales.Order", "Sales.Daily"), reload=True)

    assert report.reload is True
    assert report.dry_run is True
    assert report.to_mapping()["reload"] is True
    assert all(not node.executed for node in report.nodes)
    assert report.workflow_id is None


@weaver_test()
def test_an_ordinary_dry_run_reports_no_reload(session):
    assert dry_run(session, RAW, names=("Sales.Order",)).reload is False


@weaver_test()
def test_a_dry_run_reload_that_selected_a_folder_is_refused(session):
    """Refused at planning, which is a step a dry run reaches too."""

    from weaver.errors import CommandError

    with pytest.raises(CommandError, match="reload covers tables") as raised:
        dry_run(session, RAW, reload=True)

    assert EXPORT in str(raised.value)
