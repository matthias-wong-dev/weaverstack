"""What a load checks about its targets before it dispatches anything.

Three answers that must not be confused with each other:

.. code-block:: text

    Lakehouse/Rwa            a typo. Nothing was ever built there, so there is
                             no estate to load — and the one answer that must
                             never be given is "succeeded, nothing to do".

    Lakehouse/Sales, deleted the catalogue says the estate is there and the
                             workspace disagrees. Refused whole, rather than
                             discovered node by node once part of the run has
                             already written somewhere else.

    Lakehouse/Empty          installed, and holding nothing loadable. A
                             successful no-op, because that is exactly what was
                             asked for and exactly what happened.

The first two are refusals *before* the first primitive runs, in a dry run as
much as in a real one: a mistyped target is a mistyped target whichever was
asked for, and a dry run that reported it as a plan of nothing would be the same
misleading answer one step earlier.
"""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from weaver.errors import CommandError, LoadError
from weaver.load import run_load
from weaver.load_plan import PhysicalTargetRef
from weaver.load_report import TASK_SUCCEEDED
from weaver.load_resolution import LoadEnvironment
from weaver.resolution import LocalResolver
from weaver.workspaces import LocalWorkspace

from factories import (
    installed_catalogue,
    installed_inventories,
    load_estate,
    load_estate_bindings,
)

RAW = PhysicalTargetRef("lakehouse", "Raw_LH")
REPORTING = PhysicalTargetRef("warehouse", "Reporting_WH")
MISTYPED = PhysicalTargetRef("lakehouse", "Rwa_LH")


class Unreached:
    """Anything that touches this is a run that should have refused."""

    def __getattr__(self, name):
        def refuse(*args, **kwargs):
            raise AssertionError(f"preflight should have refused before {name}")

        return refuse


@dataclass
class PreparedSession:
    catalogue: object
    inventories: dict
    workspace: object
    resolver: object

    def read_catalogue(self):
        return self.catalogue

    def environment(self, dag):
        return LoadEnvironment(
            resolver=self.resolver,
            inventories=self.inventories,
            store=Unreached(),
            spark=Unreached(),
            sql={"Reporting_WH": Unreached()},
            workspace=self.workspace,
        )

    def open_log(self):
        raise AssertionError("preflight should have refused before opening a log")


class Refreshing(LocalResolver):
    def refresh_sql_endpoint(self, item):
        return None


@pytest.fixture
def session(tmp_path):
    repository = load_estate(tmp_path / "repository")
    bindings = load_estate_bindings()
    workspace = LocalWorkspace(
        workspace=str(tmp_path / "estate"), weaver_lakehouse="Weaver_LH"
    )
    return PreparedSession(
        catalogue=installed_catalogue(repository, bindings),
        inventories=installed_inventories(repository, bindings),
        workspace=workspace,
        resolver=Refreshing(workspace),
    )


def _run(session, *targets, dry_run=False):
    return run_load(
        session, requested=targets, fault_tolerant=False, dry_run=dry_run
    )


# --- a target nobody built into ----------------------------------------------


def test_a_target_the_catalogue_does_not_know_is_refused(session):
    with pytest.raises(CommandError, match="no installed estate"):
        _run(session, MISTYPED)


def test_the_refusal_names_the_target_that_was_not_found(session):
    with pytest.raises(CommandError) as raised:
        _run(session, MISTYPED)

    assert "Lakehouse/Rwa_LH" in str(raised.value)


def test_the_refusal_says_what_is_installed_so_a_typo_is_obvious(session):
    """The message has to be enough to fix the mistake it caught."""

    with pytest.raises(CommandError) as raised:
        _run(session, MISTYPED)

    assert "Lakehouse/Raw_LH" in str(raised.value)


def test_an_unknown_warehouse_is_refused_like_an_unknown_lakehouse(session):
    with pytest.raises(CommandError, match="no installed estate"):
        _run(session, PhysicalTargetRef("warehouse", "Repoting_WH"))


def test_a_dry_run_refuses_an_unknown_target_rather_than_planning_nothing(session):
    """A dry run that answered "a plan of no work" would be the same misleading
    answer, one step earlier and harder to notice."""

    with pytest.raises(CommandError, match="no installed estate"):
        _run(session, MISTYPED, dry_run=True)


def test_one_unknown_target_refuses_the_whole_request(session):
    """Loading the half that was spelled correctly would be worse than useless:
    it looks like the request succeeded."""

    with pytest.raises(CommandError, match="no installed estate"):
        _run(session, RAW, MISTYPED)


# --- a target the workspace no longer holds -----------------------------------


def test_a_target_that_cannot_be_read_refuses_the_run(session):
    """Installed according to the catalogue, absent according to the workspace."""

    session.inventories = {
        key: value
        for key, value in session.inventories.items()
        if key != "Lakehouse/Raw_LH"
    }

    with pytest.raises(LoadError, match="could not be read"):
        _run(session, RAW)


def test_the_two_refusals_are_told_apart_by_their_messages(session):
    """Same outcome, different fault, different fix.

    One is a typo in the request; the other is an estate that no longer matches
    its catalogue. A single message for both would send half the readers to the
    wrong place.
    """

    with pytest.raises(CommandError) as unknown:
        _run(session, MISTYPED)

    session.inventories = {
        key: value
        for key, value in session.inventories.items()
        if key != "Lakehouse/Raw_LH"
    }
    with pytest.raises(LoadError) as absent:
        _run(session, RAW)

    assert "no installed estate" in str(unknown.value)
    assert "the estate and the workspace disagree" in str(absent.value)


def test_an_upstream_target_is_checked_even_though_it_was_not_requested(session):
    """A Warehouse load depends on a Lakehouse upstream of it.

    Discovering that dependency was gone halfway through would already have
    written to the Warehouse, so the whole graph's targets are checked before
    anything runs.
    """

    session.inventories = {
        key: value
        for key, value in session.inventories.items()
        if key != "Lakehouse/Raw_LH"
    }

    with pytest.raises(LoadError, match="Lakehouse/Raw_LH"):
        _run(session, REPORTING)


def test_a_dry_run_refuses_an_absent_target_too(session):
    session.inventories = {
        key: value
        for key, value in session.inventories.items()
        if key != "Lakehouse/Raw_LH"
    }

    with pytest.raises(LoadError, match="could not be read"):
        _run(session, RAW, dry_run=True)


# --- an installed target with nothing loadable --------------------------------


def test_an_installed_target_holding_no_loadable_objects_is_a_successful_no_op(
    tmp_path,
):
    """The case the refusal must not swallow.

    "Installed and holding nothing loadable" and "never installed" both produce
    an empty graph, and telling them apart is the whole reason the check reads
    the catalogue rather than counting nodes. A view owns no load work, so an
    item of nothing but views is installed and has nothing to do.
    """

    from factories import ITEM, item_bindings, single_document_repository, spark_view

    repository = single_document_repository(
        tmp_path / "views",
        documents={
            "DWG__Customer.py": _table_document(),
            "DWG.Active.sql": spark_view("DWG.Active", depends_on="DWG.Customer"),
        },
    )
    bindings = item_bindings((ITEM, "Views_LH"))
    workspace = LocalWorkspace(
        workspace=str(tmp_path / "estate"), weaver_lakehouse="Weaver_LH"
    )
    session = Logged(
        catalogue=installed_catalogue(repository, bindings),
        inventories=installed_inventories(repository, bindings),
        workspace=workspace,
        resolver=Refreshing(workspace),
        log_root=_log_root(tmp_path),
    )

    report = run_load(
        session,
        requested=(PhysicalTargetRef("lakehouse", "Views_LH"),),
        fault_tolerant=False,
        dry_run=True,
    )

    assert report.status == TASK_SUCCEEDED


def _table_document() -> str:
    from factories import lakehouse_table

    return lakehouse_table("DWG.Customer", columns={"CustomerId": "string"})


def _log_root(tmp_path):
    from weaver.locations import Location
    from weaver.store import FilesystemStore

    root = Location(str(tmp_path / "log"))
    FilesystemStore().make_directory(root)
    return root


@dataclass
class Logged(PreparedSession):
    """The same prepared session, with somewhere real to write evidence."""

    log_root: object = None

    def open_log(self):
        from weaver.store import FilesystemStore
        from weaver.task_logging import open_task_log

        return open_task_log(
            task_type="load", folder=self.log_root, store=FilesystemStore()
        )
