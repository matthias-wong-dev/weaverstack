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
    #: Targets the workspace does not hold. Set by a test to say so *directly*,
    #: rather than by removing an inventory — the two are different facts, and
    #: conflating them is the defect these tests exist to prevent.
    missing: frozenset = frozenset()

    def read_catalogue(self):
        return self.catalogue

    def environment(self, dag, requested=()):
        return LoadEnvironment(
            resolver=self.resolver,
            inventories=self.inventories,
            missing=self.missing,
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

    session.missing = frozenset({"Lakehouse/Raw_LH"})

    with pytest.raises(LoadError, match="does not hold them"):
        _run(session, RAW)


def test_the_two_refusals_are_told_apart_by_their_messages(session):
    """Same outcome, different fault, different fix.

    One is a typo in the request; the other is an estate that no longer matches
    its catalogue. A single message for both would send half the readers to the
    wrong place.
    """

    with pytest.raises(CommandError) as unknown:
        _run(session, MISTYPED)

    session.missing = frozenset({"Lakehouse/Raw_LH"})
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

    session.missing = frozenset({"Lakehouse/Raw_LH"})

    with pytest.raises(LoadError, match="Lakehouse/Raw_LH"):
        _run(session, REPORTING)


def test_a_dry_run_refuses_an_absent_target_too(session):
    session.missing = frozenset({"Lakehouse/Raw_LH"})

    with pytest.raises(LoadError, match="does not hold them"):
        _run(session, RAW, dry_run=True)


# --- an installed target with nothing loadable --------------------------------


#: An item that owns no load work at all. A view's definition *is* its query, so
#: it is built and never loaded — an item of nothing but views is installed and
#: has genuinely nothing to do, which is the only way to get an empty graph.
#:
#: A table would not do, and the first version of this test used one: a Python
#: table is itself a load artefact, so the graph had a node in it and the case
#: under test was never reached.
VIEW_ONLY = """/*
View ID: DWG.Nothing

Description: A view over a literal, so the item owns no load work at all.

Lineage: Declared for a test.

Dependencies: []
*/
select 1 as CustomerId;
"""

VIEWS_LH = PhysicalTargetRef("lakehouse", "Views_LH")


@pytest.fixture
def empty_estate(tmp_path):
    """An installed target with an empty load graph, and a session over it."""

    from factories import ITEM, item_bindings, single_document_repository

    repository = single_document_repository(
        tmp_path / "views", documents={"DWG.Nothing.sql": VIEW_ONLY}
    )
    bindings = item_bindings((ITEM, "Views_LH"))
    workspace = LocalWorkspace(
        workspace=str(tmp_path / "estate"), weaver_lakehouse="Weaver_LH"
    )
    return Logged(
        catalogue=installed_catalogue(repository, bindings),
        inventories=installed_inventories(repository, bindings),
        workspace=workspace,
        resolver=Refreshing(workspace),
        log_root=_log_root(tmp_path),
    )


def test_the_fixture_really_does_produce_an_empty_graph(empty_estate):
    """Asserted directly, because the two tests below are vacuous without it —
    and the version of this test that used a Python table *was* vacuous."""

    from weaver.load_plan import InstalledEstate, load_dag

    estate = InstalledEstate.from_catalogue(empty_estate.catalogue)
    dag = load_dag(estate, targets=(VIEWS_LH,))

    assert VIEWS_LH in estate.targets
    assert dag.nodes == ()


def test_an_installed_target_holding_no_loadable_objects_is_a_successful_no_op(
    empty_estate,
):
    """Installed, present, and nothing to do. That is a success."""

    report = run_load(
        empty_estate, requested=(VIEWS_LH,), fault_tolerant=False, dry_run=False
    )

    assert report.status == TASK_SUCCEEDED
    assert report.nodes == ()


def test_a_requested_target_that_is_gone_is_refused_even_with_an_empty_graph(
    empty_estate,
):
    """The hole a graph-derived preflight leaves.

    An item holding nothing loadable contributes no nodes, so a check built from
    the graph never looks at it — and a request naming a Lakehouse somebody has
    since deleted comes back "succeeded, nothing to do". Which is exactly the
    answer that must never be given for a target that is not there.
    """

    empty_estate.missing = frozenset({"Lakehouse/Views_LH"})

    with pytest.raises(LoadError, match="does not hold them"):
        run_load(
            empty_estate, requested=(VIEWS_LH,), fault_tolerant=False, dry_run=False
        )


def test_the_same_holds_for_a_dry_run(empty_estate):
    empty_estate.missing = frozenset({"Lakehouse/Views_LH"})

    with pytest.raises(LoadError, match="does not hold them"):
        run_load(
            empty_estate, requested=(VIEWS_LH,), fault_tolerant=False, dry_run=True
        )


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
