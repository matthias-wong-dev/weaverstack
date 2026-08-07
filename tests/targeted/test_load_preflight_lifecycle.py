"""What a load checks about its targets before it dispatches anything.

Two answers that must not be confused with each other:

.. code-block:: text

    Lakehouse/Rwa            a typo. Nothing was ever built there, so there is
                             no estate to load — and the one answer that must
                             never be given is "succeeded, nothing to do".

    Lakehouse/Empty          installed, and holding nothing loadable. A
                             successful no-op, because that is exactly what was
                             asked for and exactly what happened.

The refusal comes *before* the first primitive runs, in a dry run as much as in
a real one: a mistyped target is a mistyped target whichever was asked for, and
a dry run that reported it as a plan of nothing would be the same misleading
answer one step earlier.

Whether the workspace still *holds* the item is a third question, and it is not
asked here. It is worth asking only where a wrong answer is cheap to act on —
in the CLI, before a Livy session costs forty seconds to start — and by the
time this code runs that session already exists. See
``tests/test_cli_load_binding.py``, where that guard is proved to reach the
answer over REST alone, without a catalogue or a graph.

An item the workspace no longer holds is not thereby silent: reading its
inventory fails, and the failure carries what actually went wrong rather than
being flattened into a guess. That is the last section below.
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


# --- an installed target with nothing loadable --------------------------------


#: An item that owns no load work at all. A view's definition *is* its query, so
#: it is built and never loaded — an item of nothing but views is installed and
#: has genuinely nothing to do, which is the only way to get an empty graph.
#:
#: This is the shape a graph-derived check cannot see: no nodes, so nothing to
#: derive a target list from. It is why the CLI's guard works from what was
#: *requested* rather than from what was planned.
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
    """Installed, and nothing to do. That is a success."""

    report = run_load(
        empty_estate, requested=(VIEWS_LH,), fault_tolerant=False, dry_run=False
    )

    assert report.status == TASK_SUCCEEDED
    assert report.nodes == ()


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


# --- an installed target the workspace cannot answer for ----------------------
#
# Every target in the graph has its inventory read before resolution runs, and
# that read can fail for reasons that are nothing like each other. What must not
# happen is the flattening: catching everything and calling it "missing" tells a
# reader with an expired credential to go looking for a Lakehouse that is
# sitting right there.


class _Node:
    def __init__(self, target):
        self.physical_target = target


class _Dag:
    def __init__(self, *targets):
        self.nodes = tuple(_Node(target) for target in targets)


@pytest.fixture
def real_session(tmp_path):
    """An actual LoadSession, because the diagnosis is built inside one."""

    from weaver.load import LoadSession

    workspace = LocalWorkspace(
        workspace=str(tmp_path / "estate"), weaver_lakehouse="Weaver_LH"
    )
    return LoadSession(workspace, ())


def _failing_reader(monkeypatch, exc):
    import weaver.build_bundle.prune as prune

    def refuse(*args, **kwargs):
        raise exc

    monkeypatch.setattr(prune, "read_lakehouse_inventory", refuse)


def test_an_inventory_that_cannot_be_read_fails_the_run(monkeypatch, real_session):
    from weaver.fabric import ItemNotFoundError

    _failing_reader(monkeypatch, ItemNotFoundError("no Lakehouse named 'Raw_LH'"))

    with pytest.raises(LoadError):
        real_session.environment(_Dag(RAW))


def test_the_failure_names_the_target_it_was_reading(monkeypatch, real_session):
    from weaver.fabric import ItemNotFoundError

    _failing_reader(monkeypatch, ItemNotFoundError("no Lakehouse named 'Raw_LH'"))

    with pytest.raises(LoadError) as raised:
        real_session.environment(_Dag(RAW))

    assert "Lakehouse/Raw_LH" in str(raised.value)


def test_a_deleted_item_is_diagnosed_as_a_deleted_item(monkeypatch, real_session):
    """The catalogue and the workspace disagree, and the message says which."""

    from weaver.fabric import ItemNotFoundError

    _failing_reader(monkeypatch, ItemNotFoundError("no Lakehouse named 'Raw_LH'"))

    with pytest.raises(LoadError) as raised:
        real_session.environment(_Dag(RAW))

    assert "ItemNotFoundError" in str(raised.value)
    assert "no Lakehouse named 'Raw_LH'" in str(raised.value)


def test_a_credential_failure_is_not_reported_as_a_deleted_item(
    monkeypatch, real_session
):
    """The flattening this exists to prevent.

    An expired token and a deleted Lakehouse are read by the same call and fixed
    in entirely different places. A run that answers "missing" for both sends
    half its readers to check something that is fine.
    """

    _failing_reader(monkeypatch, PermissionError("token expired"))

    with pytest.raises(LoadError) as raised:
        real_session.environment(_Dag(RAW))

    assert "PermissionError" in str(raised.value)
    assert "token expired" in str(raised.value)
    assert "missing" not in str(raised.value)


def test_the_original_exception_is_kept_as_the_cause(monkeypatch, real_session):
    """So a traceback still reaches the line that actually failed."""

    original = PermissionError("token expired")
    _failing_reader(monkeypatch, original)

    with pytest.raises(LoadError) as raised:
        real_session.environment(_Dag(RAW))

    assert raised.value.__cause__ is original
