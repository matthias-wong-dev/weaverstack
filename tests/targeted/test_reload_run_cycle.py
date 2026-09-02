"""Reload through a run: which objects it reaches, and which it leaves alone.

Reload is local. It reconstructs what the request selected and walks no further:

.. code-block:: text

    A → B → C

    reload A    A     reset, cleared, reconstructed
                B, C  untouched, and loaded next as they always would be

So there is no descendant invalidation here to test, and the claim worth proving
is the negative one: nothing outside the selection has its state ended. The run's
recorder is what ends it, one node at a time and only as the run reaches that
node, so a node the run never dispatched keeps the bookmark describing rows
nothing cleared.

The estate is :func:`factories.load_estate`, whose chain is exactly that shape,
and whose folder is what reload refuses. Dispatch is injected, so the seam under
test is the orchestration rather than an engine.
"""

from __future__ import annotations

import pytest
from factories import (
    installed_catalogue,
    load_estate,
    load_estate_bindings,
)
from support.catalogues import Recording
from support.weaver_test import weaver_test
from support.workspaces import given_workspace

from weaver.catalogue.state import Catalogue
from weaver.catalogue.tables import BOOKMARK, BOOKMARK_SENTINEL, LOAD_STATUS
from weaver.declaration.model import WeaverItemId
from weaver.errors import CommandError
from weaver.operations.load import _refuse_unsupported_reload, _reset_before
from weaver.run import Runner, RunRequest, RunState
from weaver.run.record import RunRecord
from weaver.runtime.load_result import LoadResult

RAW = WeaverItemId.parse("Lakehouse/Raw")
REPORTING = WeaverItemId.parse("Warehouse/Reporting")

ORDER = "load:Lakehouse/Raw_LH/Sales.Order"
DAILY = "load:Lakehouse/Raw_LH/Sales.Daily"
EXPORT = "load:Lakehouse/Raw_LH/Files/Sales.Export"
REFRESH = "refresh:Lakehouse/Raw_LH"
SUMMARY = "load:Warehouse/Reporting_WH/Sales.Summary"


@pytest.fixture
def catalogue(tmp_path):
    return installed_catalogue(
        load_estate(tmp_path / "repository"), load_estate_bindings()
    )


def _runner(catalogue, *, items=(RAW, REPORTING), names=(), reload=True) -> Runner:
    """The Runner ``run_load`` builds, without the Session it builds it inside."""

    return Runner(
        RunState(catalogue=catalogue),
        RunRequest.load(items, names=names, reload=reload),
        workspace=given_workspace(catalogue="Warehouse/Weaver_LH"),
        can_refresh=True,
    )


def _record():
    """A recorder over a catalogue that keeps what it was given."""

    writer = Recording()
    return RunRecord(
        workflow_id="workflow",
        task_type="load",
        catalogue=Catalogue(rows={}, writer=writer),
    ), writer


def _succeeds(node, **policy):
    return LoadResult(succeeded=True)


def _reset_objects(writer) -> list:
    """Which objects had their load state ended, as ``Schema.Object`` names."""

    return [
        f"{row['schema_name']}.{row['object_name']}"
        for name, row in writer.updated
        if name == LOAD_STATUS.name
    ]


# --- what a reload reaches ----------------------------------------------------


@weaver_test()
def test_a_named_reload_ends_the_state_of_exactly_what_it_selected(catalogue):
    """The locality claim, as the negative it is.

    ``names`` runs the exact nodes with no dependency expansion, so a reload of
    one table is a reload of one table. Its consumers keep their bookmarks,
    because nothing cleared the rows those bookmarks describe.
    """

    record, writer = _record()
    runner = _runner(catalogue, items=(RAW,), names=("Sales.Order",))

    runner.run(dispatch=_succeeds, before_node=_reset_before(record))

    assert _reset_objects(writer) == ["Sales.Order"]


@weaver_test()
def test_an_unselected_consumer_is_not_planned_at_all(catalogue):
    """Nothing downstream is even in the graph, so nothing downstream can run."""

    runner = _runner(catalogue, items=(RAW,), names=("Sales.Order",))

    assert [node.node_id for node in runner.plan().nodes] == [ORDER]


@weaver_test()
def test_a_reload_of_a_whole_item_reaches_every_table_it_selected(catalogue):
    """Selection is what reload follows, and an item selects all of its own."""

    record, writer = _record()
    runner = _runner(catalogue, items=(REPORTING,))

    runner.run(dispatch=_succeeds, before_node=_reset_before(record))

    assert _reset_objects(writer) == ["Sales.Summary"]


@weaver_test()
def test_only_the_loadable_tables_have_state_to_end(catalogue):
    """An endpoint refresh is not an object and a folder is not reloadable.

    Both are dispatched by this run, so what the reset skipped is a decision it
    made rather than a node it never saw.
    """

    record, writer = _record()
    runner = _runner(catalogue, items=(RAW, REPORTING))
    dispatched = []

    runner.run(
        dispatch=lambda node, **policy: (
            dispatched.append(node.node_id) or LoadResult(succeeded=True)
        ),
        before_node=_reset_before(record),
    )

    assert dispatched == [EXPORT, ORDER, DAILY, REFRESH, SUMMARY]
    assert _reset_objects(writer) == ["Sales.Order", "Sales.Daily", "Sales.Summary"]


@weaver_test()
def test_the_reset_writes_the_sentinel_and_pending_for_each_object(catalogue):
    record, writer = _record()
    runner = _runner(catalogue, items=(REPORTING,))

    runner.run(dispatch=_succeeds, before_node=_reset_before(record))

    (bookmark,) = writer.rows(BOOKMARK.name)
    (status,) = writer.rows(LOAD_STATUS.name)
    assert bookmark["bookmark_datetime"] == BOOKMARK_SENTINEL
    assert status["result"] == "pending"
    assert status["workflow_id"] == "workflow"


@weaver_test()
def test_a_node_the_run_never_reached_keeps_its_state(catalogue):
    """A failure upstream leaves the rest of the chain exactly as it was."""

    record, writer = _record()
    runner = _runner(catalogue, items=(RAW, REPORTING))

    def failing(node, **policy):
        if node.node_id == ORDER:
            raise RuntimeError("the cluster went away")
        return LoadResult(succeeded=True)

    runner.run(dispatch=failing, before_node=_reset_before(record))

    assert _reset_objects(writer) == ["Sales.Order"]


# --- what a reload refuses ----------------------------------------------------


@weaver_test()
def test_a_reload_that_selected_a_folder_is_refused(catalogue):
    runner = _runner(catalogue, items=(RAW,))

    with pytest.raises(CommandError, match="reload covers tables") as raised:
        _refuse_unsupported_reload(runner.plan())

    assert EXPORT in str(raised.value)


@weaver_test()
def test_a_reload_that_selected_only_tables_is_allowed(catalogue):
    runner = _runner(catalogue, items=(RAW,), names=("Sales.Order", "Sales.Daily"))

    assert _refuse_unsupported_reload(runner.plan()) is None
