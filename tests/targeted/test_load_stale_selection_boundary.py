"""What ``weaver load --stale-only`` runs, and where that answer comes from.

There is one implementation of Load health, :func:`weaver.health.assess_load`.
``weaver health`` renders it and a stale-only load executes the subjects it does
not call Green. These tests are about that reuse. The freshness matrix belongs to
``test_health_representation``, which holds it.

The estate is the one that module builds from rows, so both sides read the same
catalogue and the comparison is between two consumers of one assessment.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from support.weaver_test import weaver_test
from support.workspaces import given_workspace
from test_health_representation import CURATED, RAW, REPORTING, YESTERDAY, _Estate, at

import weaver
from weaver.catalogue.tables import LOAD_STATUS
from weaver.declaration.model import WeaverItemId
from weaver.errors import CommandError
from weaver.health import GREEN, assess_load
from weaver.load_plan import ENDPOINT_REFRESH, ONELAKE_PUBLICATION, load_dag
from weaver.load_report import TASK_SUCCEEDED
from weaver.operations.load import run_load
from weaver.run import RunState

RAW_ITEM = WeaverItemId.parse(RAW)
CURATED_ITEM = WeaverItemId.parse(CURATED)
REPORTING_ITEM = WeaverItemId.parse(REPORTING)

#: The nodes a barrier owns carry no logical identity of their own.
BARRIERS = (ENDPOINT_REFRESH, ONELAKE_PUBLICATION)


def selection_of(catalogue, *, items, as_of=YESTERDAY) -> set[str]:
    """What the canonical assessment says is not Green, as written identities."""

    assessment = assess_load(catalogue, as_of=as_of, items=items)
    return {
        str(subject.identity)
        for subject in assessment.subjects
        if subject.severity != GREEN
    }


def planned(catalogue, *, items, selection) -> set[str]:
    """The logical loadables one plan would run."""

    dag = load_dag(catalogue.dag(), items=items, selection=selection)
    return {
        str(node.logical_id)
        for node in dag.nodes
        if node.primitive_kind not in BARRIERS
    }


def stale_only_plan(catalogue, *, items, as_of=YESTERDAY) -> set[str]:
    """The logical loadables a stale-only load of this estate would run."""

    assessment = assess_load(catalogue, as_of=as_of, items=items)
    return planned(catalogue, items=items, selection=assessment.unsettled_identities())


def mixed_estate() -> _Estate:
    """One green table, one overdue, one never loaded, and a static pair."""

    return (
        _Estate()
        .table(f"{RAW}/Tables/Sales.Fresh", loaded=at(1), moved=at(1))
        .table(f"{RAW}/Tables/Sales.Overdue", loaded=at(48), moved=at(48))
        .table(f"{RAW}/Tables/Sales.Rebuilt")
        .table(
            f"{RAW}/Tables/Ref.Country",
            loaded=at(400),
            moved=at(400),
            is_static=True,
        )
        .table(f"{RAW}/Tables/Ref.Region", is_static=True)
    )


# --- the equivalence invariant ------------------------------------------------


@weaver_test()
def test_a_stale_only_plan_runs_exactly_the_subjects_health_does_not_call_green():
    """The acceptance criterion: one assessment, two consumers, one answer."""

    catalogue = mixed_estate().catalogue()
    expected = selection_of(catalogue, items=(RAW_ITEM,))

    assert stale_only_plan(catalogue, items=(RAW_ITEM,)) == expected


@weaver_test()
def test_health_renders_the_same_assessment_a_stale_only_load_executes():
    catalogue = mixed_estate().catalogue()
    assessment = assess_load(catalogue, as_of=YESTERDAY)
    report = mixed_estate().report()

    assert assessment.to_health_section() == report.load


@weaver_test()
def test_the_subjects_of_an_assessment_are_every_loadable_in_scope():
    catalogue = mixed_estate().catalogue()
    assessment = assess_load(catalogue, as_of=YESTERDAY, items=(RAW_ITEM,))

    assert len(assessment.subjects) == 5
    assert assessment.to_health_section().subjects == 5


# --- the cases the two must agree on ------------------------------------------


@weaver_test()
def test_a_freshly_rebuilt_object_has_no_load_status_and_is_selected():
    catalogue = mixed_estate().catalogue()

    assert f"{RAW}/Tables/Sales.Rebuilt" in stale_only_plan(
        catalogue, items=(RAW_ITEM,)
    )


@weaver_test()
def test_a_recently_loaded_object_is_green_and_is_not_selected():
    catalogue = mixed_estate().catalogue()

    assert f"{RAW}/Tables/Sales.Fresh" not in stale_only_plan(
        catalogue, items=(RAW_ITEM,)
    )


@weaver_test()
def test_an_object_loaded_before_the_threshold_is_selected():
    catalogue = mixed_estate().catalogue()

    assert f"{RAW}/Tables/Sales.Overdue" in stale_only_plan(
        catalogue, items=(RAW_ITEM,)
    )


@weaver_test()
def test_a_static_object_that_loaded_stays_green_however_long_ago():
    catalogue = mixed_estate().catalogue()

    assert f"{RAW}/Tables/Ref.Country" not in stale_only_plan(
        catalogue, items=(RAW_ITEM,)
    )


@weaver_test()
def test_a_static_object_that_has_never_loaded_is_selected():
    catalogue = mixed_estate().catalogue()

    assert f"{RAW}/Tables/Ref.Region" in stale_only_plan(catalogue, items=(RAW_ITEM,))


@weaver_test()
def test_an_object_whose_managed_ancestor_moved_is_selected():
    catalogue = (
        _Estate()
        .table(f"{RAW}/Tables/Sales.Source", loaded=at(1), moved=at(1))
        .table(f"{RAW}/Tables/Sales.Derived", loaded=at(2), moved=at(6))
        .reads(f"{RAW}/Tables/Sales.Derived", "Sales.Source")
        .catalogue()
    )

    selected = stale_only_plan(catalogue, items=(RAW_ITEM,))

    assert selected == {f"{RAW}/Tables/Sales.Derived"}


@weaver_test()
@pytest.mark.parametrize(
    "result,selected",
    [
        ("failed", True),
        ("error", True),
        ("blocked", True),
        ("rejected", True),
        ("succeeded", False),
    ],
)
def test_every_non_green_load_outcome_is_selected(result, selected):
    catalogue = (
        _Estate()
        .table(f"{RAW}/Tables/Sales.Order", loaded=at(1), moved=at(1), result=result)
        .catalogue()
    )

    plan = stale_only_plan(catalogue, items=(RAW_ITEM,))

    assert (f"{RAW}/Tables/Sales.Order" in plan) is selected


# --- the freshness instant ----------------------------------------------------


@weaver_test()
def test_an_explicit_as_of_moves_what_both_call_stale():
    catalogue = mixed_estate().catalogue()
    long_ago = at(500)

    assert stale_only_plan(catalogue, items=(RAW_ITEM,), as_of=long_ago) == {
        f"{RAW}/Tables/Sales.Rebuilt",
        f"{RAW}/Tables/Ref.Region",
    }


@weaver_test()
def test_the_default_threshold_is_a_day_before_the_operation_started():
    """Both operations resolve ``as_of`` through the one Health helper."""

    from weaver.health import DEFAULT_AGE_HOURS, resolve_as_of

    started = datetime(2026, 4, 23, 12, tzinfo=timezone.utc)

    assert resolve_as_of(None, started=started) == started - timedelta(
        hours=DEFAULT_AGE_HOURS
    )


# --- ordering, which stale-only never suspends --------------------------------


@weaver_test()
def test_two_selected_loadables_keep_their_ordering_edge():
    """A selection is not ``names=``: the ordinary traversal still applies."""

    catalogue = (
        _Estate()
        .table(f"{RAW}/Tables/Sales.Source", loaded=at(48), moved=at(48))
        .table(f"{RAW}/Tables/Sales.Derived", loaded=at(48), moved=at(48))
        .reads(f"{RAW}/Tables/Sales.Derived", "Sales.Source")
        .catalogue()
    )
    assessment = assess_load(catalogue, as_of=YESTERDAY, items=(RAW_ITEM,))
    dag = load_dag(
        catalogue.dag(),
        items=(RAW_ITEM,),
        selection=assessment.unsettled_identities(),
    )

    assert dag.edges == (
        (
            "load:Lakehouse/Raw_LH/Tables/Sales.Source",
            "load:Lakehouse/Raw_LH/Tables/Sales.Derived",
        ),
    )


@weaver_test()
def test_the_same_selection_written_as_names_carries_no_ordering():
    """Why ``names=`` is not the mechanism: it is an operator override."""

    catalogue = (
        _Estate()
        .table(f"{RAW}/Tables/Sales.Source", loaded=at(48), moved=at(48))
        .table(f"{RAW}/Tables/Sales.Derived", loaded=at(48), moved=at(48))
        .reads(f"{RAW}/Tables/Sales.Derived", "Sales.Source")
        .catalogue()
    )
    dag = load_dag(
        catalogue.dag(),
        items=(RAW_ITEM,),
        names=("Tables/Sales.Source", "Tables/Sales.Derived"),
    )

    assert dag.edges == ()


@weaver_test()
def test_a_green_upstream_is_not_pulled_in_by_a_selected_descendant():
    catalogue = (
        _Estate()
        .table(f"{RAW}/Tables/Sales.Source", loaded=at(1), moved=at(1))
        .table(f"{RAW}/Tables/Sales.Derived", loaded=at(48), moved=at(48))
        .reads(f"{RAW}/Tables/Sales.Derived", "Sales.Source")
        .catalogue()
    )

    assert stale_only_plan(catalogue, items=(RAW_ITEM,)) == {
        f"{RAW}/Tables/Sales.Derived"
    }


@weaver_test()
def test_ordering_survives_an_unselected_loadable_between_two_selected_ones():
    """A loadable the selection leaves out is crossed the way a view is."""

    catalogue = (
        _Estate()
        .table(f"{RAW}/Tables/Sales.A", loaded=at(48), moved=at(48))
        .table(f"{RAW}/Tables/Sales.B", loaded=at(1), moved=at(1))
        .table(f"{RAW}/Tables/Sales.C", loaded=at(48), moved=at(48))
        .reads(f"{RAW}/Tables/Sales.B", "Sales.A")
        .reads(f"{RAW}/Tables/Sales.C", "Sales.B")
        .catalogue()
    )
    assessment = assess_load(catalogue, as_of=YESTERDAY, items=(RAW_ITEM,))
    dag = load_dag(
        catalogue.dag(),
        items=(RAW_ITEM,),
        selection=assessment.unsettled_identities(),
    )

    assert {node.node_id for node in dag.nodes} == {
        "load:Lakehouse/Raw_LH/Tables/Sales.A",
        "load:Lakehouse/Raw_LH/Tables/Sales.C",
    }
    assert dag.edges == (
        (
            "load:Lakehouse/Raw_LH/Tables/Sales.A",
            "load:Lakehouse/Raw_LH/Tables/Sales.C",
        ),
    )


@weaver_test()
def test_an_endpoint_refresh_barrier_still_stands_between_two_targets():
    catalogue = (
        _Estate()
        .table(f"{RAW}/Tables/Sales.Order", loaded=at(48), moved=at(48))
        .table(f"{REPORTING}/Sales.Summary", loaded=at(48), moved=at(48))
        .shortcut(f"{REPORTING}/Sales.Order", f"{RAW}/Tables/Sales.Order")
        .reads(f"{REPORTING}/Sales.Summary", "Sales.Order")
        .catalogue()
    )
    items = (RAW_ITEM, REPORTING_ITEM)
    assessment = assess_load(catalogue, as_of=YESTERDAY, items=items)
    dag = load_dag(
        catalogue.dag(), items=items, selection=assessment.unsettled_identities()
    )

    refresh = [node for node in dag.nodes if node.primitive_kind == ENDPOINT_REFRESH]
    assert [node.node_id for node in refresh] == ["refresh:Lakehouse/Raw_LH"]
    assert (
        "load:Lakehouse/Raw_LH/Tables/Sales.Order",
        "refresh:Lakehouse/Raw_LH",
    ) in dag.edges
    assert (
        "refresh:Lakehouse/Raw_LH",
        "load:Warehouse/Reporting_WH/Sales.Summary",
    ) in dag.edges


# --- scope --------------------------------------------------------------------


@weaver_test()
def test_the_item_scope_still_bounds_a_stale_only_load():
    catalogue = (
        _Estate()
        .table(f"{RAW}/Tables/Sales.Order", loaded=at(48), moved=at(48))
        .table(f"{CURATED}/Tables/Sales.Daily", loaded=at(48), moved=at(48))
        .catalogue()
    )

    assert stale_only_plan(catalogue, items=(CURATED_ITEM,)) == {
        f"{CURATED}/Tables/Sales.Daily"
    }


# --- an ordinary load is untouched --------------------------------------------


@weaver_test()
def test_no_selection_runs_every_loadable_the_items_own():
    catalogue = mixed_estate().catalogue()

    assert planned(catalogue, items=(RAW_ITEM,), selection=None) == {
        f"{RAW}/Tables/Sales.Fresh",
        f"{RAW}/Tables/Sales.Overdue",
        f"{RAW}/Tables/Sales.Rebuilt",
        f"{RAW}/Tables/Ref.Country",
        f"{RAW}/Tables/Ref.Region",
    }


@weaver_test()
def test_an_empty_selection_plans_nothing():
    catalogue = mixed_estate().catalogue()

    assert planned(catalogue, items=(RAW_ITEM,), selection=()) == set()


# --- what the request may not say ---------------------------------------------


@weaver_test()
def test_as_of_without_stale_only_is_refused():
    with pytest.raises(CommandError, match="as-of is the freshness instant"):
        weaver.load(RAW, as_of="2026-09-05T00:00:00Z")


@weaver_test()
def test_reload_with_stale_only_is_refused():
    with pytest.raises(CommandError, match="reload and stale-only"):
        weaver.load(RAW, stale_only=True, reload=True)


@weaver_test()
def test_a_naive_as_of_is_refused_in_the_same_words_health_refuses_it():
    with pytest.raises(CommandError, match="must carry a timezone"):
        weaver.load(RAW, stale_only=True, as_of="2026-09-05T00:00:00")


# --- the operation ------------------------------------------------------------


@pytest.fixture
def prepared(tmp_path):
    """A Session and workspace for a dry run over a hand-written estate."""

    from support.sessions import given_session

    workspace = given_workspace(catalogue="Warehouse/Weaver")
    return workspace, given_session(workspace=workspace)


def dry_run(prepared, catalogue, **policy):
    workspace, session = prepared
    return run_load(
        session,
        workspace=workspace,
        state=RunState(catalogue=catalogue),
        dry_run=True,
        **policy,
    )


@weaver_test()
def test_a_stale_only_dry_run_reports_the_assessment_it_selected_from(prepared):
    catalogue = mixed_estate().catalogue()
    report = dry_run(
        prepared,
        catalogue,
        items=(RAW_ITEM,),
        stale_only=True,
        as_of=YESTERDAY,
    )

    assert {node.logical_id for node in report.nodes} == selection_of(
        catalogue, items=(RAW_ITEM,)
    )


@weaver_test()
def test_a_stale_only_run_that_selects_nothing_succeeds(prepared):
    """A healthy estate is nothing to do, which is a success and not a fault."""

    catalogue = (
        _Estate()
        .table(f"{RAW}/Tables/Sales.Order", loaded=at(1), moved=at(1))
        .catalogue()
    )
    report = dry_run(
        prepared, catalogue, items=(RAW_ITEM,), stale_only=True, as_of=YESTERDAY
    )

    assert report.nodes == ()
    assert report.status == TASK_SUCCEEDED
    assert report.succeeded


@weaver_test()
def test_an_empty_run_plan_that_is_not_a_dry_run_also_succeeds():
    from weaver.run.result import RUN_SUCCEEDED, run_status

    assert run_status((), dry_run=False) == RUN_SUCCEEDED
    assert run_status((), dry_run=True) == RUN_SUCCEEDED


@weaver_test()
def test_stale_only_widens_the_one_catalogue_read_to_load_status(prepared, monkeypatch):
    """One read, widened. Not a second read and not a nested health operation."""

    catalogue = mixed_estate().catalogue()
    seen = {}

    def recorder(*, session, workspace, tables=None):
        seen["tables"] = tables
        return catalogue

    monkeypatch.setattr("weaver.run.state.read_installed_catalogue", recorder)
    workspace, session = prepared
    run_load(
        session,
        workspace=workspace,
        items=(RAW_ITEM,),
        dry_run=True,
        stale_only=True,
        as_of=YESTERDAY,
    )

    assert LOAD_STATUS in seen["tables"]


@weaver_test()
def test_an_ordinary_load_reads_the_catalogue_it_always_read(prepared, monkeypatch):
    catalogue = mixed_estate().catalogue()
    seen = {}

    def recorder(*, session, workspace, tables=None):
        seen["tables"] = tables
        return catalogue

    monkeypatch.setattr("weaver.run.state.read_installed_catalogue", recorder)
    workspace, session = prepared
    run_load(session, workspace=workspace, items=(RAW_ITEM,), dry_run=True)

    assert seen["tables"] is None
