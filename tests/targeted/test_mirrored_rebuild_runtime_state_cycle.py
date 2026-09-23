"""What a build owes a mirrored object it physically rebuilds.

A mirror leaves the destination reading another catalogue's rows, including its
``_.LoadStatus`` and ``_.Bookmark``. When a build reconstructs one of those
objects locally, the state it inherited describes the previous incarnation. The
build establishes ``Pending`` and the bookmark sentinel before the drop, and
stops recording the object as borrowed only once the local table is built.

The topology is ``Source -> Aggregate -> Report``, with every object borrowed
and only ``Source`` changed, so the claim is about a dependency impact rather
than an authored change. ``Sales.Reference`` is borrowed and depends on
nothing, and proves the build leaves it alone.

The second half reads the catalogue such a build leaves and asks what
``weaver load --stale`` does with it.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from factories import (
    built_catalogue,
    catalogue_inventory,
    item_id,
    plan_actions,
    schema_document,
    warehouse_table,
)
from support.weaver_test import weaver_test
from support.workspaces import WORKSPACE
from test_health_representation import REPORTING, YESTERDAY, _Estate, at
from test_load_stale_selection_boundary import stale_plan
from warehouse_mirror import (
    ITEM,
    SOURCE_TARGET,
    batch_statements,
    mirror_bindings,
    mirror_inventory,
    mirror_object,
    with_borrowed,
)

from weaver.build_bundle import WarehouseBinding, generate_item_build_bundle
from weaver.build_bundle.catalogue_actions import DEREGISTER_MIRROR_SLUG
from weaver.catalogue.builtin import BUILTIN_ITEM
from weaver.catalogue.state import Catalogue, reconcile_catalogue_state
from weaver.catalogue.tables import (
    BOOKMARK,
    BOOKMARK_SENTINEL_TEXT,
    LOAD_STATUS,
    PENDING,
)
from weaver.declaration import parse_item_repository
from weaver.declaration.model import WeaverItemId
from weaver.health import assess_load
from weaver.load_plan import load_dag
from weaver.locations import Location
from weaver.store import FilesystemStore
from weaver.targets import ItemRef

#: Borrowed to begin with: the changed root, its dependants, and an unrelated one.
BORROWED = ("Aggregate", "Reference", "Report", "Source")

#: The changed ``Sales.Source`` declaration, which adds a column.
CHANGED = "select cast(1 as int) as SourceId, cast('x' as varchar(10)) as Name"


# --- the estate ---------------------------------------------------------------


def _estate(root: Path, *, source: str = "select cast(1 as int) as SourceId"):
    documents = {
        f"{ITEM}/schemas/Sales.yml": schema_document("Sales"),
        f"{ITEM}/Sales.Source.sql": warehouse_table(
            "Sales.Source", select=source, primary_key="SourceId"
        ),
        f"{ITEM}/Sales.Aggregate.sql": warehouse_table(
            "Sales.Aggregate",
            select="select cast(SourceId as int) as SourceId from Sales.Source",
            primary_key="SourceId",
        ),
        f"{ITEM}/Sales.Report.sql": warehouse_table(
            "Sales.Report",
            select="select cast(SourceId as int) as SourceId from Sales.Aggregate",
            primary_key="SourceId",
        ),
        f"{ITEM}/Sales.Reference.sql": warehouse_table(
            "Sales.Reference",
            select="select cast(1 as int) as ReferenceId",
            primary_key="ReferenceId",
        ),
    }
    for relative, text in documents.items():
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    return parse_item_repository(Location(str(root)))


def _settled(catalogue: Catalogue, *names: str) -> Catalogue:
    """The runtime state a mirror copies in: every object loaded and settled."""

    from factories import bookmark_row_for, load_status_row

    item = item_id(ITEM)
    rows = {each: dict(tables) for each, tables in catalogue.rows.items()}
    rows[item][LOAD_STATUS.name] = tuple(
        load_status_row(mirror_object(name), result="succeeded", completed_at=at(1))
        for name in names
    )
    rows[item][BOOKMARK.name] = tuple(
        bookmark_row_for(mirror_object(name), at(1)) for name in names
    )
    return Catalogue(
        rows=rows,
        materialised=catalogue.materialised | {LOAD_STATUS.name, BOOKMARK.name},
    )


def _build(repository, output: Path, *, catalogue, inventory):
    inventories = {
        item_id(ITEM): inventory,
        BUILTIN_ITEM: catalogue_inventory(holding=True),
    }
    reconciliation = reconcile_catalogue_state(catalogue, inventories=inventories)
    return generate_item_build_bundle(
        repository,
        bindings=mirror_bindings(),
        output=Location(str(output)),
        store=FilesystemStore(),
        target_inventories=inventories,
        catalogue=reconciliation.catalogue,
        stale_claims=reconciliation.stale_claims,
        catalogue_binding=WarehouseBinding(
            ItemRef("Weaver_Control"), workspace_name=WORKSPACE
        ),
    )


@pytest.fixture
def rebuilt(tmp_path):
    """A build of a changed ``Sales.Source`` over a fully borrowed estate."""

    installed = _estate(tmp_path / "installed")
    changed = _estate(tmp_path / "changed", source=CHANGED)
    return _build(
        changed,
        tmp_path / "bundle",
        catalogue=_settled(
            with_borrowed(built_catalogue(installed, mirror_bindings()), *BORROWED),
            *BORROWED,
        ),
        inventory=mirror_inventory(installed, borrowed=BORROWED),
    )


def _established(bundle, table) -> dict[str, dict]:
    """Every runtime-state row the build writes before physical work, by object."""

    return {
        row["object_name"]: row
        for one in bundle.plan.runtime_state_established
        if one.table == table.name
        for row in one.rows
    }


# --- selection ----------------------------------------------------------------


@weaver_test()
def test_a_changed_source_carries_its_borrowed_dependants_into_the_build(rebuilt):
    """The dependency impact, which is the reason these objects are rebuilt."""

    selection = rebuilt.plan.selection
    changed = {str(identity) for identity in selection.impact.changed}
    descendants = {str(identity) for identity in selection.impact.impacted_descendants}
    selected = {str(identity) for identity in selection.selected_for_build}

    assert str(mirror_object("Source")) in changed
    assert str(mirror_object("Aggregate")) in descendants
    assert str(mirror_object("Report")) in descendants
    assert str(mirror_object("Aggregate")) in selected
    assert str(mirror_object("Report")) in selected
    assert str(mirror_object("Reference")) not in selected


# --- runtime-state intent -----------------------------------------------------


@weaver_test()
def test_a_rebuilt_dependant_is_left_pending_with_a_sentinel_bookmark(rebuilt):
    """The inherited state described the incarnation this build destroys."""

    statuses = _established(rebuilt, LOAD_STATUS)
    bookmarks = _established(rebuilt, BOOKMARK)

    assert statuses["Aggregate"]["result"] == PENDING
    assert statuses["Report"]["result"] == PENDING
    assert bookmarks["Aggregate"]["bookmark_datetime"] == BOOKMARK_SENTINEL_TEXT
    assert bookmarks["Report"]["bookmark_datetime"] == BOOKMARK_SENTINEL_TEXT


@weaver_test()
def test_an_untouched_borrowed_object_keeps_the_state_it_borrows(rebuilt):
    """The other half, so the claim above is about selection and not about all."""

    assert "Reference" not in _established(rebuilt, LOAD_STATUS)
    assert "Reference" not in _established(rebuilt, BOOKMARK)


@weaver_test()
def test_runtime_state_is_established_before_the_first_physical_action(rebuilt):
    """A failed build must not leave settled state over a dropped table."""

    kinds = [action.kind for action in plan_actions(rebuilt)]
    physical = [
        position
        for position, kind in enumerate(kinds)
        if kind in {"drop_view", "drop_table", "build_table", "build_procedure"}
    ]

    assert kinds.index("reconcile_runtime_state") < min(physical)


@weaver_test()
def test_the_established_state_is_what_the_catalogue_action_carries(rebuilt):
    """The plan's intent and the payload the executor reads are one thing."""

    ((_number, _batch, action),) = [
        each
        for each in rebuilt.plan.actions()
        if each[2].kind == "reconcile_runtime_state"
    ]
    content = FilesystemStore().read(rebuilt.location.join(*action.payload.split("/")))
    established = {
        one["table"]: {row["object_name"]: row for row in one["rows"]}
        for one in json.loads(content.decode("utf-8"))["establish"]
    }

    assert established[LOAD_STATUS.name]["Aggregate"]["result"] == PENDING
    assert (
        established[BOOKMARK.name]["Aggregate"]["bookmark_datetime"]
        == BOOKMARK_SENTINEL_TEXT
    )
    assert "Reference" not in established[LOAD_STATUS.name]


# --- mirror deregistration ----------------------------------------------------


@weaver_test()
def test_only_the_objects_this_build_materialises_stop_being_borrowed(rebuilt):
    ((_number, _batch, action),) = [
        each for each in rebuilt.plan.actions() if each[2].id == DEREGISTER_MIRROR_SLUG
    ]
    statements = batch_statements(rebuilt, action)

    assert "N'Source'" in statements
    assert "N'Aggregate'" in statements
    assert "N'Report'" in statements
    assert "N'Reference'" not in statements


@weaver_test()
def test_the_mirror_row_goes_only_after_the_local_table_is_built(rebuilt):
    ordered = [action.id for action in plan_actions(rebuilt)]
    kinds = {action.id: action.kind for action in plan_actions(rebuilt)}

    at_deregistration = ordered.index(DEREGISTER_MIRROR_SLUG)
    built = [
        position
        for position, name in enumerate(ordered)
        if kinds[name] == "build_table"
    ]

    assert max(built) < at_deregistration


# --- what a stale load then does ----------------------------------------------

STALE_ITEM = WeaverItemId.parse(REPORTING)


def _after_the_build() -> Catalogue:
    """The estate that build leaves: local Pending tables and one still borrowed.

    ``Reference`` keeps the settled state it borrows, so a stale load selecting
    it would be selecting a mirrored subject.
    """

    return (
        _Estate()
        .table(f"{REPORTING}/Sales.Source", result="pending")
        .table(f"{REPORTING}/Sales.Aggregate", result="pending")
        .table(f"{REPORTING}/Sales.Report", result="pending")
        .table(
            f"{REPORTING}/Sales.Reference",
            object_type="view",
            loaded=at(1),
            moved=at(1),
        )
        .mirrors(f"{REPORTING}/Sales.Reference", source_target=SOURCE_TARGET)
        .reads(f"{REPORTING}/Sales.Aggregate", "Sales.Source")
        .reads(f"{REPORTING}/Sales.Report", "Sales.Aggregate")
        .catalogue()
    )


@weaver_test()
def test_a_stale_load_runs_every_rebuilt_loadable_and_not_the_borrowed_one():
    """The failure this guards is a plan holding only the directly changed table."""

    assert stale_plan(_after_the_build(), items=[STALE_ITEM]) == {
        f"{REPORTING}/Sales.Source",
        f"{REPORTING}/Sales.Aggregate",
        f"{REPORTING}/Sales.Report",
    }


@weaver_test()
def test_the_stale_plan_keeps_the_order_the_rebuilt_objects_depend_in():
    """Rebuilt descendants must not run beside their rebuilt sources."""

    catalogue = _after_the_build()
    assessment = assess_load(catalogue, as_of=YESTERDAY, items=[STALE_ITEM])
    dag = load_dag(
        catalogue.dag(),
        items=[STALE_ITEM],
        selection=assessment.unsettled_identities(),
    )
    logical = {
        node.node_id: str(node.logical_id)
        for node in dag.nodes
        if node.logical_id is not None
    }
    edges = {
        (logical.get(producer), logical.get(consumer))
        for producer, consumer in dag.edges
        if logical.get(producer) and logical.get(consumer)
    }

    assert edges
    assert (f"{REPORTING}/Sales.Source", f"{REPORTING}/Sales.Aggregate") in edges
    assert (f"{REPORTING}/Sales.Aggregate", f"{REPORTING}/Sales.Report") in edges
