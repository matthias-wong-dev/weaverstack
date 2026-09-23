"""A mirrored Warehouse through reconciliation, selection and bundle generation.

What a mirror leaves is a Registry row saying Table, a ``_.Mirror`` row saying
View, and a Warehouse holding a View. Every claim here reads all three.

Pure Python, through the two functions a build runs:
:func:`weaver.catalogue.state.reconcile_catalogue_state` and
:func:`weaver.build_bundle.planner.generate_item_build_bundle`.
"""

from __future__ import annotations

from pathlib import Path

from factories import (
    built_catalogue,
    item_id,
    plan_actions,
    schema_document,
    warehouse_table,
)
from support.weaver_test import weaver_test
from support.workspaces import WORKSPACE
from warehouse_mirror import (
    ITEM,
    batch_statements,
    mirror_bindings,
    mirror_inventory,
    mirror_object,
    with_borrowed,
)

from weaver.build_bundle import WarehouseBinding, generate_item_build_bundle
from weaver.build_bundle.catalogue_actions import DEREGISTER_MIRROR_SLUG
from weaver.catalogue.state import Catalogue, reconcile_catalogue_state
from weaver.declaration import parse_item_repository
from weaver.locations import Location
from weaver.store import FilesystemStore
from weaver.targets import ItemRef

#: Both of the item's tables, borrowed to begin with.
BORROWED = ("Customer", "Region")

#: The changed declaration, which adds a column to ``Sales.Customer``.
CHANGED = "select cast(1 as int) as CustomerId, cast('x' as varchar(10)) as Name"


# --- the estate ---------------------------------------------------------------


def _estate(root: Path, *, customer: str = "select cast(1 as int) as CustomerId"):
    """One Warehouse item with two tables that do not depend on each other."""

    documents = {
        f"{ITEM}/schemas/Sales.yml": schema_document("Sales"),
        f"{ITEM}/Sales.Customer.sql": warehouse_table(
            "Sales.Customer", select=customer
        ),
        f"{ITEM}/Sales.Region.sql": warehouse_table(
            "Sales.Region",
            select="select cast(1 as int) as RegionId",
            primary_key="RegionId",
        ),
    }
    for relative, text in documents.items():
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    return parse_item_repository(Location(str(root)))


def _build(repository, output: Path, *, catalogue, inventory):
    """One bundle, through the reconciliation a build runs before planning."""

    inventories = {item_id(ITEM): inventory}
    reconciliation = reconcile_catalogue_state(catalogue, inventories=inventories)
    bundle = generate_item_build_bundle(
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
    return reconciliation, bundle


# --- reconciliation -----------------------------------------------------------


@weaver_test()
def test_a_view_at_a_borrowed_address_leaves_the_registry_claim_standing(tmp_path):
    """The inventory is asked what should stand there, not what Registry says."""

    repository = _estate(tmp_path / "repo")
    catalogue = with_borrowed(built_catalogue(repository, mirror_bindings()), *BORROWED)

    reconciliation = reconcile_catalogue_state(
        catalogue,
        inventories={item_id(ITEM): mirror_inventory(repository, borrowed=BORROWED)},
    )

    assert reconciliation.stale_objects == ()
    registered = reconciliation.catalogue.registered[mirror_object("Customer")]
    assert registered.object_type == "table"
    assert reconciliation.catalogue.is_mirrored(mirror_object("Customer"))


@weaver_test()
def test_the_same_view_with_nothing_borrowed_disproves_the_claim(tmp_path):
    """The other half, so the claim above is about ``_.Mirror`` and not the read.

    One catalogue holds the Mirror rows and one does not. Everything else, the
    repository and the inventory included, is the same.
    """

    repository = _estate(tmp_path / "repo")

    reconciliation = reconcile_catalogue_state(
        built_catalogue(repository, mirror_bindings()),
        inventories={item_id(ITEM): mirror_inventory(repository, borrowed=BORROWED)},
    )

    assert str(mirror_object("Customer")) in reconciliation.stale_objects
    assert mirror_object("Customer") not in reconciliation.catalogue.registered


# --- an unchanged build -------------------------------------------------------


@weaver_test()
def test_an_unchanged_build_over_a_mirrored_item_plans_nothing(tmp_path):
    """The complete plan: no drop, no build, and no deregistration."""

    repository = _estate(tmp_path / "repo")

    _reconciled, bundle = _build(
        repository,
        tmp_path / "bundle",
        catalogue=with_borrowed(
            built_catalogue(repository, mirror_bindings()), *BORROWED
        ),
        inventory=mirror_inventory(repository, borrowed=BORROWED),
    )

    assert plan_actions(bundle) == []


@weaver_test()
def test_the_first_build_of_this_estate_does_do_work(tmp_path):
    """Guards the claim above from being satisfied by a planner that plans."""

    repository = _estate(tmp_path / "repo")

    _reconciled, bundle = _build(
        repository,
        tmp_path / "bundle",
        catalogue=Catalogue(rows={}),
        inventory=mirror_inventory(repository, borrowed=()),
    )

    assert plan_actions(bundle)


# --- a changed declaration ----------------------------------------------------


def _selective(tmp_path: Path):
    """A build of a changed ``Sales.Customer`` over a fully borrowed estate."""

    installed = _estate(tmp_path / "installed")
    changed = _estate(tmp_path / "changed", customer=CHANGED)
    return _build(
        changed,
        tmp_path / "bundle",
        catalogue=with_borrowed(
            built_catalogue(installed, mirror_bindings()), *BORROWED
        ),
        inventory=mirror_inventory(installed, borrowed=BORROWED),
    )


@weaver_test()
def test_only_the_changed_object_is_selected(tmp_path):
    _reconciled, bundle = _selective(tmp_path)

    selected = {str(identity) for identity in bundle.plan.selection.selected_for_build}

    assert str(mirror_object("Customer")) in selected
    assert str(mirror_object("Region")) not in selected


@weaver_test()
def test_the_borrowed_view_is_dropped_as_a_view_and_rebuilt_as_a_table(tmp_path):
    """Registry says Table and the address holds a View, so the drop reads both."""

    _reconciled, bundle = _selective(tmp_path)
    actions = plan_actions(bundle)

    dropped = [action for action in actions if action.kind == "drop_view"]
    built = [action for action in actions if action.kind == "build_table"]

    assert [action.resource_node_id for action in dropped] == [
        str(mirror_object("Customer"))
    ]
    assert [action.resource_node_id for action in built] == [
        str(mirror_object("Customer"))
    ]


@weaver_test()
def test_only_the_materialised_object_stops_being_borrowed(tmp_path):
    _reconciled, bundle = _selective(tmp_path)

    ((_number, _batch, action),) = [
        each for each in bundle.plan.actions() if each[2].id == DEREGISTER_MIRROR_SLUG
    ]
    statements = batch_statements(bundle, action)

    assert "N'Customer'" in statements
    assert "N'Region'" not in statements


@weaver_test()
def test_the_row_goes_after_the_physical_work_and_before_publication(tmp_path):
    """The transition completes only once the build that owns the rows has run."""

    _reconciled, bundle = _selective(tmp_path)
    ordered = [action.id for action in plan_actions(bundle)]
    kinds = {action.id: action.kind for action in plan_actions(bundle)}

    at = ordered.index(DEREGISTER_MIRROR_SLUG)
    physical = [
        position
        for position, name in enumerate(ordered)
        if kinds[name] in {"drop_view", "build_table", "build_procedure"}
    ]
    published = [
        position
        for position, name in enumerate(ordered)
        if kinds[name] in {"publish_catalogue", "publish_registry"} and position != at
    ]

    assert max(physical) < at < min(published)
    assert kinds[ordered[-1]] == "publish_registry"


# --- what the installed graph makes of it -------------------------------------


@weaver_test()
def test_the_installed_graph_reads_a_borrowed_node_as_a_view_it_may_not_load(tmp_path):
    """One node borrowed and one holding its own rows, from one catalogue."""

    repository = _estate(tmp_path / "repo")
    catalogue = with_borrowed(
        built_catalogue(repository, mirror_bindings()), "Customer"
    )

    dag = catalogue.dag()
    borrowed = dag.node(str(mirror_object("Customer")))
    local = dag.node(str(mirror_object("Region")))

    assert borrowed.is_installed and borrowed.is_mirrored
    assert borrowed.effective_object_type == "view"
    assert borrowed.physical.object_type == "view"
    assert not borrowed.can_load

    assert not local.is_mirrored
    assert local.effective_object_type == "table"
    assert local.can_load
