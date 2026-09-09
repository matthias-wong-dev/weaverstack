"""A mirrored Warehouse through reconciliation, selection and bundle generation.

What a mirror leaves is a Registry row saying Table, a ``_.Mirror`` row saying
View, and a Warehouse holding a View. Every claim here reads all three.

Pure Python, through the two functions a build runs:
:func:`weaver.catalogue.state.reconcile_catalogue_state` and
:func:`weaver.build_bundle.planner.generate_item_build_bundle`.
"""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

from factories import (
    FixtureInventory,
    item_bindings,
    item_id,
    schema_document,
    warehouse_table,
)
from support.weaver_test import weaver_test
from support.workspaces import WORKSPACE

from weaver.build_bundle import WarehouseBinding, generate_item_build_bundle
from weaver.build_bundle.catalogue_actions import (
    DEREGISTER_MIRROR_SLUG,
    desired_catalogue,
)
from weaver.build_bundle.planner import certifiable_identities
from weaver.catalogue.state import Catalogue, reconcile_catalogue_state
from weaver.catalogue.tables import MIRROR, PROJECTED_TABLES
from weaver.declaration import parse_item_repository
from weaver.declaration.metadata import ObjectId
from weaver.declaration.model import WeaverDocumentId
from weaver.locations import Location
from weaver.store import FilesystemStore
from weaver.targets import ItemRef

ITEM = "Warehouse/Model"
#: The Warehouse the mirror is built in, and the one it borrows rows from.
TARGET = "Model_Dev"
SOURCE_TARGET = "Model"
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


def _object(name: str) -> WeaverDocumentId:
    return WeaverDocumentId(item_id(ITEM), ObjectId("Sales", name))


def _bindings():
    return item_bindings((ITEM, TARGET))


def _installed(repository) -> Catalogue:
    """The catalogue a successful build of this estate leaves behind.

    Composed from the two functions the build itself uses. ``materialised``
    names every projected table, because reconciliation may only raise a claim
    against a table that is there.
    """

    bindings = _bindings()
    by_item = {binding.item: binding for binding in bindings.entries}
    state = desired_catalogue(
        repository,
        certifiable_identities(repository, by_item),
        {binding.item: binding.to_bound_target() for binding in bindings.entries},
    )
    return Catalogue(
        rows=state.rows,
        materialised=frozenset(table.name for table in PROJECTED_TABLES),
    )


def _borrowing(catalogue: Catalogue, *names: str) -> Catalogue:
    """The same catalogue, with those objects recorded as borrowed."""

    item = item_id(ITEM)
    rows = {each: dict(tables) for each, tables in catalogue.rows.items()}
    rows[item][MIRROR.name] = tuple(
        {
            "item_type": item.item_type,
            "item_name": item.item_name,
            "schema_name": "Sales",
            "object_name": name,
            "source_workspace_name": WORKSPACE,
            "source_target_name": SOURCE_TARGET,
            "source_schema_name": "Sales",
            "source_object_name": name,
            "physical_type": "view",
        }
        for name in names
    )
    return Catalogue(rows=rows, materialised=catalogue.materialised | {MIRROR.name})


def _inventory(repository, *, borrowed: tuple[str, ...] = BORROWED):
    """The Warehouse as a mirror leaves it: a View at each borrowed address."""

    bound = {b.item: b.to_bound_target() for b in _bindings().entries}
    inventory = FixtureInventory.from_repository(
        repository,
        item=ITEM,
        target_id=bound[item_id(ITEM)].id,
        kind="warehouse",
        target_name=TARGET,
    )
    names = {f"Sales.{name}" for name in borrowed}
    return replace(
        inventory,
        tables=tuple(name for name in inventory.tables if name not in names),
        views=tuple(sorted(set(inventory.views) | names)),
    )


def _build(repository, output: Path, *, catalogue, inventory):
    """One bundle, through the reconciliation a build runs before planning."""

    inventories = {item_id(ITEM): inventory}
    reconciliation = reconcile_catalogue_state(catalogue, inventories=inventories)
    bundle = generate_item_build_bundle(
        repository,
        bindings=_bindings(),
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


def _actions(bundle):
    return [action for _sequence, _batch, action in bundle.plan.actions()]


def _statements(bundle, action) -> str:
    """One T-SQL batch action's statements, as one string to search."""

    content = FilesystemStore().read(bundle.location.join(*action.payload.split("/")))
    return "\n".join(json.loads(content.decode("utf-8")))


# --- reconciliation -----------------------------------------------------------


@weaver_test()
def test_a_view_at_a_borrowed_address_leaves_the_registry_claim_standing(tmp_path):
    """The inventory is asked what should stand there, not what Registry says."""

    repository = _estate(tmp_path / "repo")
    catalogue = _borrowing(_installed(repository), *BORROWED)

    reconciliation = reconcile_catalogue_state(
        catalogue, inventories={item_id(ITEM): _inventory(repository)}
    )

    assert reconciliation.stale_objects == ()
    assert reconciliation.catalogue.registered[_object("Customer")].object_type == (
        "table"
    )
    assert reconciliation.catalogue.is_mirrored(_object("Customer"))


@weaver_test()
def test_the_same_view_with_nothing_borrowed_disproves_the_claim(tmp_path):
    """The other half, so the claim above is about ``_.Mirror`` and not the read.

    One catalogue holds the Mirror rows and one does not. Everything else, the
    repository and the inventory included, is the same.
    """

    repository = _estate(tmp_path / "repo")

    reconciliation = reconcile_catalogue_state(
        _installed(repository), inventories={item_id(ITEM): _inventory(repository)}
    )

    assert str(_object("Customer")) in reconciliation.stale_objects
    assert _object("Customer") not in reconciliation.catalogue.registered


# --- an unchanged build -------------------------------------------------------


@weaver_test()
def test_an_unchanged_build_over_a_mirrored_item_plans_nothing(tmp_path):
    """The complete plan: no drop, no build, and no deregistration."""

    repository = _estate(tmp_path / "repo")

    _reconciled, bundle = _build(
        repository,
        tmp_path / "bundle",
        catalogue=_borrowing(_installed(repository), *BORROWED),
        inventory=_inventory(repository),
    )

    assert _actions(bundle) == []


@weaver_test()
def test_the_first_build_of_this_estate_does_do_work(tmp_path):
    """Guards the claim above from being satisfied by a planner that plans."""

    repository = _estate(tmp_path / "repo")

    _reconciled, bundle = _build(
        repository,
        tmp_path / "bundle",
        catalogue=Catalogue(rows={}),
        inventory=_inventory(repository, borrowed=()),
    )

    assert _actions(bundle)


# --- a changed declaration ----------------------------------------------------


def _selective(tmp_path: Path):
    """A build of a changed ``Sales.Customer`` over a fully borrowed estate."""

    installed = _estate(tmp_path / "installed")
    changed = _estate(tmp_path / "changed", customer=CHANGED)
    return _build(
        changed,
        tmp_path / "bundle",
        catalogue=_borrowing(_installed(installed), *BORROWED),
        inventory=_inventory(installed),
    )


@weaver_test()
def test_only_the_changed_object_is_selected(tmp_path):
    _reconciled, bundle = _selective(tmp_path)

    selected = {str(identity) for identity in bundle.plan.selection.selected_for_build}

    assert str(_object("Customer")) in selected
    assert str(_object("Region")) not in selected


@weaver_test()
def test_the_borrowed_view_is_dropped_as_a_view_and_rebuilt_as_a_table(tmp_path):
    """Registry says Table and the address holds a View, so the drop reads both."""

    _reconciled, bundle = _selective(tmp_path)
    actions = _actions(bundle)

    dropped = [action for action in actions if action.kind == "drop_view"]
    built = [action for action in actions if action.kind == "build_table"]

    assert [action.resource_node_id for action in dropped] == [str(_object("Customer"))]
    assert [action.resource_node_id for action in built] == [str(_object("Customer"))]


@weaver_test()
def test_only_the_materialised_object_stops_being_borrowed(tmp_path):
    _reconciled, bundle = _selective(tmp_path)

    ((_number, _batch, action),) = [
        each for each in bundle.plan.actions() if each[2].id == DEREGISTER_MIRROR_SLUG
    ]
    statements = _statements(bundle, action)

    assert "N'Customer'" in statements
    assert "N'Region'" not in statements


@weaver_test()
def test_the_row_goes_after_the_physical_work_and_before_publication(tmp_path):
    """The transition completes only once the build that owns the rows has run."""

    _reconciled, bundle = _selective(tmp_path)
    ordered = [action.id for action in _actions(bundle)]
    kinds = {action.id: action.kind for action in _actions(bundle)}

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
    catalogue = _borrowing(_installed(repository), "Customer")

    dag = catalogue.dag()
    borrowed = dag.node(str(_object("Customer")))
    local = dag.node(str(_object("Region")))

    assert borrowed.is_installed and borrowed.is_mirrored
    assert borrowed.effective_object_type == "view"
    assert borrowed.physical.object_type == "view"
    assert not borrowed.is_loadable

    assert not local.is_mirrored
    assert local.effective_object_type == "table"
    assert local.is_loadable
