"""A mirrored Lakehouse through reconciliation, selection and bundle generation.

A borrowed Lakehouse relation stands at its declared address as its declared
type: a shortcut presents a table as a table and a folder as a folder, and a
wrapper view is a view. So Registry, ``_.Mirror`` and the inventory all agree,
and an unchanged build has nothing to do.

What ``_.Mirror`` decides here is the physical lifecycle. A borrowed table is a
pointer, so materialising it removes the shortcut and waits for OneLake to
release the name, rather than dropping storage the source owns.

Pure Python, through the two functions a build runs.
"""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

from factories import (
    ITEM,
    FixtureInventory,
    full_estate,
    item_bindings,
    item_id,
)
from support.weaver_test import weaver_test
from support.workspaces import WORKSPACE

from weaver.build_bundle import WarehouseBinding, generate_item_build_bundle
from weaver.build_bundle.catalogue_actions import (
    DEREGISTER_MIRROR_SLUG,
    desired_catalogue,
)
from weaver.build_bundle.planner import certifiable_identities
from weaver.build_bundle.shortcuts import ResolvedShortcutSource
from weaver.catalogue.state import Catalogue, reconcile_catalogue_state
from weaver.catalogue.tables import (
    MIRROR,
    PROJECTED_TABLES,
    STANDARD_SURFACE_TABLES,
)
from weaver.declaration import parse_item_repository
from weaver.declaration.metadata import ObjectId
from weaver.declaration.model import WeaverDocumentId
from weaver.locations import Location
from weaver.store import FilesystemStore
from weaver.targets import ItemRef

TARGET = "Sales_Dev"
SOURCE_TARGET = "Sales_LH"

#: What the fixture's Lakehouse item declares, by what a mirror puts there.
BORROWED = (
    ("Tables", "DWG", "Customer", "table"),
    ("Tables", "DWG", "Summary", "table"),
    ("Tables", "DWG", "ActiveCustomer", "view"),
    ("Files", "Raw", "CustomerCsv", "folder"),
)

#: The object whose declaration changes, and the one every claim watches.
MATERIALISED = "DWG.Customer"

#: Every runtime table a built Lakehouse presents, as its inventory reports it.
PRESENTED = tuple(table.name for table in STANDARD_SURFACE_TABLES)

RUNTIME_SOURCES = {
    table.name: ResolvedShortcutSource(
        workspace_id="ws-1",
        item_id="item-1",
        item_name="Weaver_Control",
        path=f"Tables/_/{table.name}",
    )
    for table in STANDARD_SURFACE_TABLES
}


# --- the estate ---------------------------------------------------------------


def _estate(root: Path, *, changed: bool = False):
    """The shared Lakehouse estate, optionally with one table's query changed."""

    repository = full_estate(root)
    if not changed:
        return repository
    path = root / ITEM / "Tables" / "DWG__Customer.py"
    text = path.read_text(encoding="utf-8")
    edited = text.replace("return [], []", "return [], []  # revised", 1)
    assert edited != text, "the declaration to change was not found"
    path.write_text(edited, encoding="utf-8")
    return parse_item_repository(Location(str(root)))


def _object(qualified: str, *, files: bool = False) -> WeaverDocumentId:
    schema, _, name = qualified.partition(".")
    return WeaverDocumentId(item_id(ITEM), ObjectId(schema, name), is_files=files)


def _bindings():
    return item_bindings((ITEM, TARGET))


def _installed(repository) -> Catalogue:
    """The catalogue a successful build of this estate leaves behind."""

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


def _borrowing(catalogue: Catalogue) -> Catalogue:
    """The same catalogue, with every data relation recorded as borrowed."""

    item = item_id(ITEM)
    rows = {each: dict(tables) for each, tables in catalogue.rows.items()}
    rows[item][MIRROR.name] = tuple(
        {
            "item_type": item.item_type,
            "item_name": item.item_name,
            "schema_name": f"{area}/{schema}",
            "object_name": name,
            "source_workspace_name": WORKSPACE,
            "source_target_name": SOURCE_TARGET,
            "source_schema_name": schema,
            "source_object_name": name,
            "physical_type": physical,
        }
        for area, schema, name, physical in BORROWED
    )
    return Catalogue(rows=rows, materialised=catalogue.materialised | {MIRROR.name})


def _inventory(repository):
    """The Lakehouse as a mirror leaves it.

    The same inventory a build leaves: a shortcut presents a table as a table,
    so nothing here says which objects are borrowed.
    """

    bound = {b.item: b.to_bound_target() for b in _bindings().entries}
    return replace(
        FixtureInventory.from_repository(
            repository,
            item=ITEM,
            target_id=bound[item_id(ITEM)].id,
            kind="lakehouse",
            target_name=TARGET,
        ),
        runtime_references=PRESENTED,
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
        shortcut_sources=RUNTIME_SOURCES,
    )
    return reconciliation, bundle


def _actions(bundle):
    return [action for _sequence, _batch, action in bundle.plan.actions()]


def _payload(bundle, action):
    content = FilesystemStore().read(bundle.location.join(*action.payload.split("/")))
    return json.loads(content.decode("utf-8"))


def _selective(tmp_path: Path):
    """A build of a changed ``DWG.Customer`` over a fully borrowed estate."""

    installed = _estate(tmp_path / "installed")
    changed = _estate(tmp_path / "changed", changed=True)
    return _build(
        changed,
        tmp_path / "bundle",
        catalogue=_borrowing(_installed(installed)),
        inventory=_inventory(installed),
    )


# --- an unchanged build -------------------------------------------------------


@weaver_test()
def test_a_borrowed_relation_stands_at_its_declared_address_as_itself(tmp_path):
    """Registry, ``_.Mirror`` and the inventory agree, so nothing is stale."""

    repository = _estate(tmp_path / "repo")
    catalogue = _borrowing(_installed(repository))

    reconciliation = reconcile_catalogue_state(
        catalogue, inventories={item_id(ITEM): _inventory(repository)}
    )

    assert reconciliation.stale_objects == ()
    assert reconciliation.catalogue.effective_physical_type(_object(MATERIALISED)) == (
        "table"
    )
    assert reconciliation.catalogue.is_mirrored(_object("Raw.CustomerCsv", files=True))


@weaver_test()
def test_an_unchanged_build_over_a_mirrored_lakehouse_plans_nothing(tmp_path):
    """The complete plan: no drop, no build, and no deregistration."""

    repository = _estate(tmp_path / "repo")

    _reconciled, bundle = _build(
        repository,
        tmp_path / "bundle",
        catalogue=_borrowing(_installed(repository)),
        inventory=_inventory(repository),
    )

    assert _actions(bundle) == []


# --- one changed declaration --------------------------------------------------


@weaver_test()
def test_a_borrowed_table_comes_off_as_a_shortcut(tmp_path):
    """A Spark drop would reach the storage the source owns."""

    _reconciled, bundle = _selective(tmp_path)
    dropped = [action for action in _actions(bundle) if action.kind == "drop_shortcut"]

    assert [action.resource_node_id for action in dropped] == [
        str(_object(MATERIALISED))
    ]
    assert not [
        action
        for action in _actions(bundle)
        if action.kind in {"drop_table", "prune_table"}
    ]


@weaver_test()
def test_the_name_is_waited_on_before_an_owned_object_takes_it(tmp_path):
    """Fabric stops listing the shortcut before OneLake releases its namespace."""

    _reconciled, bundle = _selective(tmp_path)
    ((dropped,),) = (
        [action for action in _actions(bundle) if action.kind == "drop_shortcut"],
    )

    assert dropped.awaits_name_release


@weaver_test()
def test_the_changed_object_is_built_and_the_rest_stays_borrowed(tmp_path):
    _reconciled, bundle = _selective(tmp_path)

    selected = {str(identity) for identity in bundle.plan.selection.selected_for_build}

    assert str(_object(MATERIALISED)) in selected
    assert str(_object("DWG.Summary")) not in selected
    assert str(_object("Raw.CustomerCsv", files=True)) not in selected


@weaver_test()
def test_only_the_materialised_object_stops_being_borrowed(tmp_path):
    _reconciled, bundle = _selective(tmp_path)

    ((_number, _batch, action),) = [
        each for each in bundle.plan.actions() if each[2].id == DEREGISTER_MIRROR_SLUG
    ]
    statements = "\n".join(_payload(bundle, action))

    assert "N'Customer'" in statements
    assert "N'Summary'" not in statements
    assert "N'CustomerCsv'" not in statements
