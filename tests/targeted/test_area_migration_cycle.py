"""Moving a Lakehouse source under ``Tables/`` moves its module, not its data.

An estate whose load modules sit at the runtime root, ``_/Load/DWG__Customer.py``,
is one built before the areas were explicit. Moving the sources under ``Tables/``
replaces one runtime file per Lakehouse document. The Delta table is not
dropped, not rebuilt and not reloaded, because the authored path reaches neither
the physical name nor the signature, and the build after that has nothing left
to do.

What the catalogue stores changed too, in a second step: a Lakehouse relation is
keyed ``Tables/DWG`` now, where it was keyed ``DWG``. That is a breaking
catalogue migration and is asserted separately, in
:mod:`test_area_keyed_catalogue_cycle`. Here the catalogue is the current one
throughout, so what this isolates is the file move.

The estate is the one :mod:`test_build_fixed_point_cycle` reaches a fixed point
over, and the harness is imported from it. What is asserted here is the
difference between two builds of that estate, so the second build has to be the
same build.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone

import pytest
import test_build_fixed_point_cycle as harness
from factories import ITEM, full_estate
from support.weaver_test import weaver_test
from test_build_fixed_point_cycle import actions, build, installed_catalogue

from weaver.catalogue.state import Catalogue
from weaver.declaration.model import WeaverDocumentId
from weaver.etl import LOAD_ROOT

#: Where a Lakehouse load module sat before the area was part of a deployed
#: path, and where it sits now.
OLD_ROOT = LOAD_ROOT
NEW_ROOT = f"{LOAD_ROOT}/Tables"

TABLE = f"{ITEM}/Tables/DWG.Customer"
MODULES = ("DWG__Customer.py", "DWG__Summary.py")


@pytest.fixture
def estate(tmp_path):
    return full_estate(tmp_path / "repo")


def _before_the_move(catalogue: Catalogue) -> Catalogue:
    """The same catalogue, with each Lakehouse load module where it used to sit.

    Only the file rows move. A table's Registry row names the area the table
    sits in, not the path its module was deployed to, so the file move leaves it
    alone.
    """

    rows = {
        item: {
            name: tuple(
                {**row, "schema_name": OLD_ROOT}
                if str(row.get("schema_name") or "") == NEW_ROOT
                else row
                for row in table_rows
            )
            for name, table_rows in tables.items()
        }
        for item, tables in catalogue.rows.items()
    }
    return Catalogue(rows=rows, materialised=catalogue.materialised)


def _old_inventory(inventory):
    """The physical Lakehouse a build before the move left behind."""

    return replace(
        inventory,
        files=tuple(
            sorted(
                path.replace(f"{NEW_ROOT}/", f"{OLD_ROOT}/", 1)
                if path.startswith(f"{NEW_ROOT}/")
                else path
                for path in inventory.files
            )
        ),
    )


def _migrating_build(estate, tmp_path):
    """The first build after the sources moved under ``Tables/``.

    The repository is the new one, and the state it is planned against is the
    old one: an unchanged catalogue but for the file rows, and a Lakehouse
    holding the modules where the previous build put them.
    """

    settled = installed_catalogue(estate)
    original = harness._inventories

    def before_the_move(repository, bound):
        return {
            item: _old_inventory(inventory)
            if inventory.kind == "lakehouse"
            else inventory
            for item, inventory in original(repository, bound).items()
        }

    harness._inventories = before_the_move
    try:
        return build(estate, tmp_path, catalogue=_before_the_move(settled))
    finally:
        harness._inventories = original


def _paths(bundle, kind: str) -> set[str]:
    return {
        action.resource_node_id for action in actions(bundle) if action.kind == kind
    }


# --- the property -------------------------------------------------------------


@weaver_test()
def test_the_move_replaces_the_load_module_and_nothing_else(estate, tmp_path):
    """One file out and one file in per Lakehouse document, and no more."""

    bundle = _migrating_build(estate, tmp_path)

    assert {action.kind for action in actions(bundle)} == {
        "write_file",
        "delete_file",
        # The pruned file rows leaving Registry, and Registry written again.
        "delete_catalogue_claims",
        "publish_registry",
    }
    assert _paths(bundle, "delete_file") == {
        f"{ITEM}/file:{OLD_ROOT}/{module}" for module in MODULES
    }
    assert _paths(bundle, "write_file") == {
        f"{ITEM}/file:{NEW_ROOT}/{module}" for module in MODULES
    }


@weaver_test()
def test_the_delta_table_is_neither_dropped_nor_rebuilt(estate, tmp_path):
    """The source moved and the object did not, so the data has nothing to answer for."""

    bundle = _migrating_build(estate, tmp_path)
    table = WeaverDocumentId.parse(TABLE)

    assert table not in bundle.plan.selection.selected_for_build
    assert not [
        action
        for action in actions(bundle)
        if action.kind in ("drop_table", "build_table", "load_table")
    ]


@weaver_test()
def test_the_moved_source_keeps_its_structural_signature(estate, tmp_path):
    """Unchanged content, so an unchanged signature, so nothing to select.

    A document's signature covers its own bytes and the helpers it can reach,
    and the authored path is in neither. That is what the claim above rests on.
    """

    from weaver.build_bundle.incremental import declared_signatures

    table = WeaverDocumentId.parse(TABLE)
    settled = installed_catalogue(estate)

    assert (
        declared_signatures(estate, {table})[table]
        == settled.registered[table].signature
    )


@weaver_test()
def test_the_migration_is_one_build_long(estate, tmp_path):
    """The state the migrating build converges to plans nothing at all."""

    assert actions(_migrating_build(estate, tmp_path))
    assert actions(build(estate, tmp_path, catalogue=installed_catalogue(estate))) == []


# --- the catalogue key, which moved separately ---------------------------------
#
# An estate built before the area was stored keys its Lakehouse relations bare:
# `DWG`, where this Weaver writes `Tables/DWG`. That is a breaking catalogue
# migration, and what matters is which of its consequences are physical.


def _before_the_key_moved(catalogue: Catalogue) -> Catalogue:
    """The same catalogue, with each Lakehouse relation keyed as it once was.

    Only the relations. A Folder always carried ``Files/``, and a load artefact's
    schema is a path, so neither is touched. That asymmetry is the thing this
    change removed.
    """

    rows = {
        item: {
            name: tuple(
                {**row, "schema_name": str(row["schema_name"]).removeprefix("Tables/")}
                if str(row.get("schema_name", "")).startswith("Tables/")
                else row
                for row in table_rows
            )
            for name, table_rows in tables.items()
        }
        for item, tables in catalogue.rows.items()
    }
    return Catalogue(rows=rows, materialised=catalogue.materialised)


@weaver_test()
def test_the_old_key_still_names_the_object_it_named(estate, tmp_path):
    """A bare Lakehouse relation reads back as the relation, not as a validation.

    This is what keeps an upgrading estate's tables recognised. Read as
    validations they would every one look new, and a build would rebuild the lot.
    """

    old = _before_the_key_moved(installed_catalogue(estate))
    table = WeaverDocumentId.parse(TABLE)

    assert table in old.registered
    assert old.registered[table].object_type == "table"


@weaver_test()
def test_the_key_moving_rebuilds_no_table_and_drops_nothing(estate, tmp_path):
    """The physical name never carried the area, so no physical work follows."""

    bundle = build(
        estate, tmp_path, catalogue=_before_the_key_moved(installed_catalogue(estate))
    )
    planned = actions(bundle)

    assert WeaverDocumentId.parse(TABLE) not in bundle.plan.selection.selected_for_build
    assert not [
        action
        for action in planned
        if action.kind in ("drop_table", "drop_folder", "build_table", "load_table")
    ]


@weaver_test()
def test_the_key_moving_republishes_the_registry_row(estate, tmp_path):
    """The row is rewritten under the area, which is the whole of the change."""

    bundle = build(
        estate, tmp_path, catalogue=_before_the_key_moved(installed_catalogue(estate))
    )

    assert "publish_registry" in {action.kind for action in actions(bundle)}


@weaver_test()
def test_the_key_moving_costs_the_bookmark_and_says_so(estate, tmp_path):
    """The one behavioural consequence the key move has.

    A bookmark is keyed as Registry keys the object, so a row written under the
    old spelling is not the row the new one reads. The table loads from the
    sentinel once, reading its source from the beginning. A Weaver table is
    upserted by its declared key, so that pass rewrites the rows it holds. The
    old row stays behind, orphaned, and nothing reads it.
    """

    from weaver.catalogue.claims import bookmark_row
    from weaver.catalogue.state import BOOKMARK_SENTINEL

    loaded_at = datetime(2026, 9, 1, 6, 0, tzinfo=timezone.utc)
    table = WeaverDocumentId.parse(TABLE)
    settled = installed_catalogue(estate)
    rows = {item: dict(tables) for item, tables in settled.rows.items()}
    rows[table.item]["Bookmark"] = (bookmark_row(table, loaded_at),)
    loaded = Catalogue(rows=rows, materialised=settled.materialised)

    assert loaded.bookmark(table) == loaded_at
    assert _before_the_key_moved(loaded).bookmark(table) == BOOKMARK_SENTINEL
