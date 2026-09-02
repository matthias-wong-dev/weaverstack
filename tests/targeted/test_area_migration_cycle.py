"""Moving a Lakehouse source under ``Tables/`` moves its module, not its data.

An estate built before the areas were explicit holds its load modules at the
runtime root: ``_/Load/DWG__Customer.py``. Its Delta tables are unaffected by the
move, because the catalogue never stored an area. ``schema_name`` is ``DWG`` and
``object_name`` is ``Customer``, and the row an old build wrote reads back as
``Lakehouse/Sales/Tables/DWG.Customer``, which is the same key the new
declaration claims.

So the first build after the upgrade replaces one runtime file per Lakehouse
document and touches nothing else. The table is not dropped, not rebuilt and not
reloaded, and the build after that has nothing left to do.

The estate is the one :mod:`test_build_fixed_point_cycle` reaches a fixed point
over, and the harness is imported from it rather than restated: what is asserted
here is the difference between two builds of that estate, so the second build has
to be the same build.
"""

from __future__ import annotations

from dataclasses import replace

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

    Only the file rows move. A table's Registry row carries its relational
    schema and its object name and no area at all, so there is nothing in it for
    the move to change.
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
    """The catalogue key did not move, so the data has nothing to answer for."""

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

    This is what makes the claim above hold for a reason rather than by
    accident: had the authored path reached the signature, every Lakehouse table
    in the estate would have been selected for rebuild by the move alone.
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
