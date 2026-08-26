"""The standard Weaver catalogue surface, composed before resolution.

Every normal bound item presents ``_.Installation`` and the operational tables
as ordinary repository content. Nothing injects references after the
repository exists, and there is one shortcut collection.
"""

from __future__ import annotations

import pytest
from factories import (
    full_estate,
    lakehouse_table,
    single_document_repository,
    warehouse_table,
)
from support.weaver_test import weaver_test

from weaver.catalogue.builtin import BUILTIN_ITEM
from weaver.catalogue.tables import INSTALLATION, STANDARD_SURFACE_TABLES
from weaver.declaration import parse_item_repository
from weaver.declaration.model import WeaverItemId
from weaver.errors import DiscoveryError
from weaver.locations import Location

ITEM = "Lakehouse/Sales"
WAREHOUSE_ITEM = "Warehouse/Reporting"


def _surface(repository, item_text: str):
    """The Weaver-owned shortcut declarations one item presents."""

    item = WeaverItemId.parse(item_text)
    declarations = [
        declaration
        for declaration in repository.shortcuts
        if declaration.owner == item
        and declaration.logical_source.item == BUILTIN_ITEM
    ]
    pairs = {
        pair.destination.object_id.object
        for pair in repository.logical_shortcuts
        if pair.destination.item == item and pair.source.item == BUILTIN_ITEM
    }
    return declarations, pairs


@pytest.fixture
def estate(tmp_path):
    return full_estate(tmp_path / "repo")


@pytest.mark.parametrize(
    "item_text,document",
    [
        (ITEM, {"DWG__Customer.py": lakehouse_table("DWG.Customer")}),
        (WAREHOUSE_ITEM, {"DWG.Customer.sql": warehouse_table("DWG.Customer")}),
    ],
)
@weaver_test()
def test_every_normal_item_presents_the_whole_surface(tmp_path, item_text, document):
    repository = single_document_repository(tmp_path / "repo", item=item_text, documents=document)
    declarations, pairs = _surface(repository, item_text)

    assert {
        declaration.destination.object_id.object for declaration in declarations
    } == {table.name for table in STANDARD_SURFACE_TABLES}
    assert pairs == {table.name for table in STANDARD_SURFACE_TABLES}
    assert all(declaration.is_logical for declaration in declarations)


@weaver_test()
def test_an_item_with_no_loads_still_carries_the_surface(tmp_path):
    """The surface is uniform; it is not a reward for having something to run."""

    repository = single_document_repository(
        tmp_path / "repo",
        item=WAREHOUSE_ITEM,
        schemas=("DWG", "Rpt"),
        documents={"Rpt.Summary.sql": warehouse_table("Rpt.Summary", has_load_procedure=False)},
    )
    _declarations, pairs = _surface(repository, WAREHOUSE_ITEM)
    assert INSTALLATION.name in pairs


@weaver_test()
def test_the_surface_is_one_collection(estate):
    """There is no second planning collection beside ``shortcuts``."""

    assert "planned_shortcuts" not in estate.__dataclass_fields__
    weaver_owned = [
        declaration
        for declaration in estate.shortcuts
        if declaration.destination_identity is not None
    ]
    assert weaver_owned, "the standard surface is part of repository.shortcuts"


@weaver_test()
def test_authored_content_may_not_claim_a_weaver_surface_destination(tmp_path):
    """``_.Bookmark`` names Weaver's own relation, wherever it is written."""

    root = tmp_path / "repo"
    (root / WAREHOUSE_ITEM / "schemas").mkdir(parents=True)
    (root / WAREHOUSE_ITEM / "schemas" / "DWG.yml").write_text(
        "Schema ID: DWG\nDescription: DWG objects.\n"
    )
    (root / WAREHOUSE_ITEM / "DWG.Customer.sql").write_text(
        warehouse_table("DWG.Customer")
    )
    (root / WAREHOUSE_ITEM / "shortcuts.yml").write_text(
        "logical:\n"
        "  Warehouse/Reporting/_.Bookmark: Warehouse/_weaver/_.Bookmark\n"
    )
    # Whatever the refusal says, the claim is refused: the destination names
    # Weaver's own relation, and no authored spelling may take it.
    with pytest.raises(DiscoveryError):
        parse_item_repository(Location(str(root)))
