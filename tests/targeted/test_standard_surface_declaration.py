"""What every normal item receives from Weaver, and nothing else.

The standard fragment for the item's type, and the catalogue surface as ordinary
logical shortcut declarations. Both are composed in before resolution, so
nothing is injected into a repository that already exists.
"""

from __future__ import annotations

import pytest
from factories import lakehouse_table, single_document_repository, warehouse_table
from support.weaver_test import weaver_test

from weaver.catalogue.builtin import BUILTIN_ITEM
from weaver.catalogue.tables import (
    BOOKMARK,
    BOOKMARK_SENTINEL_TEXT,
    INSTALLATION,
    LOAD_STATISTIC,
    LOAD_STATUS,
    LOG,
    STANDARD_SURFACE_TABLES,
    TEST_STATUS,
)
from weaver.declaration import parse_item_repository
from weaver.declaration.model import LAKEHOUSE, WAREHOUSE, WeaverItemId
from weaver.errors import DiscoveryError
from weaver.fragments import standard_fragment
from weaver.locations import Location

ITEM = "Lakehouse/Sales"
WAREHOUSE_ITEM = "Warehouse/Reporting"

#: The runtime tables each entry point writes. A table that gains a column has
#: to be given a value in the checked-in SQL, and this is where that is noticed.
_WRITES = {
    "Load": (LOG, LOAD_STATUS, LOAD_STATISTIC, BOOKMARK),
    "Test": (LOG, TEST_STATUS),
}


def _surface(repository, item_text: str):
    """The Weaver-owned shortcut destinations one item presents."""

    item = WeaverItemId.parse(item_text)
    declarations = [
        declaration
        for declaration in repository.shortcuts
        if declaration.owner == item and declaration.logical_source.item == BUILTIN_ITEM
    ]
    pairs = {
        pair.destination.object_id.object
        for pair in repository.logical_shortcuts
        if pair.destination.item == item and pair.source.item == BUILTIN_ITEM
    }
    return declarations, pairs


@pytest.mark.parametrize(
    "item_text,document",
    [
        (ITEM, {"DWG__Customer.py": lakehouse_table("DWG.Customer")}),
        (WAREHOUSE_ITEM, {"DWG.Customer.sql": warehouse_table("DWG.Customer")}),
    ],
)
@weaver_test()
def test_every_normal_item_presents_the_whole_surface(tmp_path, item_text, document):
    repository = single_document_repository(
        tmp_path / "repo", item=item_text, documents=document
    )
    declarations, pairs = _surface(repository, item_text)

    expected = {table.name for table in STANDARD_SURFACE_TABLES}
    assert {each.destination.object_id.object for each in declarations} == expected
    assert pairs == expected
    assert INSTALLATION.name in pairs
    assert all(declaration.is_logical for declaration in declarations)


@weaver_test()
def test_an_item_with_nothing_to_run_still_carries_the_surface(tmp_path):
    """The surface is uniform; it is not a reward for having something to run."""

    repository = single_document_repository(
        tmp_path / "repo",
        item=WAREHOUSE_ITEM,
        schemas=("DWG", "Rpt"),
        documents={
            "Rpt.Summary.sql": warehouse_table("Rpt.Summary", has_load_procedure=False)
        },
    )
    _declarations, pairs = _surface(repository, WAREHOUSE_ITEM)

    assert pairs == {table.name for table in STANDARD_SURFACE_TABLES}


@weaver_test()
def test_a_warehouse_receives_weavers_schema_and_the_two_entry_points(tmp_path):
    repository = single_document_repository(
        tmp_path / "repo",
        item=WAREHOUSE_ITEM,
        documents={"DWG.Customer.sql": warehouse_table("DWG.Customer")},
    )
    item = repository[WAREHOUSE_ITEM]

    assert "_" in {schema.schema for schema in item.schemas}
    entry_points = {
        programmable.identity.object_id.object: programmable
        for programmable in item.programmables
        if programmable.role == "programmable"
    }
    assert set(entry_points) == {"Load", "Test"}
    # Weaver's own content, so no item signature moves when it changes.
    assert all(each.relative_path is None for each in entry_points.values())
    assert all(each.origin is None for each in entry_points.values())


@weaver_test()
def test_a_lakehouse_receives_weavers_schema(tmp_path):
    repository = single_document_repository(
        tmp_path / "repo",
        item=ITEM,
        documents={"DWG__Customer.py": lakehouse_table("DWG.Customer")},
    )

    assert "_" in {schema.schema for schema in repository[ITEM].schemas}


@pytest.mark.parametrize("item_type", [WAREHOUSE, LAKEHOUSE])
@weaver_test()
def test_the_standard_fragment_holds_only_what_weaver_owns(item_type):
    """Every fragment file is named in ``_``, so none collides with authored work."""

    for relative in standard_fragment(item_type):
        assert relative.rsplit("/", 1)[-1].startswith("_"), relative


@pytest.mark.parametrize("name", sorted(_WRITES))
@weaver_test()
def test_the_fixed_entry_point_supplies_every_column_it_writes(name):
    """A catalogue table that gains a column is noticed here, not by a build."""

    sql = standard_fragment(WAREHOUSE)[f"programmables/_.{name}.sql"].decode("utf-8")

    for table in _WRITES[name]:
        assert f"merge into [_].[{table.name}]" in sql, table.name
        for column in table.column_names:
            public = table.public_name_of(column)
            assert f"[{public}]" in sql, (table.name, public)


@weaver_test()
def test_the_load_entry_point_carries_the_whole_load_abi():
    """``_.Load`` is checked in, so a changed ABI has to be carried into it."""

    from weaver.declaration.tsql_load import RESULT_PARAMETER_NAMES

    sql = standard_fragment(WAREHOUSE)["programmables/_.Load.sql"].decode("utf-8")

    for physical in RESULT_PARAMETER_NAMES.values():
        assert f"@{physical} = @{physical} output" in sql, physical


@weaver_test()
def test_the_load_entry_point_passes_reload_to_the_implementation():
    """``_.Load`` is the recorder; the procedure it calls is what clears."""

    sql = standard_fragment(WAREHOUSE)["programmables/_.Load.sql"].decode("utf-8")

    assert "@reload bit = 0" in sql
    assert "@reload = @reload" in sql


@weaver_test()
def test_the_load_entry_point_ends_the_load_state_before_it_calls():
    """The barrier a reload rests on, and the order it is written in.

    The status goes to Pending and the bookmark row goes, while the target still
    holds the rows they describe. Both are MERGEs, the removal included, because
    in every Warehouse but the catalogue's these are views across databases.
    """

    sql = standard_fragment(WAREHOUSE)["programmables/_.Load.sql"].decode("utf-8")

    reset = sql.index("if @weaver_call is not null and @reload = 1")
    called = sql.index("exec sp_executesql @weaver_call")
    assert reset < called
    ending = sql[reset:called]
    assert "N'Pending'" in ending
    assert "when matched then delete;" in ending
    # The status first: nothing may read Succeeded beside an absent bookmark.
    assert ending.index("[_].[LoadStatus]") < ending.index("[_].[Bookmark]")


@weaver_test()
def test_the_load_entry_point_stores_no_bookmark_sentinel():
    """The sentinel is what an absent row reads as, and is never written.

    Two physical shapes for "no clean load has established progress" would give
    the Static gate and every incremental read two things to agree about.
    """

    sql = standard_fragment(WAREHOUSE)["programmables/_.Load.sql"].decode("utf-8")

    assert BOOKMARK_SENTINEL_TEXT not in sql


@weaver_test()
def test_the_load_entry_point_records_the_mode_it_ran_in():
    """``_.LoadStatistic`` says what the load was, and reload is one of them."""

    sql = standard_fragment(WAREHOUSE)["programmables/_.Load.sql"].decode("utf-8")

    assert "cast(@reload as bit)" in sql


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
        "logical:\n  Warehouse/Reporting/_.Bookmark: Warehouse/_weaver/_.Bookmark\n"
    )

    with pytest.raises(DiscoveryError):
        parse_item_repository(Location(str(root)))
