"""What a Warehouse mirror stands up, and what it records.

The statements are settled here; whether Fabric reads through them is
``tests/fabric/test_warehouse_mirror_journey.py``.
"""

from __future__ import annotations

from dataclasses import dataclass

import pytest
from support.weaver_test import weaver_test

from weaver.catalogue.borrow import (
    BORROWED_TYPE,
    Borrowed,
    borrow_statements,
    borrowable,
    executable,
    missing_programmables,
    pointer_shortcuts,
    programmable_statements,
    record_statements,
    schema_statements,
    surface_shortcuts,
    surface_statements,
    view_statement,
    wrapper_view_statement,
)
from weaver.catalogue.state import RegisteredDocument
from weaver.catalogue.tables import (
    CATALOGUE_SCHEMA,
    MIRROR,
    ROLE_DATA,
    ROLE_LOAD,
    ROLE_PROGRAMMABLE,
    ROLE_TEST,
    STANDARD_SURFACE_TABLES,
)
from weaver.declaration.metadata import ObjectId
from weaver.declaration.model import (
    LAKEHOUSE,
    OBJECT_SHAPE,
    PROCEDURE_SHAPE,
    WAREHOUSE,
    WeaverDocumentId,
    WeaverItemId,
)

ITEM = WeaverItemId.parse("Warehouse/Model")
LAKE = WeaverItemId.parse("Lakehouse/Input")


def _id(schema: str, name: str) -> WeaverDocumentId:
    return WeaverDocumentId(ITEM, ObjectId(schema, name))


def _borrowed(schema: str, name: str, physical: str = BORROWED_TYPE) -> Borrowed:
    return Borrowed(_id(schema, name), declared="table", physical=physical)


def _registered(**roles) -> dict:
    return {
        _id("Core", name): RegisteredDocument(_id("Core", name), "table", "sig", role)
        for name, role in roles.items()
    }


# --- what a mirror points at --------------------------------------------------


@weaver_test()
def test_only_data_relations_are_borrowed():
    """A load procedure is code, and a mirror recreates code locally."""

    borrowed = borrowable(
        _registered(Customer=ROLE_DATA, Refresh=ROLE_LOAD), kind=WAREHOUSE
    )

    assert [each.name for each in borrowed] == ["Customer"]
    assert [each.physical for each in borrowed] == [BORROWED_TYPE]


@weaver_test()
def test_a_borrowed_relation_stands_as_a_view():
    """A table and a view both read the same way through one."""

    assert BORROWED_TYPE == "view"
    assert view_statement(_id("Core", "Customer"), source_target="PROD") == (
        "create or alter view [Core].[Customer] as select * from "
        "[PROD].[Core].[Customer];"
    )


@weaver_test()
def test_a_view_is_replaced_rather_than_dropped_and_remade():
    """A mirror reruns, and a second one stands on the address of the first."""

    statement = view_statement(_id("Core", "Customer"), source_target="PROD")

    assert statement.startswith("create or alter view")
    assert "drop" not in statement


@weaver_test()
def test_the_schemas_a_mirror_needs_are_made_first():
    statements = borrow_statements(
        [_borrowed("Core", "Customer"), _borrowed("Rpt", "Summary")],
        source_target="PROD",
        catalogue_name="Weaver_Dev",
    )
    schemas = [i for i, each in enumerate(statements) if "create schema" in each]
    views = [
        i for i, each in enumerate(statements) if "create or alter view [C" in each
    ]

    assert schemas and views
    assert max(schemas) < min(views)


@weaver_test()
def test_the_catalogue_schema_is_never_made_as_an_application_one():
    """``_`` is Weaver's, and the surface is what puts it there."""

    assert schema_statements(["_"]) == ()


# --- what a Lakehouse borrows -------------------------------------------------


def _lake(area: str, schema: str, name: str, declared: str) -> Borrowed:
    identity = WeaverDocumentId(LAKE, ObjectId(schema, name), is_files=area == "Files")
    return Borrowed(identity, declared=declared, physical=declared)


def _lake_registered(*rows) -> dict:
    registered = {}
    for area, schema, name, object_type in rows:
        identity = WeaverDocumentId(
            LAKE, ObjectId(schema, name), is_files=area == "Files"
        )
        registered[identity] = RegisteredDocument(
            identity, object_type, "sig", ROLE_DATA
        )
    return registered


#: A Lakehouse item as Registry holds it, including the runtime tree. Weaver
#: declares ``Files/_.Load`` as a Folder, so it carries the same data role as an
#: authored one and reaches every candidate rule alongside them.
LAKE_ESTATE = _lake_registered(
    ("Tables", "Core", "Customer", "table"),
    ("Tables", "Core", "ActiveCustomer", "view"),
    ("Files", "Raw", "CustomerCsv", "folder"),
    ("Files", CATALOGUE_SCHEMA, "Load", "folder"),
)


@weaver_test()
def test_a_lakehouse_borrows_storage_as_itself_and_wraps_a_view():
    """A shortcut addresses storage, so only a table and a folder get one."""

    borrowed = {each.name: each for each in borrowable(LAKE_ESTATE, kind=LAKEHOUSE)}

    assert borrowed["Customer"].physical == "table"
    assert borrowed["CustomerCsv"].physical == "folder"
    assert borrowed["ActiveCustomer"].physical == "view"
    assert borrowed["Customer"].is_pointer
    assert borrowed["CustomerCsv"].is_pointer
    assert not borrowed["ActiveCustomer"].is_pointer


@weaver_test()
def test_the_deployed_runtime_tree_is_never_borrowed():
    """A shortcut at ``Files/_.Load`` would send a build's writes to the source.

    The tree holds the modules a run imports where Spark is, and a mirror copies
    it into the destination's own storage.
    """

    borrowed = {each.name: each for each in borrowable(LAKE_ESTATE, kind=LAKEHOUSE)}

    assert "Load" not in borrowed
    assert not [each for each in borrowed.values() if each.schema == CATALOGUE_SCHEMA]


@weaver_test()
def test_the_same_estate_in_a_warehouse_is_read_through_views():
    """One classification per kind, from one call."""

    borrowed = borrowable(LAKE_ESTATE, kind=WAREHOUSE)

    assert {each.physical for each in borrowed} == {BORROWED_TYPE}


@weaver_test()
def test_an_area_is_read_back_off_the_stored_schema():
    """A Folder stores ``Files/Raw`` and a table ``Tables/Core``."""

    borrowed = {each.name: each for each in borrowable(LAKE_ESTATE, kind=LAKEHOUSE)}

    assert (borrowed["Customer"].area, borrowed["Customer"].schema) == (
        "Tables",
        "Core",
    )
    assert (borrowed["CustomerCsv"].area, borrowed["CustomerCsv"].schema) == (
        "Files",
        "Raw",
    )


@weaver_test()
def test_a_shortcut_keeps_the_estates_own_address_and_the_sources_spelling():
    """The destination path is the declaration's; the source path is storage's."""

    item = _Item(id="lh-1", name="INPUT", workspace_id="ws-1")
    requests = pointer_shortcuts(
        borrowable(LAKE_ESTATE, kind=LAKEHOUSE),
        source=item,
        path_of=lambda each: f"{each.area}/{each.schema}/{each.name.lower()}",
    )
    by_name = {each["name"]: each for each in requests}

    assert set(by_name) == {"Customer", "CustomerCsv"}
    assert by_name["Customer"]["path"] == "Tables/Core"
    assert by_name["Customer"]["source_path"] == "Tables/Core/customer"
    assert by_name["Customer"]["type"] == "table"
    assert by_name["CustomerCsv"]["path"] == "Files/Raw"
    assert by_name["Customer"]["source"] is item


@weaver_test()
def test_a_wrapper_view_names_both_sides_four_part():
    """No ambient catalogue, and the source's definition is never read."""

    from weaver.spark import FabricSparkTarget

    statement = wrapper_view_statement(
        _lake("Tables", "Core", "ActiveCustomer", "view"),
        destination=FabricSparkTarget(workspace="Dev", lakehouse="DEV_INPUT"),
        source=FabricSparkTarget(workspace="Prod", lakehouse="INPUT"),
    )

    assert statement == (
        "CREATE OR REPLACE VIEW `Dev`.`DEV_INPUT`.`Core`.`ActiveCustomer` "
        "AS SELECT * FROM `Prod`.`INPUT`.`Core`.`ActiveCustomer`"
    )


@dataclass(frozen=True)
class _Item:
    """A resolved source item, as the shortcut transport addresses one."""

    id: str
    name: str
    workspace_id: str


@weaver_test()
def test_a_mirrored_lakehouse_gets_the_surface_a_built_one_declares():
    """One definition: the standard references, turned into shortcuts."""

    from weaver.catalogue.builtin import standard_surface_references
    from weaver.catalogue.tables import STANDARD_SURFACE_TABLES

    item = _Item(id="wh-1", name="Weaver_Dev", workspace_id="ws-1")
    requests = surface_shortcuts(LAKE, catalogue=item)
    declarations, _pairs = standard_surface_references(LAKE)

    assert [each["name"] for each in requests] == [
        table.name for table in STANDARD_SURFACE_TABLES
    ]
    assert {each["path"] for each in requests} == {f"Tables/{CATALOGUE_SCHEMA}"}
    assert [each["type"] for each in requests] == [
        declaration.shortcut_type for declaration in declarations
    ]
    assert requests[0]["source"] is item
    assert requests[0]["source_path"] == (
        f"Tables/{CATALOGUE_SCHEMA}/{STANDARD_SURFACE_TABLES[0].name}"
    )


# --- the surface a target reads Weaver state through --------------------------


@weaver_test()
def test_a_mirror_gives_its_target_the_same_surface_a_build_would():
    statements = surface_statements("Weaver_Dev")

    for table in STANDARD_SURFACE_TABLES:
        assert (
            f"create or alter view [_].[{table.name}] as select * from "
            f"[Weaver_Dev].[_].[{table.name}];" in statements
        )


@weaver_test()
def test_the_surface_does_not_present_what_is_borrowed():
    """Nothing running inside a target asks where its data came from."""

    body = "\n".join(surface_statements("Weaver_Dev"))

    assert f"[{MIRROR.name}]" not in body


# --- the procedures it copies ------------------------------------------------------------


def _certified(*rows) -> dict:
    """A Registry the fork copied, as ``(schema, name, type, role)`` rows."""

    registered = {}
    for schema, name, object_type, role in rows:
        shape = PROCEDURE_SHAPE if object_type == "stored_procedure" else OBJECT_SHAPE
        identity = WeaverDocumentId(ITEM, ObjectId(schema, name), shape=shape)
        registered[identity] = RegisteredDocument(identity, object_type, "sig", role)
    return registered


CERTIFIED = _certified(
    ("Core", "Customer", "table", ROLE_DATA),
    ("_", "Load Core.Customer", "stored_procedure", ROLE_LOAD),
    ("_", "Test Core.Integrity", "stored_procedure", ROLE_TEST),
    ("Rpt", "Refresh", "stored_procedure", ROLE_PROGRAMMABLE),
)


@weaver_test()
def test_every_certified_procedure_is_copied_whatever_schema_it_is_in():
    """``weaver test`` dispatches ``_.[Test Core.Integrity]`` by name."""

    assert {identity.object_id.qualified for identity in executable(CERTIFIED)} == {
        "Rpt.Refresh",
        "_.Load Core.Customer",
        "_.Test Core.Integrity",
    }


@weaver_test()
def test_a_source_that_does_not_hold_certified_code_is_named():

    absent = missing_programmables(
        executable(CERTIFIED), ["rpt.refresh", "_.Load Core.Customer"]
    )

    assert [identity.object_id.qualified for identity in absent] == [
        "_.Test Core.Integrity"
    ]


@weaver_test()
def test_a_source_holding_everything_certified_leaves_nothing_missing():
    copied = [identity.object_id.qualified for identity in executable(CERTIFIED)]

    assert missing_programmables(executable(CERTIFIED), copied) == ()


@weaver_test()
@pytest.mark.parametrize(
    "written",
    [
        "CREATE PROCEDURE [Rpt].[Refresh] AS SELECT 1",
        "create or alter procedure [Rpt].[Refresh] as select 1",
    ],
)
def test_a_copied_programmable_can_be_copied_again(written):
    """Fabric returns the module as it was written, so a plain create would
    fail on a second mirror."""

    ((statement,),) = (programmable_statements([written]),)

    assert statement.casefold().startswith("create or alter")


@weaver_test()
def test_nothing_is_copied_for_a_source_with_no_code():
    assert programmable_statements([]) == ()
    assert programmable_statements(["", "   "]) == ()


# --- what is recorded ---------------------------------------------------------


@weaver_test()
def test_recording_creates_the_table_before_it_writes_to_it():
    """No document declares ``_.Mirror``, so no build has made one."""

    statements = record_statements(
        [_borrowed("Core", "Customer")],
        source_workspace="Analytics",
        source_target="PROD",
    )

    assert "create table [_].[Mirror]" in statements[0]
    assert statements[1].startswith("delete from [_].[Mirror]")
    assert statements[2].startswith("insert into [_].[Mirror]")


@weaver_test()
def test_a_row_says_whose_data_it_reads_and_what_stands_at_its_address():
    _create, _delete, insert = record_statements(
        [_borrowed("Core", "Customer")],
        source_workspace="Analytics",
        source_target="PROD",
    )

    assert "N'Warehouse', N'Model', N'Core', N'Customer'" in insert
    assert "N'Analytics', N'PROD', N'Core', N'Customer'" in insert
    assert "N'View'" in insert


@weaver_test()
def test_a_rerun_replaces_rather_than_duplicates():
    """A mirror is reconstruction, so its rows are keyed and rewritten."""

    _create, delete, _insert = record_statements(
        [_borrowed("Core", "Customer")],
        source_workspace="Analytics",
        source_target="PROD",
    )

    assert "[Item type] = N'Warehouse'" in delete
    assert "[Object name] = N'Customer'" in delete


@weaver_test()
def test_nothing_is_recorded_where_nothing_is_borrowed():
    assert record_statements([], source_workspace="A", source_target="P") == ()
