"""What a Warehouse mirror stands up, and what it records.

Data borrowed, code local. The statements are settled here; whether Fabric reads
through them is ``tests/fabric/test_warehouse_mirror_primitive.py``.
"""

from __future__ import annotations

import pytest
from support.weaver_test import weaver_test

from weaver.catalogue.borrow import (
    BORROWED_TYPE,
    borrow_statements,
    borrowable,
    executable,
    missing_programmables,
    programmable_statements,
    record_statements,
    schema_statements,
    surface_statements,
    view_statement,
)
from weaver.catalogue.state import RegisteredDocument
from weaver.catalogue.tables import (
    MIRROR,
    ROLE_DATA,
    ROLE_LOAD,
    ROLE_PROGRAMMABLE,
    ROLE_TEST,
    STANDARD_SURFACE_TABLES,
)
from weaver.declaration.metadata import ObjectId
from weaver.declaration.model import (
    OBJECT_SHAPE,
    PROCEDURE_SHAPE,
    WeaverDocumentId,
    WeaverItemId,
)

ITEM = WeaverItemId.parse("Warehouse/Model")


def _id(schema: str, name: str) -> WeaverDocumentId:
    return WeaverDocumentId(ITEM, ObjectId(schema, name))


def _registered(**roles) -> dict:
    return {
        _id("Core", name): RegisteredDocument(_id("Core", name), "table", "sig", role)
        for name, role in roles.items()
    }


# --- what a mirror points at --------------------------------------------------


@weaver_test()
def test_only_data_relations_are_borrowed():
    """A load procedure is code, and a mirror recreates code locally."""

    borrowed = borrowable(_registered(Customer=ROLE_DATA, Refresh=ROLE_LOAD))

    assert [identity.object_id.object for identity in borrowed] == ["Customer"]


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
    identities = [_id("Core", "Customer"), _id("Rpt", "Summary")]

    statements = borrow_statements(
        identities, source_target="PROD", catalogue_name="Weaver_Dev"
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

    assert schema_statements([_id("_", "Registry")]) == ()


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


# --- code is local ------------------------------------------------------------


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
    """Weaver's generated load and validation procedures sit in ``_``.

    The copied Registry certifies them, and ``weaver test`` dispatches
    ``_.[Test Core.Integrity]`` by name, so the mirror holds them too.
    """

    assert {identity.object_id.qualified for identity in executable(CERTIFIED)} == {
        "Rpt.Refresh",
        "_.Load Core.Customer",
        "_.Test Core.Integrity",
    }


@weaver_test()
def test_a_source_that_does_not_hold_certified_code_is_named():
    """Registry and the source Warehouse are two readings of one estate."""

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
        [_id("Core", "Customer")],
        source_workspace="Analytics",
        source_target="PROD",
    )

    assert "create table [_].[Mirror]" in statements[0]
    assert statements[1].startswith("delete from [_].[Mirror]")
    assert statements[2].startswith("insert into [_].[Mirror]")


@weaver_test()
def test_a_row_says_whose_data_it_reads_and_what_stands_at_its_address():
    _create, _delete, insert = record_statements(
        [_id("Core", "Customer")],
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
        [_id("Core", "Customer")],
        source_workspace="Analytics",
        source_target="PROD",
    )

    assert "[Item type] = N'Warehouse'" in delete
    assert "[Object name] = N'Customer'" in delete


@weaver_test()
def test_nothing_is_recorded_where_nothing_is_borrowed():
    assert record_statements([], source_workspace="A", source_target="P") == ()
