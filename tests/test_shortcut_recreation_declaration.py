"""What a mirror stands up again from the shortcuts a fork copied.

A fork copies ``_.Shortcut``, so the destination certifies every pointer the
source had. These settle what a mirror recreates and where each one points;
whether Fabric reads through the result is
``tests/fabric/test_warehouse_mirror_journey.py`` and
``tests/fabric/test_lakehouse_mirror_journey.py``.
"""

from __future__ import annotations

import pytest
from support.weaver_test import weaver_test

from weaver.catalogue.shortcuts import (
    UnresolvedShortcut,
    recreatable,
    schemas_of,
    shortcut_request,
    unsupported,
    view_statement,
)
from weaver.declaration.metadata import ObjectId
from weaver.declaration.model import (
    LAKEHOUSE,
    WAREHOUSE,
    WeaverDocumentId,
    WeaverItemId,
    WeaverSchemaId,
)
from weaver.installed import InstalledShortcut

CURATED = WeaverItemId.parse("Warehouse/Curated")
LANDING = WeaverItemId.parse("Lakehouse/Landing")
DROP = WeaverItemId.parse("Lakehouse/Drop")

#: Where this run leaves every item, which is what a logical pointer follows.
BINDINGS = {LANDING: "DEV_Landing", CURATED: "DEV_Curated"}


def _document(item, schema: str, name: str, *, is_files: bool = False):
    return WeaverDocumentId(item, ObjectId(schema, name), is_files=is_files)


def _stored_schema(source) -> str:
    """The schema as the catalogue stores it: a Lakehouse one names its area."""

    if source.item.item_type != LAKEHOUSE:
        return source.object_id.schema
    area = "Files" if source.is_files else "Tables"
    return f"{area}/{source.object_id.schema}"


def _logical(destination, source, *, shortcut_type: str) -> InstalledShortcut:
    """One pointer at a Weaver item, as ``_.Shortcut`` records it."""

    return InstalledShortcut(
        destination=destination,
        source=source,
        shortcut_type=shortcut_type,
        target_type="logical",
        target_item=source.item,
        target_schema=_stored_schema(source),
        target_object=source.object_id.object,
    )


def _physical(destination, *, shortcut_type: str, schema: str, workspace=None):
    return InstalledShortcut(
        destination=destination,
        shortcut_type=shortcut_type,
        target_type="physical",
        target_item=DROP,
        target_schema=schema,
        target_workspace=workspace,
    )


#: The Warehouse view a logical shortcut into a Lakehouse becomes.
DELTA = _logical(
    _document(CURATED, "ACQSC", "ComplaintSubtypeDelta"),
    _document(LANDING, "ACQSC", "ComplaintSubtype"),
    shortcut_type="view",
)
#: The Lakehouse folder shortcut a physical target becomes.
XLSX = _physical(
    _document(LANDING, "ACQSC", "HarmSurveyXlsx", is_files=True),
    shortcut_type="folder",
    schema="ACQSC/HarmSurveyXlsx",
    workspace="35 South Data",
)
#: The ``_`` surface, which a mirror stands up from its own declaration.
SURFACE = _logical(
    _document(CURATED, "_", "Bookmark"),
    _document(WeaverItemId.parse("Warehouse/_weaver"), "_", "Bookmark"),
    shortcut_type="view",
)


# --- which pointers a mirror stands up ----------------------------------------


@weaver_test()
def test_a_pointer_is_recreated_rather_than_borrowed():
    """It holds no data, so there is nothing to point at another target's rows."""

    found = recreatable([DELTA], item=CURATED, bindings=BINDINGS)

    assert [str(each.destination) for each in found] == [
        "Warehouse/Curated/ACQSC.ComplaintSubtypeDelta"
    ]


@weaver_test()
def test_the_surface_is_left_to_the_declaration_that_makes_it():
    """``_`` is Weaver's own, and a mirror stands it up for every item alike.

    Recreating it here as well would write the same view twice, and from a
    forked row rather than from
    :func:`weaver.catalogue.borrow.surface_statements`.
    """

    assert recreatable([SURFACE], item=CURATED, bindings=BINDINGS) == ()


@weaver_test()
def test_another_items_pointers_are_not_this_ones():
    assert recreatable([DELTA, XLSX], item=LANDING, bindings=BINDINGS) == (
        recreatable([XLSX], item=LANDING, bindings=BINDINGS)[0],
    )


# --- where a recreated pointer points -----------------------------------------


@weaver_test()
def test_a_logical_pointer_follows_the_forks_own_bindings():
    """The logical relationship moves with the fork.

    ``Warehouse/Curated`` read ``Lakehouse/Landing``, so the item filling
    ``DEV_Curated`` reads the one filling ``DEV_Landing``.
    """

    pointer = recreatable([DELTA], item=CURATED, bindings=BINDINGS)[0]

    assert pointer.target_name == "DEV_Landing"
    assert view_statement(pointer) == (
        "create or alter view [ACQSC].[ComplaintSubtypeDelta] as select * from "
        "[DEV_Landing].[ACQSC].[ComplaintSubtype];"
    )


@weaver_test()
def test_a_physical_pointer_stays_on_the_target_it_was_recorded_with():
    """It names a Fabric item Weaver does not manage, so no binding moves it."""

    pointer = recreatable([XLSX], item=LANDING, bindings=BINDINGS)[0]

    assert pointer.target_name == "Drop"
    assert pointer.target_workspace == "35 South Data"
    assert pointer.source_components == ("Files", "ACQSC", "HarmSurveyXlsx")


@weaver_test()
def test_an_unbound_logical_target_stops_the_run():
    """A mirror reconstructs the estate it was asked for, or says what it could not.

    Nothing here can say where the target is, so there is no address to point
    at. Left out, the estate would come back missing a pointer its own Registry
    certifies.
    """

    with pytest.raises(UnresolvedShortcut, match="Lakehouse/Landing"):
        recreatable([DELTA], item=CURATED, bindings={})


@weaver_test()
def test_a_logical_pointer_at_an_item_this_run_leaves_alone_keeps_its_target():
    """Partial mirrors are coherent: the binding map is the whole answer."""

    pointer = recreatable(
        [DELTA], item=CURATED, bindings={**BINDINGS, LANDING: "Landing"}
    )[0]

    assert pointer.target_name == "Landing"


# --- how each kind is addressed -----------------------------------------------


@weaver_test()
def test_a_lakehouse_pointer_carries_the_two_addresses_a_shortcut_needs():
    pointer = recreatable([XLSX], item=LANDING, bindings=BINDINGS)[0]

    assert shortcut_request(pointer, source="item", source_path="Files/x") == {
        "shortcut": "Lakehouse/Landing/Files/ACQSC.HarmSurveyXlsx",
        "type": "folder",
        "path": "Files/ACQSC",
        "name": "HarmSurveyXlsx",
        "source": "item",
        "source_path": "Files/x",
    }


@weaver_test()
def test_a_schema_pointer_sits_directly_under_tables():
    """Measured against Fabric: ``path=Tables, name=<Schema>``."""

    pointer = recreatable(
        [
            _physical(
                WeaverSchemaId(LANDING, "Reference"),
                shortcut_type="schema",
                schema="Reference",
            )
        ],
        item=LANDING,
        bindings=BINDINGS,
    )[0]

    assert (pointer.path, pointer.name) == ("Tables", "Reference")
    assert pointer.source_components == ("Tables", "Reference")


@weaver_test()
def test_a_logical_table_pointer_reads_the_targets_tables_area():
    source = _document(LANDING, "ACQSC", "ComplaintSubtype")
    pointer = recreatable(
        [
            _logical(
                _document(CURATED, "ACQSC", "Copy"), source, shortcut_type="view"
            )
        ],
        item=CURATED,
        bindings=BINDINGS,
    )[0]

    assert pointer.source_components == ("Tables", "ACQSC", "ComplaintSubtype")


@weaver_test()
def test_the_schemas_a_pointer_needs_are_named():
    """A schema holding only pointers has no borrowed relation to have made it."""

    assert schemas_of(recreatable([DELTA], item=CURATED, bindings=BINDINGS)) == (
        "ACQSC",
    )


# --- what a kind cannot stand up ----------------------------------------------


@weaver_test()
@pytest.mark.parametrize(
    "kind, shortcut, expected",
    [
        (LAKEHOUSE, DELTA, "Warehouse view"),
        (WAREHOUSE, XLSX, "OneLake shortcut"),
    ],
    ids=["view-in-a-lakehouse", "folder-in-a-warehouse"],
)
def test_a_pointer_its_item_has_no_form_for_is_refused(kind, shortcut, expected):
    """``_.Shortcut`` records what was declared, so a mismatch is a contradiction."""

    pointer = recreatable(
        [shortcut], item=shortcut.destination.item, bindings=BINDINGS
    )[0]

    assert expected in unsupported(pointer, kind=kind)


@weaver_test()
def test_each_kind_stands_up_the_pointer_that_belongs_to_it():
    assert unsupported(
        recreatable([DELTA], item=CURATED, bindings=BINDINGS)[0], kind=WAREHOUSE
    ) is None
    assert unsupported(
        recreatable([XLSX], item=LANDING, bindings=BINDINGS)[0], kind=LAKEHOUSE
    ) is None
