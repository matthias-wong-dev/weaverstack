"""Level-three identities: parsing, normalisation and round-tripping."""

from __future__ import annotations

import pytest

from weaver.errors import IdentityError
from weaver.targets import DeltaTarget, FolderTarget, ItemRef, WarehouseTarget

ROUND_TRIP = [
    (FolderTarget, "Sales/Files"),
    (FolderTarget, "Control/Files"),
    (DeltaTarget, "Sales"),
    (WarehouseTarget, "Reporting"),
    (ItemRef, "Weaver"),
]


@pytest.mark.parametrize(
    "kind,text", ROUND_TRIP, ids=[f"{k.__name__}:{t}" for k, t in ROUND_TRIP]
)
def test_parse_then_str_is_identity(kind, text):
    assert str(kind.parse(text)) == text


@pytest.mark.parametrize(
    "kind,text", ROUND_TRIP, ids=[f"{k.__name__}:{t}" for k, t in ROUND_TRIP]
)
def test_parsing_is_stable(kind, text):
    assert kind.parse(text) == kind.parse(str(kind.parse(text)))


def test_folder_target_names_a_lakehouse_and_its_files_area():
    assert FolderTarget.parse("Sales/Files").lakehouse == ItemRef("Sales")


def test_folder_target_refuses_anything_beneath_the_files_area():
    """A folder object lands at Files/<Schema>/<Object>, derived from its identity.

    A configurable root would make that derivation false, so the binding cannot
    offer one — authored code composes this path from identity alone.
    """

    with pytest.raises(IdentityError, match="nothing to configure"):
        FolderTarget.parse("Sales/Files/Extracts")


def test_folder_target_requires_the_files_area():
    with pytest.raises(IdentityError, match="Files"):
        FolderTarget.parse("Sales/Tables/Thing")


def test_folder_target_requires_more_than_a_lakehouse():
    with pytest.raises(IdentityError, match="folder target"):
        FolderTarget.parse("Sales")


def test_delta_target_rejects_an_explicit_tables_area():
    with pytest.raises(IdentityError, match="implicit"):
        DeltaTarget.parse("Sales/Tables")


def test_warehouse_target_rejects_a_path():
    with pytest.raises(IdentityError):
        WarehouseTarget.parse("Reporting/dbo")


def test_the_same_name_serves_different_slots():
    """Kind comes from the slot, never from the string."""
    assert (
        DeltaTarget.parse("Shared").lakehouse
        == WarehouseTarget.parse("Shared").warehouse
    )


@pytest.mark.parametrize("bad", ["", "   ", "a\\b", "a:b", "a*b", "..", "a|b"])
def test_illegal_names_are_rejected(bad):
    with pytest.raises(IdentityError):
        ItemRef.parse(bad)


def test_surrounding_whitespace_is_normalised():
    assert ItemRef("  Sales  ").name == "Sales"


def test_identities_are_immutable():
    target = DeltaTarget.parse("Sales")
    with pytest.raises(Exception):
        target.lakehouse = ItemRef("Other")


# --- the binding grammar ------------------------------------------------------
#
# `Lakehouse/SalesDev=Sales`: the left-hand side is typed and supplies the type
# for both sides, so the logical item is named alone.


def test_a_binding_types_both_sides_from_the_physical_one():
    from weaver.build_bundle.targets import parse_item_binding

    binding = parse_item_binding("Lakehouse/SalesDev=Sales")

    assert str(binding.item) == "Lakehouse/Sales"
    assert binding.target.item.name == "SalesDev"


def test_a_warehouse_binding_reads_the_same_way():
    from weaver.build_bundle.targets import parse_item_binding

    binding = parse_item_binding("Warehouse/ReportingDev=Reporting")

    assert str(binding.item) == "Warehouse/Reporting"
    assert binding.target.item.name == "ReportingDev"


def test_the_same_bare_name_under_two_types_is_two_items():
    """`Lakehouse/Sales` and `Warehouse/Sales` are distinct logical items.

    Which is why the physical side supplying the type is enough: the bare name
    on the right is never ambiguous once the left has been read.
    """

    from weaver.build_bundle.targets import parse_item_binding

    lakehouse = parse_item_binding("Lakehouse/SalesDev=Sales")
    warehouse = parse_item_binding("Warehouse/SalesWh=Sales")

    assert lakehouse.item != warehouse.item


def test_a_typed_logical_item_is_refused_and_says_what_to_write():
    """Not a type mismatch to check — a sentence that cannot be written.

    The old grammar said the word twice and let the two disagree, so the error
    it needed was about disagreement. This one has nothing to disagree with.
    """

    import pytest

    from weaver.build_bundle.targets import parse_item_binding
    from weaver.errors import BuildError

    with pytest.raises(BuildError, match="Lakehouse/SalesDev=Sales"):
        parse_item_binding("Lakehouse/SalesDev=Lakehouse/Sales")


def test_a_logical_item_of_the_wrong_type_cannot_be_smuggled_in():
    import pytest

    from weaver.build_bundle.targets import parse_item_binding
    from weaver.errors import BuildError

    with pytest.raises(BuildError, match="named without a type"):
        parse_item_binding("Lakehouse/SalesDev=Warehouse/Sales")
