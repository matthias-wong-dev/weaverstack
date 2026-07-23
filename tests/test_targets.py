"""Level-three identities: parsing, normalisation and round-tripping."""

from __future__ import annotations

import pytest

from weaver import (
    DeltaTarget,
    FolderTarget,
    ItemRef,
    RepositoryRef,
    WarehouseTarget,
)
from weaver.errors import IdentityError

ROUND_TRIP = [
    (FolderTarget, "Sales/Files"),
    (FolderTarget, "Sales/Files/Extracts"),
    (FolderTarget, "Weaver/Files/repos"),
    (FolderTarget, "Inventory/Files/Forecasts/Daily"),
    (DeltaTarget, "Sales"),
    (WarehouseTarget, "Reporting"),
    (ItemRef, "Weaver"),
    (RepositoryRef, "sales-etl"),
]


@pytest.mark.parametrize("kind,text", ROUND_TRIP, ids=[f"{k.__name__}:{t}" for k, t in ROUND_TRIP])
def test_parse_then_str_is_identity(kind, text):
    assert str(kind.parse(text)) == text


@pytest.mark.parametrize("kind,text", ROUND_TRIP, ids=[f"{k.__name__}:{t}" for k, t in ROUND_TRIP])
def test_parsing_is_stable(kind, text):
    assert kind.parse(text) == kind.parse(str(kind.parse(text)))


def test_folder_target_splits_lakehouse_and_subpath():
    target = FolderTarget.parse("Sales/Files/Extracts/Daily")
    assert target.lakehouse == ItemRef("Sales")
    assert target.subpath == ("Extracts", "Daily")


def test_folder_target_may_be_the_files_root():
    assert FolderTarget.parse("Sales/Files").subpath == ()


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
    assert DeltaTarget.parse("Shared").lakehouse == WarehouseTarget.parse("Shared").warehouse


def test_repository_is_a_name_not_a_path():
    with pytest.raises(IdentityError, match="single name"):
        RepositoryRef.parse("Weaver/Files/repos/sales-etl")


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
