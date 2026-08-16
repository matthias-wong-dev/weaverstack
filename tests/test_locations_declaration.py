"""Locations join by string so URL roots survive."""

from __future__ import annotations

from pathlib import Path

import pytest
from support.weaver_test import weaver_test

from weaver.errors import IdentityError
from weaver.locations import Location

ABFSS = "abfss://workspace-id@onelake.dfs.fabric.microsoft.com/lakehouse-id"


@weaver_test()
def test_a_filesystem_location_offers_a_path():
    assert Location("/srv/.local/Sales").path == Path("/srv/.local/Sales")


@weaver_test()
def test_a_url_location_refuses_to_become_a_path():
    """pathlib collapses '//' — better to raise than to corrupt the root."""
    with pytest.raises(IdentityError, match="URL location"):
        Location(ABFSS).path


@weaver_test()
def test_joining_preserves_a_url_root():
    joined = Location(ABFSS) / "Files" / "Budget"
    assert joined.value == f"{ABFSS}/Files/Budget"
    assert joined.is_url


@weaver_test()
def test_pathlib_would_have_corrupted_it():
    """The reason this type exists, asserted."""
    assert str(Path(ABFSS)) != ABFSS


@weaver_test()
def test_joining_a_filesystem_location():
    assert (
        Location("/srv/.local") / "Sales" / "Files"
    ).value == "/srv/.local/Sales/Files"


@weaver_test()
def test_join_takes_several_segments():
    assert Location("/srv").join("a", "b", "c").value == "/srv/a/b/c"


@weaver_test()
def test_redundant_separators_are_normalised():
    assert (Location("/srv/.local/") / "/Sales/").value == "/srv/.local/Sales"


@weaver_test()
def test_the_filesystem_root_survives_normalisation():
    assert Location("/").value == "/"


@weaver_test()
def test_backslashes_are_separators_like_any_other():
    """A Windows caller arrives with them, and everything here splits on "/".

    A root normalised through `Path` means that on Windows `str()` of it
    uses backslashes. Left alone they are not separators to `join` or `name`,
    and an Weaver document repository read from a Windows checkout takes its whole path as
    its catalogue name.
    """

    assert (
        Location("D:\\a\\weaverstack\\sales-etl").value == "D:/a/weaverstack/sales-etl"
    )
    assert Location("D:\\a\\weaverstack\\sales-etl").name == "sales-etl"
    assert (Location("\\srv\\.local") / "Sales").value == "/srv/.local/Sales"


@weaver_test()
def test_a_windows_root_still_becomes_a_path():
    assert Location("C:\\data\\Weaver").path == Path("C:/data/Weaver")


@weaver_test()
def test_name_is_the_final_segment():
    assert (Location(ABFSS) / "Files" / "Budget").name == "Budget"


@pytest.mark.parametrize("bad", ["", "   "])
@weaver_test()
def test_empty_locations_are_rejected(bad):
    with pytest.raises(IdentityError):
        Location(bad)


@weaver_test()
def test_empty_segments_are_rejected():
    with pytest.raises(IdentityError, match="segment"):
        Location("/srv").join("  ")


@weaver_test()
def test_locations_are_immutable():
    location = Location("/srv")
    with pytest.raises(Exception):
        location.value = "/elsewhere"
