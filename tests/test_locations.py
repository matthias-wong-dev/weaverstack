"""Locations join by string so URL roots survive."""

from __future__ import annotations

from pathlib import Path

import pytest

from weaver import Location
from weaver.errors import IdentityError

ABFSS = "abfss://workspace-id@onelake.dfs.fabric.microsoft.com/lakehouse-id"


def test_a_filesystem_location_offers_a_path():
    assert Location("/srv/.local/Sales").path == Path("/srv/.local/Sales")


def test_a_url_location_refuses_to_become_a_path():
    """pathlib collapses '//' — better to raise than to corrupt the root."""
    with pytest.raises(IdentityError, match="URL location"):
        Location(ABFSS).path


def test_joining_preserves_a_url_root():
    joined = Location(ABFSS) / "Files" / "Budget"
    assert joined.value == f"{ABFSS}/Files/Budget"
    assert joined.is_url


def test_pathlib_would_have_corrupted_it():
    """The reason this type exists, asserted."""
    assert str(Path(ABFSS)) != ABFSS


def test_joining_a_filesystem_location():
    assert (Location("/srv/.local") / "Sales" / "Files").value == "/srv/.local/Sales/Files"


def test_join_takes_several_segments():
    assert Location("/srv").join("a", "b", "c").value == "/srv/a/b/c"


def test_redundant_separators_are_normalised():
    assert (Location("/srv/.local/") / "/Sales/").value == "/srv/.local/Sales"


def test_the_filesystem_root_survives_normalisation():
    assert Location("/").value == "/"


def test_name_is_the_final_segment():
    assert (Location(ABFSS) / "Files" / "Budget").name == "Budget"


@pytest.mark.parametrize("bad", ["", "   "])
def test_empty_locations_are_rejected(bad):
    with pytest.raises(IdentityError):
        Location(bad)


def test_empty_segments_are_rejected():
    with pytest.raises(IdentityError, match="segment"):
        Location("/srv").join("  ")


def test_locations_are_immutable():
    location = Location("/srv")
    with pytest.raises(Exception):
        location.value = "/elsewhere"
