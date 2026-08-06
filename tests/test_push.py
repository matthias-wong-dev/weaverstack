"""Whole-repository push validates first and replaces one canonical tree."""

from __future__ import annotations

import pytest

from weaver.store import FilesystemStore
from weaver.locations import Location
from weaver.push import push_item_repository
from weaver.errors import DiscoveryError

from test_item_repository import _estate, _write


def test_push_replaces_destination_without_an_extra_source_root(tmp_path):
    source = _estate(tmp_path)
    destination = tmp_path / "Weaver" / "Files" / "weaver_items"
    (destination / "obsolete.txt").parent.mkdir(parents=True)
    (destination / "obsolete.txt").write_text("old", encoding="utf-8")

    result = push_item_repository(
        Location(str(source)),
        Location(str(destination)),
        destination_store=FilesystemStore(),
    )

    assert not (destination / "obsolete.txt").exists()
    assert (destination / "Lakehouse" / "Raw" / "Sales__Customer.py").is_file()
    assert not (destination / source.name).exists()
    assert "Lakehouse/Raw/Sales__Customer.py" in result.files
    assert not any("_ignore" in path for path in result.files)
    assert not (destination / "Lakehouse" / "_weaver").exists()


def test_invalid_source_fails_before_existing_destination_is_touched(tmp_path):
    source = _estate(tmp_path)
    _write(source, "invalid.txt", "not an item")
    destination = tmp_path / "remote"
    destination.mkdir()
    sentinel = destination / "keep.txt"
    sentinel.write_text("keep", encoding="utf-8")

    with pytest.raises(DiscoveryError):
        push_item_repository(
            Location(str(source)),
            Location(str(destination)),
            destination_store=FilesystemStore(),
        )

    assert sentinel.read_text(encoding="utf-8") == "keep"


def test_push_does_not_expose_selective_document_arguments():
    import inspect

    assert set(inspect.signature(push_item_repository).parameters) == {
        "source",
        "destination",
        "destination_store",
        "source_store",
    }
