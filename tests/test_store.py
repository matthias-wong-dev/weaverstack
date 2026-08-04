"""Transport primitives. Policy lives with its caller, not here."""

from __future__ import annotations

from pathlib import Path

import pytest

from weaver.store import Entry, LocalStore, Store
from weaver.locations import Location
from weaver.errors import CommandError
from weaver.store import StoreError


@pytest.fixture
def store() -> LocalStore:
    return LocalStore()


@pytest.fixture
def root(tmp_path: Path) -> Location:
    return Location(str(tmp_path))


def test_the_local_store_satisfies_the_protocol():
    assert isinstance(LocalStore(), Store)


def test_write_creates_missing_parents(store, root):
    store.write(root / "Budget" / "Expense" / "part.csv", b"id,name\n")
    assert store.read(root / "Budget" / "Expense" / "part.csv") == b"id,name\n"


def test_exists_and_is_directory(store, root):
    store.write(root / "a" / "file.txt", b"x")
    assert store.exists(root / "a" / "file.txt")
    assert store.is_directory(root / "a")
    assert not store.is_directory(root / "a" / "file.txt")
    assert not store.exists(root / "missing")


def test_listing_carries_metadata_not_just_names(store, root):
    """Every incremental strategy depends on this."""
    store.write(root / "file.txt", b"1234567890")
    entry = store.list(root)[0]
    assert isinstance(entry, Entry)
    assert entry.name == "file.txt"
    assert entry.size == 10
    assert entry.modified is not None
    assert not entry.is_directory


def test_listing_is_shallow_by_default(store, root):
    store.write(root / "top.txt", b"x")
    store.write(root / "nested" / "deep.txt", b"x")
    assert {entry.name for entry in store.list(root)} == {"top.txt", "nested"}


def test_listing_recursively_reaches_nested_files(store, root):
    store.write(root / "nested" / "deep.txt", b"x")
    names = {entry.location.value for entry in store.list(root, recursive=True)}
    assert (root / "nested" / "deep.txt").value in names


def test_directories_report_no_size(store, root):
    store.write(root / "nested" / "deep.txt", b"x")
    directory = next(entry for entry in store.list(root) if entry.is_directory)
    assert directory.size is None




def test_deleting_a_directory_needs_recursive(store, root):
    store.write(root / "tree" / "file.txt", b"x")
    with pytest.raises(StoreError, match="recursive"):
        store.delete(root / "tree")
    store.delete(root / "tree", recursive=True)
    assert not store.exists(root / "tree")


def test_deleting_something_absent_is_quiet(store, root):
    store.delete(root / "never-existed")


def test_make_directory_is_idempotent(store, root):
    store.make_directory(root / "a" / "b")
    store.make_directory(root / "a" / "b")
    assert store.is_directory(root / "a" / "b")


def test_listing_a_missing_location_is_an_error(store, root):
    with pytest.raises(StoreError, match="does not exist"):
        store.list(root / "absent")



def test_a_link_points_at_the_source_without_copying_it(store, root):
    """The emulator's counterpart of a OneLake shortcut: no bytes of its own."""

    store.write(root / "source" / "part.parquet", b"rows")
    store.link(root / "source", root / "alias")

    assert store.read(root / "alias" / "part.parquet") == b"rows"
    store.write(root / "source" / "part.parquet", b"more rows")
    assert store.read(root / "alias" / "part.parquet") == b"more rows"


def test_a_link_replaces_one_that_is_already_there(store, root):
    store.make_directory(root / "first")
    store.make_directory(root / "second")
    store.link(root / "first", root / "alias")
    store.link(root / "second", root / "alias")

    assert (root / "alias").path.resolve() == (root / "second").path.resolve()


def test_linking_to_something_absent_is_an_error_rather_than_a_dangling_link(store, root):
    with pytest.raises(StoreError, match="does not exist"):
        store.link(root / "absent", root / "alias")


def test_deleting_a_link_removes_the_link_and_never_what_it_points_at(store, root):
    store.write(root / "source" / "part.parquet", b"rows")
    store.link(root / "source", root / "alias")

    store.delete(root / "alias", recursive=True)

    assert not store.exists(root / "alias")
    assert store.read(root / "source" / "part.parquet") == b"rows"


def test_the_local_store_refuses_url_locations(store):
    remote = Location("abfss://ws@onelake.dfs.fabric.microsoft.com/lh/Files")
    with pytest.raises(CommandError, match="URL location"):
        store.exists(remote)


def test_the_store_takes_locations_not_strings(store, tmp_path):
    with pytest.raises(CommandError, match="Location"):
        store.exists(str(tmp_path))
