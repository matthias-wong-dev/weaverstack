"""The OneLake store, against real OneLake.

A **cross-boundary** test: it exercises OneLakeDfsClient reaching into a workspace
over DFS from the laptop, which is how the CLI pushes and inspects. It is not a
test of Weaver running inside Fabric.
"""

from __future__ import annotations

import pytest
from support.weaver_test import weaver_test

from weaver.errors import CommandError
from weaver.fabric import onelake_url, parse_onelake
from weaver.locations import Location
from weaver.store import Store, StoreError


@pytest.fixture
def store(rest_session, fabric_workspace):
    return rest_session.transport_store(fabric_workspace)


#: Where these tests work, beneath the staging Lakehouse's Files area. Staging
#: rather than a target, because a target's inventory would report what is
#: written here as declared folders.
PRIMITIVE_AREA = "primitive_tests"


@pytest.fixture
def files_root(store, fabric_staging_lakehouse, request):
    """A subtree of the fixed staging Lakehouse that this test owns outright.

    Named for the test, and emptied on the way in as well as out: the item is
    permanent, so residue from an interrupted run is possible in a way it was
    not when each test got a new Lakehouse.
    """

    root = Location(
        onelake_url(
            fabric_staging_lakehouse.workspace_id,
            fabric_staging_lakehouse.id,
            f"Files/{PRIMITIVE_AREA}/{request.node.name}",
        )
    )
    store.delete(root, recursive=True)
    try:
        yield root
    finally:
        store.delete(root, recursive=True)


@weaver_test(remote=True)
def test_the_onelake_store_satisfies_the_protocol(store):
    assert isinstance(store, Store)


@weaver_test(remote=True, resources={"onelake"})
def test_write_then_read(store, files_root):
    store.write(files_root / "a.csv", b"id,amount\n1,10\n")
    assert store.read(files_root / "a.csv") == b"id,amount\n1,10\n"


@weaver_test(remote=True, resources={"onelake"})
def test_an_empty_file_is_written(store, files_root):
    store.write(files_root / "empty.txt", b"")
    assert store.read(files_root / "empty.txt") == b""


@weaver_test(remote=True, resources={"onelake"})
def test_exists_and_is_directory(store, files_root):
    store.write(files_root / "folder" / "a.csv", b"x")
    assert store.exists(files_root / "folder" / "a.csv")
    assert store.is_directory(files_root / "folder")
    assert not store.is_directory(files_root / "folder" / "a.csv")
    assert not store.exists(files_root / "absent.csv")


@weaver_test(remote=True, resources={"onelake"})
def test_listing_carries_metadata(store, files_root):
    store.write(files_root / "listed" / "a.csv", b"0123456789")
    entry = next(
        e for e in store.list(files_root / "listed") if e.location.name == "a.csv"
    )
    assert entry.size == 10
    assert entry.modified is not None
    assert not entry.is_directory


@weaver_test(remote=True, resources={"onelake"})
def test_listing_is_shallow_by_default(store, files_root):
    store.write(files_root / "shallow" / "top.csv", b"x")
    store.write(files_root / "shallow" / "nested" / "deep.csv", b"x")
    assert {e.location.name for e in store.list(files_root / "shallow")} == {
        "top.csv",
        "nested",
    }


@weaver_test(remote=True, resources={"onelake"})
def test_listing_recursively_reaches_nested_files(store, files_root):
    store.write(files_root / "deep" / "nested" / "deep.csv", b"x")
    names = {e.location.value for e in store.list(files_root / "deep", recursive=True)}
    assert (files_root / "deep" / "nested" / "deep.csv").value in names


@weaver_test(remote=True, resources={"onelake"})
def test_delete_removes_a_tree(store, files_root):
    store.write(files_root / "doomed" / "a.csv", b"x")
    store.delete(files_root / "doomed", recursive=True)
    assert not store.exists(files_root / "doomed")


@weaver_test(remote=True, resources={"onelake"})
def test_deleting_something_absent_is_quiet(store, files_root):
    store.delete(files_root / "never-existed", recursive=True)


@weaver_test(remote=True, resources={"onelake"})
def test_make_directory_is_idempotent(store, files_root):
    store.make_directory(files_root / "made")
    store.make_directory(files_root / "made")
    assert store.is_directory(files_root / "made")


@weaver_test(remote=True, resources={"onelake"})
def test_listing_a_missing_location_is_an_error(store, files_root):
    with pytest.raises(StoreError, match="does not exist"):
        store.list(files_root / "absent")


# --- url handling ------------------------------------------------------------


@weaver_test(remote=True)
def test_a_guid_item_needs_no_type_suffix():
    url = onelake_url("ws-id", "3fa85f64-5717-4562-b3fc-2c963f66afa6", "Files/x")
    assert "3fa85f64-5717-4562-b3fc-2c963f66afa6/Files/x" in url


@weaver_test(remote=True)
def test_a_named_item_carries_its_type():
    assert onelake_url("MyWorkspace", "Weaver", "Files").endswith(
        "Weaver.Lakehouse/Files"
    )


@weaver_test(remote=True)
def test_a_onelake_location_splits_back_into_its_parts():
    parsed = parse_onelake(
        Location(onelake_url("ws", "item-id", "Files/weaver_items/x"))
    )
    assert (parsed.workspace, parsed.relative) == ("ws", "Files/weaver_items/x")


@weaver_test(remote=True)
def test_a_local_path_is_not_a_onelake_location():
    with pytest.raises(CommandError, match="not a OneLake location"):
        parse_onelake(Location("/srv/.local/Sales_LH"))
