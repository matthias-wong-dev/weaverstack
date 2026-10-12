"""Native and DFS publication refuse destination overwrite for protected copies."""

from types import SimpleNamespace

import pytest
from support.weaver_test import weaver_test

from weaver.fabric.onelake import OneLakeDfsClient, onelake_url
from weaver.fabric.store import FabricStore
from weaver.locations import Location
from weaver.store import StoreOutcomeUnknown


@weaver_test()
def test_native_protected_directory_publication_never_requests_overwrite():
    calls = []
    fs = SimpleNamespace(
        mv=lambda *args, **kwargs: calls.append((args, kwargs)) or True
    )
    store = FabricStore(fs)
    root = "abfss://workspace@onelake.dfs.fabric.microsoft.com/item"
    source, destination = (
        Location(root + "/Files/private"),
        Location(root + "/Files/Raw/Retained"),
    )
    assert hasattr(store, "publish_directory")
    store.publish_directory(source, destination)
    assert calls == [
        ((source.value, destination.value), {"create_path": True, "overwrite": False})
    ]


@weaver_test()
@pytest.mark.parametrize("area", ["Files", "Tables"])
def test_dfs_protected_directory_publication_is_conditional(monkeypatch, area):
    store = OneLakeDfsClient.__new__(OneLakeDfsClient)
    store.base_url = "https://onelake.dfs.fabric.microsoft.com"
    item = "5c1893a3-a288-46a5-8cc2-a55c4c89b4b7"
    source = Location(onelake_url("workspace", item, "Files/private"))
    destination = Location(onelake_url("workspace", item, f"{area}/Raw/Retained"))
    calls = []
    monkeypatch.setattr(
        store,
        "_request",
        lambda *args, **kwargs: (
            calls.append((args, kwargs)) or SimpleNamespace(headers={})
        ),
    )
    assert hasattr(store, "publish_directory")
    store.publish_directory(source, destination)
    assert len(calls) == 1
    assert calls[0][0] == ("PUT", destination.value)
    assert calls[0][1]["headers"]["If-None-Match"] == "*"
    assert calls[0][1]["expected"] == (201,)


@weaver_test()
def test_dfs_copy_preserves_binary_bytes(monkeypatch):
    store = OneLakeDfsClient.__new__(OneLakeDfsClient)
    source, destination = Location("source"), Location("destination")
    calls = []
    monkeypatch.setattr(store, "read", lambda location: b"\x00\xff\x80")
    monkeypatch.setattr(
        store, "write", lambda location, data: calls.append((location, data))
    )
    assert hasattr(store, "copy")
    store.copy(source, destination)
    assert calls == [(destination, b"\x00\xff\x80")]


@weaver_test()
def test_native_publication_preserves_uncertainty():
    def lost(*args, **kwargs):
        raise StoreOutcomeUnknown("rename reply lost")

    store = FabricStore(SimpleNamespace(mv=lost))
    root = "abfss://workspace@onelake.dfs.fabric.microsoft.com/item"
    with pytest.raises(StoreOutcomeUnknown, match="reply lost"):
        store.publish_directory(
            Location(root + "/Files/private"), Location(root + "/Files/Raw/Retained")
        )
