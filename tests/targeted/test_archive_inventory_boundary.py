"""Archive byte snapshots use the DFS namespace that serves their reads."""

from dataclasses import replace

import pytest
from support.weaver_test import weaver_test

from weaver.locations import Location
from weaver.sessions.archive_runtime import ArchiveDfs, ArchiveStore
from weaver.store import Entry, StoreError


@weaver_test()
def test_archive_inventory_and_reads_share_the_dfs_backend():
    root = Location(
        "abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse/Tables/DWG/Customer"
    )
    active = root.join("active.parquet")
    phantom = root.join("metadata", "expired.avro")
    calls = []

    class Native:
        def list(self, location, *, recursive=False):
            return [Entry(active, False), Entry(phantom, False)]

    class Dfs:
        def list(self, location, *, recursive=False):
            calls.append((location, recursive))
            return [Entry(active, False)]

        def read(self, location):
            assert location == active
            return b"\xffparquet"

    store = ArchiveStore(Native(), Dfs())
    assert store.list(root, recursive=True) == [Entry(active, False)]
    assert store.read(active) == b"\xffparquet"
    assert calls == [(root, True)]


@weaver_test()
@pytest.mark.parametrize("suffix", ["", "Files", "Tables", "Tables/DWG/Customer"])
def test_archive_dfs_listing_preserves_the_callers_location_namespace(
    monkeypatch, suffix
):
    from weaver.fabric.onelake import OneLakeDfsClient

    root = Location("abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse")
    if suffix:
        root = root.join(suffix)
    client = ArchiveDfs(token="test")
    remote = Location(
        "https://onelake.dfs.fabric.microsoft.com/workspace/lakehouse/" + suffix
    )
    assert client._url(root) == remote.value
    entries = [
        Entry(remote.join("_delta_log"), True),
        Entry(remote.join("_delta_log", "00000000000000000000.json"), False),
    ]
    calls = []

    def inventory(self, location, *, recursive=False):
        assert location == remote
        calls.append((location, recursive))
        return entries

    monkeypatch.setattr(OneLakeDfsClient, "list", inventory)
    client = ArchiveDfs(token="test")
    assert client.list(root, recursive=True) == [
        replace(
            entry, location=root.join(entry.location.value[len(remote.value) + 1 :])
        )
        for entry in entries
    ]
    assert calls == [(remote, True)]


@weaver_test()
def test_archive_read_support_does_not_relax_publication_binding():
    client = ArchiveDfs(token="test")
    root = Location(
        "abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse/Files"
    )
    with pytest.raises(StoreError, match="bound OneLake path"):
        client._publication_location(root)


@weaver_test()
@pytest.mark.parametrize("folder", ["Monthly files", "文档 é", "literal%20name"])
@pytest.mark.parametrize("native", [False, True])
def test_archive_real_dfs_listing_accepts_canonically_encoded_names(
    monkeypatch, folder, native
):
    from types import SimpleNamespace
    from urllib.parse import quote

    client = ArchiveDfs(token="test")
    name = "lakehouse/Files/Raw/" + folder + "/nested file.bin"
    response = SimpleNamespace(
        status_code=200,
        headers={},
        json=lambda: {"paths": [{"name": name, "isDirectory": "false"}]},
    )
    monkeypatch.setattr(client, "_request", lambda *args, **kwargs: response)
    root = Location(
        (
            "abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse/Files/Raw/"
            + folder
        )
        if native
        else (
            "https://onelake.dfs.fabric.microsoft.com/workspace/lakehouse/Files/Raw/"
            + quote(folder, safe="")
        )
    )
    entries = client.list(root, recursive=True)
    assert [e.location for e in entries] == [
        root.join("nested file.bin" if native else "nested%20file.bin")
    ]


@weaver_test()
def test_archive_listing_still_rejects_a_different_directory(monkeypatch):
    from types import SimpleNamespace

    client = ArchiveDfs(token="test")
    response = SimpleNamespace(
        status_code=200,
        headers={},
        json=lambda: {"paths": [{"name": "lakehouse/Files/Raw/Sales extra/file.bin"}]},
    )
    monkeypatch.setattr(client, "_request", lambda *args, **kwargs: response)
    with pytest.raises(StoreError, match="escaped"):
        client.list(
            Location(
                "abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse/Files/Raw/Sales"
            )
        )
