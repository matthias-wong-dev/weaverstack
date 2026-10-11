"""Archive byte snapshots use the DFS namespace that serves their reads."""

from dataclasses import replace

from support.weaver_test import weaver_test

from weaver.build_bundle.executors.materialise_mirror import MaterialiseMirrorExecutor
from weaver.locations import Location
from weaver.sessions.archive_runtime import ArchiveDfs, ArchiveStore
from weaver.store import Entry


@weaver_test()
def test_archive_snapshot_uses_the_read_backends_inventory():
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
    snapshot = MaterialiseMirrorExecutor._snapshot(root, store)
    assert set(snapshot) == {"active.parquet"}
    assert calls == [(root, True)]


@weaver_test()
def test_archive_dfs_listing_preserves_the_callers_location_namespace(monkeypatch):
    from weaver.fabric.onelake import OneLakeDfsClient

    root = Location(
        "abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse/Tables/DWG/Customer"
    )
    client = ArchiveDfs(token="test")
    remote = client._publication_location(root)
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
