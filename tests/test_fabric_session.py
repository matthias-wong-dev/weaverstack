"""Within-host Fabric resolution and storage, without a live tenant."""

from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace

import pytest

from weaver import FabricHost, ItemRef, Location, Store
from weaver.errors import CommandError
from weaver.fabric import FabricSessionResolver, FabricStore


class _LakehouseUtils:
    def __init__(self):
        self.calls = []

    def get(self, name, *, workspaceId):
        self.calls.append((name, workspaceId))
        return SimpleNamespace(
            id="11111111-1111-1111-1111-111111111111",
            displayName=name,
        )


def _runtime(name="Analytics"):
    return SimpleNamespace(
        context={
            "currentWorkspaceName": name,
            "currentWorkspaceId": "workspace-id",
        }
    )


def test_session_resolution_stays_in_the_current_workspace():
    lakehouse = _LakehouseUtils()
    resolver = FabricSessionResolver(
        FabricHost(workspace="Analytics"),
        runtime=_runtime(),
        lakehouse=lakehouse,
    )

    tables = resolver.tables_root(ItemRef("Sales"))

    assert tables.value == (
        "abfss://workspace-id@onelake.dfs.fabric.microsoft.com/"
        "11111111-1111-1111-1111-111111111111/Tables"
    )
    assert lakehouse.calls == [("Sales", "workspace-id")]


def test_session_resolution_refuses_a_different_host_workspace():
    with pytest.raises(CommandError, match="not host workspace"):
        FabricSessionResolver(
            FabricHost(workspace="Other"),
            runtime=_runtime(),
            lakehouse=_LakehouseUtils(),
        )


@dataclass
class _Info:
    path: str
    name: str
    isDir: bool
    size: int = 0


class _Fs:
    def __init__(self, root):
        self.root = root
        self.deleted = []

    def exists(self, path):
        return path == self.root

    def ls(self, path):
        assert path == self.root
        return [
            _Info(f"{path}/Sales", "Sales", True),
            _Info(f"{path}/notes.txt", "notes.txt", False, 12),
        ]

    def rm(self, path, *, recurse):
        self.deleted.append((path, recurse))
        return True

    def mkdirs(self, path):
        return True


def test_fabric_store_lists_and_deletes_through_notebookutils():
    root = "abfss://workspace-id@onelake.dfs.fabric.microsoft.com/lakehouse/Tables"
    fs = _Fs(root)
    store = FabricStore(fs)

    entries = store.list(Location(root))
    store.delete(entries[0].location, recursive=True)

    assert isinstance(store, Store)
    assert [(entry.name, entry.is_directory, entry.size) for entry in entries] == [
        ("Sales", True, None),
        ("notes.txt", False, 12),
    ]
    assert fs.deleted == [(f"{root}/Sales", True)]
