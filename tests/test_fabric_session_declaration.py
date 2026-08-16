"""Within-workspace Fabric resolution and storage, without a live tenant."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace

import pytest
from support.weaver_test import weaver_test

from weaver.errors import CommandError
from weaver.fabric import FabricSessionResolver, FabricStore
from weaver.locations import Location
from weaver.store import Store
from weaver.targets import ItemRef
from weaver.workspaces import Workspace


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


@weaver_test()
def test_session_resolution_stays_in_the_current_workspace():
    lakehouse = _LakehouseUtils()
    resolver = FabricSessionResolver(
        Workspace(workspace="Analytics"),
        runtime=_runtime(),
        lakehouse=lakehouse,
    )

    tables = resolver.tables_root(ItemRef("Sales"))

    assert tables.value == (
        "abfss://workspace-id@onelake.dfs.fabric.microsoft.com/"
        "11111111-1111-1111-1111-111111111111/Tables"
    )
    assert lakehouse.calls == [("Sales", "workspace-id")]


class _NamedLakehouses:
    """A workspace where each Lakehouse name has its own id."""

    def get(self, name, *, workspaceId):
        return SimpleNamespace(id=f"id-of-{name}", displayName=name)


@weaver_test()
def test_a_lakehouse_resolves_to_what_authored_code_addresses():
    """What ``lakehouse_for`` composes inside a session — attached or not.

    The OneLake root is the durable identity, and a table is addressed by it
    directly. A folder is reached through a mount of that same root, resolved on
    use, so an object reaches a Lakehouse this session never attached exactly as
    it reaches the one it did.
    """

    from weaver import lakehouse_for

    resolver = FabricSessionResolver(
        Workspace(workspace="Analytics"),
        runtime=_runtime(),
        lakehouse=_NamedLakehouses(),
    )

    lakehouse = lakehouse_for(resolver, ItemRef("Sales"))

    assert lakehouse.table_path("Sales", "Order") == (
        "abfss://workspace-id@onelake.dfs.fabric.microsoft.com/"
        "id-of-Sales/Tables/Sales/Order"
    )
    assert lakehouse.location.files_root == (
        "abfss://workspace-id@onelake.dfs.fabric.microsoft.com/id-of-Sales/Files"
    )
    assert lakehouse.qualify("Sales", "Order") == "`Analytics`.`Sales`.`Sales`.`Order`"


@weaver_test()
def test_session_resolution_refuses_a_different_configured_workspace():
    with pytest.raises(CommandError, match="not configured Workspace"):
        FabricSessionResolver(
            Workspace(workspace="Other"),
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


@weaver_test()
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


@weaver_test()
def test_fabric_store_copies_between_onelake_and_the_driver_without_byte_decoding(
    tmp_path,
):
    class CopyFs:
        def __init__(self, remote_tree, remote_archive):
            self.calls = []
            self.remote_tree = remote_tree
            self.remote_archive = remote_archive

        def exists(self, path):
            return path in {self.remote_tree, self.remote_archive}

        def ls(self, path):
            if path == self.remote_tree.rsplit("/", 1)[0]:
                return [_Info(self.remote_tree, "Estate", True)]
            if path == self.remote_tree:
                return [
                    _Info(f"{path}/table.py", "table.py", False, 12),
                    _Info(f"{path}/.authored.crc", ".authored.crc", False, 8),
                ]
            if path == self.remote_archive.rsplit("/", 1)[0]:
                return [_Info(self.remote_archive, "record.weaver.zip", False, 12)]
            raise AssertionError(path)

        def cp(self, source, destination, recurse):
            self.calls.append((source, destination, recurse))
            local = Path(destination.removeprefix("file:"))
            if recurse:
                local.mkdir(parents=True)
                (local / "table.py").write_text("table", encoding="utf-8")
                (local / ".table.py.crc").write_bytes(b"generated")
                (local / ".authored.crc").write_bytes(b"authored")
            elif source.startswith("abfss://"):
                local.write_bytes(b"PK fake binary")
                (local.parent / f".{local.name}.crc").write_bytes(b"generated")
            return True

    remote_tree = Location(
        "abfss://workspace-id@onelake.dfs.fabric.microsoft.com/lakehouse/Files/weaver_items"
    )
    remote_archive = Location(
        "abfss://workspace-id@onelake.dfs.fabric.microsoft.com/lakehouse/Files/"
        "build_bundles/record.weaver.zip"
    )
    fs = CopyFs(remote_tree.value, remote_archive.value)
    store = FabricStore(fs)
    local_tree = tmp_path / "Estate"
    downloaded_archive = tmp_path / "downloaded.weaver.zip"
    local_archive = tmp_path / "record.weaver.zip"
    local_archive.write_bytes(b"PK fake binary")

    store.copy_to_local(remote_tree, local_tree)
    store.copy_to_local(remote_archive, downloaded_archive)
    store.copy_from_local(local_archive, remote_archive)

    assert (local_tree / ".authored.crc").is_file()
    assert not (local_tree / ".table.py.crc").exists()
    assert downloaded_archive.read_bytes() == b"PK fake binary"
    assert not (tmp_path / ".downloaded.weaver.zip.crc").exists()
    assert fs.calls == [
        (remote_tree.value, f"file:{local_tree.as_posix()}", True),
        (
            remote_archive.value,
            f"file:{downloaded_archive.as_posix()}",
            False,
        ),
        (f"file:{local_archive.as_posix()}", remote_archive.value, False),
    ]
