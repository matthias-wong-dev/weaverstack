"""Protected materialisation uses native operations on a disposable destination."""

import json
import shutil
from types import SimpleNamespace

import pytest
from support.weaver_test import weaver_test

from weaver.build_bundle.executors import (
    InstallationContext,
    ResolvedTarget,
    default_executors,
)
from weaver.build_bundle.models import InstallAction
from weaver.build_bundle.targets import BoundTarget
from weaver.fabric.shortcuts import Shortcut
from weaver.locations import Location
from weaver.spark import FabricSparkTarget
from weaver.targets import ItemRef


def native_case(tmp_path, *, kind="folder", name="Retained", warehouse=False):
    calls = []
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    area = "Files" if kind == "folder" else "Tables"
    source_path = source / area / "Raw" / name
    destination_path = destination / area / "Raw" / name
    if kind == "folder":
        (source_path / "nested").mkdir(parents=True)
        (source_path / "nested" / "data.bin").write_bytes(b"\x00\xffretained")
    source_type = "Warehouse" if warehouse else "Lakehouse"
    shortcut = Shortcut(
        f"{area}/Raw", name, "workspace-id", "source-id", f"{area}/Raw/{name}"
    )

    class Resolver:
        workspace = SimpleNamespace(id="workspace-id", name="Demo")

        def external_item(self, target, *, item_type, workspace):
            assert (target, item_type, workspace) == ("Source", source_type, "Demo")
            return SimpleNamespace(
                id="source-id", workspace_id="workspace-id", name="Source"
            )

        def external_root(self, item):
            return Location(str(source))

        def lakehouse(self, item):
            return Location(str(destination))

        def onelake_shortcuts(self, item):
            return (shortcut,)

        def remove_onelake_shortcut(self, item, *, path, name):
            assert item.name == "Destination" and (path, name) == (
                f"{area}/Raw",
                shortcut.name,
            )
            calls.append("remove-shortcut")

    class Store:
        def copy_directory(self, src, dst):
            assert calls == ["remove-shortcut"]
            assert src == Location(str(source_path)) and dst == Location(
                str(destination_path)
            )
            calls.append("copy-directory")
            shutil.copytree(src.path, dst.path)

        def exists(self, location):
            return False

        def read(self, *args):
            pytest.fail("Materialisation read file bytes")

        def list(self, *args, **kwargs):
            pytest.fail("Materialisation enumerated files")

        def delete(self, *args, **kwargs):
            pytest.fail("Materialisation used generic deletion")

    target = BoundTarget(
        "destination",
        "warehouse" if warehouse else "lakehouse",
        "dest-id",
        item_name="Destination",
        workspace_id="workspace-id",
    )
    context = InstallationContext(
        resolver=Resolver(),
        store=Store(),
        target=ResolvedTarget(
            target,
            ItemRef("Destination"),
            destination=None if warehouse else FabricSparkTarget("Demo", "Destination"),
        ),
        spark_sql=lambda statement, **options: calls.append(statement),
        sql=SimpleNamespace(execute_script=lambda statement: calls.append(statement)),
    )
    instruction = {
        "identity": f"{source_type}/Sales/{area}/Raw.{name}",
        "physical_type": kind,
        "area": area,
        "schema": "Raw",
        "name": name,
        "source_workspace": "Demo",
        "source_target": "Source",
        "source_schema": "Raw",
        "source_object": name,
    }
    action = InstallAction(
        id="materialise",
        kind="materialise_mirror",
        resource_node_id=instruction["identity"],
        executor="materialise_mirror",
        payload="materialise.materialise-mirror.json",
        payload_sha256="recording",
    )
    return (
        context,
        action,
        json.dumps(instruction).encode(),
        calls,
        source_path,
        destination_path,
    )


@weaver_test()
@pytest.mark.parametrize(
    "name", ["Retained", "Monthly files", "文档 é", "literal%20name"]
)
def test_folder_removes_only_shortcut_then_copies_nested_binary_contents(
    tmp_path, name
):
    context, action, payload, calls, source, destination = native_case(
        tmp_path, name=name
    )
    default_executors()["materialise_mirror"].execute(action, payload, context)
    assert calls == ["remove-shortcut", "copy-directory"]
    assert (destination / "nested/data.bin").read_bytes() == (
        source / "nested/data.bin"
    ).read_bytes()
    (destination / "nested/data.bin").write_bytes(b"development")
    assert (source / "nested/data.bin").read_bytes() == b"\x00\xffretained"


@weaver_test()
def test_delta_table_removes_only_shortcut_and_shallow_clones_physical_source(tmp_path):
    context, action, payload, calls, *_ = native_case(tmp_path, kind="table")
    default_executors()["materialise_mirror"].execute(action, payload, context)
    assert calls == [
        "remove-shortcut",
        "CREATE TABLE `Demo`.`Destination`.`Raw`.`Retained` SHALLOW CLONE `Demo`.`Source`.`Raw`.`Retained`",
    ]


@weaver_test()
@pytest.mark.parametrize("kind", ["folder", "table"])
def test_materialisation_waits_for_removed_shortcut_namespace_before_native_write(
    tmp_path, kind
):
    from weaver.build_bundle.executors.base import Waiting

    context, action, payload, calls, *_ = native_case(tmp_path, kind=kind)
    occupied = iter((True, False))
    context.store.exists = lambda location: next(occupied)
    executor = default_executors()["materialise_mirror"]
    waiting = executor.execute(action, payload, context)
    assert isinstance(waiting, Waiting)
    assert calls == ["remove-shortcut"]
    context.resolver.onelake_shortcuts = lambda item: ()
    result = executor.execute(action, payload, context, state=waiting.state)
    assert result["mechanism"] == (
        "recursive_copy" if kind == "folder" else "shallow_clone"
    )
    assert calls[0] == "remove-shortcut" and len(calls) == 2


@weaver_test()
def test_warehouse_table_replaces_local_wrapper_view_with_source_rows_via_ctas(
    tmp_path,
):
    context, action, payload, calls, *_ = native_case(
        tmp_path, kind="table", warehouse=True
    )
    default_executors()["materialise_mirror"].execute(action, payload, context)
    assert calls == [
        "DROP VIEW [Raw].[Retained];",
        "CREATE TABLE [Raw].[Retained] AS SELECT * FROM [Source].[Raw].[Retained];",
    ]


@weaver_test()
def test_warehouse_own_source_with_implicit_workspace_is_refused_before_drop(tmp_path):
    from dataclasses import replace

    from weaver.errors import InstallError

    context, action, payload, calls, *_ = native_case(
        tmp_path, kind="table", warehouse=True
    )
    context = replace(
        context,
        target=replace(
            context.target,
            bound=replace(context.target.bound, item_id="source-id", workspace_id=None),
        ),
    )
    with pytest.raises(InstallError, match="overlaps"):
        default_executors()["materialise_mirror"].execute(action, payload, context)
    assert calls == []


@weaver_test()
def test_warehouse_ctas_uses_resolved_workspace_when_bound_target_omits_id(tmp_path):
    from dataclasses import replace

    context, action, payload, calls, *_ = native_case(
        tmp_path, kind="table", warehouse=True
    )
    context = replace(
        context,
        target=replace(
            context.target, bound=replace(context.target.bound, workspace_id=None)
        ),
    )
    default_executors()["materialise_mirror"].execute(action, payload, context)
    assert calls == [
        "DROP VIEW [Raw].[Retained];",
        "CREATE TABLE [Raw].[Retained] AS SELECT * FROM [Source].[Raw].[Retained];",
    ]


class NotebookFs:
    """``notebookutils.fs`` as Fabric copies directories.

    ``fastcp`` copies a named directory as a child of its destination, copies
    the contents of ``directory/*``, and skips empty directories. ``cp`` copies
    a directory to an absent destination exactly.
    """

    def __init__(self, tree, *, fast=True):
        self.tree = dict(tree)
        if fast:
            self.fastcp = self._fastcp

    def exists(self, path):
        return path in self.tree

    def ls(self, path):
        prefix = path + "/"
        return [
            SimpleNamespace(name=key[len(prefix) :], path=key, isDir=value is None)
            for key, value in sorted(self.tree.items())
            if key.startswith(prefix) and "/" not in key[len(prefix) :]
        ]

    def mkdirs(self, path):
        while path not in self.tree and "/" in path:
            self.tree[path] = None
            path = path.rsplit("/", 1)[0]
        return True

    def _below(self, root):
        return {
            key[len(root) :]: value
            for key, value in self.tree.items()
            if key.startswith(root + "/")
        }

    def _fastcp(self, source, destination, recurse):
        if source.endswith("/*"):
            source = source[:-2]
        else:
            destination += "/" + source.rsplit("/", 1)[1]
        for relative, value in self._below(source).items():
            if value is not None:
                self.mkdirs((destination + relative).rsplit("/", 1)[0])
                self.tree[destination + relative] = value
        return True

    def cp(self, source, destination, recurse):
        assert destination not in self.tree
        self.mkdirs(destination)
        for relative, value in self._below(source).items():
            self.tree[destination + relative] = value
        return True


SOURCE = (
    "abfss://workspace@onelake.dfs.fabric.microsoft.com/source/Files/Raw/Monthly 文档"
)
DESTINATION = (
    "abfss://workspace@onelake.dfs.fabric.microsoft.com/dev/Files/Raw/Monthly 文档"
)


@weaver_test()
@pytest.mark.parametrize("fast", [False, True])
@pytest.mark.parametrize(
    "contents",
    [
        {"/a.csv": b"a", "/_changes": None, "/_changes/1.json": b"{}", "/empty": None},
        {"/empty": None, "/empty/deeper": None},
        {},
    ],
    ids=["files-and-empty-directories", "only-empty-directories", "empty"],
)
def test_fabric_store_copies_directory_contents_exactly(fast, contents):
    from weaver.fabric.store import FabricStore

    fs = NotebookFs(
        {SOURCE: None, **{SOURCE + key: value for key, value in contents.items()}},
        fast=fast,
    )
    FabricStore(fs).copy_directory(Location(SOURCE), Location(DESTINATION))
    assert DESTINATION in fs.tree and fs.tree[DESTINATION] is None
    assert fs._below(DESTINATION) == fs._below(SOURCE) == contents


@weaver_test()
def test_native_directory_copy_false_result_is_a_failure():
    from weaver.fabric.store import FabricStore
    from weaver.store import StoreError

    fs = NotebookFs({SOURCE: None, SOURCE + "/a.csv": b"a"})
    fs.fastcp = lambda *args: False
    with pytest.raises(StoreError, match="copy"):
        FabricStore(fs).copy_directory(Location(SOURCE), Location(DESTINATION))


@weaver_test()
def test_archive_store_routes_directory_copy_to_native_transport():
    from weaver.sessions.archive_runtime import ArchiveStore

    calls = []
    native = SimpleNamespace(copy_directory=lambda *args: calls.append(args))
    source, destination = (
        Location("abfss://workspace@host/source/Files/A"),
        Location("abfss://workspace@host/dev/Files/A"),
    )
    ArchiveStore(native=native, dfs=SimpleNamespace()).copy_directory(
        source, destination
    )
    assert calls == [(source, destination)]


@weaver_test()
def test_failed_native_copy_leaves_source_unchanged_and_propagates_failure(tmp_path):
    from weaver.errors import InstallError

    context, action, payload, calls, source, destination = native_case(tmp_path)

    def fail(*args):
        raise InstallError("native copy failed")

    context.store.copy_directory = fail
    with pytest.raises(InstallError, match="native copy failed"):
        default_executors()["materialise_mirror"].execute(action, payload, context)
    assert calls == ["remove-shortcut"]
    assert not destination.exists()
    assert (source / "nested/data.bin").read_bytes() == b"\x00\xffretained"


@weaver_test()
def test_changed_destination_pointer_is_refused_without_source_or_destination_mutation(
    tmp_path,
):
    from weaver.errors import InstallError

    context, action, payload, calls, *_ = native_case(tmp_path)
    context.resolver.onelake_shortcuts = lambda item: (
        Shortcut(
            "Files/Raw",
            "Retained",
            "workspace-id",
            "different-source",
            "Files/Raw/Retained",
        ),
    )
    with pytest.raises(InstallError, match="recorded source"):
        default_executors()["materialise_mirror"].execute(action, payload, context)
    assert calls == []
