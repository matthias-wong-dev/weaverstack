"""Materialisation copies and verifies source bytes before pointer cutover."""

import json
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
from weaver.errors import InstallError
from weaver.fabric.shortcuts import Shortcut
from weaver.locations import Location
from weaver.store import FilesystemStore, StoreOutcomeUnknown
from weaver.targets import ItemRef


def copy_case(tmp_path, *, kind="folder"):
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    area = "Files" if kind == "folder" else "Tables"
    original = source / area / "Raw/Retained"
    if kind == "folder":
        (original / "nested").mkdir(parents=True)
        (original / "nested/data.bin").write_bytes(b"\x00\xffretained\x80")
        (original / "empty").mkdir()
    else:
        from deltalake import write_deltalake

        write_deltalake(original, delta_rows([1, 2], ["one", "two"]))
    pointer = destination / area / "Raw/Retained"
    pointer.mkdir(parents=True)
    calls = []
    shortcut = Shortcut(
        f"{area}/Raw",
        "Retained",
        "source-workspace",
        "source-item",
        f"{area}/Raw/Retained",
    )

    class Resolver:
        borrowed = True

        def files_root(self, item):
            return Location(str(destination / "Files"))

        def lakehouse(self, item):
            return Location(str(destination))

        def external_item(self, name, *, item_type, workspace):
            assert (name, item_type, workspace) == (
                "Source",
                "Lakehouse",
                "SourceWorkspace",
            )
            return SimpleNamespace(id="source-item", workspace_id="source-workspace")

        def external_root(self, item):
            return Location(str(source))

        def onelake_shortcuts(self, item):
            return (shortcut,) if self.borrowed else ()

        def remove_onelake_shortcut(self, item, *, path, name):
            stages = [
                p
                for p in (destination / "Files/_weaver_materialisation").iterdir()
                if p.is_dir()
            ]
            assert len(stages) == 1
            before = {
                p.relative_to(original).as_posix(): p.read_bytes()
                for p in original.rglob("*")
                if p.is_file()
            }
            copied = {
                p.relative_to(stages[0]).as_posix(): p.read_bytes()
                for p in stages[0].rglob("*")
                if p.is_file()
            }
            assert copied == before
            calls.append("remove")
            pointer.rmdir()
            self.borrowed = False

    target = BoundTarget(
        "destination",
        "lakehouse",
        "destination-item",
        item_name="Destination",
        workspace_id="destination-workspace",
    )
    context = InstallationContext(
        resolver=Resolver(),
        store=FilesystemStore(),
        target=ResolvedTarget(target, ItemRef("Destination")),
    )
    instruction = {
        "identity": f"Lakehouse/Sales/{area}/Raw.Retained",
        "physical_type": kind,
        "area": area,
        "schema": "Raw",
        "name": "Retained",
        "source_workspace": "SourceWorkspace",
        "source_target": "Source",
        "source_schema": "Raw",
        "source_object": "Retained",
    }
    action = InstallAction(
        id="materialise-retained",
        kind="materialise_mirror",
        resource_node_id=instruction["identity"],
        executor="materialise_mirror",
        payload="instruction.materialise-mirror.json",
        payload_sha256="unused",
    )
    return context, action, json.dumps(instruction).encode(), original, pointer, calls


def execute(case):
    context, action, payload, *_ = case
    return default_executors()["materialise_mirror"].execute(action, payload, context)


def delta_rows(ids, values):
    from arro3.core import Array, DataType, Table

    return Table.from_pydict(
        {"Id": Array(ids, DataType.int64()), "Value": Array(values, DataType.string())}
    )


def delta_read(table):
    from deltalake import QueryBuilder

    rows = (
        QueryBuilder()
        .register("retained", table)
        .execute("select * from retained order by Id")
        .read_all()
    )
    return {name: rows.column(name).to_pylist() for name in rows.column_names}


@weaver_test()
def test_protected_folder_bytes_are_copied_before_shortcut_removal(tmp_path):
    case = copy_case(tmp_path)
    _, _, _, original, pointer, calls = case
    result = execute(case)
    assert calls == ["remove"]
    assert (pointer / "nested/data.bin").read_bytes() == (
        original / "nested/data.bin"
    ).read_bytes()
    assert (pointer / "empty").is_dir()
    (pointer / "nested/data.bin").write_bytes(b"owned edit")
    assert (original / "nested/data.bin").read_bytes() == b"\x00\xffretained\x80"
    assert result["verified"] is True


@weaver_test()
def test_protected_delta_log_schema_protocol_history_and_rows_survive(tmp_path):
    from deltalake import DeltaTable, write_deltalake

    case = copy_case(tmp_path, kind="table")
    _, _, _, original, pointer, calls = case
    before = DeltaTable(original)
    history, protocol, schema = before.history(), before.protocol(), before.schema()
    assert execute(case)["verified"] is True
    owned = DeltaTable(pointer)
    assert delta_read(owned) == delta_read(before)
    assert (
        owned.history() == history
        and owned.protocol() == protocol
        and owned.schema() == schema
    )
    write_deltalake(pointer, delta_rows([3], ["owned"]), mode="append")
    assert DeltaTable(original).version() == before.version()
    assert delta_read(DeltaTable(original)) == {"Id": [1, 2], "Value": ["one", "two"]}
    assert calls == ["remove"]


@weaver_test()
def test_failed_copy_keeps_the_source_and_pointer(tmp_path, monkeypatch):
    case = copy_case(tmp_path)
    context, _, _, original, pointer, calls = case

    def fail(*args):
        raise InstallError("copy failed")

    monkeypatch.setattr(context.store, "copy", fail)
    with pytest.raises(InstallError, match="copy failed"):
        execute(case)
    assert context.resolver.borrowed and pointer.is_dir() and calls == []
    assert (original / "nested/data.bin").read_bytes() == b"\x00\xffretained\x80"
    assert list((pointer.parents[1] / "_weaver_materialisation").iterdir()) == []
    monkeypatch.undo()
    assert execute(case)["verified"] is True


@weaver_test()
def test_lost_publication_response_remains_uncertain(tmp_path, monkeypatch):
    case = copy_case(tmp_path)
    context, *_ = case
    publish = context.store.publish_directory

    def lost(source, destination):
        publish(source, destination)
        raise StoreOutcomeUnknown("publication reply lost")

    monkeypatch.setattr(context.store, "publish_directory", lost)
    with pytest.raises(StoreOutcomeUnknown, match="reply lost"):
        execute(case)


@weaver_test()
def test_missing_shortcut_does_not_certify_unverified_local_contents(tmp_path):
    case = copy_case(tmp_path)
    context, *_ = case
    context.resolver.borrowed = False
    with pytest.raises(InstallError, match="verified copy"):
        execute(case)


@weaver_test()
def test_a_new_invocation_verifies_a_cutover_whose_reply_was_lost(
    tmp_path, monkeypatch
):
    case = copy_case(tmp_path)
    context, _, _, _, pointer, calls = case
    publish = context.store.publish_directory

    def lost(source, destination):
        publish(source, destination)
        raise StoreOutcomeUnknown("publication reply lost")

    monkeypatch.setattr(context.store, "publish_directory", lost)
    with pytest.raises(StoreOutcomeUnknown):
        execute(case)
    monkeypatch.undo()
    assert execute(case)["already_owned"] is True
    assert calls == ["remove"] and (pointer / "nested/data.bin").is_file()


@weaver_test()
def test_private_copy_proofs_are_not_managed_folder_inventory(tmp_path):
    from weaver.build_bundle.prune import read_lakehouse_inventory

    private = tmp_path / "Files/_weaver_materialisation/staged"
    private.mkdir(parents=True)
    (private / "data.bin").write_bytes(b"retained")
    target = BoundTarget("destination", "lakehouse", "destination-item")
    resolver = SimpleNamespace(
        files_root=lambda item: Location(str(tmp_path / "Files")),
        tables_root=lambda item: Location(str(tmp_path / "Tables")),
    )
    found = read_lakehouse_inventory(target, resolver=resolver, store=FilesystemStore())
    assert found.folder_schemas == () and found.folders == ()


@weaver_test()
def test_delta_external_file_references_are_refused_before_cutover(tmp_path):
    from deltalake import DeltaTable

    case = copy_case(tmp_path, kind="table")
    context, _, _, original, _, calls = case
    log = next((original / "_delta_log").glob("*.json"))
    entries = [json.loads(line) for line in log.read_text().splitlines()]
    for entry in entries:
        if "add" in entry:
            entry["add"]["path"] = (original / entry["add"]["path"]).as_uri()
    log.write_bytes(("\n".join(json.dumps(entry) for entry in entries) + "\n").encode())
    assert delta_read(DeltaTable(original)) == {"Id": [1, 2], "Value": ["one", "two"]}
    with pytest.raises(InstallError, match="external"):
        execute(case)
    assert context.resolver.borrowed and calls == []


@weaver_test()
def test_missing_active_delta_data_file_is_refused_before_cutover(tmp_path):
    from deltalake import DeltaTable

    case = copy_case(tmp_path, kind="table")
    context, _, _, original, _, calls = case
    path = DeltaTable(original).get_add_actions().column("path").to_pylist()[0]
    (original / path).unlink()
    with pytest.raises(InstallError, match="missing"):
        execute(case)
    assert context.resolver.borrowed and calls == []
