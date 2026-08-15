"""Pure proofs for the Fabric-session-local build workflow."""

from __future__ import annotations

import json
import zipfile
from datetime import datetime, timezone

import pytest
from support.sessions import given_session
from support.workspaces import WORKSPACE, given_resolver, given_workspace
from test_item_repository import _estate

from weaver.build_bundle import (
    BuildState,
    ItemBinding,
    ItemBindings,
    LakehouseBinding,
    build_item_repository_source,
    generate_item_build_bundle,
    install_bundle_archive,
    materialise_bundle_archive,
    persist_bundle_archive,
    timestamped_archive_name,
)
from weaver.build_bundle.prune import TargetInventory, read_lakehouse_inventory
from weaver.catalogue.state import Catalogue
from weaver.declaration import parse_item_repository
from weaver.declaration.model import WeaverItemId
from weaver.errors import BuildError
from weaver.locations import Location
from weaver.store import FilesystemStore
from weaver.targets import ItemRef


class CountingStore:
    """A remote-shaped Store with no native-copy shortcut."""

    def __init__(self):
        self.delegate = FilesystemStore()
        self.reads: dict[str, int] = {}
        self.writes: list[str] = []

    def exists(self, location):
        return self.delegate.exists(location)

    def is_directory(self, location):
        return self.delegate.is_directory(location)

    def list(self, location, *, recursive=False):
        return self.delegate.list(location, recursive=recursive)

    def read(self, location):
        self.reads[location.value] = self.reads.get(location.value, 0) + 1
        return self.delegate.read(location)

    def write(self, location, data):
        self.writes.append(location.value)
        self.delegate.write(location, data)

    def delete(self, location, *, recursive=False):
        self.delegate.delete(location, recursive=recursive)

    def make_directory(self, location):
        self.delegate.make_directory(location)


class NoopExecutor:
    def __init__(self, name):
        self.name = name

    def execute(self, action, payload, context):
        return {"payload_bytes": len(payload or b"")}


def _executors():
    return {
        name: NoopExecutor(name)
        for name in (
            "spark_sql",
            "spark_sql_batch",
            "spark_schema",
            "spark_table",
            "tsql",
            "folder",
            "alias",
            "tsql_batch",
            "sql_endpoint_refresh",
            "load_file",
        )
    }


def _bindings():
    return ItemBindings(
        (
            ItemBinding(
                WeaverItemId.parse("Lakehouse/Raw"),
                LakehouseBinding(ItemRef("Raw_Dev"), workspace_name=WORKSPACE),
            ),
        )
    )


def _control():
    return LakehouseBinding(ItemRef("Weaver_Control"), workspace_name=WORKSPACE)


def _inventories(bindings=None):
    bindings = bindings or _bindings()
    return {
        binding.item: TargetInventory(
            target_id=binding.to_bound_target().id,
            kind=binding.to_bound_target().kind,
            target_name=binding.to_bound_target().name,
        )
        for binding in bindings.entries
    }


@pytest.fixture
def prepared_state(monkeypatch):
    """The one boundary read, stubbed — these tests are about stores, not state.

    One patch rather than two, because the build now takes a single Python
    handover: catalogue and inventories arrive together as a BuildState, and the
    Builder reconciles them itself.
    """

    monkeypatch.setattr(
        "weaver.build_bundle.workflow.read_build_state",
        lambda bindings, **_kwargs: BuildState(
            catalogue=Catalogue({}), target_inventories=_inventories(bindings)
        ),
    )


def test_direct_build_reads_each_remote_repository_file_once_and_no_bundle_file(
    tmp_path, prepared_state
):
    root = Location(str(_estate(tmp_path)))
    remote = CountingStore()
    expected_files = {
        entry.location.value
        for entry in FilesystemStore().list(root, recursive=True)
        if not entry.is_directory
    }
    result = build_item_repository_source(
        root,
        source_store=remote,
        bindings=_bindings(),
        session=given_session(
            store=remote,
            lakehouses=("Weaver", "Weaver_Control", "Raw_Dev", "Sales_LH"),
        ),
        catalogue_binding=_control(),
        executors=_executors(),
    )

    assert result.report.status == "succeeded"
    assert set(remote.reads) == expected_files
    assert set(remote.reads.values()) == {1}
    assert result.archive is None
    assert not any(path.endswith("plan.yml") for path in remote.reads)


def test_explicit_local_source_does_not_use_the_target_store_for_repository_reads(
    tmp_path, prepared_state
):
    root = Location(str(_estate(tmp_path)))
    target_store = CountingStore()

    result = build_item_repository_source(
        root,
        source_store=FilesystemStore(),
        bindings=_bindings(),
        session=given_session(
            store=target_store,
            lakehouses=("Weaver", "Weaver_Control", "Raw_Dev", "Sales_LH"),
        ),
        catalogue_binding=_control(),
        executors=_executors(),
    )

    assert result.report.status == "succeeded"
    assert target_store.reads == {}


def test_invalid_request_fails_before_target_state_is_read(tmp_path, monkeypatch):
    parse_item_repository(Location(str(_estate(tmp_path))), store=FilesystemStore())
    unknown = ItemBindings(
        (
            ItemBinding(
                WeaverItemId.parse("Lakehouse/Missing"),
                LakehouseBinding(ItemRef("Missing_Dev"), workspace_name=WORKSPACE),
            ),
        )
    )
    monkeypatch.setattr(
        "weaver.build_bundle.workflow.read_target_inventories",
        lambda *_args, **_kwargs: pytest.fail("target state was contacted"),
    )

    with pytest.raises(BuildError, match="absent from the repository"):
        build_item_repository_source(
            Location(str(_estate(tmp_path))),
            source_store=FilesystemStore(),
            bindings=unknown,
            session=given_session(
                store=FilesystemStore(),
                lakehouses=("Weaver", "Weaver_Control", "Raw_Dev", "Sales_LH"),
            ),
            catalogue_binding=_control(),
            executors=_executors(),
        )


def test_build_state_json_round_trip_preserves_epochs_and_inventory():
    item = WeaverItemId.parse("Lakehouse/Raw")
    build_datetime = datetime(2026, 7, 27, 1, 2, 3, tzinfo=timezone.utc)
    state = BuildState(
        catalogue=Catalogue(
            {
                item: {
                    "Registry": (
                        {
                            "item_type": "Lakehouse",
                            "item_name": "Raw",
                            "schema_name": "Sales",
                            "object_name": "Customer",
                            "object_type": "table",
                            "object_role": "data",
                            "signature": "abc123",
                            "build_datetime": build_datetime,
                        },
                    )
                }
            },
            present_tables=frozenset({"Registry"}),
        ),
        target_inventories=_inventories(),
    )

    encoded = json.loads(json.dumps(state.to_mapping()))
    restored = BuildState.from_mapping(encoded)

    assert restored.to_mapping() == state.to_mapping()
    assert (
        restored.catalogue.registered[
            next(iter(restored.catalogue.registered))
        ].build_datetime
        == build_datetime
    )


def test_cli_area_is_reserved_from_inventory_but_weaver_items_is_not(tmp_path):

    workspace = given_workspace(catalogue="Warehouse/Control")
    resolver = given_resolver(
        workspace=workspace,
        lakehouses=("Weaver", "Raw_Dev", "Sales_LH", "Curated_Dev"),
        root=tmp_path,
    )
    store = FilesystemStore()
    target = _bindings().entries[0].to_bound_target()
    files = resolver.files_root(ItemRef("Raw_Dev"))
    tables = resolver.tables_root(ItemRef("Raw_Dev"))
    locations = (
        files,
        tables,
        files / "cli",
        files / "build_bundles",
        files / "weaver_items",
    )
    for location in locations:
        store.make_directory(location)

    inventory = read_lakehouse_inventory(target, resolver=resolver, store=store)

    assert "cli" not in inventory.folder_schemas
    assert "build_bundles" not in inventory.folder_schemas
    assert "weaver_items" in inventory.folder_schemas


def test_direct_build_can_upload_one_archive_after_install_without_rereading_source(
    tmp_path, prepared_state
):
    root = Location(str(_estate(tmp_path)))
    remote = CountingStore()
    archive = Location(str(tmp_path / "records" / "record.weaver.zip"))
    expected_files = {
        entry.location.value
        for entry in FilesystemStore().list(root, recursive=True)
        if not entry.is_directory
    }
    result = build_item_repository_source(
        root,
        source_store=remote,
        bindings=_bindings(),
        session=given_session(
            store=remote,
            lakehouses=("Weaver", "Weaver_Control", "Raw_Dev", "Sales_LH"),
        ),
        catalogue_binding=_control(),
        archive=archive,
        executors=_executors(),
    )

    assert result.report.status == "succeeded"
    assert result.archive == archive
    assert set(remote.reads) == expected_files
    assert set(remote.reads.values()) == {1}
    assert remote.writes == [archive.value]


def test_bundle_archive_round_trip_preserves_identity_and_payloads(tmp_path):
    root = Location(str(_estate(tmp_path)))
    store = FilesystemStore()
    repository = parse_item_repository(root, store=store)
    bundle = generate_item_build_bundle(
        repository,
        bindings=_bindings(),
        output=Location(str(tmp_path / "bundle")),
        store=store,
        target_inventories=_inventories(),
        catalogue=Catalogue({}),
        catalogue_binding=_control(),
    )
    archive = Location(str(tmp_path / "20260727T010203000004Z.weaver.zip"))

    persist_bundle_archive(bundle, archive, store=store)

    assert archive.path.is_file()
    with materialise_bundle_archive(archive, store=store) as reloaded:
        assert reloaded.plan == bundle.plan
        assert reloaded.bundle_id == bundle.bundle_id
        names = {
            entry.location.value.removeprefix(reloaded.location.value + "/")
            for entry in reloaded.store.list(reloaded.location, recursive=True)
            if not entry.is_directory
        }
        assert "plan.yml" in names
        assert any(name.startswith("payload/") for name in names)
        assert not any(name.startswith("repository/") for name in names)


def test_archive_installer_reads_payloads_locally_not_from_target_store(tmp_path):
    root = Location(str(_estate(tmp_path)))
    store = FilesystemStore()
    repository = parse_item_repository(root, store=store)
    bundle = generate_item_build_bundle(
        repository,
        bindings=_bindings(),
        output=Location(str(tmp_path / "bundle")),
        store=store,
        target_inventories=_inventories(),
        catalogue=Catalogue({}),
        catalogue_binding=_control(),
    )
    archive = Location(str(tmp_path / "handover.weaver.zip"))
    persist_bundle_archive(bundle, archive, store=store)
    target = CountingStore()

    report = install_bundle_archive(
        archive,
        archive_store=store,
        session=given_session(
            store=target,
            lakehouses=("Weaver", "Weaver_Control", "Raw_Dev", "Sales_LH"),
        ),
        executors=_executors(),
    )

    assert report.status == "succeeded"
    assert target.reads == {}


def test_archive_rejects_traversal_before_extracting(tmp_path):
    archive = tmp_path / "bad.weaver.zip"
    with zipfile.ZipFile(archive, "w") as zipped:
        zipped.writestr("../outside.txt", b"no")

    with pytest.raises(BuildError, match="unsafe path"):
        with materialise_bundle_archive(
            Location(str(archive)), store=FilesystemStore()
        ):
            pass
    assert not (tmp_path / "outside.txt").exists()


def test_timestamped_archive_name_is_utc_and_has_the_weaver_suffix():
    at = datetime(2026, 7, 27, 1, 2, 3, 4, tzinfo=timezone.utc)
    assert timestamped_archive_name(at) == "20260727T010203000004Z.weaver.zip"
