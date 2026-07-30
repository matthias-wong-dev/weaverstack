"""Pure proofs for the Fabric-session-local build workflow."""

from __future__ import annotations

import zipfile
from datetime import datetime, timezone
from pathlib import Path

import pytest

from weaver import ItemRef, LocalStore, Location
from weaver.build_bundle import (
    InstallationEnvironment,
    ItemBinding,
    ItemBindings,
    LakehouseBinding,
    build_uploaded_item_repository,
    generate_item_build_bundle,
    install_bundle_archive,
    materialise_bundle_archive,
    persist_bundle_archive,
    timestamped_archive_name,
)
from weaver.errors import BuildError
from weaver.declaration import parse_item_repository
from weaver.declaration.model import WeaverItemId
from weaver.build_bundle.prune import TargetInventory
from weaver.catalogue.state import ReconciledCatalogue

from test_item_repository import _estate


class CountingStore:
    """A remote-shaped Store with no native-copy shortcut."""

    def __init__(self):
        self.delegate = LocalStore()
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
        )
    }


def _bindings():
    return ItemBindings(
        (
            ItemBinding(
                WeaverItemId.parse("Lakehouse/Raw"),
                LakehouseBinding(ItemRef("Raw_Dev")),
            ),
        )
    )


def _control():
    return LakehouseBinding(ItemRef("Weaver_Control"))


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
    monkeypatch.setattr(
        "weaver.build_bundle.workflow.read_target_inventories",
        lambda bindings, **_kwargs: _inventories(bindings),
    )
    monkeypatch.setattr(
        "weaver.build_bundle.workflow.read_reconciled_catalogue",
        lambda *_args, **_kwargs: ReconciledCatalogue({}),
    )


def test_direct_build_reads_each_remote_repository_file_once_and_no_bundle_file(
    tmp_path, prepared_state
):
    root = Location(str(_estate(tmp_path)))
    remote = CountingStore()
    expected_files = {
        entry.location.value
        for entry in LocalStore().list(root, recursive=True)
        if not entry.is_directory
    }
    environment = InstallationEnvironment(
        store=remote,
        resolver=None,
        executors=_executors(),
    )

    result = build_uploaded_item_repository(
        root,
        bindings=_bindings(),
        environment=environment,
        control_lakehouse=_control(),
    )

    assert result.report.status == "succeeded"
    assert set(remote.reads) == expected_files
    assert set(remote.reads.values()) == {1}
    assert result.archive is None
    assert not any(path.endswith("plan.yml") for path in remote.reads)


def test_direct_build_can_upload_one_archive_after_install_without_rereading_source(
    tmp_path, prepared_state
):
    root = Location(str(_estate(tmp_path)))
    remote = CountingStore()
    archive = Location(str(tmp_path / "records" / "record.weaver.zip"))
    expected_files = {
        entry.location.value
        for entry in LocalStore().list(root, recursive=True)
        if not entry.is_directory
    }
    result = build_uploaded_item_repository(
        root,
        bindings=_bindings(),
        environment=InstallationEnvironment(
            store=remote,
            resolver=None,
            executors=_executors(),
        ),
        control_lakehouse=_control(),
        archive=archive,
    )

    assert result.report.status == "succeeded"
    assert result.archive == archive
    assert set(remote.reads) == expected_files
    assert set(remote.reads.values()) == {1}
    assert remote.writes == [archive.value]


def test_bundle_archive_round_trip_preserves_identity_payloads_and_snapshot(tmp_path):
    root = Location(str(_estate(tmp_path)))
    store = LocalStore()
    repository = parse_item_repository(root, store=store)
    bundle = generate_item_build_bundle(
        repository,
        bindings=_bindings(),
        output=Location(str(tmp_path / "bundle")),
        store=store,
        target_inventories=_inventories(),
        reconciled_catalogue=ReconciledCatalogue({}),
        control_lakehouse=_control(),
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
        assert any(name.startswith("repository/") for name in names)


def test_archive_installer_reads_payloads_locally_not_from_target_store(tmp_path):
    root = Location(str(_estate(tmp_path)))
    store = LocalStore()
    repository = parse_item_repository(root, store=store)
    bundle = generate_item_build_bundle(
        repository,
        bindings=_bindings(),
        output=Location(str(tmp_path / "bundle")),
        store=store,
        target_inventories=_inventories(),
        reconciled_catalogue=ReconciledCatalogue({}),
        control_lakehouse=_control(),
    )
    archive = Location(str(tmp_path / "handover.weaver.zip"))
    persist_bundle_archive(bundle, archive, store=store)
    target = CountingStore()

    report = install_bundle_archive(
        archive,
        archive_store=store,
        environment=InstallationEnvironment(
            store=target,
            resolver=None,
            executors=_executors(),
        ),
    )

    assert report.status == "succeeded"
    assert target.reads == {}


def test_archive_rejects_traversal_before_extracting(tmp_path):
    archive = tmp_path / "bad.weaver.zip"
    with zipfile.ZipFile(archive, "w") as zipped:
        zipped.writestr("../outside.txt", b"no")

    with pytest.raises(BuildError, match="unsafe path"):
        with materialise_bundle_archive(Location(str(archive)), store=LocalStore()):
            pass
    assert not (tmp_path / "outside.txt").exists()


def test_timestamped_archive_name_is_utc_and_has_the_weaver_suffix():
    at = datetime(2026, 7, 27, 1, 2, 3, 4, tzinfo=timezone.utc)
    assert timestamped_archive_name(at) == "20260727T010203000004Z.weaver.zip"
