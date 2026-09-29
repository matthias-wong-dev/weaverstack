"""Direct Delta creation validates a private commit before publication."""

from __future__ import annotations

import json
import threading
import tomllib
from pathlib import Path
from types import SimpleNamespace

import pytest
from support.weaver_test import weaver_test

from weaver.fabric.onelake import OneLakeDfsClient, abfss_root, onelake_url
from weaver.locations import Location
from weaver.sessions.direct_delta import (
    create_bound_delta_table,
    create_staged_delta_table,
    direct_profile_supported,
    run_direct_delta_actions,
    table_identity,
)
from weaver.spark import FabricSparkTarget
from weaver.store import FilesystemStore


@weaver_test()
def test_direct_creation_transfers_local_commits_through_the_store(
    tmp_path, monkeypatch
):
    DeltaTable = pytest.importorskip("deltalake").DeltaTable

    original_create = DeltaTable.create
    writer_paths = []
    writes = []

    @classmethod
    def local_create(cls, table_uri, *args, **kwargs):
        writer_paths.append(str(table_uri))
        assert Path(table_uri).is_absolute()
        assert kwargs.get("storage_options") in (None, {})
        return original_create(table_uri, *args, **kwargs)

    class RecordingStore(FilesystemStore):
        def write(self, location, data):
            writes.append((location.value, data))
            super().write(location, data)

    monkeypatch.setattr(DeltaTable, "create", local_create)
    stage = tmp_path / "Files" / "private"
    destination = tmp_path / "Tables" / "dbo" / "Customer"
    destination.parent.mkdir(parents=True)

    def publish(source, target):
        Path(source.value).rename(target.value)

    create_staged_delta_table(
        stage=Location(str(stage)),
        destination=Location(str(destination)),
        store=RecordingStore(),
        columns=(("Id", "bigint", True), ("Value", "string", False)),
        identity_column="Id",
        publish=publish,
    )

    assert len(writer_paths) == 1
    assert sorted(Path(path).name for path, _ in writes) == [
        "00000000000000000000.json",
        "00000000000000000001.json",
    ]
    assert all(Path(path).parent.name == "_delta_log" for path, _ in writes)
    assert all(
        (destination / "_delta_log" / Path(path).name).read_bytes() == data
        for path, data in writes
    )
    assert not stage.exists()


@weaver_test()
def test_direct_creation_publishes_only_the_verified_profile(tmp_path):
    stage = tmp_path / "Files" / "private"
    destination = tmp_path / "Tables" / "dbo" / "Customer"
    destination.parent.mkdir(parents=True)
    observed = []

    def publish(source, target):
        observed.append((source, target))
        assert stage.is_dir() and not destination.exists()
        stage.rename(destination)

    allocation = create_staged_delta_table(
        stage=Location(str(stage)),
        destination=Location(str(destination)),
        store=FilesystemStore(),
        columns=(("Id", "bigint", True), ("Value", "string", False)),
        identity_column="Id",
        publish=publish,
    )
    assert len(observed) == 1
    assert not stage.exists() and destination.is_dir()
    assert allocation.physical_names.keys() == {"Id", "Value"}
    logs = sorted((destination / "_delta_log").glob("*.json"))
    assert len(logs) == 2
    final = [json.loads(line) for line in logs[-1].read_text().splitlines()]
    assert any(
        "identityColumns" in action.get("protocol", {}).get("writerFeatures", [])
        for action in final
    )


@weaver_test()
def test_direct_creation_refuses_variant_before_writing(tmp_path):
    stage = tmp_path / "Files" / "private"
    destination = tmp_path / "Tables" / "dbo" / "Customer"
    with pytest.raises(ValueError, match="VARIANT.*not supported"):
        create_staged_delta_table(
            stage=Location(str(stage)),
            destination=Location(str(destination)),
            store=FilesystemStore(),
            columns=(("Payload", "variant", False),),
            identity_column=None,
            publish=lambda *_: pytest.fail("published unsupported Table"),
        )
    assert not stage.exists() and not destination.exists()


@weaver_test()
def test_direct_creation_never_publishes_on_profile_drift(tmp_path):
    stage = tmp_path / "Files" / "private"
    destination = tmp_path / "Tables" / "dbo" / "Customer"
    destination.parent.mkdir(parents=True)

    def alter(store, location):
        log = Path(location.value) / "_delta_log" / "00000000000000000000.json"
        rows = [json.loads(line) for line in log.read_text().splitlines()]
        for row in rows:
            if "metaData" in row:
                row["metaData"]["configuration"]["delta.enableDeletionVectors"] = "true"
        log.write_text("\n".join(json.dumps(row) for row in rows) + "\n")

    class AlteringStore(FilesystemStore):
        def read(self, location):
            if location.name.endswith(".json"):
                alter(self, stage_location)
            return super().read(location)

    stage_location = Location(str(stage))
    with pytest.raises(ValueError, match="configuration"):
        create_staged_delta_table(
            stage=stage_location,
            destination=Location(str(destination)),
            store=AlteringStore(),
            columns=(("Id", "bigint", True),),
            identity_column=None,
            publish=lambda *_: pytest.fail("published drifted Table"),
        )
    assert stage.is_dir() and not destination.exists()


@weaver_test()
@pytest.mark.parametrize("native_store", [False, True])
def test_onelake_publishes_a_directory_once_without_overwriting(
    monkeypatch, native_store
):
    store = OneLakeDfsClient.__new__(OneLakeDfsClient)
    store.base_url = "https://onelake.dfs.fabric.microsoft.com"
    item_id = "5c1893a3-a288-46a5-8cc2-a55c4c89b4b7"
    native_root = abfss_root("workspace", item_id)
    stage = Location(
        f"{native_root}/Files/private"
        if native_store
        else onelake_url("workspace", item_id, "Files/private")
    )
    destination = Location(
        f"{native_root}/Tables/dbo/Customer"
        if native_store
        else onelake_url("workspace", item_id, "Tables/dbo/Customer")
    )
    expected_url = onelake_url("workspace", item_id, "Tables/dbo/Customer")
    requests = []

    def request(method, url, *, headers, expected):
        requests.append((method, url, headers, expected))
        return type("Response", (), {"headers": {}, "status_code": 201})()

    monkeypatch.setattr(store, "_request", request)
    store.rename_directory(stage, destination)
    assert len(requests) == 1
    method, url, headers, expected = requests[0]
    assert method == "PUT" and url == expected_url and expected == (201,)
    assert headers == {
        "x-ms-rename-source": "/workspace/5c1893a3-a288-46a5-8cc2-a55c4c89b4b7/Files/private",
        "If-None-Match": "*",
    }


@weaver_test()
def test_direct_table_identity_comes_from_the_bound_spark_target():
    target = FabricSparkTarget("Demo", "Sales LH")
    assert table_identity(target, target.qualify("DWG", "Customer Order")) == (
        "DWG",
        "Customer Order",
    )
    with pytest.raises(ValueError, match="bound target"):
        table_identity(
            target,
            FabricSparkTarget("Other", "Sales LH").qualify("DWG", "Customer Order"),
        )


@weaver_test()
@pytest.mark.parametrize("native_store", [False, True])
def test_direct_table_uses_resolved_item_and_private_stage(monkeypatch, native_store):
    target = FabricSparkTarget("Demo", "Sales LH")
    item_id = "5c1893a3-a288-46a5-8cc2-a55c4c89b4b7"
    lakehouse_root = Location(
        abfss_root("workspace", item_id)
        if native_store
        else onelake_url("workspace", item_id)
    )
    resolved = []

    def lakehouse(item):
        resolved.append(item.name)
        return lakehouse_root

    resolver = SimpleNamespace(
        configuration=SimpleNamespace(workspace="Demo"),
        lakehouse=lakehouse,
    )
    calls = []

    def writer(**kwargs):
        calls.append(kwargs)
        return "allocation"

    monkeypatch.setattr(
        "weaver.sessions.direct_delta.create_staged_delta_table", writer
    )
    store = SimpleNamespace(rename_directory=lambda *_: None)
    assert (
        create_bound_delta_table(
            qualified_name=target.qualify("DWG", "Customer Order"),
            columns=(("Id", "bigint", True),),
            identity_column=None,
            resolver=resolver,
            store=store,
            publish=store.rename_directory,
        )
        == "allocation"
    )
    assert len(calls) == 1
    assert resolved == ["Sales LH"]
    call = calls[0]
    assert call["destination"] == Location(
        f"{lakehouse_root.value}/Tables/DWG/Customer Order"
    )
    assert call["stage"].value.startswith(f"{lakehouse_root.value}/Files/weaver-stage-")
    assert "uri" not in call and "storage_options" not in call
    assert call["publish"] == store.rename_directory


@weaver_test()
def test_onelake_address_does_not_double_encode_a_table_name():
    store = OneLakeDfsClient.__new__(OneLakeDfsClient)
    store.base_url = "https://onelake.dfs.fabric.microsoft.com"
    location = Location(
        onelake_url(
            "workspace",
            "5c1893a3-a288-46a5-8cc2-a55c4c89b4b7",
            "Tables/DWG/Customer Order",
        )
    )
    assert store._url(location) == location.value


@weaver_test()
def test_parallel_direct_tables_publish_independent_verified_logs(tmp_path):
    pytest.importorskip("deltalake")
    workers = 4
    barrier = threading.Barrier(workers)

    class ConcurrentStore(FilesystemStore):
        def write(self, location, data):
            if location.name == "00000000000000000000.json":
                barrier.wait(timeout=20)
            return super().write(location, data)

    store = ConcurrentStore()
    (tmp_path / "Tables").mkdir()

    def publish(source, target):
        Path(source.value).rename(target.value)

    def create(qualified, columns, *, identity_column, workspace):
        stage = Location(str(tmp_path / "Files" / f"stage-{qualified}"))
        destination = Location(str(tmp_path / "Tables" / f"table-{qualified}"))
        return create_staged_delta_table(
            stage=stage,
            destination=destination,
            store=store,
            columns=columns,
            identity_column=identity_column,
            publish=publish,
        )

    actions = [
        (str(index), str(index), (("Id", "bigint", True),), None)
        for index in range(workers)
    ]
    outcomes = run_direct_delta_actions(create, actions, max_workers=workers)
    assert [outcome["succeeded"] for outcome in outcomes] == [True] * workers
    assert sorted(path.name for path in (tmp_path / "Tables").iterdir()) == [
        f"table-{index}" for index in range(workers)
    ]


@weaver_test()
def test_direct_profile_does_not_silently_send_variant_to_spark():
    assert direct_profile_supported((("Id", "bigint", True),), None, True)
    assert not direct_profile_supported((("Lines", "array<int>", False),), None, True)
    with pytest.raises(ValueError, match="VARIANT.*not supported"):
        direct_profile_supported((("Payload", "variant", False),), None, True)


@weaver_test()
def test_product_dependency_pins_the_delta_writer():
    configuration = tomllib.loads(
        (Path(__file__).resolve().parents[1] / "pyproject.toml").read_text()
    )
    assert "deltalake==1.6.6" in configuration["project"]["dependencies"]
