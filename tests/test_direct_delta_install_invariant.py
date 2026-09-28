"""Direct Delta creation validates a private commit before publication."""

from __future__ import annotations

import json
import tomllib
from pathlib import Path
from types import SimpleNamespace

import pytest
from support.weaver_test import weaver_test

from weaver.fabric.onelake import OneLakeDfsClient, onelake_url
from weaver.locations import Location
from weaver.sessions.direct_delta import (
    create_bound_delta_table,
    create_staged_delta_table,
    direct_profile_supported,
    table_identity,
)
from weaver.spark import FabricSparkTarget
from weaver.store import FilesystemStore


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
        uri=str(stage),
        stage=Location(str(stage)),
        destination=Location(str(destination)),
        store=FilesystemStore(),
        columns=(("Id", "bigint", True), ("Value", "string", False)),
        identity_column="Id",
        storage_options=None,
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
            uri=str(stage),
            stage=Location(str(stage)),
            destination=Location(str(destination)),
            store=FilesystemStore(),
            columns=(("Payload", "variant", False),),
            identity_column=None,
            storage_options=None,
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
            uri=str(stage),
            stage=stage_location,
            destination=Location(str(destination)),
            store=AlteringStore(),
            columns=(("Id", "bigint", True),),
            identity_column=None,
            storage_options=None,
            publish=lambda *_: pytest.fail("published drifted Table"),
        )
    assert stage.is_dir() and not destination.exists()


@weaver_test()
def test_onelake_publishes_a_directory_once_without_overwriting(monkeypatch):
    store = OneLakeDfsClient.__new__(OneLakeDfsClient)
    store.base_url = "https://onelake.dfs.fabric.microsoft.com"
    stage = Location(
        onelake_url(
            "workspace", "5c1893a3-a288-46a5-8cc2-a55c4c89b4b7", "Files/private"
        )
    )
    destination = Location(
        onelake_url(
            "workspace", "5c1893a3-a288-46a5-8cc2-a55c4c89b4b7", "Tables/dbo/Customer"
        )
    )
    requests = []

    def request(method, url, *, headers, expected):
        requests.append((method, url, headers, expected))
        return type("Response", (), {"headers": {}, "status_code": 201})()

    monkeypatch.setattr(store, "_request", request)
    store.rename_directory(stage, destination)
    assert len(requests) == 1
    method, url, headers, expected = requests[0]
    assert method == "PUT" and url == destination.value and expected == (201,)
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
def test_direct_table_uses_resolved_item_and_private_stage(monkeypatch):
    target = FabricSparkTarget("Demo", "Sales LH")
    item_id = "5c1893a3-a288-46a5-8cc2-a55c4c89b4b7"
    resolver = SimpleNamespace(
        configuration=SimpleNamespace(workspace="Demo"),
        workspace=SimpleNamespace(id="workspace"),
        resolve=lambda item, *, item_type: (
            SimpleNamespace(id=item_id, name=item.name)
            if item.name == "Sales LH" and item_type == "Lakehouse"
            else pytest.fail("resolved a different item")
        ),
    )
    calls = []

    def writer(**kwargs):
        calls.append(kwargs)
        return "allocation"

    monkeypatch.setattr(
        "weaver.sessions.direct_delta.create_staged_delta_table", writer
    )
    store = SimpleNamespace(token="not-secret", rename_directory=lambda *_: None)
    assert (
        create_bound_delta_table(
            qualified_name=target.qualify("DWG", "Customer Order"),
            columns=(("Id", "bigint", True),),
            identity_column=None,
            resolver=resolver,
            store=store,
        )
        == "allocation"
    )
    assert len(calls) == 1
    call = calls[0]
    assert call["destination"] == Location(
        onelake_url("workspace", item_id, "Tables/DWG/Customer Order")
    )
    assert call["stage"].value.startswith(
        onelake_url("workspace", item_id, "Files/weaver-stage-")
    )
    assert call["uri"].startswith(
        f"abfss://workspace@onelake.dfs.fabric.microsoft.com/{item_id}/Files/weaver-stage-"
    )
    assert call["storage_options"] == {
        "bearer_token": "not-secret",
        "use_fabric_endpoint": "true",
    }
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
