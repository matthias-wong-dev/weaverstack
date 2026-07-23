"""Level-four host records, constructible without any configuration file."""

from __future__ import annotations

from pathlib import Path

import pytest

from weaver import FabricHost, LocalHost, WarehouseSettings
from weaver.errors import ConfigError, IdentityError


def test_a_fabric_host_needs_only_a_workspace():
    host = FabricHost(workspace="Analytics")
    assert host.workspace == "Analytics"
    assert host.weaver_lakehouse is None
    assert host.alias is None


def test_host_sub_parameters_are_item_names():
    host = FabricHost(
        workspace="Analytics",
        weaver_lakehouse="Weaver",
        fabric_environment="Weaver_Env",
    )
    assert host.weaver_lakehouse == "Weaver"
    assert host.fabric_environment == "Weaver_Env"


def test_warehouse_settings_attach_by_real_name():
    host = FabricHost(
        workspace="Analytics",
        warehouse_config={"Reporting": WarehouseSettings(degrees_of_parallelism=8)},
    )
    assert host.settings_for_warehouse("Reporting").degrees_of_parallelism == 8


def test_a_warehouse_without_settings_is_normal():
    host = FabricHost(workspace="Analytics")
    assert host.settings_for_warehouse("Reporting").degrees_of_parallelism is None


def test_warehouse_config_is_not_mutable_through_the_host():
    host = FabricHost(
        workspace="Analytics",
        warehouse_config={"Reporting": WarehouseSettings()},
    )
    with pytest.raises(TypeError):
        host.warehouse_config["Inventory"] = WarehouseSettings()


def test_degrees_of_parallelism_must_be_positive():
    with pytest.raises(ConfigError):
        WarehouseSettings(degrees_of_parallelism=0)
    with pytest.raises(ConfigError):
        WarehouseSettings(degrees_of_parallelism=True)


def test_a_bad_workspace_name_is_rejected():
    with pytest.raises(IdentityError):
        FabricHost(workspace="  ")


def test_a_local_host_is_a_root_directory():
    host = LocalHost(root=".local")
    assert host.root == Path(".local")


def test_only_fabric_supports_sql():
    assert FabricHost(workspace="Analytics").supports_sql is True
    assert LocalHost(root=".local").supports_sql is False


def test_configurable_keys_are_derived_from_the_record():
    assert FabricHost.configurable_keys() == {
        "type",
        "workspace",
        "weaver_lakehouse",
        "fabric_environment",
        "warehouse_config",
    }
    assert LocalHost.configurable_keys() == {"type", "root", "weaver_lakehouse"}
