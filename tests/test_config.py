"""The hosts file is a named lookup, expressing nothing the constructors can't."""

from __future__ import annotations

from pathlib import Path

import pytest

from weaver import FabricHost, LocalHost, load_hosts, parse_hosts
from weaver.errors import ConfigError

PAYLOAD = {
    "hosts": {
        "MyFabric": {
            "type": "Fabric",
            "workspace": "Analytics",
            "weaver_lakehouse": "Weaver",
            "fabric_environment": "Weaver_Env",
            "warehouse_config": {"Reporting": {"degrees_of_parallelism": 8}},
        },
        "MyLocal": {
            "type": "Local",
            "root": ".local",
            "weaver_lakehouse": "Weaver",
        },
    }
}


def test_parses_both_host_types():
    hosts = parse_hosts(PAYLOAD)
    assert isinstance(hosts["MyFabric"], FabricHost)
    assert isinstance(hosts["MyLocal"], LocalHost)


def test_the_alias_is_carried_for_diagnostics():
    assert parse_hosts(PAYLOAD)["MyFabric"].alias == "MyFabric"


def test_warehouse_settings_survive_parsing():
    host = parse_hosts(PAYLOAD)["MyFabric"]
    assert host.settings_for_warehouse("Reporting").degrees_of_parallelism == 8


def test_a_configured_host_equals_the_constructed_one():
    """Nothing is expressible in the file that the constructor cannot express."""
    from weaver import WarehouseSettings

    configured = parse_hosts(PAYLOAD)["MyFabric"]
    constructed = FabricHost(
        alias="MyFabric",
        workspace="Analytics",
        weaver_lakehouse="Weaver",
        fabric_environment="Weaver_Env",
        warehouse_config={"Reporting": WarehouseSettings(degrees_of_parallelism=8)},
    )
    assert configured == constructed


def test_unknown_keys_are_named_not_ignored():
    payload = {"hosts": {"H": {"type": "Fabric", "workspace": "W", "wraehouse_config": {}}}}
    with pytest.raises(ConfigError, match="wraehouse_config"):
        parse_hosts(payload)


def test_a_key_from_the_wrong_host_type_is_rejected():
    payload = {"hosts": {"H": {"type": "Local", "root": ".local", "workspace": "W"}}}
    with pytest.raises(ConfigError, match="workspace"):
        parse_hosts(payload)


def test_unknown_warehouse_settings_are_rejected():
    payload = {
        "hosts": {
            "H": {
                "type": "Fabric",
                "workspace": "W",
                "warehouse_config": {"T2": {"degrees_of_parallelisim": 8}},
            }
        }
    }
    with pytest.raises(ConfigError, match="degrees_of_parallelisim"):
        parse_hosts(payload)


def test_an_unknown_host_type_is_rejected():
    with pytest.raises(ConfigError, match="type"):
        parse_hosts({"hosts": {"H": {"type": "Snowflake", "workspace": "W"}}})


def test_a_missing_required_field_is_reported():
    with pytest.raises(ConfigError, match="incomplete"):
        parse_hosts({"hosts": {"H": {"type": "Fabric"}}})


def test_an_empty_hosts_mapping_is_rejected():
    with pytest.raises(ConfigError, match="non-empty"):
        parse_hosts({"hosts": {}})


def test_relative_local_roots_resolve_against_the_file(tmp_path: Path):
    config = tmp_path / "hosts.yml"
    config.write_text(
        "hosts:\n  MyLocal:\n    type: Local\n    root: .local\n",
        encoding="utf-8",
    )
    assert load_hosts(config)["MyLocal"].root == tmp_path / ".local"


def test_absolute_local_roots_are_left_alone(tmp_path: Path):
    config = tmp_path / "hosts.yml"
    config.write_text(
        f"hosts:\n  MyLocal:\n    type: Local\n    root: {tmp_path / 'elsewhere'}\n",
        encoding="utf-8",
    )
    assert load_hosts(config)["MyLocal"].root == tmp_path / "elsewhere"


def test_a_missing_file_is_reported(tmp_path: Path):
    with pytest.raises(ConfigError, match="not found"):
        load_hosts(tmp_path / "absent.yml")
