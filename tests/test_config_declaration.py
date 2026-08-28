"""One configuration file describes exactly one Workspace."""

from pathlib import Path

import pytest
from support.weaver_test import weaver_test

from weaver.config import load_workspace, parse_workspace, resolve_workspace
from weaver.errors import ConfigError
from weaver.workspaces import EnvironmentRef, Workspace


@weaver_test()
def test_a_workspace_is_named_and_nothing_else_is_required():
    workspace = parse_workspace({"workspace": "Analytics"})
    assert isinstance(workspace, Workspace)
    assert workspace.workspace == "Analytics"


@weaver_test()
def test_typical_configuration_maps_logical_items_to_physical_ones():
    workspace = parse_workspace(
        {
            "workspace": "Analytics",
            "environment": "WeaverRuntime",
            "catalogue": "Warehouse/Weaver",
            "execution": {"parallel_workers": 8},
            "targets": {
                "Lakehouse/Sales": "Dev_Data",
                "Warehouse/Reporting": {
                    "name": "Dev_Reporting",
                    "execution": {"parallel_workers": 4},
                },
            },
        }
    )
    assert workspace.environment == EnvironmentRef(None, "WeaverRuntime")
    assert workspace.execution.parallel_workers == 8
    assert str(workspace.target_for(_item("Lakehouse/Sales"))) == "Dev_Data"
    assert workspace.settings_for_warehouse("Dev_Reporting").parallel_workers == 4


@weaver_test()
def test_the_logical_key_decides_which_kind_of_item_the_value_names():
    """One mapping serves both, because `Lakehouse/` and `Warehouse/` say which."""

    from weaver.targets import DeltaTarget, WarehouseTarget

    workspace = parse_workspace(
        {
            "workspace": "Analytics",
            "targets": {
                "Lakehouse/Sales": "Shared",
                "Warehouse/Reporting": "Shared",
            },
        }
    )

    assert workspace.target_for(_item("Lakehouse/Sales")) == DeltaTarget.parse("Shared")
    assert workspace.target_for(_item("Warehouse/Reporting")) == WarehouseTarget.parse(
        "Shared"
    )


@pytest.mark.parametrize("kind", ["Lakehouse", "Warehouse"])
@weaver_test()
def test_two_logical_items_may_share_one_physical_item(kind):
    """Valid configuration. The mapping says where each item is deployed.

    A physical Lakehouse hosts as many logical items as an estate puts in it.
    What is unsafe is two logical objects at one address inside it, and the
    installed graph refuses that where an operation has to address it.
    """

    workspace = parse_workspace(
        {
            "workspace": "Analytics",
            "targets": {f"{kind}/A": "Shared", f"{kind}/B": "Shared"},
        }
    )

    assert workspace.target_for(_item(f"{kind}/A")) == workspace.target_for(
        _item(f"{kind}/B")
    )


@weaver_test()
def test_a_logical_item_with_no_entry_says_what_to_add():
    workspace = parse_workspace(
        {"workspace": "Analytics", "targets": {"Lakehouse/Sales": "Dev_Data"}}
    )

    with pytest.raises(ConfigError, match="no physical target"):
        workspace.target_for(_item("Lakehouse/Inventory"))


@pytest.mark.parametrize("retired", ["lakehouses", "warehouses"])
@weaver_test()
def test_the_retired_physical_keyed_sections_are_refused(retired):
    with pytest.raises(ConfigError, match="logical-first targets: mapping"):
        parse_workspace(
            {"workspace": "Analytics", retired: {"Data": "Lakehouse/Sales"}}
        )


def _item(text: str):
    from weaver.declaration.model import WeaverItemId

    return WeaverItemId.parse(text)


@weaver_test()
def test_configuration_accepts_a_qualified_environment():
    workspace = parse_workspace(
        {"workspace": "Analytics", "environment": "Platform/WeaverRuntime"}
    )
    assert workspace.environment == EnvironmentRef("Platform", "WeaverRuntime")


@weaver_test()
def test_cli_environment_override_replaces_the_configured_reference(tmp_path):
    config = tmp_path / "workspace.yml"
    config.write_text(
        "workspace: Analytics\nenvironment: LocalRuntime\n", encoding="utf-8"
    )
    workspace = resolve_workspace(
        workspace_config=config, environment="Platform/SharedRuntime"
    )
    assert workspace.environment == EnvironmentRef("Platform", "SharedRuntime")


@weaver_test()
def test_an_untyped_target_key_is_rejected():
    with pytest.raises(ConfigError, match="logical item identity"):
        parse_workspace({"workspace": "Analytics", "targets": {"Data": "Dev_Data"}})


@weaver_test()
def test_unknown_keys_are_rejected():
    with pytest.raises(ConfigError, match="wraehouses"):
        parse_workspace({"workspace": "Analytics", "wraehouses": {}})


@weaver_test()
def test_unknown_workspace_type_is_rejected():
    with pytest.raises(ConfigError, match="workspace_type"):
        parse_workspace({"workspace": "Analytics", "workspace_type": "snowflake"})


@weaver_test()
def test_workspace_is_required():
    with pytest.raises(ConfigError, match="define 'workspace'"):
        parse_workspace({})


@weaver_test()
def test_parallel_workers_must_be_positive():
    with pytest.raises(ConfigError, match="parallel_workers"):
        parse_workspace(
            {"workspace": "Analytics", "execution": {"parallel_workers": 0}}
        )


@weaver_test()
def test_missing_file_is_reported(tmp_path: Path):
    with pytest.raises(ConfigError, match="not found"):
        load_workspace(tmp_path / "absent.yml")
