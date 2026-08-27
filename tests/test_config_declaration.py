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
def test_typical_configuration_parses_physical_defaults():
    workspace = parse_workspace(
        {
            "workspace": "Analytics",
            "environment": "WeaverRuntime",
            "catalogue": "Warehouse/Weaver",
            "execution": {"parallel_workers": 8},
            "lakehouses": {"Dev_Data": "Lakehouse/Sales"},
            "warehouses": {
                "Dev_Reporting": {
                    "item": "Warehouse/Reporting",
                    "execution": {"parallel_workers": 4},
                }
            },
        }
    )
    assert workspace.environment == EnvironmentRef(None, "WeaverRuntime")
    assert workspace.execution.parallel_workers == 8
    assert str(workspace.lakehouses["Dev_Data"].item) == "Lakehouse/Sales"
    assert workspace.warehouses["Dev_Reporting"].execution.parallel_workers == 4


@weaver_test()
def test_physical_names_can_overlap_across_types():
    workspace = parse_workspace(
        {
            "workspace": "Analytics",
            "lakehouses": {"Data": "Lakehouse/Sales"},
            "warehouses": {"Data": "Warehouse/Reporting"},
        }
    )
    assert "Data" in workspace.lakehouses
    assert "Data" in workspace.warehouses


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
def test_target_type_mismatch_is_rejected():
    with pytest.raises(ConfigError, match="must name a Lakehouse"):
        parse_workspace(
            {
                "workspace": "Analytics",
                "lakehouses": {"Data": "Warehouse/Reporting"},
            }
        )


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
