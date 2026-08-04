"""Workspace values are directly constructible without configuration files."""

from pathlib import Path

import pytest

from weaver.workspaces import ExecutionSettings, FabricWorkspace, LocalWorkspace, TargetDeclaration
from weaver.declaration.model import WeaverItemId
from weaver.errors import ConfigError, IdentityError


def test_fabric_workspace_needs_only_its_name():
    workspace = FabricWorkspace(workspace="Analytics")
    assert workspace.workspace == "Analytics"
    assert workspace.workspace_type == "fabric"
    assert workspace.environment is None


def test_workspace_sub_parameters_are_item_names():
    workspace = FabricWorkspace(
        workspace="Analytics",
        weaver_lakehouse="Weaver",
        environment="WeaverRuntime",
    )
    assert workspace.weaver_lakehouse == "Weaver"
    assert workspace.environment == "WeaverRuntime"


def test_local_workspace_is_a_folder_path():
    workspace = LocalWorkspace(workspace=".local")
    assert workspace.workspace == Path(".local")
    assert workspace.workspace_type == "local"


def test_only_fabric_supports_sql():
    assert FabricWorkspace(workspace="Analytics").supports_sql is True
    assert LocalWorkspace(workspace=".local").supports_sql is False


def test_target_configuration_is_immutable():
    workspace = FabricWorkspace(
        workspace="Analytics",
        warehouses={
            "Reporting": TargetDeclaration(
                WeaverItemId.parse("Warehouse/Reporting"),
                ExecutionSettings(parallel_workers=8),
            )
        },
    )
    assert workspace.settings_for_warehouse("Reporting").parallel_workers == 8
    with pytest.raises(TypeError):
        workspace.warehouses["Inventory"] = workspace.warehouses["Reporting"]


def test_bad_workspace_name_is_rejected():
    with pytest.raises(IdentityError):
        FabricWorkspace(workspace="  ")


def test_bad_local_path_is_rejected():
    with pytest.raises(ConfigError):
        LocalWorkspace(workspace="  ")
