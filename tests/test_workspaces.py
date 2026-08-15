"""Workspace values are directly constructible without configuration files."""

import pytest

from weaver.declaration.model import WeaverItemId
from weaver.errors import IdentityError
from weaver.workspaces import ExecutionSettings, TargetDeclaration, Workspace


def test_fabric_workspace_needs_only_its_name():
    workspace = Workspace(workspace="Analytics")
    assert workspace.workspace == "Analytics"
    assert workspace.environment is None


def test_workspace_sub_parameters_are_item_names():
    workspace = Workspace(
        workspace="Analytics",
        catalogue="Lakehouse/Weaver",
        environment="WeaverRuntime",
    )
    assert workspace.catalogue == "Lakehouse/Weaver"
    assert workspace.environment == "WeaverRuntime"


def test_target_configuration_is_immutable():
    workspace = Workspace(
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
        Workspace(workspace="  ")
