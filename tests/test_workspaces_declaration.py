"""Workspace values are directly constructible without configuration files."""

import pytest
from support.weaver_test import weaver_test

from weaver.declaration.model import WeaverItemId
from weaver.errors import ConfigError, IdentityError
from weaver.workspaces import (
    EnvironmentRef,
    ExecutionSettings,
    TargetDeclaration,
    Workspace,
)


@weaver_test()
def test_fabric_workspace_needs_only_its_name():
    workspace = Workspace(workspace="Analytics")
    assert workspace.workspace == "Analytics"
    assert workspace.environment is None


@weaver_test()
def test_workspace_sub_parameters_are_item_names():
    workspace = Workspace(
        workspace="Analytics",
        catalogue="Warehouse/Weaver",
        environment="WeaverRuntime",
    )
    assert workspace.catalogue == "Warehouse/Weaver"
    assert workspace.environment == EnvironmentRef(None, "WeaverRuntime")


@weaver_test()
def test_environment_reference_grammar_preserves_its_owner():
    assert EnvironmentRef.parse("Runtime") == EnvironmentRef(None, "Runtime")
    assert EnvironmentRef.parse("Platform/Runtime") == EnvironmentRef(
        "Platform", "Runtime"
    )
    assert str(EnvironmentRef.parse("Platform/Runtime")) == "Platform/Runtime"


@pytest.mark.parametrize("value", ["/Runtime", "Platform/", "A/B/C"])
@weaver_test()
def test_malformed_environment_references_are_rejected(value):
    with pytest.raises((ConfigError, IdentityError)):
        EnvironmentRef.parse(value)


@weaver_test()
def test_workspace_accepts_a_qualified_environment_reference():
    workspace = Workspace(workspace="Analytics", environment="Platform/WeaverRuntime")
    assert workspace.environment == EnvironmentRef("Platform", "WeaverRuntime")


@weaver_test()
def test_target_configuration_is_immutable():
    item = WeaverItemId.parse("Warehouse/Curated")
    workspace = Workspace(
        workspace="Analytics",
        targets={item: TargetDeclaration("Reporting", ExecutionSettings(8))},
    )
    assert workspace.settings_for(item).parallel_workers == 8
    with pytest.raises(TypeError):
        workspace.targets[WeaverItemId.parse("Warehouse/Other")] = workspace.targets[
            item
        ]


@weaver_test()
def test_execution_settings_reach_only_the_item_that_declared_them():
    """Two items may deploy to one Warehouse, and each keeps its own settings."""

    fast = WeaverItemId.parse("Warehouse/Curated")
    slow = WeaverItemId.parse("Warehouse/Archive")
    workspace = Workspace(
        workspace="Analytics",
        execution=ExecutionSettings(parallel_workers=1),
        targets={
            fast: TargetDeclaration("Shared", ExecutionSettings(parallel_workers=8)),
            slow: TargetDeclaration("Shared"),
        },
    )

    assert workspace.settings_for(fast).parallel_workers == 8
    assert workspace.settings_for(slow).parallel_workers == 1
    assert (
        workspace.settings_for(WeaverItemId.parse("Warehouse/Absent"))
        == workspace.execution
    )


@weaver_test()
def test_bad_workspace_name_is_rejected():
    with pytest.raises(IdentityError):
        Workspace(workspace="  ")
