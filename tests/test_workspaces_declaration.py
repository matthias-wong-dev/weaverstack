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
        targets={
            item: TargetDeclaration(
                item, "Reporting", ExecutionSettings(parallel_workers=8)
            )
        },
    )
    assert workspace.settings_for_warehouse("Reporting").parallel_workers == 8
    with pytest.raises(TypeError):
        workspace.targets[WeaverItemId.parse("Warehouse/Other")] = workspace.targets[
            item
        ]


@weaver_test()
def test_a_declaration_must_agree_with_the_key_it_is_filed_under():
    """The key is the logical item, so a declaration naming another is a mistake."""

    with pytest.raises(ConfigError, match="must name one logical item"):
        Workspace(
            workspace="Analytics",
            targets={
                WeaverItemId.parse("Lakehouse/Landing"): TargetDeclaration(
                    WeaverItemId.parse("Lakehouse/Other"), "Landing_Dev"
                )
            },
        )


@weaver_test()
def test_bad_workspace_name_is_rejected():
    with pytest.raises(IdentityError):
        Workspace(workspace="  ")
