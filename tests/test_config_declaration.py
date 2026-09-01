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
    assert workspace.settings_for(_item("Warehouse/Reporting")).parallel_workers == 4


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
    """Valid configuration, which a constrained environment writes.

    Configuration says where each item deploys. Only one is installed there at a
    time: a build into a target another item is installed to is refused.
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
    with pytest.raises(ConfigError, match="item-keyed targets: mapping"):
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
    with pytest.raises(ConfigError, match="item identity"):
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


# --- what a configuration file must and need not say --------------------------
#
# One required key, and every optional structure validated where it is written.
# The parser is the schema, and a malformed file is refused with a ConfigError
# naming the field it was written under.


@pytest.mark.parametrize(
    "payload",
    [
        {"workspace": "Analytics"},
        {"workspace": "Analytics", "catalogue": "Warehouse/Weaver"},
        {"workspace": "Analytics", "targets": {}},
        {"workspace": "Analytics", "catalogue": "Warehouse/Weaver", "targets": {}},
    ],
    ids=["workspace only", "with catalogue", "empty targets", "both"],
)
@weaver_test()
def test_targets_are_optional(payload):
    """A workspace that installs into items of its own name needs no mapping."""

    workspace = parse_workspace(payload)

    assert workspace.workspace == "Analytics"
    assert workspace.targets == {}


@weaver_test()
def test_an_absent_targets_key_and_an_empty_one_are_the_same_configuration():
    assert (
        parse_workspace({"workspace": "Analytics"}).targets
        == parse_workspace({"workspace": "Analytics", "targets": {}}).targets
    )


@pytest.mark.parametrize(
    ("payload", "named"),
    [
        ({"workspace": "Analytics", "targtes": {}}, "targtes"),
        (
            {"workspace": "Analytics", "execution": {"paralell_workers": 2}},
            "paralell_workers",
        ),
        (
            {
                "workspace": "Analytics",
                "targets": {"Lakehouse/Landing": {"name": "L", "exec": {}}},
            },
            "exec",
        ),
        (
            {
                "workspace": "Analytics",
                "targets": {
                    "Lakehouse/Landing": {
                        "name": "L",
                        "execution": {"workers": 2},
                    }
                },
            },
            "workers",
        ),
    ],
    ids=["top level", "execution", "target declaration", "target execution"],
)
@weaver_test()
def test_an_unknown_key_is_refused_by_name(payload, named):
    with pytest.raises(ConfigError, match=named):
        parse_workspace(payload)


@pytest.mark.parametrize(
    ("payload", "named"),
    [
        (["workspace: Analytics"], "must be a mapping"),
        ({"workspace": {"name": "Analytics"}}, "workspace"),
        ({"workspace": ""}, "workspace"),
        ({"workspace": "Analytics", "execution": 4}, "execution"),
        (
            {"workspace": "Analytics", "execution": {"parallel_workers": "many"}},
            "parallel_workers",
        ),
        ({"workspace": "Analytics", "targets": ["Lakehouse/Landing"]}, "targets"),
        ({"workspace": "Analytics", "targets": {"Lakehouse/Landing": None}}, "targets"),
        ({"workspace": "Analytics", "targets": {"Lakehouse/Landing": 7}}, "targets"),
        ({"workspace": "Analytics", "targets": {"Lakehouse/Landing": {}}}, "name"),
        (
            {"workspace": "Analytics", "targets": {"Lakehouse/Landing": {"name": 7}}},
            "name",
        ),
        ({"workspace": "Analytics", "environment": 7}, "environment"),
        ({"workspace": "Analytics", "environment": "A/B/C"}, "environment"),
        ({"workspace": "Analytics", "catalogue": "Weaver"}, "catalogue"),
        ({"workspace": "Analytics", "catalogue": "Lakehouse/Weaver"}, "catalogue"),
    ],
    ids=[
        "top level is a list",
        "workspace is a mapping",
        "workspace is empty",
        "execution is a scalar",
        "parallel_workers is text",
        "targets is a list",
        "target value is empty",
        "target value is a number",
        "target mapping names nothing",
        "target name is a number",
        "environment is a number",
        "environment is three parts",
        "catalogue carries no kind",
        "catalogue names a Lakehouse",
    ],
)
@weaver_test()
def test_a_malformed_value_is_refused_saying_which_field(payload, named):
    with pytest.raises(ConfigError, match=named):
        parse_workspace(payload)


@weaver_test()
def test_an_illegal_physical_name_is_a_configuration_error():
    """The identity rules answer what a name may contain.

    Reported as a configuration error, because a configuration file is what the
    reader has to change.
    """

    with pytest.raises(ConfigError, match="Landing/Dev"):
        parse_workspace(
            {"workspace": "Analytics", "targets": {"Lakehouse/Landing": "Landing/Dev"}}
        )
