import pytest
from support.weaver_test import weaver_test
from test_powerbi_project_declaration import native, parse, write

from weaver.config import parse_workspace
from weaver.operations.build import _item_bindings


def workspace():
    return parse_workspace(
        {
            "workspace": "Demo",
            "catalogue": "Warehouse/Catalogue",
            "targets": {
                "SemanticModel/Revenue": "Revenue_Dev",
                "Warehouse/Serving": "Serving_Dev",
            },
        }
    )


@weaver_test()
def test_aggregate_composition_keeps_source_and_powerbi_builds_separate(
    tmp_path, monkeypatch
):
    import importlib

    import weaver
    from weaver.errors import BuildError

    write(tmp_path, "PowerBI/Sales/Revenue.tmdl", "model Model\n")
    write(tmp_path, "Warehouse/Serving/.gitkeep", "")
    write(
        tmp_path,
        "workspace-config.yml",
        "workspace: Demo\ncatalogue: Warehouse/Catalogue\ntargets:\n  SemanticModel/Revenue: Revenue_Dev\n  Warehouse/Serving: Serving_Dev\n",
    )
    operation = importlib.import_module("weaver.operations.build")

    def preflight(*args, **kwargs):
        pytest.fail("mixed source and Power BI selection reached target preflight")

    monkeypatch.setattr(operation, "_preflight", preflight)
    with pytest.raises(
        BuildError, match="separate step.*Warehouse/Serving.*SemanticModel/Revenue"
    ):
        weaver.build(
            tmp_path,
            items=["Warehouse", "PowerBI/Sales"],
            workspace_config=tmp_path / "workspace-config.yml",
        )


@weaver_test()
def test_report_target_vocabulary_does_not_enable_wipe():
    from weaver.errors import CommandError
    from weaver.operations.wipe import REMOVE, WipePlan, WipeTarget
    from weaver.workspaces import Workspace

    report = WipeTarget.parse("Report/Dashboard")
    with pytest.raises(CommandError, match="weaver unbind Report/Dashboard"):
        WipePlan(
            workspace=Workspace(workspace="Analytics"),
            targets=(report,),
            catalogue=None,
            catalogue_action=REMOVE,
        )


@weaver_test()
def test_report_build_reaches_typed_target_preflight(tmp_path, monkeypatch):
    import importlib

    import weaver
    from weaver.errors import BuildError

    native(tmp_path)
    write(
        tmp_path,
        "workspace-config.yml",
        "workspace: Demo\ncatalogue: Warehouse/Catalogue\ntargets:\n  Report/Executive: Dashboard_Dev\n",
    )
    operation = importlib.import_module("weaver.operations.build")

    def preflight(*args, **kwargs):
        raise BuildError("typed Report preflight reached")

    monkeypatch.setattr(operation, "_preflight", preflight)
    with pytest.raises(BuildError, match="typed Report preflight reached"):
        weaver.build(
            tmp_path, items="Report", workspace_config=tmp_path / "workspace-config.yml"
        )


@pytest.mark.parametrize(
    "selector",
    [
        None,
        "Lakehouse=Lakehouse/Serving",
        "PowerBI/Sales=Report/Serving",
        "PowerBI/Missing",
        "Report",
    ],
)
@weaver_test()
def test_invalid_aggregate_selection_fails_as_build_error(tmp_path, selector):
    from weaver.errors import BuildError

    write(tmp_path, "PowerBI/Sales/Revenue.tmdl", "model Model\n")
    with pytest.raises(BuildError):
        _item_bindings([selector], workspace(), repository=parse(tmp_path))


@pytest.mark.parametrize("selector", ["PowerBI", "PowerBI/Sales"])
@weaver_test()
def test_powerbi_selector_names_actual_model_and_reports(tmp_path, selector):
    native(tmp_path)
    workspace_config = parse_workspace(
        {
            "workspace": "Demo",
            "targets": {
                "SemanticModel/Revenue": "Model_Dev",
                "Report/Executive": "Dashboard_Dev",
            },
        }
    )
    selected = _item_bindings(
        [selector, "Report", "Report/Executive"],
        workspace_config,
        repository=parse(tmp_path),
    )
    assert {str(b.item): b.target.item.name for b in selected.entries} == {
        "SemanticModel/Revenue": "Model_Dev",
        "Report/Executive": "Dashboard_Dev",
    }


@weaver_test()
def test_public_build_expands_type_selector_before_parsing_bindings(tmp_path):
    from test_semantic_model_catalogue_free_cycle import session_without_catalogue

    import weaver

    session = session_without_catalogue()
    config = tmp_path / "workspace-config.yml"
    write(
        tmp_path,
        "workspace-config.yml",
        "workspace: Demo\ntargets:\n  SemanticModel/Reporting: Reporting_Dev\n",
    )
    write(tmp_path, "PowerBI/Sales/Reporting.tmdl", "model Model\n")
    from weaver.semantic_models.definition import encode_definition

    session.semantic_model("Reporting_Dev").definition = encode_definition(
        {"model": {"culture": "en-US", "defaultPowerBIDataSourceVersion": "powerBI_V3"}}
    )
    result = weaver.build(
        tmp_path, items="SemanticModel", workspace_config=config, session=session
    )
    assert result.succeeded, result.errors
    assert result.items == ("SemanticModel/Reporting",)
    assert not session.tsql and not session.spark_sql


@weaver_test()
def test_type_selector_expands_before_binding_and_deduplicates_exact_items(tmp_path):
    write(tmp_path, "PowerBI/Sales/Revenue.tmdl", "model Model\n")
    write(tmp_path, "Warehouse/Serving/.gitkeep", "")
    repository = parse(tmp_path)
    selected = _item_bindings(
        [
            "SemanticModel",
            "Warehouse",
            "SemanticModel/Revenue=SemanticModel/Override",
            "SemanticModel",
        ],
        workspace(),
        repository=repository,
    )
    assert [(str(b.item), b.target.item.name) for b in selected.entries] == [
        ("SemanticModel/Revenue", "Override"),
        ("Warehouse/Serving", "Serving_Dev"),
    ]
    assert (
        _item_bindings("SemanticModel/Revenue", workspace(), repository=repository)
        .entries[0]
        .target.item.name
        == "Revenue_Dev"
    )
