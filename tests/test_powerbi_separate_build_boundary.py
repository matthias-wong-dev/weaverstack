import pytest
from support.weaver_test import weaver_test
from test_report_build_cycle import prepared_project

import weaver
from weaver.declaration.model import WeaverItemId
from weaver.errors import BuildError
from weaver.workspaces import TargetDeclaration, Workspace


@pytest.mark.parametrize("source_type", ["Warehouse", "Lakehouse"])
@pytest.mark.parametrize(
    "powerbi",
    ["SemanticModel/Reporting", "Report/Executive", "PowerBI/Reporting", "both"],
)
@weaver_test()
def test_a_semantic_model_builds_apart_from_its_sources(tmp_path, source_type, powerbi):
    root, session, events, model, report = prepared_project(tmp_path)
    source = root / source_type / "Serving"
    if source_type == "Lakehouse":
        source /= "Tables"
    source.mkdir(parents=True)
    (source / "Cake.yml").write_text("Schema ID: Cake\nDescription: Sales records.\n")
    session._default_workspace = Workspace(
        workspace="Demo",
        catalogue="Warehouse/Catalogue",
        targets={
            WeaverItemId(source_type, "Serving"): TargetDeclaration(
                physical="Serving_Dev"
            ),
            WeaverItemId("SemanticModel", "Reporting"): TargetDeclaration(
                physical="Reporting_Dev"
            ),
            WeaverItemId("Report", "Executive"): TargetDeclaration(
                physical="Executive_Dev"
            ),
        },
    )
    if powerbi == "both":
        selection = ["SemanticModel/Reporting", "Report/Executive"]
    else:
        selection = [powerbi]
    session._resolver.client.requested.clear()
    with session:
        with pytest.raises(
            BuildError, match="Power BI items must be built as a separate step"
        ) as raised:
            weaver.build(
                root,
                items=[f"{source_type}/Serving", *selection],
                catalogue="Warehouse/Catalogue",
                session=session,
            )
        assert f"Build {source_type}/Serving, then" in str(raised.value)
        assert not events and not model.calls and not report.calls
        assert not session.tsql and not session.spark_sql and not session.python
        assert not session._resolver.client.requested


@weaver_test()
def test_model_and_report_still_build_together(tmp_path):
    root, session, events, model, report = prepared_project(tmp_path)
    with session:
        result = weaver.build(
            root,
            items=[
                "SemanticModel/Reporting=SemanticModel/Reporting_Dev",
                "Report/Executive=Report/Executive_Dev",
            ],
            session=session,
        )
        assert result.succeeded, result.errors
        assert events == ["model_update", "model_read", "report_update", "report_read"]
        assert not session.tsql and not session.spark_sql and not session.python
