from support.weaver_test import weaver_test
from test_report_build_cycle import prepared_project

from weaver.build_bundle.targets import ItemBindings, parse_build_item
from weaver.build_bundle.workflow import catalogue_items_for_build
from weaver.declaration.model import WeaverItemId
from weaver.declaration.repository import parse_item_repository
from weaver.locations import Location


@weaver_test()
def test_report_only_preflight_reads_installed_model_binding(tmp_path):
    root, *_ = prepared_project(tmp_path)
    repository = parse_item_repository(Location(root.as_posix()))
    bindings = ItemBindings(
        (parse_build_item("Report/Executive=Report/Executive_Dev"),)
    )
    required = catalogue_items_for_build(repository, bindings)
    assert set(required) == {
        WeaverItemId("Report", "Executive"),
        WeaverItemId("SemanticModel", "Reporting"),
    }


@weaver_test()
def test_report_only_binding_requires_certified_installed_model(tmp_path):
    import pytest
    from test_report_definition_representation import BINDING

    from weaver.build_bundle.reports import bind_reports
    from weaver.catalogue.state import Catalogue
    from weaver.errors import BuildError

    root, *_ = prepared_project(tmp_path)
    repository = parse_item_repository(Location(root.as_posix()))
    report = WeaverItemId("Report", "Executive")
    model = WeaverItemId("SemanticModel", "Reporting")
    catalogue = Catalogue(
        {model: {"Installation": ({"target_name": "Reporting_Dev", **BINDING},)}}
    )
    with pytest.raises(BuildError, match="certified"):
        bind_reports(repository, {report: None}, catalogue)


@weaver_test()
def test_report_only_preflight_refuses_installed_model_physical_drift(
    tmp_path, monkeypatch
):

    import pytest
    from test_report_definition_representation import BINDING

    from weaver.build_bundle.workflow import read_build_state
    from weaver.catalogue.state import Catalogue
    from weaver.errors import BuildError
    from weaver.workspaces import Workspace

    root, session, *_ = prepared_project(tmp_path)
    repository = parse_item_repository(Location(root.as_posix()))
    model = WeaverItemId("SemanticModel", "Reporting")
    model_rows = dict(Catalogue.from_repository(repository).rows[model])
    model_rows["Installation"] = (
        {
            "item_type": "SemanticModel",
            "item_name": "Reporting",
            "target_name": "Reporting_Dev",
            **BINDING,
        },
    )
    monkeypatch.setattr(
        "weaver.build_bundle.workflow._read_catalogue",
        lambda **k: Catalogue({model: model_rows}),
    )
    monkeypatch.setattr(
        "weaver.build_bundle.workflow._refuse_occupied_targets", lambda *a, **k: {}
    )
    bindings = ItemBindings(
        (parse_build_item("Report/Executive=Report/Executive_Dev"),)
    )
    with pytest.raises(BuildError, match="binding.*changed"):
        read_build_state(
            bindings,
            required_catalogue_items=(model,),
            repository=repository,
            session=session,
            workspace=Workspace(workspace="Demo", catalogue="Warehouse/Catalogue"),
        )
    assert not session.tsql and not session.spark_sql
