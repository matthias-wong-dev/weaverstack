import json

import pytest
from support.weaver_test import weaver_test
from test_powerbi_project_declaration import native, parse, write
from test_report_build_cycle import prepared_project

import weaver
from weaver.catalogue.powerbi import project_report
from weaver.declaration.model import WeaverDocumentId, WeaverItemId
from weaver.report_definition import decode_report


@pytest.mark.parametrize(
    "reference",
    [
        {"byPath": {"path": "../Normal.SemanticModel"}},
        {
            "byConnection": {
                "connectionString": "physical unrelated",
                "pbiModelDatabaseName": "other-id",
            }
        },
    ],
)
@weaver_test()
def test_report_naming_conventions_ignore_connection_metadata(tmp_path, reference):
    native(tmp_path, model="Normal", report="Executive")
    write(tmp_path, "PowerBI/Sales/Executive.tmdl", "model Model\n")
    write(tmp_path, "PowerBI/Sales/Public.tmdl", "model Model\n")
    write(
        tmp_path,
        "PowerBI/Sales/Reports/Executive.Report/definition.pbir",
        json.dumps({"datasetReference": reference}),
    )
    write(
        tmp_path,
        "PowerBI/Sales/Dashboard.Report/definition.pbir",
        json.dumps({"datasetReference": reference}),
    )
    repository = parse(tmp_path)
    executive = WeaverItemId("Report", "Executive")
    dashboard = WeaverItemId("Report", "Dashboard")
    assert repository.reports[executive].model == WeaverItemId(
        "SemanticModel", "Executive"
    )
    assert repository.reports[dashboard].model is None
    edges = repository.dependency_edges
    assert any(
        edge.consumer == WeaverDocumentId.report_root(executive)
        and edge.producer
        == WeaverDocumentId.model_root(WeaverItemId("SemanticModel", "Executive"))
        for edge in edges
    )
    assert not any(
        edge.consumer == WeaverDocumentId.report_root(dashboard) for edge in edges
    )
    rows = project_report(dashboard, repository.reports[dashboard])
    assert rows["Dependency"] == ()
    assert len(rows["Registry"]) == 2


@weaver_test()
def test_public_as_authored_report_build_uses_no_model_target_and_preserves_all_bytes(
    tmp_path,
):
    root, session, events, model, report = prepared_project(tmp_path)
    write(root, "PowerBI/Reporting/Public.tmdl", "model Model\n")
    path = root / "PowerBI/Reporting/Executive.Report/definition.pbir"
    native_bytes = b'{"version":"4.0","datasetReference":{"byPath":{"path":"../Reporting.SemanticModel"}}}\r\n'
    path.write_bytes(native_bytes)
    before = {
        p.relative_to(root).as_posix(): p.read_bytes()
        for p in root.rglob("*")
        if p.is_file()
    }
    result = weaver.build(
        root, items=["Report/Executive=Report/Executive_Dev"], session=session
    )
    assert result.succeeded, result.errors
    assert events == ["report_update", "report_read"]
    assert not model.calls
    assert decode_report(report.definition)["definition.pbir"] == native_bytes
    assert before == {
        p.relative_to(root).as_posix(): p.read_bytes()
        for p in root.rglob("*")
        if p.is_file()
    }
    assert not session.tsql and not session.spark_sql


@weaver_test()
def test_report_without_local_models_deploys_as_authored_without_inference(tmp_path):
    root, session, events, model, report = prepared_project(tmp_path)
    (root / "PowerBI/Reporting/Reporting.tmdl").unlink()
    repository = parse(root)
    item = WeaverItemId("Report", "Executive")
    assert not repository.semantic_models
    assert repository.reports[item].model is None
    assert project_report(item, repository.reports[item])["Dependency"] == ()
    result = weaver.build(
        root, items="Report/Executive=Report/Executive_Dev", session=session
    )
    assert result.succeeded, result.errors
    assert events == ["report_update", "report_read"]
    assert not model.calls
    assert decode_report(report.definition) == repository.reports[item].parts


@weaver_test()
def test_as_authored_report_certification_is_independent_of_always_selected_model(
    tmp_path,
):
    from test_semantic_model_build_cycle import (
        ITEM,
        bundle_for,
        engine_model,
        installed_state,
    )

    from weaver.build_bundle.execution_plan import execute_bundle
    from weaver.build_bundle.targets import ItemBindings, parse_build_item
    from weaver.build_bundle.workflow import read_build_state

    root, session, events, model, report = prepared_project(tmp_path)
    write(root, "PowerBI/Reporting/Public.tmdl", "model Model\n")
    repository = parse(root)
    bindings = ItemBindings(
        tuple(
            parse_build_item(s)
            for s in (
                f"{ITEM}=SemanticModel/Reporting_Dev",
                "Report/Executive=Report/Executive_Dev",
            )
        )
    )
    observed = read_build_state(bindings, required_catalogue_items=(), session=session)
    initial = bundle_for(tmp_path, repository, bindings, observed, "as-authored-first")
    assert execute_bundle(initial, session).succeeded
    installed = installed_state(
        repository, bindings, engine_model(repository), observed.target_inventories
    )
    report_item = WeaverItemId("Report", "Executive")
    rows = installed.catalogue.rows[report_item]
    assert not rows["Dependency"]
    assert (
        rows["Installation"][0]["signature"]
        == repository.reports[report_item].signature
    )
    fixed = bundle_for(tmp_path, repository, bindings, installed, "as-authored-fixed")
    assert not any(a.executor.startswith("report_") for _, _, a in fixed.plan.actions())
    assert any(a.executor == "semantic_model" for _, _, a in fixed.plan.actions())
    report_bindings = ItemBindings(
        (parse_build_item("Report/Executive=Report/Executive_Dev"),)
    )
    assert not list(
        bundle_for(
            tmp_path, repository, report_bindings, installed, "as-authored-noop"
        ).plan.actions()
    )
    page = root / "PowerBI/Reporting/Executive.Report/definition/report.json"
    page.write_text('{"displayName":"Edited"}')
    changed = parse(root)
    edited = bundle_for(
        tmp_path, changed, report_bindings, installed, "as-authored-edit"
    )
    assert {
        a.executor
        for _, _, a in edited.plan.actions()
        if a.executor.startswith(("report_", "semantic_"))
    } == {"report_definition", "report_readback"}

    def refuse_readback():
        from weaver.errors import InstallError

        raise InstallError("independent report readback refused")

    report.get_definition = refuse_readback
    result = execute_bundle(edited, session)
    assert not result.succeeded
    outcomes = {a.action_id: a.status for a in result.action_results()}
    assert outcomes["publish-catalogue"] == outcomes["publish-registry"] == "skipped"
