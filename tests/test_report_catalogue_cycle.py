from dataclasses import replace

from support.weaver_test import weaver_test
from test_report_build_cycle import prepared_project
from test_report_definition_representation import BINDING, contribution
from test_semantic_model_build_cycle import (
    ITEM,
    bundle_for,
    engine_model,
    installed_state,
)

from weaver.build_bundle.prune import TargetInventory
from weaver.build_bundle.reports import bind_reports
from weaver.build_bundle.semantic import bind_semantic_target
from weaver.build_bundle.targets import ItemBindings, parse_build_item
from weaver.build_bundle.workflow import read_build_state
from weaver.catalogue.powerbi import project_report
from weaver.catalogue.state import Catalogue, reconcile_catalogue_state
from weaver.declaration.model import WeaverDocumentId, WeaverItemId
from weaver.declaration.repository import parse_item_repository
from weaver.locations import Location


@weaver_test()
def test_report_rebound_target_removes_certification_and_source_claims():
    item = WeaverItemId("Report", "Executive")
    rows = project_report(item, replace(contribution(), binding=BINDING))
    rows["Installation"] = (
        {
            "item_type": "Report",
            "item_name": "Executive",
            "target_name": "Executive_Dev",
            **BINDING,
        },
    )
    catalogue = Catalogue({item: rows})
    inventory = TargetInventory(
        "report",
        "report",
        "Executive_Dev",
        workspace_id=BINDING["workspace_id"],
        item_id="ffffffff-bbbb-cccc-dddd-eeeeeeeeeeee",
    )
    actual = reconcile_catalogue_state(catalogue, inventories={item: inventory})
    assert WeaverDocumentId.report_root(item) not in actual.catalogue.registered
    assert not actual.catalogue.rows[item]["Registry"]
    assert not actual.catalogue.rows[item]["Dependency"]


@weaver_test()
def test_bound_project_catalogue_fixed_point_report_edit_and_eager_model_impact(
    tmp_path,
):
    from weaver.build_bundle.catalogue_actions import desired_catalogue
    from weaver.build_bundle.execution_plan import execute_bundle
    from weaver.build_bundle.planner import certifiable_identities

    root, session, events, model, report = prepared_project(tmp_path)
    repository = parse_item_repository(Location(root.as_posix()))
    bindings = ItemBindings(
        tuple(
            parse_build_item(s)
            for s in (
                f"{ITEM}=SemanticModel/Reporting_Dev",
                "Report/Executive=Report/Executive_Dev",
            )
        )
    )
    state = read_build_state(bindings, required_catalogue_items=(), session=session)
    first = bundle_for(tmp_path, repository, bindings, state, "first")
    actions = {a.id: a for _, _, a in first.plan.actions()}
    report_update = next(
        a for a in actions.values() if a.executor == "report_definition"
    )
    model_verify = next(
        a for a in actions.values() if a.executor == "semantic_readback"
    )
    report_verify = next(a for a in actions.values() if a.executor == "report_readback")
    assert model_verify.id in report_update.depends_on
    assert report_update.id in report_verify.depends_on
    assert report_verify.id in actions["complete-physical-work"].depends_on

    def ancestors(action):
        direct = set(actions[action].depends_on)
        return direct | {p for parent in direct for p in ancestors(parent)}

    assert report_verify.id in ancestors("publish-registry")
    assert "publish-catalogue" in ancestors("publish-registry")
    assert model_verify.id in ancestors("publish-registry")
    assert execute_bundle(first, session).succeeded
    targets = {
        i: bind_semantic_target(b.to_bound_target(), state.target_inventories[i])
        for i, b in bindings.by_item.items()
    }
    bound = bind_reports(repository, targets, state.catalogue)
    report_item = WeaverItemId("Report", "Executive")
    desired = desired_catalogue(
        bound, certifiable_identities(bound, bindings.by_item), targets
    )
    assert (
        desired.rows[report_item]["Installation"][0]["signature"]
        == bound.reports[report_item].signature
    )
    installed = installed_state(
        bound, bindings, engine_model(repository), state.target_inventories
    )
    fixed = bundle_for(tmp_path, repository, bindings, installed, "fixed")
    assert list(fixed.plan.actions()) == []
    page = root / "PowerBI/Reporting/Executive.Report/definition/report.json"
    page.write_text('{"displayName":"Edited"}')
    changed = parse_item_repository(Location(root.as_posix()))
    report_only = bundle_for(tmp_path, changed, bindings, installed, "report-only")
    assert {
        a.executor
        for _, _, a in report_only.plan.actions()
        if a.executor.startswith(("report_", "semantic_"))
    } == {"report_definition", "report_readback"}
    page.write_text("{}")
    (root / "PowerBI/Reporting/Reporting.tmdl").write_text(
        'table Calendar\n\tdescription: Changed model\n\tpartition Calendar = calculated\n\t\tsource = ROW("Year", 2026)\n'
    )
    changed_model = parse_item_repository(Location(root.as_posix()))
    eager = bundle_for(tmp_path, changed_model, bindings, installed, "eager")
    assert {
        a.executor
        for _, _, a in eager.plan.actions()
        if a.executor.startswith(("report_", "semantic_"))
    } == {
        "semantic_model",
        "semantic_readback",
        "semantic_catalogue",
        "report_definition",
        "report_readback",
    }
    assert (
        changed_model.reports[report_item].source_signature
        == repository.reports[report_item].source_signature
    )


@weaver_test()
def test_failed_report_readback_blocks_registry_and_installation_publication(tmp_path):
    from weaver.build_bundle.execution_plan import execute_bundle
    from weaver.errors import InstallError

    root, session, _, _, report = prepared_project(tmp_path)
    repository = parse_item_repository(Location(root.as_posix()))
    bindings = ItemBindings(
        tuple(
            parse_build_item(s)
            for s in (
                f"{ITEM}=SemanticModel/Reporting_Dev",
                "Report/Executive=Report/Executive_Dev",
            )
        )
    )
    state = read_build_state(bindings, required_catalogue_items=(), session=session)
    bundle = bundle_for(tmp_path, repository, bindings, state, "failure")

    def fail():
        raise InstallError("report binding refused")

    report.get_definition = fail
    result = execute_bundle(bundle, session)
    outcomes = {a.action_id: a for a in result.action_results()}
    assert not result.succeeded
    assert outcomes["publish-catalogue"].status == "skipped"
    assert outcomes["publish-registry"].status == "skipped"
    assert not any(
        "MERGE [_].[Installation]" in sql or "MERGE [_].[Registry]" in sql
        for _, sql in session.tsql
    )
