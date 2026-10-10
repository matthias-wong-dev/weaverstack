from support.weaver_test import weaver_test
from test_powerbi_project_declaration import native, parse, write

from weaver.declaration.model import WeaverDocumentId, WeaverItemId


@weaver_test()
def test_report_has_logical_artifact_root_and_model_dependency(tmp_path):
    native(tmp_path)
    repository = parse(tmp_path)
    report = WeaverItemId("Report", "Executive")
    model = WeaverItemId("SemanticModel", "Revenue")
    root = WeaverDocumentId.report_root(report)
    assert root.object_id.schema == "Definition"
    assert root.object_id.object == "Executive.Report"
    assert WeaverDocumentId.parse(str(root)) == root
    assert repository[str(report)].documents == (root,)
    assert repository.dependency_graph.upstream_of(str(root)) == (str(model),)
    assert repository.item_graph.upstream_of(str(report)) == (str(model),)
    edge = next(e for e in repository.dependency_edges if e.consumer == root)
    assert edge.producer == WeaverDocumentId.model_root(model)
    assert edge.reference == str(model)
    assert not edge.is_within_item


@weaver_test()
def test_normal_build_selection_contains_report_artifact_roots(tmp_path):
    from weaver.build_bundle.planner import installable_identities

    native(tmp_path)
    repository = parse(tmp_path)
    model = WeaverItemId("SemanticModel", "Revenue")
    report = WeaverItemId("Report", "Executive")
    assert installable_identities(repository, {model: None, report: None}) == {
        WeaverDocumentId.model_root(model),
        WeaverDocumentId.report_root(report),
    }


@weaver_test()
def test_report_signatures_are_independent_of_model_content(tmp_path):
    from dataclasses import replace

    native(tmp_path)
    report = WeaverItemId("Report", "Executive")
    model = WeaverItemId("SemanticModel", "Revenue")
    repository = parse(tmp_path)
    original = repository.reports[report]
    assert original.source_signature
    assert original.signature == original.effective_signature()
    assert repository[str(report)].signature == original.signature
    write(tmp_path, "PowerBI/Sales/Revenue.tmdl", "model Model\n\tculture: en-GB\n")
    changed_model = parse(tmp_path)
    assert (
        changed_model.semantic_models[model].signature
        != repository.semantic_models[model].signature
    )
    assert changed_model.reports[report].signature == original.signature
    changed_report = replace(
        original, parts={**original.parts, "StaticResources/theme.json": b"{}"}
    )
    assert changed_report.source_signature != original.source_signature
    assert changed_report.signature != original.signature
    other_model = replace(original, model=WeaverItemId("SemanticModel", "Other"))
    assert other_model.source_signature != original.source_signature
    assert (
        original.effective_signature(binding={"workspace_id": "w", "item_id": "m"})
        != original.signature
    )
    assert original.effective_signature(
        binding={"workspace_id": "w", "item_id": "m"}
    ) != original.effective_signature(binding={"workspace_id": "w", "item_id": "other"})
    assert (
        replace(original, path="PowerBI/Elsewhere/Executive.Report").signature
        == original.signature
    )


@weaver_test()
def test_report_catalogue_roundtrip_keeps_build_edge_without_runnable_node(tmp_path):
    from weaver.catalogue.claims import (
        CatalogueClaim,
        claim_rules_for_object_type,
        without_claims,
    )
    from weaver.catalogue.state import Catalogue

    native(tmp_path)
    repository = parse(tmp_path)
    report = WeaverItemId("Report", "Executive")
    model = WeaverItemId("SemanticModel", "Revenue")
    root = WeaverDocumentId.report_root(report)
    logical = Catalogue.from_repository(repository)
    rows = {
        item: {
            **tables,
            "Installation": (
                {
                    "item_type": item.item_type,
                    "item_name": item.item_name,
                    "target_name": item.item_name,
                },
            ),
        }
        for item, tables in logical.rows.items()
    }
    restored = Catalogue.from_mapping(Catalogue(rows).to_mapping())
    assert restored.registered[root].object_type == "report"
    assert restored.registered[root].signature == repository.reports[report].signature
    source = WeaverDocumentId.artifact(report, "Source", "Executive.Report")
    assert (
        restored.registered[source].signature
        == repository.reports[report].source_signature
    )
    (dependency,) = restored.rows[report]["Dependency"]
    assert dependency["referencing_schema_name"] == "Definition"
    assert dependency["referencing_object_name"] == "Executive.Report"
    assert dependency["dependency_reference"] == str(model)
    assert dependency["referenced_item_type"] == "SemanticModel"
    assert dependency["referenced_item_name"] == "Revenue"
    assert (
        dependency["referenced_schema_name"]
        == dependency["referenced_object_name"]
        == ""
    )
    dag = restored.dag()
    assert dag.graph.upstream_of(str(root)) == (str(model),)
    node = dag.node(root)
    assert node.target.kind == "report"
    assert not node.can_load
    assert node.artefact is None
    assert not node.is_validation
    assert str(source) not in dag.graph
    pruned = without_claims(
        restored,
        [CatalogueClaim(root, rule) for rule in claim_rules_for_object_type("report")],
    )
    assert not pruned.rows[report]["Dependency"]
    assert not pruned.rows[report]["Registry"]
    assert (
        restored.registered[WeaverDocumentId.model_root(model)]
        == pruned.registered[WeaverDocumentId.model_root(model)]
    )


@weaver_test()
def test_report_physical_presence_does_not_enable_load_or_validation_dispatch(tmp_path):
    from dataclasses import replace

    from weaver.catalogue.state import Catalogue
    from weaver.health import is_load_subject, participates_in_load_state

    native(tmp_path)
    logical = Catalogue.from_repository(parse(tmp_path))
    rows = {
        item: {
            **tables,
            "Installation": (
                {
                    "item_type": item.item_type,
                    "item_name": item.item_name,
                    "target_name": item.item_name,
                },
            ),
        }
        for item, tables in logical.rows.items()
    }
    dag = Catalogue(rows).dag()
    report = WeaverItemId("Report", "Executive")
    root = WeaverDocumentId.report_root(report)
    node = replace(dag.node(root), artefact_type="report")
    assert not node.can_load
    assert not node.is_validation
    assert not is_load_subject(node)
    assert not participates_in_load_state(node)
    assert root not in {n.identity for n in dag.loadables()}
    assert root not in {n.identity for n in dag.validations()}
    assert dag.graph.upstream_of(str(root)) == ("SemanticModel/Revenue",)


@weaver_test()
def test_eager_model_impact_rebuilds_selected_reports_without_drops(tmp_path):
    from weaver.build_bundle.incremental import select_build
    from weaver.build_bundle.prune import TargetInventory
    from weaver.catalogue.state import Catalogue

    native(tmp_path)
    native(tmp_path, report="Operations")
    before = parse(tmp_path)
    registered = Catalogue.from_repository(before).registered
    model = WeaverItemId("SemanticModel", "Revenue")
    reports = [WeaverItemId("Report", n) for n in ("Executive", "Operations")]
    model_root = WeaverDocumentId.model_root(model)
    roots = {model_root, *(WeaverDocumentId.report_root(i) for i in reports)}
    inventories = {
        i: TargetInventory(
            target_id=str(i),
            target_name=i.item_name,
            kind="semanticmodel" if i == model else "report",
        )
        for i in [model, *reports]
    }
    fixed = select_build(before, registered, selected=roots, inventories=inventories)
    assert set(fixed.selected_for_build) == roots
    assert not fixed.impact.changed
    report_fixed = select_build(
        before, registered, selected=roots - {model_root}, inventories=inventories
    )
    assert not report_fixed.selected_for_build
    write(tmp_path, "PowerBI/Sales/Revenue.tmdl", "model Model\n\tculture: en-GB\n")
    after = parse(tmp_path)
    impacted = select_build(after, registered, selected=roots, inventories=inventories)
    assert set(impacted.selected_for_build) == roots
    assert impacted.impact.changed == (model_root,)
    assert not impacted.selected_for_drop
    selected_report = WeaverDocumentId.report_root(reports[0])
    limited = select_build(
        after,
        registered,
        selected={model_root, selected_report},
        inventories=inventories,
    )
    assert set(limited.selected_for_build) == {model_root, selected_report}
    write(tmp_path, "PowerBI/Sales/Revenue.tmdl", "model Model\n\tculture: en-US\n")
    # Restore the native effective model by removing its overlay.
    (tmp_path / "PowerBI/Sales/Revenue.tmdl").unlink()
    write(
        tmp_path,
        "PowerBI/Sales/Reports/Executive.Report/StaticResources/theme.json",
        "{}",
    )
    report_edit = select_build(
        parse(tmp_path),
        registered,
        selected=roots - {model_root},
        inventories=inventories,
    )
    assert report_edit.selected_for_build == (selected_report,)
    assert model_root not in report_edit.impact.impacted


@weaver_test()
def test_model_contribution_claims_explain_inputs_without_changing_effective_gate(
    tmp_path,
):
    from weaver.catalogue.state import Catalogue

    native(tmp_path)
    write(tmp_path, "PowerBI/policy.tmdl", "model Model\n\tculture: en-US\n")
    write(tmp_path, "PowerBI/Sales/Revenue.tmdl", "model Model\n\tculture: en-GB\n")
    write(
        tmp_path,
        "PowerBI/annotations/ACME__Noop.py",
        'from weaver.semantic_models.annotation import Annotation\nclass ACME__Noop(Annotation):\n    scopes = {"model"}\n    def apply(self, target):\n        pass\n',
    )
    item = WeaverItemId("SemanticModel", "Revenue")
    before = parse(tmp_path)
    rows = Catalogue.from_repository(before).rows[item]["Registry"]
    claims = {
        (r["schema_name"], r["object_name"]): r
        for r in rows
        if r["object_role"] == "source"
    }
    assert set(claims) == {
        ("Definition", "Revenue.SemanticModel"),
        ("Definition", "Revenue.tmdl"),
        ("Policy", "policy.tmdl"),
        ("Annotations", "ACME__Noop.py"),
    }
    assert all(r["object_type"] == "source_artifact" for r in claims.values())
    write(tmp_path, "PowerBI/policy.tmdl", "model Model\n\tculture: en-AU\n")
    after = parse(tmp_path)
    assert (
        after.semantic_models[item].signature == before.semantic_models[item].signature
    )
    updated = {
        (r["schema_name"], r["object_name"]): r
        for r in Catalogue.from_repository(after).rows[item]["Registry"]
        if r["object_role"] == "source"
    }
    changed = {
        key for key in claims if claims[key]["signature"] != updated[key]["signature"]
    }
    assert changed == {("Policy", "policy.tmdl")}
    root = WeaverDocumentId.model_root(item)
    assert (
        Catalogue.from_repository(after).registered[root].signature
        == after.semantic_models[item].signature
    )
