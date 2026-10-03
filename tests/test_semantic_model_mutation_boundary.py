"""Semantic Build uses the shared frozen plan, Session binding and success DAG."""

from dataclasses import replace

import pytest
from support.weaver_test import weaver_test
from support.workspaces import InventoryClient
from test_semantic_model_build_cycle import (
    ITEM,
    ROOT,
    bundle_for,
    engine_model,
    prepared,
)
from test_semantic_source_build_cycle import SubmittedDefinition

import weaver
from weaver.errors import BuildError, InstallError, OutcomeUnknown
from weaver.fabric.resolution import FabricResolver
from weaver.graph import Graph
from weaver.mutation.bundle import compute_bundle_id, plan_from_yaml, plan_to_yaml
from weaver.mutation.executor import MutationExecutor
from weaver.mutation.models import MutationPlan
from weaver.semantic_models.definition import encode_definition
from weaver.sessions import TestSession
from weaver.sessions.console import ConsoleSession
from weaver.store import FilesystemStore
from weaver.workspaces import Workspace


def success_graph(plan):
    actions = [a for _, _, a in plan.actions()]
    return Graph(
        (a.id for a in actions),
        ((dep, a.id) for a in actions for dep in a.depends_on),
    )


def observe_execution(monkeypatch, session):
    calls = []
    execute = session.execute_mutation

    def observing(plan, payloads, **options):
        assert isinstance(plan, MutationPlan)
        decoded = plan_from_yaml(plan_to_yaml(plan))
        assert decoded.to_mapping() == plan.to_mapping()
        assert decoded.bundle_id == compute_bundle_id(plan)
        report = execute(decoded, payloads, **{**options, "workers": 4})
        calls.append((decoded, payloads, report))
        return report

    monkeypatch.setattr(session, "execute_mutation", observing)
    return calls


@weaver_test()
@pytest.mark.parametrize("pbip", [False, True])
def test_public_build_executes_frozen_format5_and_certifies_after_readback(
    tmp_path, monkeypatch, pbip
):
    root, repository, _, session, _ = prepared(tmp_path, pbip)
    session.semantic_model("Reporting_Dev").definition = encode_definition(
        engine_model(repository)
    )
    seen = observe_execution(monkeypatch, session)
    shared = []
    execute = MutationExecutor.execute

    def executing(self, plan, payloads, **options):
        shared.append(plan)
        return execute(self, plan, payloads, **options)

    monkeypatch.setattr(MutationExecutor, "execute", executing)
    result = weaver.build(
        root, items=f"{ITEM}=SemanticModel/Reporting_Dev", session=session
    )
    assert result.succeeded, result.errors
    ((plan, payloads, report),) = seen
    assert shared == [plan]
    assert plan.format_version == 5 and report.succeeded
    assert plan.execution.spark_home_target_id is None
    assert not session.python and not session.spark_sql
    assert payloads and all(type(content) is bytes for content in payloads.values())
    with pytest.raises(TypeError):
        plan.build_envelope["selection"]["selected_for_build"] = ()
    actions = [a for _, _, a in plan.actions()]
    (deploy,) = [a for a in actions if a.executor == "semantic_model"]
    (publication,) = [a for a in actions if a.executor == "semantic_catalogue"]
    (registry,) = [a for a in actions if a.kind == "publish_registry"]
    success = success_graph(plan)
    assert deploy.id in success.ancestors(publication.id)
    assert publication.id in success.ancestors(registry.id)
    assert registry.id in success.ancestors(plan.required_completion[0])
    catalogue = next(t for t in plan.targets if t.id == publication.target_id)
    assert publication.resources == (f"warehouse:{catalogue.item_id}",)
    assert report.by_id[publication.id].value["semantic_definition"] == engine_model(
        repository
    )


@weaver_test()
@pytest.mark.parametrize(
    "failure", ["update", "readback", "uncertain", "readback_uncertain"]
)
def test_shared_executor_blocks_certification_without_stopping_independent_models(
    tmp_path, monkeypatch, failure
):
    workspace = Workspace(workspace="Demo", catalogue="Warehouse/Catalogue")
    inventory = InventoryClient(
        "Demo",
        [
            ("Warehouse", "Catalogue"),
            ("SemanticModel", "Broken"),
            ("SemanticModel", "Other"),
        ],
    )
    root = tmp_path / "project"
    for name in ("Broken", "Other"):
        folder = root / "SemanticModel" / name
        folder.mkdir(parents=True)
        (folder / "addon.yml").write_text(
            "model:\n  description: Published\n", encoding="utf-8"
        )
    broken, other = SubmittedDefinition(), SubmittedDefinition()
    if failure == "update":
        broken.failure = InstallError("deployment refused")
    elif failure == "uncertain":
        broken.failure = OutcomeUnknown("deployment response lost")
    elif failure == "readback_uncertain":
        broken.read_failure = OutcomeUnknown("readback response lost")
    else:
        broken.read_failure = InstallError("readback refused")
    with TestSession(
        workspace=workspace,
        store=FilesystemStore(),
        resolver=FabricResolver(workspace, client=inventory),
    ) as session:
        session.answer_semantic_model("Demo", "Broken", broken)
        session.answer_semantic_model("Demo", "Other", other)
        seen = observe_execution(monkeypatch, session)
        result = weaver.build(
            root,
            items=[
                "SemanticModel/Broken=SemanticModel/Broken",
                "SemanticModel/Other=SemanticModel/Other",
            ],
            session=session,
        )
        assert not result.succeeded
        plan, _, report = seen[0]
        actions = [a for _, _, a in plan.actions()]
        broken_update = next(
            a
            for a in actions
            if a.executor == "semantic_model"
            and a.resource_node_id == "SemanticModel/Broken"
        )
        other_update = next(
            a
            for a in actions
            if a.executor == "semantic_model"
            and a.resource_node_id == "SemanticModel/Other"
        )
        broken_read = next(
            a
            for a in actions
            if a.executor == "semantic_catalogue"
            and a.resource_node_id == "SemanticModel/Broken"
        )
        assert report.by_id[other_update.id].status == "succeeded"
        assert [method for method, _ in other.calls].count("update_definition") == 1
        assert (
            report.by_id[broken_update.id].status
            == {
                "update": "failed",
                "uncertain": "uncertain",
                "readback": "succeeded",
                "readback_uncertain": "succeeded",
            }[failure]
        )
        if failure in {"readback", "readback_uncertain"}:
            assert report.by_id[broken_read.id].status == (
                "failed" if failure == "readback" else "uncertain"
            )
        else:
            assert report.by_id[broken_read.id].status in {"blocked", "not_dispatched"}
            assert [method for method, _ in broken.calls] == ["update_definition"]
        assert all(
            report.by_id[key].status != "succeeded" for key in plan.required_completion
        )
        assert not any("MERGE" in s and "[_].[Registry]" in s for s in session.tsql)
        assert not any(
            "MERGE" in s and "[_].[SemanticModel]" in s for s in session.tsql
        )
        assert not any(
            "Succeeded" in s and "[_].[LoadStatus]" in s for s in session.tsql
        )


@weaver_test()
@pytest.mark.parametrize("mismatch", [None, "item_id", "workspace_id"])
def test_desktop_mutation_binds_semantic_session_without_spark(
    tmp_path, monkeypatch, mismatch
):
    _, repository, bindings, recording, state = prepared(tmp_path)
    bundle = bundle_for(tmp_path, repository, bindings, state, "desktop")
    plan = bundle.plan
    if mismatch:
        targets = tuple(
            replace(t, **{mismatch: "11111111-2222-3333-4444-555555555555"})
            if t.kind == "semanticmodel"
            else t
            for t in plan.targets
        )
        plan = replace(plan, targets=targets, bundle_id="")
        plan = replace(plan, bundle_id=compute_bundle_id(plan))
    payloads = {
        a.payload: bundle.store.read(bundle.location / a.payload)
        for _, _, a in plan.actions()
        if a.payload
    }
    model = recording.semantic_model("Reporting_Dev")
    model.definition = encode_definition(engine_model(repository))
    with ConsoleSession(
        workspace=recording.workspace,
        resolver=recording.resolver(),
        store=FilesystemStore(),
        progress=False,
    ) as desktop:
        monkeypatch.setattr(desktop, "sql_executor", recording.sql_executor)
        monkeypatch.setattr(desktop, "semantic_model", recording.semantic_model)
        report = desktop.execute_mutation(plan, payloads)
        assert report.succeeded is (mismatch is None)
        assert not desktop.scope().livy.acquired
        if mismatch:
            assert model.calls == []
            assert any("different" in (row.error or "") for row in report.results)
        else:
            assert [method for method, _ in model.calls] == [
                "update_definition",
                "get_definition",
            ]


@weaver_test()
def test_semantic_target_has_no_spark_address(tmp_path):
    _, repository, bindings, _, state = prepared(tmp_path)
    bundle = bundle_for(tmp_path, repository, bindings, state, "typed")
    target = next(t for t in bundle.plan.targets if t.kind == "semanticmodel")
    target = replace(target, workspace_name="Demo")
    with pytest.raises(BuildError, match="no Spark destination"):
        _ = target.spark_target
    assert target.logical_item_type == "SemanticModel"
    assert ROOT.item == ITEM


@weaver_test()
@pytest.mark.parametrize(
    "fault", ["format4", "logical_kind", "extension", "identity", "payload"]
)
def test_semantic_plan_uses_shared_codec_refusals_before_dispatch(tmp_path, fault):
    import yaml

    _, repository, bindings, session, state = prepared(tmp_path)
    bundle = bundle_for(tmp_path, repository, bindings, state, "refusal")
    mapping = bundle.plan.to_mapping()
    if fault == "format4":
        mapping["format_version"] = 4
    elif fault == "logical_kind":
        mapping["bundle_id"] = ""
        next(t for t in mapping["targets"] if t["kind"] == "semanticmodel")[
            "logical_item_type"
        ] = "Warehouse"
    elif fault == "extension":
        mapping["bundle_id"] = ""
        next(
            a
            for s in mapping["sequences"]
            for b in s["batches"]
            for a in b["actions"]
            if a["executor"] == "semantic_model"
        )["payload"] = "payload/model.sql"
    elif fault == "identity":
        mapping["build_envelope"]["repository_signature"] = "changed"
    with pytest.raises(BuildError):
        if fault == "payload":
            payloads = {
                a.payload: bundle.store.read(bundle.location / a.payload)
                for _, _, a in bundle.plan.actions()
                if a.payload
            }
            action = next(
                a for _, _, a in bundle.plan.actions() if a.executor == "semantic_model"
            )
            payloads[action.payload] += b" "
            session.execute_mutation(bundle.plan, payloads)
        else:
            plan_from_yaml(yaml.safe_dump(mapping))
    assert session.semantic_model("Reporting_Dev").calls == []


@weaver_test()
@pytest.mark.parametrize("kind", ["Table", "View"])
def test_lakehouse_source_requires_object_success_and_endpoint_readiness(
    tmp_path, kind
):
    from test_semantic_source_build_cycle import SourceSession
    from test_semantic_source_session_boundary import EndpointInventory

    from weaver.locations import Location
    from weaver.mutation.bundle import load_bundle

    root = tmp_path / "project"
    folder = root / "Lakehouse/Serving/Tables"
    folder.mkdir(parents=True)
    (folder / "Cake.yml").write_text(
        "Schema ID: Cake\nDescription: Sales records.\n", encoding="utf-8"
    )
    (folder / "Cake.Sales.sql").write_text(
        f"/*\n{kind} ID: Cake.Sales\nDescription: Sales facts.\nLineage: Constant\nDependencies: []\n*/\nSELECT 1 AS Id;\n",
        encoding="utf-8",
    )
    model = root / str(ITEM)
    model.mkdir(parents=True)
    (model / "addon.yml").write_text(
        "tables:\n  Sales:\n    .source: Lakehouse/Serving/Cake.Sales\n"
        "    columns:\n      Id:\n        dataType: int64\n        sourceColumn: Id\n",
        encoding="utf-8",
    )
    workspace = Workspace(workspace="Demo", catalogue="Warehouse/Catalogue")
    inventory = EndpointInventory(
        "Demo",
        [
            ("Lakehouse", "Serving_Dev"),
            ("Warehouse", "Catalogue"),
            ("SemanticModel", "Reporting_Dev"),
        ],
    )
    with SourceSession(
        workspace=workspace,
        resolver=FabricResolver(
            workspace, client=inventory, base_url=(tmp_path / "targets").as_posix()
        ),
        store=FilesystemStore(),
    ) as session:
        result = weaver.build(
            root,
            items=[
                "Lakehouse/Serving=Lakehouse/Serving_Dev",
                f"{ITEM}=SemanticModel/Reporting_Dev",
            ],
            bundle_only=True,
            bundle_path=tmp_path / "bundle",
            session=session,
        )
        plan = load_bundle(Location(result.bundle_path), store=FilesystemStore()).plan
    actions = [a for _, _, a in plan.actions()]
    source = next(
        a
        for a in actions
        if a.resource_node_id == "Lakehouse/Serving/Tables/Cake.Sales"
        and a.kind == f"build_{kind.lower()}"
    )
    deployment = next(a for a in actions if a.executor == "semantic_model")
    refreshed = next(a for a in actions if a.kind == "await_sql_endpoint_refresh")
    success = success_graph(plan)
    assert source.id in success.ancestors(deployment.id)
    assert refreshed.id in success.ancestors(deployment.id)
    assert source.id not in success.ancestors(refreshed.id)
