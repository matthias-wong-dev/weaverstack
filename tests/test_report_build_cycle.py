import json
import shutil

import pytest
from support.weaver_test import weaver_test
from support.workspaces import InventoryClient
from test_semantic_model_build_cycle import (
    ITEM,
    DefinitionClient,
    engine_model,
    project,
)

import weaver
from weaver.declaration.repository import parse_item_repository
from weaver.fabric.resolution import FabricResolver
from weaver.locations import Location
from weaver.semantic_models.definition import encode_definition
from weaver.sessions import TestSession
from weaver.store import FilesystemStore
from weaver.workspaces import Workspace


class ReportBoundary:
    def __init__(self, events):
        self.calls = []
        self.events = events
        self.definition = None

    def update_definition(self, definition, **options):
        self.calls.append("update")
        self.events.append("report_update")
        self.definition = definition

    def get_definition(self):
        self.calls.append("read")
        self.events.append("report_read")
        return self.definition


def prepared_project(tmp_path):
    root = project(tmp_path, False)
    report = root / "PowerBI/Reporting/Executive.Report"
    report.mkdir(parents=True)
    (root / "SemanticModel/Reporting/Reporting.tmdl").rename(
        root / "PowerBI/Reporting/Reporting.tmdl"
    )
    (root / "SemanticModel/Reporting").rmdir()
    (root / "SemanticModel").rmdir()
    (report / "definition.pbir").write_text(
        json.dumps(
            {
                "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definitionProperties/2.0.0/schema.json",
                "version": "4.0",
                "datasetReference": {"byPath": {"path": "../Reporting.SemanticModel"}},
            }
        )
    )
    (report / "definition").mkdir()
    (report / "definition/report.json").write_text("{}")
    (report / "definition/version.json").write_text('{"version":"4.0.0"}')
    repository = parse_item_repository(Location(root.as_posix()))
    workspace = Workspace(workspace="Demo")
    session = TestSession(
        workspace=workspace,
        store=FilesystemStore(),
        resolver=FabricResolver(
            workspace,
            client=InventoryClient(
                "Demo",
                [("SemanticModel", "Reporting_Dev"), ("Report", "Executive_Dev")],
            ),
        ),
    )
    events = []

    class ModelBoundary(DefinitionClient):
        def update_definition(self, definition, **options):
            events.append("model_update")
            return super().update_definition(definition, **options)

        def get_definition(self):
            events.append("model_read")
            return super().get_definition()

    model = ModelBoundary()
    from types import SimpleNamespace

    model.dataset_path = "groups/workspace/datasets/model"
    model.power_bi = SimpleNamespace(get_json=lambda path: {"value": []})
    model.definition = encode_definition(engine_model(repository))
    session.answer_semantic_model("Demo", "Reporting_Dev", model)
    report_client = ReportBoundary(events)
    session.answer_report("Demo", "Executive_Dev", report_client)
    return root, session, events, model, report_client


@weaver_test()
def test_public_catalogue_free_project_verifies_model_before_report_and_stays_eager(
    tmp_path,
):
    root, session, events, model, report = prepared_project(tmp_path)
    source = (root / "PowerBI/Reporting/Executive.Report/definition.pbir").read_bytes()
    plans = []
    execute = session.execute_mutation

    def capture(plan, payloads=None, **options):
        plans.append(plan)
        return execute(plan, payloads, **options)

    session.execute_mutation = capture
    for _ in range(2):
        events.clear()
        result = weaver.build(
            root,
            items=[
                f"{ITEM}=SemanticModel/Reporting_Dev",
                "Report/Executive=Report/Executive_Dev",
            ],
            session=session,
        )
        assert result.succeeded, result.errors
        assert events == ["model_update", "model_read", "report_update", "report_read"]
        assert not session.tsql and not session.spark_sql
    assert (
        root / "PowerBI/Reporting/Executive.Report/definition.pbir"
    ).read_bytes() == source
    actions = {
        a.executor: a
        for _, _, a in plans[0].actions()
        if a.executor != "completion_gate"
    }
    assert actions["semantic_model"].id in actions["semantic_readback"].depends_on
    assert actions["semantic_readback"].id in actions["report_definition"].depends_on
    assert actions["report_definition"].id in actions["report_readback"].depends_on
    assert report.calls == ["update", "read", "update", "read"]


@pytest.mark.parametrize(
    "failure",
    ["model_update", "model_read", "report_update", "report_read", "report_uncertain"],
)
@weaver_test()
def test_public_project_failure_blocks_dependants_and_continues_independent_reports(
    tmp_path, failure
):
    from weaver.errors import InstallError, OutcomeUnknown

    root, session, events, model, report = prepared_project(tmp_path)
    shutil.copytree(
        root / "PowerBI/Reporting/Executive.Report",
        root / "PowerBI/Reporting/Operations.Report",
    )
    session._resolver.client.items.append(("Report", "Operations_Dev"))
    sibling = ReportBoundary(events)
    session.answer_report("Demo", "Operations_Dev", sibling)

    def fail(*args, **kwargs):
        raise (
            OutcomeUnknown("lost Report submission")
            if failure == "report_uncertain"
            else InstallError("qualification refusal")
        )

    if failure == "model_update":
        model.update_definition = fail
    elif failure == "model_read":
        model.get_definition = fail
    elif failure in {"report_update", "report_uncertain"}:
        report.update_definition = fail
    else:
        report.get_definition = fail
    result = weaver.build(
        root,
        items=[
            f"{ITEM}=SemanticModel/Reporting_Dev",
            "Report/Executive=Report/Executive_Dev",
            "Report/Operations=Report/Operations_Dev",
        ],
        session=session,
    )
    assert not result.succeeded
    assert sibling.calls == ([] if failure.startswith("model") else ["update", "read"])
    assert not session.tsql and not session.spark_sql
    mapping = result.installation_report.to_mapping()
    assert (
        "lost Report submission" in str(mapping)
        if failure == "report_uncertain"
        else "qualification refusal" in str(mapping)
    )
