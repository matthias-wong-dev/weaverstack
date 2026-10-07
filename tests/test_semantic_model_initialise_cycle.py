"""Initialise owns typed semantic item creation and addon-only scaffolding."""

from types import SimpleNamespace

import pytest
from support.weaver_test import weaver_test
from support.workspaces import InventoryClient

import weaver
from weaver.config import load_workspace
from weaver.declaration.model import WeaverItemId
from weaver.declaration.repository import parse_item_repository
from weaver.fabric.environment_definition import (
    EXTERNAL_LIBRARIES,
    EnvironmentDefinition,
)
from weaver.locations import Location
from weaver.semantic_models.definition import decode_parts
from weaver.sessions import TestSession
from weaver_cli.main import build_parser


class CreationClient(InventoryClient):
    def __init__(self):
        super().__init__(
            "Demo", [("Warehouse", "Catalogue"), ("Environment", "Weaver")]
        )
        self.writes = []

    def request(self, method, path, **kwargs):
        self.writes.append((method, path, kwargs))
        return SimpleNamespace(status_code=201, json=lambda: {"id": "model-id"})


@weaver_test()
def test_initialise_creates_semantic_model_and_reuses_without_overwriting(
    tmp_path, monkeypatch
):
    client = CreationClient()
    monkeypatch.setattr(
        "weaver.fabric.environment.read_definition",
        lambda *a, **k: EnvironmentDefinition(
            {EXTERNAL_LIBRARIES: b"dependencies:\n  - pip:\n      - weaverstack\n"}
        ),
    )
    session = TestSession(resolver=SimpleNamespace(client=client))
    arguments = dict(
        workspace="Demo", semantic_model="Reporting", session=session, client=client
    )
    dry = weaver.initialise(tmp_path, dry_run=True, **arguments)
    assert any(
        r.role == "SemanticModel" and r.status == "planned" for r in dry.resources
    )
    assert not client.writes and not list(tmp_path.iterdir())
    first = weaver.initialise(tmp_path, **arguments)
    assert "SemanticModel/Reporting" in first.created
    assert len(client.writes) == 1
    method, path, request = client.writes[0]
    assert method == "POST" and path.endswith("/semanticModels")
    assert request["retry_transient"] is False
    parts = decode_parts(request["payload"]["definition"])
    assert (
        b"defaultPowerBIDataSourceVersion: powerBI_V3" in parts["definition/model.tmdl"]
    )
    workspace = load_workspace(tmp_path / "workspace-config.yml")
    assert (
        workspace.target_for(WeaverItemId.parse("SemanticModel/Reporting")).model.name
        == "Reporting"
    )
    repository = parse_item_repository(Location(tmp_path.as_posix()))
    assert WeaverItemId.parse("SemanticModel/Reporting") in repository.semantic_models
    assert (
        parts
        == repository.semantic_models[
            WeaverItemId.parse("SemanticModel/Reporting")
        ].parts
    )
    client.items.append(("SemanticModel", "Reporting"))
    second = weaver.initialise(tmp_path, **arguments)
    assert all(r.status == "existing" for r in second.resources)
    assert len(client.writes) == 1


@weaver_test()
def test_noninteractive_initialise_cli_accepts_semantic_only_project():
    from weaver_cli.initialise import collect, equivalent_command

    args = build_parser().parse_args(
        [
            "initialise",
            "--workspace",
            "Demo",
            "--project-folder",
            "project",
            "--semantic-model",
            "Reporting",
            "--non-interactive",
        ]
    )
    collect(args, ask=False)
    assert "--semantic-model Reporting" in equivalent_command(args)


@weaver_test()
def test_semantic_build_cli_requests_tds_and_no_lakehouse_resources():
    from weaver.sessions.requirements import AUTH, RESOLVER, TDS

    args = build_parser().parse_args(
        [
            "build",
            "project",
            "--item",
            "SemanticModel/Reporting=SemanticModel/Reporting_Dev",
        ]
    )
    assert args.requires(args) == frozenset({AUTH, RESOLVER, TDS})


@weaver_test()
def test_semantic_target_is_not_dispatched_to_warehouse_or_lakehouse_wipe():
    from test_semantic_wipe_cycle import setup

    from weaver.semantic_models.wipe import prepare_reset
    from weaver.wipe_plan import wipe_mutation_plan

    session, client = setup()
    plan = weaver.plan_wipe("SemanticModel/Reporting", session=session)
    spec = prepare_reset(client, "Reporting", preserve_data_source=False)
    mutation, payloads = wipe_mutation_plan(
        plan, semantic_wipes={"SemanticModel/Reporting": spec}
    )
    actions = [action for _, _, action in mutation.actions()]
    assert len(actions) == 1 and actions[0].executor == "semantic_wipe"
    assert actions[0].kind == "wipe_semantic_model"
    assert mutation.targets[0].kind == "semanticmodel"
    assert mutation.targets[0].item_id == client.model_id
    assert mutation.execution.spark_home_target_id is None
    assert set(payloads) == {actions[0].payload}


@weaver_test()
def test_missing_semantic_target_does_not_create_an_item(tmp_path):
    from test_semantic_model_build_cycle import project, session_for

    session = session_for()
    session.resolver().client.items = [("Warehouse", "Catalogue")]
    with pytest.raises(weaver.errors.BuildError, match="initialise"):
        weaver.build(
            project(tmp_path, False),
            items="SemanticModel/Reporting=SemanticModel/Reporting_Dev",
            session=session,
        )
    assert not any(
        path.startswith("POST") for path in session.resolver().client.requested
    )


@pytest.mark.parametrize(
    "cultures,name,expected",
    [
        ({"Finance": "en-AU", "Reporting": "en-AU"}, "DEV_Finance", "en-AU"),
        ({"Finance": "en-AU", "Reporting": "fr-FR"}, "Reporting", "fr-FR"),
        ({"Finance": "en-AU", "Reporting": "fr-FR"}, "DEV_Finance", "en-US"),
        ({}, "Reporting", "en-US"),
    ],
)
@weaver_test()
def test_new_semantic_model_starts_in_the_project_culture(
    tmp_path, monkeypatch, cultures, name, expected
):
    # Fabric rejects a definition whose culture differs from the model's.
    import shutil
    from pathlib import Path

    probe = Path(__file__).parent / "fixtures/semantic_model/Probe"
    for model, culture in cultures.items():
        folder = tmp_path / "PowerBI" / model
        shutil.copytree(probe, folder)
        (folder / "Probe.SemanticModel").rename(folder / f"{model}.SemanticModel")
        pbir = folder / "Probe.Report/definition.pbir"
        pbir.write_text(
            pbir.read_text().replace("Probe.SemanticModel", f"{model}.SemanticModel")
        )
        (folder / "Probe.Report").rename(folder / f"{model}.Report")
        pbip = folder / "Probe.pbip"
        pbip.write_text(pbip.read_text().replace("Probe.Report", f"{model}.Report"))
        definition = folder / f"{model}.SemanticModel/definition/model.tmdl"
        definition.write_bytes(
            definition.read_bytes().replace(
                b"culture: en-US", f"culture: {culture}".encode()
            )
        )
    client = CreationClient()
    monkeypatch.setattr(
        "weaver.fabric.environment.read_definition",
        lambda *a, **k: EnvironmentDefinition(
            {EXTERNAL_LIBRARIES: b"dependencies:\n  - pip:\n      - weaverstack\n"}
        ),
    )
    session = TestSession(resolver=SimpleNamespace(client=client))
    weaver.initialise(
        tmp_path, workspace="Demo", semantic_model=name, session=session, client=client
    )
    (write,) = [w for w in client.writes if w[1].endswith("/semanticModels")]
    model = decode_parts(write[2]["payload"]["definition"])["definition/model.tmdl"]
    assert f"\tculture: {expected}\n".encode() in model
