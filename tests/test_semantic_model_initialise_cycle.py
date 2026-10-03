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
from weaver.semantic_models.definition import decode_model
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
    assert (
        decode_model(request["payload"]["definition"])["model"][
            "defaultPowerBIDataSourceVersion"
        ]
        == "powerBI_V3"
    )
    workspace = load_workspace(tmp_path / "workspace-config.yml")
    assert (
        workspace.target_for(WeaverItemId.parse("SemanticModel/Reporting")).model.name
        == "Reporting"
    )
    repository = parse_item_repository(Location(tmp_path.as_posix()))
    assert WeaverItemId.parse("SemanticModel/Reporting") in repository.semantic_models
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
def test_semantic_target_is_not_dispatched_to_physical_wipe():
    from weaver.errors import CommandError
    from weaver.operations.wipe import WipeTarget

    with pytest.raises(CommandError, match="SemanticModel.*not supported"):
        WipeTarget.parse("SemanticModel/Reporting")


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
