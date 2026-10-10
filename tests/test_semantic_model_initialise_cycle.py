"""Initialise scaffolds semantic models; Build creates them."""

from types import SimpleNamespace

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
def test_initialise_scaffolds_a_semantic_model_and_creates_no_power_bi_item(
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
    assert {r.role for r in dry.resources} == {"Catalogue", "Environment"}
    assert not list(tmp_path.iterdir())

    first = weaver.initialise(tmp_path, **arguments)

    assert not client.writes and not first.created
    workspace = load_workspace(tmp_path / "workspace-config.yml")
    assert (
        workspace.target_for(WeaverItemId.parse("SemanticModel/Reporting")).model.name
        == "Reporting"
    )
    repository = parse_item_repository(Location(tmp_path.as_posix()))
    assert WeaverItemId.parse("SemanticModel/Reporting") in repository.semantic_models
    assert (tmp_path / "PowerBI/Reporting/Reporting.tmdl").is_file()
    assert first.next_commands[1:3] == ("weaver build --item PowerBI", "weaver load")
    assert weaver.initialise(tmp_path, **arguments).succeeded
    assert not client.writes


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
