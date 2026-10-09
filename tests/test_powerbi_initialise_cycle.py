from types import SimpleNamespace

from support.weaver_test import weaver_test
from test_powerbi_project_declaration import write
from test_semantic_composition_build_cycle import variant_project
from test_semantic_model_initialise_cycle import CreationClient

import weaver
from weaver.fabric.environment_definition import (
    EXTERNAL_LIBRARIES,
    EnvironmentDefinition,
)
from weaver.sessions import TestSession


@weaver_test()
def test_initialise_existing_named_project_expands_models_reports_and_actual_targets(
    tmp_path, monkeypatch
):
    variant_project(tmp_path)
    write(
        tmp_path,
        "workspace-config.yml",
        "workspace: Demo\nenvironment: Weaver\ncatalogue: Warehouse/Catalogue\ntargets:\n  SemanticModel/Normal: Normal_Dev\n  SemanticModel/Executive: Executive_Dev\n  SemanticModel/Public: Public_Dev\n  Report/Executive: Executive_Report_Dev\n  Report/Public: Public_Report_Dev\n",
    )
    before = {
        p.relative_to(tmp_path).as_posix(): p.read_bytes()
        for p in (tmp_path / "PowerBI").rglob("*")
        if p.is_file()
    }
    client = CreationClient()
    client.items.extend(
        [
            ("SemanticModel", "Normal_Dev"),
            ("SemanticModel", "Executive_Dev"),
            ("SemanticModel", "Public_Dev"),
            ("Report", "Executive_Report_Dev"),
            ("Report", "Public_Report_Dev"),
        ]
    )
    monkeypatch.setattr(
        "weaver.fabric.environment.read_definition",
        lambda *a, **k: EnvironmentDefinition(
            {EXTERNAL_LIBRARIES: b"dependencies:\n  - pip:\n      - weaverstack\n"}
        ),
    )
    session = TestSession(resolver=SimpleNamespace(client=client))
    dry = weaver.initialise(
        tmp_path, workspace="Demo", session=session, client=client, dry_run=True
    )
    assert {r.role for r in dry.resources} == {"Catalogue", "Environment"}
    actual = weaver.initialise(
        tmp_path, workspace="Demo", session=session, client=client
    )
    assert actual.succeeded
    assert not client.writes
    assert before == {
        p.relative_to(tmp_path).as_posix(): p.read_bytes()
        for p in (tmp_path / "PowerBI").rglob("*")
        if p.is_file()
    }


@weaver_test()
def test_initialise_builds_sources_then_power_bi_items(tmp_path, monkeypatch):
    import yaml

    from weaver_cli.workflow import load_workflow

    variant_project(tmp_path)
    client = CreationClient()
    monkeypatch.setattr(
        "weaver.fabric.environment.read_definition",
        lambda *a, **k: EnvironmentDefinition(
            {EXTERNAL_LIBRARIES: b"dependencies:\n  - pip:\n      - weaverstack\n"}
        ),
    )
    result = weaver.initialise(
        tmp_path,
        workspace="Demo",
        lakehouse="Landing",
        session=TestSession(resolver=SimpleNamespace(client=client)),
        client=client,
    )

    builds = ["build --item Lakehouse", "build --item PowerBI"]
    assert load_workflow("full", file=tmp_path / "workflow.yml")[0] == [
        *builds,
        "load",
        "test",
        "health",
    ]
    assert load_workflow("build-only", file=tmp_path / "workflow.yml")[0] == builds
    assert result.next_commands[1:3] == tuple(f"weaver {b}" for b in builds)
    assert "weaver build --item PowerBI" in (tmp_path / "README.md").read_text()
    assert [w[1].rsplit("/", 1)[1] for w in client.writes] == ["lakehouses"]
    assert (
        yaml.safe_load((tmp_path / "workspace-config.yml").read_text())["targets"][
            "Report/Executive"
        ]
        == "Executive"
    )


@weaver_test()
def test_noninteractive_initialise_cli_accepts_existing_named_powerbi_project(tmp_path):
    from weaver_cli.initialise import collect
    from weaver_cli.main import build_parser

    variant_project(tmp_path)
    args = build_parser().parse_args(
        [
            "initialise",
            "--workspace",
            "Demo",
            "--project-folder",
            str(tmp_path),
            "--non-interactive",
        ]
    )
    assert collect(args, ask=False) is False


@weaver_test()
def test_initialise_adoption_adds_missing_targets_and_keeps_configured_estate(
    tmp_path, monkeypatch
):
    from weaver.config import load_workspace
    from weaver.declaration.model import WeaverItemId

    write(tmp_path, "PowerBI/Sales/Normal.tmdl", "model Model\n")
    write(
        tmp_path,
        "PowerBI/Sales/Executive.tmdl",
        "model Model\n\tannotation Weaver.BaseSemanticModels = Normal\n",
    )
    write(
        tmp_path,
        "workspace-config.yml",
        "workspace: Demo\ncatalogue: Warehouse/State\nenvironment: Runtime\ntargets:\n  SemanticModel/Normal: Normal_Dev\n",
    )
    client = CreationClient()
    client.items = [
        ("Warehouse", "State"),
        ("Environment", "Runtime"),
        ("SemanticModel", "Normal_Dev"),
        ("SemanticModel", "Executive"),
    ]
    monkeypatch.setattr(
        "weaver.fabric.environment.read_definition",
        lambda *a, **k: EnvironmentDefinition(
            {EXTERNAL_LIBRARIES: b"dependencies:\n  - pip:\n      - weaverstack\n"}
        ),
    )
    result = weaver.initialise(
        tmp_path,
        workspace="Demo",
        session=TestSession(resolver=SimpleNamespace(client=client)),
        client=client,
    )
    assert result.succeeded
    assert not client.writes
    configured = load_workspace(tmp_path / "workspace-config.yml")
    assert configured.catalogue == "Warehouse/State"
    assert configured.environment.name == "Runtime"
    assert (
        configured.targets[WeaverItemId("SemanticModel", "Normal")].physical
        == "Normal_Dev"
    )
    assert (
        configured.targets[WeaverItemId("SemanticModel", "Executive")].physical
        == "Executive"
    )
