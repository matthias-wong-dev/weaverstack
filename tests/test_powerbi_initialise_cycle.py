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
    assert {
        (r.role, r.name, r.status)
        for r in dry.resources
        if r.role in {"SemanticModel", "Report"}
    } == {
        (kind, name, "existing")
        for kind, name in client.items
        if kind in {"SemanticModel", "Report"}
    }
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
def test_initialise_discovered_models_precede_bound_report_creation(
    tmp_path, monkeypatch
):
    import json

    from support.workspaces import _identifier

    from weaver.report_definition import decode_report
    from weaver.semantic_models.definition import decode_parts

    variant_project(tmp_path)

    class CreatingClient(CreationClient):
        def request(self, method, path, **kwargs):
            response = super().request(method, path, **kwargs)
            kind = "SemanticModel" if path.endswith("/semanticModels") else "Report"
            self.items.append((kind, kwargs["payload"]["displayName"]))
            return response

    client = CreatingClient()
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
    assert (
        len(
            [
                r
                for r in dry.resources
                if r.role in {"SemanticModel", "Report"} and r.status == "planned"
            ]
        )
        == 5
    )
    assert not client.writes
    result = weaver.initialise(
        tmp_path, workspace="Demo", session=session, client=client
    )
    assert result.succeeded
    assert [w[2]["payload"]["displayName"] for w in client.writes] == [
        "Executive",
        "Normal",
        "Public",
        "Executive",
        "Public",
    ]
    cultures = {"Executive": "en-GB", "Normal": "en-AU", "Public": "en-NZ"}
    for _, path, options in client.writes:
        name = options["payload"]["displayName"]
        definition = options["payload"]["definition"]
        if path.endswith("/semanticModels"):
            assert (
                f"culture: {cultures[name]}".encode()
                in decode_parts(definition)["definition/model.tmdl"]
            )
        else:
            binding = json.loads(decode_report(definition)["definition.pbir"])[
                "datasetReference"
            ]["byConnection"]["connectionString"]
            assert _identifier("SemanticModel", name) in binding
    assert not any(
        p.parent.name == name
        for name in ("Executive", "Normal", "Public")
        for p in (tmp_path / "PowerBI").glob("*/*.tmdl")
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
def test_initialise_as_authored_report_has_no_binding_requirement(
    tmp_path, monkeypatch
):
    import json

    from weaver.report_definition import decode_report

    write(tmp_path, "PowerBI/Independent/A.tmdl", "model Model\n")
    write(tmp_path, "PowerBI/Independent/B.tmdl", "model Model\n")
    authored = json.dumps(
        {
            "version": "4.0",
            "datasetReference": {"byPath": {"path": "../A.SemanticModel"}},
        }
    ).encode()
    write(tmp_path, "PowerBI/Independent/Analyst.Report/definition.pbir", authored)
    write(tmp_path, "PowerBI/Independent/Analyst.Report/report.json", b"{}\r\n")
    client = CreationClient()
    client.items.extend([("SemanticModel", "A"), ("SemanticModel", "B")])
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
    (request,) = client.writes
    assert request[1].endswith("/reports")
    assert decode_report(request[2]["payload"]["definition"]) == {
        "definition.pbir": authored,
        "report.json": b"{}\r\n",
    }


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
