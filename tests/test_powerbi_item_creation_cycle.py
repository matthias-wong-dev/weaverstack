"""Build creates missing SemanticModel and Report targets, and nothing else."""

import json

import pytest
from support.weaver_test import weaver_test
from support.workspaces import CreatingInventoryClient, _identifier
from test_report_build_cycle import ReportBoundary, prepared_project
from test_semantic_model_build_cycle import ITEM, engine_model, project, session_for

import weaver
from weaver.declaration.repository import parse_item_repository
from weaver.errors import BuildError
from weaver.locations import Location
from weaver.report_definition import decode_report
from weaver.semantic_models.definition import decode_parts, encode_definition

SELECTION = [
    f"{ITEM}=SemanticModel/Reporting_Dev",
    "Report/Executive=Report/Executive_Dev",
]


def _bound_model(definition) -> str:
    return json.loads(decode_report(definition)["definition.pbir"])["datasetReference"][
        "byConnection"
    ]["connectionString"]


@pytest.mark.parametrize("position", ["desktop", "fabric"])
@weaver_test()
def test_build_creates_a_missing_model_then_its_report_and_deploys_both(
    tmp_path, monkeypatch, position
):
    root, session, events, _model, _report = prepared_project(tmp_path)
    client = CreatingInventoryClient("Demo", [])
    session._resolver.client = client
    if position == "fabric":
        monkeypatch.setattr(
            "weaver.operations.build._inside_fabric_session", lambda workspace: True
        )

    result = weaver.build(root, items=SELECTION, session=session)

    assert result.succeeded, result.errors
    assert [(kind, name) for kind, name, _ in client.created] == [
        ("SemanticModel", "Reporting_Dev"),
        ("Report", "Executive_Dev"),
    ]
    assert _identifier("SemanticModel", "Reporting_Dev") in _bound_model(
        client.created[1][2]
    )
    assert events == ["model_update", "model_read", "report_update", "report_read"]

    client.created.clear()
    assert weaver.build(root, items=SELECTION, session=session).succeeded
    assert not client.created


@weaver_test()
def test_build_with_a_catalogue_creates_a_missing_model(tmp_path):
    session = session_for()
    client = CreatingInventoryClient(
        "Demo", [("Warehouse", "Catalogue"), ("Warehouse", "Reporting_Dev")]
    )
    session._resolver.client = client
    root = project(tmp_path, False)
    session.semantic_model("Reporting_Dev").definition = encode_definition(
        engine_model(parse_item_repository(Location(root.as_posix())))
    )

    result = weaver.build(
        root,
        items="SemanticModel/Reporting=SemanticModel/Reporting_Dev",
        session=session,
    )

    assert result.succeeded, result.errors
    assert [(kind, name) for kind, name, _ in client.created] == [
        ("SemanticModel", "Reporting_Dev")
    ]


@weaver_test()
def test_a_new_model_starts_in_its_own_culture(tmp_path, monkeypatch):
    # Fabric fixes a model's culture at creation and refuses a definition in another.
    import builtins

    from test_semantic_composition_build_cycle import variant_project, variant_session

    monkeypatch.setattr(builtins, "_weaver_composition_calls", [], raising=False)
    variant_project(tmp_path)
    session, *_ = variant_session()
    client = CreatingInventoryClient("Demo", [])
    session._resolver.client = client

    result = weaver.build(tmp_path, items="PowerBI", session=session)

    assert result.succeeded, result.errors
    assert [(kind, name) for kind, name, _ in client.created] == [
        ("SemanticModel", "Executive_Dev"),
        ("SemanticModel", "Normal_Dev"),
        ("SemanticModel", "Public_Dev"),
        ("Report", "Executive_Report_Dev"),
        ("Report", "Public_Report_Dev"),
    ]
    cultures = {
        name: decode_parts(definition)["definition/model.tmdl"]
        for kind, name, definition in client.created
        if kind == "SemanticModel"
    }
    for name, culture in (
        ("Executive_Dev", "en-GB"),
        ("Normal_Dev", "en-AU"),
        ("Public_Dev", "en-NZ"),
    ):
        assert f"\tculture: {culture}\n".encode() in cultures[name]
    for model in ("Executive", "Public"):
        definition = next(
            d for _, name, d in client.created if name == f"{model}_Report_Dev"
        )
        assert _identifier("SemanticModel", f"{model}_Dev") in _bound_model(definition)


@weaver_test()
def test_a_report_with_a_fixed_connection_is_created_as_authored(tmp_path):
    from test_powerbi_project_declaration import write

    authored = json.dumps(
        {
            "version": "4.0",
            "datasetReference": {
                "byConnection": {
                    "connectionString": "Data Source=powerbi://api.powerbi.com/"
                    "v1.0/myorg/Shared;Initial Catalog=Shared model"
                }
            },
        }
    ).encode()
    write(tmp_path, "PowerBI/Independent/A.tmdl", "model Model\n")
    write(tmp_path, "PowerBI/Independent/B.tmdl", "model Model\n")
    write(tmp_path, "PowerBI/Independent/Analyst.Report/definition.pbir", authored)
    write(tmp_path, "PowerBI/Independent/Analyst.Report/report.json", b"{}")
    root, session, events, *_ = prepared_project(tmp_path / "other")
    client = CreatingInventoryClient("Demo", [("SemanticModel", "A")])
    session._resolver.client = client
    session.answer_report("Demo", "Analyst", ReportBoundary(events))

    result = weaver.build(
        tmp_path, items="Report/Analyst=Report/Analyst", session=session
    )

    assert result.succeeded, result.errors

    ((kind, name, definition),) = client.created
    assert (kind, name) == ("Report", "Analyst")
    assert decode_report(definition) == {
        "definition.pbir": authored,
        "report.json": b"{}",
    }


@weaver_test()
def test_a_bundle_only_build_creates_nothing_and_says_build_creates_it(tmp_path):
    root, session, *_ = prepared_project(tmp_path)
    client = CreatingInventoryClient("Demo", [("Report", "Executive_Dev")])
    session._resolver.client = client

    with pytest.raises(
        BuildError,
        match="SemanticModel 'Reporting_Dev' does not exist in 'Demo' yet. "
        "weaver build creates it",
    ):
        weaver.build(root, items=SELECTION, session=session, bundle_only=True)
    assert not client.created


@weaver_test()
def test_a_missing_report_is_created_only_with_its_model(tmp_path):
    root, session, *_ = prepared_project(tmp_path)
    client = CreatingInventoryClient("Demo", [("SemanticModel", "Reporting_Dev")])
    session._resolver.client = client

    with pytest.raises(
        BuildError, match="Select SemanticModel/Reporting in the same Build"
    ):
        weaver.build(root, items=SELECTION[1:], session=session)
    assert not client.created


@weaver_test()
def test_a_report_may_share_its_models_name(tmp_path):
    root, session, *_ = prepared_project(tmp_path)
    client = CreatingInventoryClient("Demo", [("SemanticModel", "Reporting_Dev")])
    session._resolver.client = client
    session.answer_report("Demo", "Reporting_Dev", ReportBoundary([]))

    result = weaver.build(
        root,
        items=[SELECTION[0], "Report/Executive=Report/Reporting_Dev"],
        session=session,
    )

    assert result.succeeded, result.errors
    assert [(kind, name) for kind, name, _ in client.created] == [
        ("Report", "Reporting_Dev")
    ]
