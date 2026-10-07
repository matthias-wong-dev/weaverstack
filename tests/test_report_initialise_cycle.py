from dataclasses import replace
from types import SimpleNamespace

import pytest
from support.weaver_test import weaver_test
from test_report_definition_representation import BINDING, contribution
from test_semantic_model_initialise_cycle import CreationClient

import weaver
from weaver.fabric.environment_definition import (
    EXTERNAL_LIBRARIES,
    EnvironmentDefinition,
)
from weaver.report_definition import encode_report
from weaver.sessions import TestSession


@weaver_test()
def test_initialise_provisions_two_complete_bound_reports_and_reuses_typed_items(
    tmp_path, monkeypatch
):
    client = CreationClient()
    client.items.extend([("SemanticModel", "Reporting"), ("Report", "Operations")])
    monkeypatch.setattr(
        "weaver.fabric.environment.read_definition",
        lambda *a, **k: EnvironmentDefinition(
            {EXTERNAL_LIBRARIES: b"dependencies:\n  - pip:\n      - weaverstack\n"}
        ),
    )
    session = TestSession(resolver=SimpleNamespace(client=client))
    definition = encode_report(replace(contribution(), binding=BINDING))
    options = dict(
        workspace="Demo",
        semantic_model="Reporting",
        reports={"Executive": definition, "Operations": definition},
        session=session,
        client=client,
    )
    dry = weaver.initialise(tmp_path, dry_run=True, **options)
    assert [(r.name, r.status) for r in dry.resources if r.role == "Report"] == [
        ("Executive", "planned"),
        ("Operations", "existing"),
    ]
    assert not client.writes
    actual = weaver.initialise(tmp_path, **options)
    assert "Report/Executive" in actual.created
    assert client.writes == [
        (
            "POST",
            client.writes[0][1],
            {
                "payload": {"displayName": "Executive", "definition": definition},
                "expected": (201, 202),
                "retry_transient": False,
            },
        )
    ]
    assert client.writes[0][1].endswith("/reports")
    client.items.append(("Report", "Executive"))
    assert all(
        r.status == "existing" for r in weaver.initialise(tmp_path, **options).resources
    )
    assert len(client.writes) == 1


@weaver_test()
def test_initialise_refuses_incomplete_report_before_remote_or_local_mutation(tmp_path):
    from weaver.errors import ConfigError

    client = CreationClient()
    with pytest.raises(ConfigError, match="Report"):
        weaver.initialise(
            tmp_path,
            workspace="Demo",
            semantic_model="Reporting",
            reports={"Executive": {"parts": []}},
            client=client,
            session=TestSession(resolver=SimpleNamespace(client=client)),
        )
    assert not client.requested and not client.writes
    assert not list(tmp_path.iterdir())
