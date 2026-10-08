import json
from dataclasses import replace

import pytest
from support.weaver_test import weaver_test

from weaver.declaration.model import WeaverItemId
from weaver.powerbi import ReportContribution

WORKSPACE = "11111111-2222-3333-4444-555555555555"
MODEL = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
BINDING = {"workspace_id": WORKSPACE, "item_id": MODEL}


def contribution(schema="2.0.0"):
    return ReportContribution(
        "PowerBI/Sales/Executive.Report",
        WeaverItemId("SemanticModel", "Revenue"),
        {
            "definition.pbir": json.dumps(
                {
                    "$schema": f"https://developer.microsoft.com/json-schemas/fabric/item/report/definitionProperties/{schema}/schema.json",
                    "version": "4.0",
                    "datasetReference": {
                        "byPath": {"path": "../Revenue.SemanticModel"}
                    },
                }
            ).encode(),
            "definition/version.json": b'{"version":"4.0.0"}',
            "definition/report.json": b'{"themeCollection":{}}\r\n',
            "StaticResources/theme.json": b"\x00\xff\r\n",
            ".platform": b'{"metadata":{"displayName":"Executive"}}',
        },
    )


@pytest.mark.parametrize("schema", ["1.0.0", "2.0.0"])
@weaver_test()
def test_report_binding_changes_only_effective_definition_bytes(schema):
    from weaver.report_definition import decode_report, encode_report

    authored = contribution(schema)
    before = dict(authored.parts)
    bound = replace(authored, binding=BINDING)
    parts = decode_report(encode_report(bound))
    assert authored.parts == before
    assert bound.source_signature == authored.source_signature
    assert bound.signature != authored.signature
    assert {p: b for p, b in parts.items() if p != "definition.pbir"} == {
        p: b for p, b in before.items() if p != "definition.pbir"
    }
    reference = json.loads(parts["definition.pbir"])["datasetReference"]
    assert set(reference) == {"byConnection"}
    connection = reference["byConnection"]
    expected_connection = f"Data Source=powerbi://api.powerbi.com/v1.0/myorg/{WORKSPACE};Initial Catalog={MODEL};"
    if schema == "2.0.0":
        expected_connection += f"semanticModelId={MODEL};"
    assert connection["connectionString"] == expected_connection
    if schema == "2.0.0":
        assert set(connection) == {"connectionString"}
    else:
        assert connection["pbiModelDatabaseName"] == MODEL
        assert connection["connectionType"] == "pbiServiceXmlaStyleLive"


@weaver_test()
def test_modern_report_binding_carries_explicit_service_model_id():
    from weaver.report_definition import decode_report, encode_report

    parts = decode_report(encode_report(replace(contribution(), binding=BINDING)))
    connection = json.loads(parts["definition.pbir"])["datasetReference"][
        "byConnection"
    ]
    parameters = dict(
        field.split("=", 1)
        for field in connection["connectionString"].split(";")
        if field
    )
    assert parameters["semanticModelId"] == MODEL
    assert set(connection) == {"connectionString"}


@weaver_test()
def test_bound_report_recertifies_after_service_binding_encoder_change():
    from weaver.semantic_models.compiler import content_signature

    authored = contribution()
    bound = replace(authored, binding=BINDING)
    previous = content_signature(
        {"compiler": 1, "source": bound.source_signature, "binding": BINDING}
    )
    assert bound.signature != previous
    assert authored.signature == content_signature(
        {"compiler": 1, "source": authored.source_signature, "binding": None}
    )


@weaver_test()
def test_report_verification_rejects_wrong_binding_and_changed_resource():
    from weaver.errors import InstallError
    from weaver.report_definition import encode_report, verify_report

    bound = replace(contribution(), binding=BINDING)
    desired = encode_report(bound)
    verify_report(desired, desired)
    changed = json.loads(json.dumps(desired))
    changed["parts"][-1]["payload"] = "e30="
    with pytest.raises(InstallError, match="Report"):
        verify_report(desired, changed)
    other = replace(
        bound, binding={**BINDING, "item_id": "ffffffff-bbbb-cccc-dddd-eeeeeeeeeeee"}
    )
    with pytest.raises(InstallError, match="binding"):
        verify_report(desired, encode_report(other))
