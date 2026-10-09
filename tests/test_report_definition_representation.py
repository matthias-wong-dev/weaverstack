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
def test_report_verification_rejects_a_wrong_binding_and_reports_a_changed_part():
    from weaver.errors import InstallError
    from weaver.report_definition import encode_report, verify_report

    bound = replace(contribution(), binding=BINDING)
    desired = encode_report(bound)
    assert verify_report(desired, desired) == ()
    changed = json.loads(json.dumps(desired))
    changed["parts"][-1]["payload"] = "e30="
    assert verify_report(desired, changed) == ("definition/version.json changed",)
    other = replace(
        bound, binding={**BINDING, "item_id": "ffffffff-bbbb-cccc-dddd-eeeeeeeeeeee"}
    )
    with pytest.raises(InstallError, match="binding"):
        verify_report(desired, encode_report(other))


def service_normalized_report(desired):
    import base64

    observed = json.loads(json.dumps(desired))
    for part in observed["parts"]:
        if part["path"] == "definition.pbir":
            props = json.loads(base64.b64decode(part["payload"]))
            props["datasetReference"] = {
                "byConnection": {
                    "connectionString": f"Data Source=pbiazure://api.powerbi.com;Initial Catalog=ServiceModel;Integrated Security=ClaimsToken;semanticModelId={MODEL};"
                }
            }
            part["payload"] = base64.b64encode(json.dumps(props).encode()).decode()
        elif part["path"] == ".platform":
            props = json.loads(base64.b64decode(part["payload"]))
            props["metadata"]["displayName"] = "Executive_Dev"
            props["config"]["logicalId"] = "00000000-0000-0000-0000-000000000000"
            part["payload"] = base64.b64encode(json.dumps(props).encode()).decode()
    return observed


@weaver_test()
def test_bound_report_readback_accepts_service_normalization_with_verified_identity():
    from weaver.report_definition import encode_report, verify_report

    authored = contribution()
    authored.parts[".platform"] = json.dumps(
        {
            "metadata": {"type": "Report", "displayName": "Executive"},
            "config": {"version": "2.0", "logicalId": MODEL},
        }
    ).encode()
    desired = encode_report(replace(authored, binding=BINDING))
    observed = service_normalized_report(desired)
    assert (
        verify_report(
            desired,
            observed,
            binding=BINDING,
            service_binding={"datasetId": MODEL, "datasetWorkspaceId": WORKSPACE},
        )
        == ()
    )


@pytest.mark.parametrize(
    "change, outcome",
    [
        ("model", None),
        ("workspace", None),
        ("missing-workspace", None),
        ("connection-model", None),
        ("resource", ("StaticResources/theme.json changed",)),
        ("platform-type", (".platform changed",)),
        ("platform-name", ()),
        ("platform-logical-id", ()),
        ("pbir-version", ("definition.pbir changed",)),
        ("as-authored", ("definition.pbir datasetReference changed",)),
    ],
)
@weaver_test()
def test_service_normalization_keeps_binding_strict_and_reports_native_changes(
    change, outcome
):
    import base64

    from weaver.errors import InstallError
    from weaver.report_definition import encode_report, verify_report

    authored = contribution()
    authored = replace(
        authored,
        parts={
            **authored.parts,
            ".platform": json.dumps(
                {
                    "metadata": {"type": "Report", "displayName": "Executive"},
                    "config": {"version": "2.0", "logicalId": MODEL},
                }
            ).encode(),
        },
    )
    desired = encode_report(replace(authored, binding=BINDING))
    observed = service_normalized_report(desired)
    identity = {"datasetId": MODEL, "datasetWorkspaceId": WORKSPACE}
    wrong = "ffffffff-bbbb-cccc-dddd-eeeeeeeeeeee"
    if change == "model":
        identity["datasetId"] = wrong
    elif change == "workspace":
        identity["datasetWorkspaceId"] = wrong
    elif change == "missing-workspace":
        del identity["datasetWorkspaceId"]
    else:
        for part in observed["parts"]:
            raw = base64.b64decode(part["payload"])
            if change == "resource" and part["path"] == "StaticResources/theme.json":
                part["payload"] = base64.b64encode(b"changed").decode()
            elif change == "connection-model" and part["path"] == "definition.pbir":
                part["payload"] = base64.b64encode(
                    raw.replace(MODEL.encode(), wrong.encode())
                ).decode()
            elif change == "pbir-version" and part["path"] == "definition.pbir":
                props = json.loads(raw)
                props["version"] = "5.0"
                part["payload"] = base64.b64encode(json.dumps(props).encode()).decode()
            elif change.startswith("platform-") and part["path"] == ".platform":
                props = json.loads(raw)
                if change == "platform-type":
                    props["metadata"]["type"] = "SemanticModel"
                elif change == "platform-name":
                    props["metadata"]["displayName"] = "Wrong"
                else:
                    props["config"]["logicalId"] = wrong
                part["payload"] = base64.b64encode(json.dumps(props).encode()).decode()

    def verify():
        return verify_report(
            desired,
            observed,
            binding=None if change == "as-authored" else BINDING,
            service_binding=identity,
        )

    if outcome is None:
        with pytest.raises(InstallError, match="binding"):
            verify()
    else:
        assert verify() == outcome


@weaver_test()
def test_bound_report_trusts_fabric_binding_when_its_definition_names_no_model():
    import base64

    from weaver.report_definition import encode_report, verify_report

    desired = encode_report(replace(contribution(), binding=BINDING))
    observed = json.loads(json.dumps(desired))
    for part in observed["parts"]:
        if part["path"] == "definition.pbir":
            props = json.loads(base64.b64decode(part["payload"]))
            props["datasetReference"] = {
                "byConnection": {"connectionString": "Data Source=pbiazure://x;"}
            }
            part["payload"] = base64.b64encode(json.dumps(props).encode()).decode()
    identity = {"datasetId": MODEL, "datasetWorkspaceId": WORKSPACE}
    assert (
        verify_report(desired, observed, binding=BINDING, service_binding=identity)
        == ()
    )


@weaver_test()
def test_as_authored_report_naming_another_model_after_deployment_fails():
    import base64

    from weaver.errors import InstallError
    from weaver.report_definition import encode_report, verify_report

    authored = replace(
        contribution(),
        model=None,
        parts={
            **contribution().parts,
            "definition.pbir": json.dumps(
                {
                    "version": "4.0",
                    "datasetReference": {
                        "byConnection": {
                            "connectionString": f"Data Source=x;semanticModelId={MODEL};"
                        }
                    },
                }
            ).encode(),
        },
    )
    desired = encode_report(authored)
    observed = json.loads(json.dumps(desired))
    for part in observed["parts"]:
        if part["path"] == "definition.pbir":
            part["payload"] = base64.b64encode(
                base64.b64decode(part["payload"]).replace(
                    MODEL.encode(), b"ffffffff-bbbb-cccc-dddd-eeeeeeeeeeee"
                )
            ).decode()
    with pytest.raises(InstallError, match="authored model"):
        verify_report(desired, observed)


def as_authored_report():
    return ReportContribution(
        "PowerBI/Sales/Executive.Report",
        None,
        {
            "definition.pbir": b'{\r\n  "version": "4.0",\r\n  "datasetReference": '
            b'{"byConnection": {"connectionString": "external"}}\r\n}\r\n',
            "definition/version.json": b'{"version":"4.0.0"}\n',
            "definition/report.json": b'{\r\n  "themeCollection": {}\r\n}\r\n',
            "definition/pages/Overview/page.json": b'{"name": "Overview"}\n',
            "StaticResources/theme.json": b"\x00\xff\r\n",
            ".platform": json.dumps(
                {
                    "metadata": {"type": "Report", "displayName": "Executive"},
                    "config": {"version": "2.0", "logicalId": MODEL},
                },
                indent=2,
            ).encode()
            + b"\n",
        },
    )


def fabric_copy(desired, edit):
    import base64

    observed = json.loads(json.dumps(desired))
    for part in list(observed["parts"]):
        content = edit(part["path"], base64.b64decode(part["payload"]))
        if content is None:
            observed["parts"].remove(part)
        else:
            part["payload"] = base64.b64encode(content).decode()
    return observed


def fabric_import(path, content):
    """Fabric's JSON rewrite: compact LF output with no final newline."""
    if path == "StaticResources/theme.json":
        return content
    value = json.loads(content)
    if path == ".platform":
        value["metadata"]["displayName"] = "Executive_Dev"
        value["config"]["logicalId"] = "00000000-0000-0000-0000-000000000000"
    return json.dumps(value).encode()


@weaver_test()
def test_as_authored_report_readback_accepts_fabric_rename_and_json_rewrite():
    from weaver.report_definition import encode_report, verify_report

    desired = encode_report(as_authored_report())
    assert verify_report(desired, fabric_copy(desired, fabric_import)) == ()


@pytest.mark.parametrize(
    "rewrite",
    [
        lambda content: content.rstrip(b"\r\n"),
        lambda content: content.replace(b"\r\n", b"\n"),
        lambda content: content.replace(b"\n", b"\r\n").replace(b"\r\r", b"\r"),
    ],
    ids=["final-newline", "lf", "crlf"],
)
@weaver_test()
def test_report_readback_compares_json_parts_by_value(rewrite):
    from weaver.report_definition import encode_report, verify_report

    desired = encode_report(as_authored_report())
    assert (
        verify_report(
            desired,
            fabric_copy(
                desired,
                lambda path, content: (
                    content
                    if path == "StaticResources/theme.json"
                    else rewrite(content)
                ),
            ),
        )
        == ()
    )


@pytest.mark.parametrize(
    "path, difference",
    [
        ("definition/report.json", "changed"),
        ("definition/pages/Overview/page.json", "missing"),
        ("StaticResources/theme.json", "changed"),
        (".platform", "changed"),
    ],
)
@weaver_test()
def test_report_readback_names_each_native_part_fabric_changed(path, difference):
    from weaver.report_definition import encode_report, verify_report

    def edit(part, content):
        content = fabric_import(part, content)
        if part != path:
            return content
        if difference == "missing":
            return None
        if part == "StaticResources/theme.json":
            return content.rstrip(b"\r\n")
        if part == ".platform":
            return content.replace(b'"Report"', b'"Other"')
        return b'{"themeCollection":{"baseTheme":"Edited"}}'

    desired = encode_report(as_authored_report())
    assert verify_report(desired, fabric_copy(desired, edit)) == (
        f"{path} {difference}",
    )


@weaver_test()
def test_report_readback_names_an_extra_part():
    import base64

    from weaver.report_definition import encode_report, verify_report

    desired = encode_report(as_authored_report())
    observed = json.loads(json.dumps(desired))
    observed["parts"].append(
        {
            "path": "definition/pages/extra.json",
            "payloadType": "InlineBase64",
            "payload": base64.b64encode(b"{}").decode(),
        }
    )
    assert verify_report(desired, observed) == ("definition/pages/extra.json extra",)


@weaver_test()
def test_report_readback_reports_a_service_format_upgrade():
    import base64

    from weaver.report_definition import encode_report, verify_report

    legacy = ReportContribution(
        "PowerBI/Sales/Executive.Report",
        None,
        {
            "definition.pbir": b'{"version": "4.0", "datasetReference": {}}',
            "report.json": b'{"sections": []}',
        },
    )
    desired = encode_report(legacy)
    upgraded = {
        "format": "PBIR",
        "parts": [
            {
                "path": path,
                "payloadType": "InlineBase64",
                "payload": base64.b64encode(content).decode(),
            }
            for path, content in (
                ("definition.pbir", b'{"version": "4.0", "datasetReference": {}}'),
                ("definition/report.json", b"{}"),
                ("definition/version.json", b'{"version": "2.0.0"}'),
            )
        ],
    }
    assert verify_report(desired, upgraded) == (
        "definition/report.json extra",
        "definition/version.json extra",
        "report.json missing",
    )


def fabric_layout(content):
    return json.dumps(json.loads(content), separators=(",", ":")).encode()


@weaver_test()
def test_report_signature_ignores_json_layout_but_not_json_values():
    authored = as_authored_report()
    relaid = replace(
        authored,
        parts={
            p: b if p == "StaticResources/theme.json" else fabric_layout(b)
            for p, b in authored.parts.items()
        },
    )
    assert relaid.source_signature == authored.source_signature
    edited = replace(
        authored,
        parts={**authored.parts, "definition/report.json": b'{"themeCollection":[]}'},
    )
    assert edited.source_signature != authored.source_signature
    resource = replace(
        authored,
        parts={**authored.parts, "StaticResources/theme.json": b"\x00\xff\n"},
    )
    assert resource.source_signature != authored.source_signature


@weaver_test()
def test_as_authored_report_verifies_under_a_different_physical_name():
    import base64

    from weaver.report_definition import encode_report, verify_report

    authored = replace(
        contribution(),
        model=None,
        parts={
            **contribution().parts,
            ".platform": json.dumps(
                {
                    "metadata": {"type": "Report", "displayName": "Executive"},
                    "config": {"version": "2.0", "logicalId": MODEL},
                }
            ).encode(),
        },
    )
    desired = encode_report(authored)
    observed = json.loads(json.dumps(desired))
    for part in observed["parts"]:
        if part["path"] == ".platform":
            props = json.loads(base64.b64decode(part["payload"]))
            props["metadata"]["displayName"] = "Executive_Dev"
            props["config"]["logicalId"] = "00000000-0000-0000-0000-000000000000"
            part["payload"] = base64.b64encode(json.dumps(props).encode()).decode()
    assert verify_report(desired, observed) == ()
