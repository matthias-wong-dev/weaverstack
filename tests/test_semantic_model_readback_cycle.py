"""Post-update certification verifies requested semantic values and removals."""

import pytest
from support.weaver_test import weaver_test
from test_semantic_model_build_cycle import (
    ITEM,
    bundle_for,
    engine_model,
    prepared,
)

from weaver.build_bundle.execution_plan import execute_bundle
from weaver.declaration.repository import parse_item_repository
from weaver.locations import Location
from weaver.semantic_models.definition import encode_definition


@weaver_test()
def test_role_members_must_be_empty_before_build_can_certify_removal(tmp_path):
    root, _, bindings, session, state = prepared(tmp_path)
    addon = root / str(ITEM) / "extension.tmdl"
    addon.write_text(
        addon.read_text() + "\nrole Reader\n\tmodelPermission: read\n",
        encoding="utf-8",
    )
    repository = parse_item_repository(Location(root.as_posix()))
    deployed = engine_model(repository)
    deployed["model"]["roles"] = [{"name": "Reader", "modelPermission": "read"}]
    deployed["model"]["roles"][0]["members"] = [
        {"memberName": "reader@example.invalid", "identityProvider": "AzureAD"}
    ]
    session.semantic_model("Reporting_Dev").definition = encode_definition(deployed)
    bundle = bundle_for(tmp_path, repository, bindings, state, "remove-members")
    session.calls.clear()
    report = execute_bundle(bundle, session)
    assert not report.succeeded
    assert any(
        "members" in (action.error_message or "") for action in report.action_results()
    )
    assert not any(
        "MERGE" in statement
        and ("[_].[Registry]" in statement or "[_].[SemanticModel]" in statement)
        for statement in session.tsql
    )


@pytest.mark.parametrize(
    "owner,property_name,stale_value",
    [
        ("model", "discourageImplicitMeasures", True),
        ("table", "isHidden", True),
        ("table", "description", "Removed description"),
    ],
)
@weaver_test()
def test_removed_writable_property_prevents_certification(
    tmp_path, owner, property_name, stale_value
):
    _, repository, bindings, session, state = prepared(tmp_path)
    deployed = engine_model(repository)
    properties = (
        deployed["model"] if owner == "model" else deployed["model"]["tables"][0]
    )
    assert property_name not in properties
    properties[property_name] = stale_value
    session.semantic_model("Reporting_Dev").definition = encode_definition(deployed)
    bundle = bundle_for(tmp_path, repository, bindings, state, "remove-property")
    report = execute_bundle(bundle, session)
    assert not report.succeeded
    assert any(
        property_name in (action.error_message or "")
        for action in report.action_results()
    )
    assert not any(
        "MERGE" in statement
        and ("[_].[Registry]" in statement or "[_].[SemanticModel]" in statement)
        for statement in session.tsql
    )


@pytest.mark.parametrize("field", ["description", "value", "filterExpression"])
@weaver_test()
def test_multiline_text_equivalence_allows_build_certification(tmp_path, field):
    root, _, bindings, session, state = prepared(tmp_path)
    lines = ["Calendar[Year] > 2020", "  && Calendar[Year] < 2030"]
    text = "\n".join(lines)
    description_lines = ["Calendar[Year] > 2020", "&& Calendar[Year] < 2030"]
    extension = root / str(ITEM) / "extension.tmdl"
    extension.write_text(
        extension.read_text().replace(
            "table Calendar",
            "/// Calendar[Year] > 2020\n/// && Calendar[Year] < 2030\ntable Calendar",
            1,
        )
        + "\nannotation Note = ```\n\tCalendar[Year] > 2020\n\t  && Calendar[Year] < 2030\n\t```\n"
        + "\nrole Reader\n\tmodelPermission: read\n\ttablePermission Calendar = ```\n\t\tCalendar[Year] > 2020\n\t\t  && Calendar[Year] < 2030\n\t\t```\n",
        encoding="utf-8",
    )
    repository = parse_item_repository(Location(root.as_posix()))
    deployed = engine_model(repository)
    deployed["model"]["tables"][0]["description"] = "\n".join(description_lines)
    deployed["model"]["annotations"] = [{"name": "Note", "value": text}]
    deployed["model"]["roles"] = [
        {
            "name": "Reader",
            "modelPermission": "read",
            "tablePermissions": [{"name": "Calendar", "filterExpression": text}],
        }
    ]
    owner = {
        "description": deployed["model"]["tables"][0],
        "value": deployed["model"]["annotations"][0],
        "filterExpression": deployed["model"]["roles"][0]["tablePermissions"][0],
    }[field]
    owner[field] = description_lines if field == "description" else lines
    session.semantic_model("Reporting_Dev").definition = encode_definition(deployed)
    bundle = bundle_for(tmp_path, repository, bindings, state, "text-equivalence")
    report = execute_bundle(bundle, session)
    assert report.succeeded, report.to_mapping()
    assert any(
        "MERGE" in statement and "[_].[SemanticModel]" in statement
        for statement in session.tsql
    )
