"""Creation policy is common; schema features remain table-specific."""

from __future__ import annotations

import pytest
from support.sessions import given_installer
from support.weaver_test import weaver_test

from weaver.sessions.delta_profile import compile_delta_profile_v1
from weaver.sessions.direct_delta import direct_profile_supported


@weaver_test()
def test_explicit_legacy_minima_override_creation_defaults_for_an_ordinary_table():
    profile = compile_delta_profile_v1(
        (("Value", "string", False),),
        protocol_minima={"minReaderVersion": 2, "minWriterVersion": 5},
    )
    assert profile.protocol == {"minReaderVersion": 2, "minWriterVersion": 5}


@weaver_test()
@pytest.mark.parametrize("type_name", ["variant", "timestamp_ntz"])
def test_supported_feature_types_are_eligible_for_direct_creation(type_name):
    assert direct_profile_supported((("Value", type_name, False),), None, True)


@weaver_test()
def test_timestamp_without_timezone_declares_its_reader_and_writer_feature():
    profile = compile_delta_profile_v1((("WrittenAt", "timestamp_ntz", False),))
    assert profile.schema["fields"][0]["type"] == "timestamp_ntz"
    assert set(profile.protocol["readerFeatures"]) == {"columnMapping", "timestampNtz"}
    assert set(profile.protocol["writerFeatures"]) == {"columnMapping", "timestampNtz"}


@weaver_test()
@pytest.mark.parametrize("identity", [None, "Id"])
def test_all_new_tables_use_the_common_reader_three_writer_seven_floor(identity):
    profile = compile_delta_profile_v1(
        (("Id", "bigint", True), ("Value", "string", False)),
        identity_column=identity,
    )
    assert profile.protocol["minReaderVersion"] == 3
    assert profile.protocol["minWriterVersion"] == 7
    assert set(profile.protocol["readerFeatures"]) == {"columnMapping"}
    assert set(profile.protocol["writerFeatures"]) == (
        {"columnMapping", "invariants"} | ({"identityColumns"} if identity else set())
    )


@weaver_test()
@pytest.mark.parametrize("route", ["direct", "spark"])
@pytest.mark.parametrize(
    "minima",
    [
        None,
        {"minReaderVersion": 2, "minWriterVersion": 5},
        {"minReaderVersion": 3, "minWriterVersion": 7},
    ],
)
def test_bound_installer_creator_forwards_frozen_protocol_minima(route, minima):
    installer = given_installer()
    bound = installer.bind(installer.session.workspace)
    creator = (
        bound.direct_delta_table_creator()
        if route == "direct"
        else bound.delta_table_creator()
    )
    creator(
        "`Demo`.`Sales_LH`.`DWG`.`Value`",
        (("Value", "string", False),),
        protocol_minima=minima,
    )
    call = installer.session.calls[-1]
    assert call.kind == "delta_table"
    if minima is None:
        assert "protocol_minima" not in call.body
    else:
        assert call.body["protocol_minima"] == minima


@weaver_test()
@pytest.mark.parametrize("route", ["direct", "spark"])
def test_bound_installer_creator_preserves_legacy_session_keyword_shape(route):
    installer = given_installer()
    seen = []

    def legacy_create(
        name, columns, *, identity_column=None, column_mapping=True, workspace=None
    ):
        seen.append((name, columns, identity_column, column_mapping, workspace))

    if route == "direct":
        installer.session.create_direct_delta_table = legacy_create
    else:
        installer.session.create_delta_table = legacy_create
    bound = installer.bind(installer.session.workspace)
    creator = (
        bound.direct_delta_table_creator()
        if route == "direct"
        else bound.delta_table_creator()
    )
    columns = (("Value", "string", False),)
    creator("Value", columns)
    assert seen == [("Value", columns, None, True, installer.session.workspace)]
