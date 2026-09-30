"""Creation policy is common; schema features remain table-specific."""

from __future__ import annotations

import pytest
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
