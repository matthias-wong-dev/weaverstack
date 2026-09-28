"""The Weaver Delta v1 profile is compiled, not inferred from writer defaults."""

from __future__ import annotations

import copy
import json

import pytest
from support.weaver_test import weaver_test

from weaver.locations import Location
from weaver.sessions.delta_profile import (
    compile_delta_profile_v1,
    read_delta_snapshot,
    verify_delta_profile_v1,
)
from weaver.store import FilesystemStore

COLUMNS = (
    ("Id", "bigint", True),
    ("Amount", "decimal(18, 2)", False),
    ("WrittenAt", "timestamp", True),
)


def _actual(profile):
    fields = []
    for i, field in enumerate(profile.schema["fields"], 1):
        physical = f"col-{i:08x}-1234-4123-8123-123456789abc"
        fields.append(
            {
                **field,
                "metadata": {
                    **field["metadata"],
                    "delta.columnMapping.physicalName": physical,
                },
            }
        )
    return {
        "version": profile.final_version,
        "protocol": copy.deepcopy(profile.protocol),
        "metaData": {
            "id": "e62a7b6e-58ee-40c1-b6f2-585d3d47c914",
            "name": None,
            "description": None,
            "createdTime": 1790000000000,
            "format": {"provider": "parquet", "options": {}},
            "schemaString": json.dumps({"type": "struct", "fields": fields}),
            "partitionColumns": [],
            "configuration": copy.deepcopy(profile.configuration),
        },
    }


@weaver_test()
def test_profile_compiles_schema_properties_and_allocated_names_invariant():
    profile = compile_delta_profile_v1(COLUMNS)
    assert profile.version == "weaver-delta/v1"
    assert profile.writer_version == "1.6.6"
    assert profile.protocol == {"minReaderVersion": 2, "minWriterVersion": 5}
    assert profile.configuration == {
        "delta.columnMapping.mode": "name",
        "delta.columnMapping.maxColumnId": "3",
        "delta.checkpointInterval": "10",
        "delta.checkpointPolicy": "classic",
        "delta.checkpoint.writeStatsAsJson": "true",
        "delta.checkpoint.writeStatsAsStruct": "false",
        "delta.logRetentionDuration": "interval 30 days",
        "delta.deletedFileRetentionDuration": "interval 7 days",
    }
    assert [
        (f["name"], f["type"], f["nullable"]) for f in profile.schema["fields"]
    ] == [
        ("Id", "long", False),
        ("Amount", "decimal(18,2)", True),
        ("WrittenAt", "timestamp", False),
    ]
    allocation = verify_delta_profile_v1(profile, _actual(profile))
    assert allocation.table_id == "e62a7b6e-58ee-40c1-b6f2-585d3d47c914"
    assert allocation.physical_names["Id"].startswith("col-")


@weaver_test()
def test_profile_rejects_unexpected_writer_property_and_feature():
    profile = compile_delta_profile_v1(COLUMNS)
    actual = _actual(profile)
    actual["metaData"]["configuration"]["delta.enableDeletionVectors"] = "true"
    with pytest.raises(ValueError, match="configuration"):
        verify_delta_profile_v1(profile, actual)
    actual = _actual(profile)
    actual["protocol"]["readerFeatures"] = ["variantType"]
    with pytest.raises(ValueError, match="protocol"):
        verify_delta_profile_v1(profile, actual)


@weaver_test()
def test_profile_rejects_invalid_mapping_and_schema_metadata():
    profile = compile_delta_profile_v1(COLUMNS)
    actual = _actual(profile)
    actual["metaData"]["schemaString"] = actual["metaData"]["schemaString"].replace(
        '"delta.columnMapping.id": 2', '"delta.columnMapping.id": 1'
    )
    with pytest.raises(ValueError, match="columnMapping.id"):
        verify_delta_profile_v1(profile, actual)


@weaver_test()
def test_profile_reads_the_complete_committed_log_before_accepting_a_table(tmp_path):
    profile = compile_delta_profile_v1(COLUMNS, identity_column="Id")
    actual = _actual(profile)
    first = tmp_path / "_delta_log" / "00000000000000000000.json"
    first.parent.mkdir(parents=True)
    first.write_text(
        json.dumps({"protocol": {"minReaderVersion": 2, "minWriterVersion": 5}})
        + "\n"
        + json.dumps({"metaData": actual["metaData"]})
        + "\n"
    )
    second = first.with_name("00000000000000000001.json")
    second.write_text(json.dumps({"protocol": actual["protocol"]}) + "\n")

    snapshot = read_delta_snapshot(FilesystemStore(), Location(str(tmp_path)))
    assert (
        verify_delta_profile_v1(profile, snapshot).table_id == actual["metaData"]["id"]
    )
    second.write_text(
        json.dumps({"protocol": {"minReaderVersion": 2, "minWriterVersion": 5}}) + "\n"
    )
    with pytest.raises(ValueError, match="protocol"):
        verify_delta_profile_v1(
            profile, read_delta_snapshot(FilesystemStore(), Location(str(tmp_path)))
        )


@weaver_test()
def test_feature_protocol_declares_non_null_invariants_and_conditional_variant():
    identity = compile_delta_profile_v1(
        (("Id", "bigint", True), ("Value", "string", False)),
        identity_column="Id",
    )
    assert set(identity.protocol["writerFeatures"]) == {
        "columnMapping",
        "identityColumns",
        "invariants",
    }
    assert set(identity.protocol["readerFeatures"]) == {"columnMapping"}

    variant = compile_delta_profile_v1(
        (("Id", "bigint", True), ("Payload", "variant", False))
    )
    assert set(variant.protocol["writerFeatures"]) == {
        "columnMapping",
        "variantType",
        "invariants",
    }
    assert set(variant.protocol["readerFeatures"]) == {
        "columnMapping",
        "variantType",
    }
    nullable_variant = compile_delta_profile_v1((("Payload", "variant", False),))
    assert "invariants" not in nullable_variant.protocol["writerFeatures"]


@weaver_test()
def test_generated_column_requires_feature_when_variant_raises_writer_to_seven():
    profile = compile_delta_profile_v1(
        (
            ("Id", "bigint", True),
            ("Twice", "bigint", False),
            ("Payload", "variant", False),
        ),
        generated_columns={"Twice": "Id * 2"},
    )
    assert set(profile.protocol["writerFeatures"]) == {
        "columnMapping",
        "invariants",
        "variantType",
        "generatedColumns",
    }
