"""Authored Delta protocol minima are declaration intent."""

from __future__ import annotations

import hashlib
import json

import pytest
from support.weaver_test import weaver_test

from weaver import delta_protocol
from weaver.build_bundle.executors.base import InstallationContext, ResolvedTarget
from weaver.build_bundle.executors.spark_table import SparkTableExecutor
from weaver.build_bundle.models import BoundTarget, InstallAction
from weaver.declaration.model import LAKEHOUSE
from weaver.declaration.source import read_source_document
from weaver.errors import MetadataError
from weaver.spark import FabricSparkTarget
from weaver.store import FilesystemStore
from weaver.targets import ItemRef


def _source(protocol=""):
    return read_source_document(
        "DWG.Protocol.sql",
        (
            "/*\nTable ID: DWG.Protocol\nDescription: Protocol contract\n"
            "Lineage: Inline scalar\nDependencies: []\n"
            "Schema:\n  Value: string\n"
            + protocol
            + "*/\nSELECT CAST('value' AS STRING) AS Value;\n"
        ).encode(),
        LAKEHOUSE,
    )


@weaver_test()
@pytest.mark.parametrize("direct", [False, True])
def test_authored_protocol_reaches_both_creation_capabilities(direct):
    source = _source("Delta minReaderVersion: 2\nDelta minWriterVersion: 5\n")
    destination = FabricSparkTarget("Demo", "Sales")
    payload = source.create_ddl(destination=destination).content.encode()
    calls = []

    def create(_qualified, _columns, **kwargs):
        calls.append(kwargs)

    target = ResolvedTarget(
        bound=BoundTarget(id="lakehouse-Sales", kind="lakehouse", item_id="Sales"),
        lakehouse=ItemRef("Sales"),
        destination=destination,
    )
    context = InstallationContext(
        resolver=None,
        store=FilesystemStore(),
        target=target,
        spark_sql_batch=lambda *_a, **_k: [
            {"col_name": "Value", "data_type": "string"}
        ],
        create_delta_table=create,
        create_direct_delta_table=create if direct else None,
    )
    action = InstallAction(
        id="create",
        kind="build_table",
        executor="spark_table",
        resource_node_id=None,
        payload="payload/create.json",
        payload_sha256=hashlib.sha256(payload).hexdigest(),
    )
    SparkTableExecutor().execute(action, payload, context)
    assert calls[0]["protocol_minima"] == {"minReaderVersion": 2, "minWriterVersion": 5}


@weaver_test()
def test_delta_protocol_metadata_is_refused_on_warehouse_declarations():
    from weaver.declaration.metadata import SQL, parse_document

    header = "Table ID: DWG.Protocol\nDescription: Protocol contract\nLineage: Inline scalar\nDependencies: []\nDelta minReaderVersion: 3\nDelta minWriterVersion: 7\n"
    with pytest.raises(
        MetadataError, match="Delta protocol minimums require a Lakehouse Table"
    ):
        parse_document(header, language=SQL)


@weaver_test()
@pytest.mark.parametrize("value", ["true", "null", "1.5", "0", "4", "'3'"])
def test_authored_reader_minimum_rejects_invalid_values(value):
    with pytest.raises(
        MetadataError, match="Delta minReaderVersion must be an integer"
    ):
        _source(f"Delta minReaderVersion: {value}\n")


@weaver_test()
@pytest.mark.parametrize("value", ["true", "null", "1.5", "0", "8", "'7'"])
def test_authored_writer_minimum_rejects_invalid_values(value):
    with pytest.raises(
        MetadataError, match="Delta minWriterVersion must be an integer"
    ):
        _source(f"Delta minWriterVersion: {value}\n")


@weaver_test()
def test_implicit_default_upgrade_changes_creation_payload_not_table_signature(
    monkeypatch,
):
    monkeypatch.setattr(delta_protocol, "DEFAULT_MIN_READER_VERSION", 2)
    monkeypatch.setattr(delta_protocol, "DEFAULT_MIN_WRITER_VERSION", 5)
    previous = _source()
    destination = FabricSparkTarget("Demo", "Sales")
    previous_payload = json.loads(previous.create_ddl(destination=destination).content)
    monkeypatch.setattr(delta_protocol, "DEFAULT_MIN_READER_VERSION", 3)
    monkeypatch.setattr(delta_protocol, "DEFAULT_MIN_WRITER_VERSION", 7)
    current = _source()
    current_payload = json.loads(current.create_ddl(destination=destination).content)
    assert previous.physical_signature == current.physical_signature
    assert previous_payload["protocol_minima"] == {
        "minReaderVersion": 2,
        "minWriterVersion": 5,
    }
    assert current_payload["protocol_minima"] == {
        "minReaderVersion": 3,
        "minWriterVersion": 7,
    }
    assert "Delta minReaderVersion" not in current.document.raw
    assert "Delta minWriterVersion" not in current.document.raw


@weaver_test()
def test_protocol_minimums_cross_the_frozen_table_creation_payload():
    source = _source("Delta minReaderVersion: 2\nDelta minWriterVersion: 5\n")
    instruction = json.loads(
        source.create_ddl(destination=FabricSparkTarget("Demo", "Sales")).content
    )
    assert instruction["protocol_minima"] == {
        "minReaderVersion": 2,
        "minWriterVersion": 5,
    }


@weaver_test()
def test_authored_protocol_minimums_are_read_and_signature_bearing():
    implicit = _source()
    explicit = _source("Delta minReaderVersion: 2\nDelta minWriterVersion: 5\n")
    assert explicit.document.delta_min_reader_version == 2
    assert explicit.document.delta_min_writer_version == 5
    assert explicit.physical_signature != implicit.physical_signature
