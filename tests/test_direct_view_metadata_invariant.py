"""A saved Spark View uses one live, verified Fabric metadata shape."""

from __future__ import annotations

import json
import zlib

import pytest
from support.weaver_test import weaver_test

from weaver.sessions.direct_view import (
    VIEW_PROPERTIES,
    compile_view_metadata,
    load_native_view_template,
    parse_build_view_statement,
)


def _template():
    return {
        "tableType": "VIEW",
        "storage": {"compressed": False, "properties": {}},
        "allColumns": [
            {"name": "Old", "colType": '"long"', "nullable": False, "metadata": "{}"}
        ],
        "partitionColumnNames": [],
        "properties": {
            "view.referredTempFunctionsNames": "[]",
            "view.referredTempVariablesNames": "[]",
            "view.referredTempViewNames": "[]",
            "view.catalogAndNamespace.numParts": "2",
            "view.catalogAndNamespace.part.0": "spark_catalog",
            "view.catalogAndNamespace.part.1": "opaque-target-namespace",
            "view.schemaMode": "COMPENSATION",
            "view.query.out.numCols": "1",
            "view.query.out.col.0": "Old",
            "view.sqlConfig.spark.sql.caseSensitive": "false",
        },
        "owner": "",
        "createTime": 1,
        "lastAccessTime": -1,
        "createVersion": "4.1.1.5.5.20260910.235373633",
        "viewText": "SELECT Old FROM source",
        "unsupportedFeatures": [],
        "tracksPartitionsInCatalog": False,
        "schemaPreservesCase": True,
        "ignoredProperties": {},
        "viewOriginalText": "SELECT Old FROM source",
    }


@weaver_test()
def test_live_view_template_compiles_its_own_query_and_spark_columns():
    template = _template()
    schema = {"type": "struct", "fields": [
        {"name": "BusinessKey", "type": "long", "nullable": False, "metadata": {}},
        {"name": "Notes", "type": {"type": "array", "elementType": "string", "containsNull": True},
         "nullable": True, "metadata": {"comment": "owned"}},
    ]}
    payload = compile_view_metadata(template, schema, "SELECT BusinessKey, Notes FROM source", now_ms=300)
    result = json.loads(payload)
    assert result["viewText"] == result["viewOriginalText"] == "SELECT BusinessKey, Notes FROM source"
    assert result["createTime"] == 300
    assert result["createVersion"] == template["createVersion"]
    assert result["properties"]["view.catalogAndNamespace.part.1"] == "opaque-target-namespace"
    assert result["properties"]["view.sqlConfig.spark.sql.caseSensitive"] == "false"
    assert result["properties"]["view.query.out.numCols"] == "2"
    assert result["properties"]["view.query.out.col.0"] == "BusinessKey"
    assert result["properties"]["view.query.out.col.1"] == "Notes"
    assert result["allColumns"] == [
        {"name": "BusinessKey", "colType": '"long"', "nullable": False, "metadata": "{}"},
        {"name": "Notes", "colType": json.dumps(schema["fields"][1]["type"], separators=(",", ":")),
         "nullable": True, "metadata": '{"comment":"owned"}'},
    ]
    assert template["properties"]["view.query.out.col.0"] == "Old"
    assert "view.query.out.col.1" not in template["properties"]


@pytest.mark.parametrize("change", [
    lambda d: d.update(unexpected="new native format"),
    lambda d: d.update(tableType="TABLE"),
    lambda d: d["properties"].update({"view.schemaMode": "UNKNOWN"}),
    lambda d: d["properties"].update({"view.undocumentedMode": "enabled"}),
    lambda d: d["properties"].update({"view.query.out.col.3": "extra"}),
    lambda d: d["properties"].update({"view.catalogAndNamespace.part.1": ""}),
])
@weaver_test()
def test_unknown_native_view_shape_refuses_direct_publication(change):
    template = _template()
    change(template)
    with pytest.raises(ValueError, match="View metadata profile"):
        compile_view_metadata(template, {"type": "struct", "fields": []}, "SELECT 1", now_ms=300)


@pytest.mark.parametrize("schema", [
    {"type": "struct", "fields": [{"name": "X", "type": "long", "nullable": "true", "metadata": {}}]},
    {"type": "struct", "fields": [{"name": "X", "type": "long", "nullable": True, "metadata": {}},
                                    {"name": "x", "type": "long", "nullable": True, "metadata": {}}]},
    {"type": "struct", "fields": [{"name": "X", "type": "variant", "nullable": True, "metadata": {}}]},
    {"type": "struct", "fields": [{"name": "", "type": "long", "nullable": True, "metadata": {}}]},
])
@weaver_test()
def test_unsupported_query_shape_refuses_direct_publication(schema):
    with pytest.raises(ValueError, match="View output shape"):
        compile_view_metadata(_template(), schema, "SELECT 1", now_ms=300)


@pytest.mark.parametrize("mime,encoding,properties", [
    ("application/octet-stream", "deflate", VIEW_PROPERTIES),
    ("application/json", "identity", VIEW_PROPERTIES),
    ("application/json", "deflate", "xCatalogMetadataVersion=MjAyNDA1"),
])
@weaver_test()
def test_native_view_requires_observed_headers(mime, encoding, properties):
    with pytest.raises(ValueError, match="View metadata profile"):
        load_native_view_template(json.dumps(_template()).encode(),
                                  content_type=mime, content_encoding=encoding,
                                  properties=properties)


@weaver_test()
def test_native_view_payload_and_statement_are_strictly_bounded():
    decoded = json.dumps(_template()).encode()
    assert load_native_view_template(decoded, content_type="application/json",
                                     content_encoding="deflate", properties=VIEW_PROPERTIES) == _template()
    assert zlib.decompress(zlib.compress(decoded)) == decoded
    qualified, query = parse_build_view_statement(
        "CREATE VIEW `Work`.`Lake`.`Sales`.`V` AS\nSELECT 1 AS BusinessKey\n"
    )
    assert qualified == "`Work`.`Lake`.`Sales`.`V`"
    assert query == "SELECT 1 AS BusinessKey"
    with pytest.raises(ValueError, match="View statement"):
        parse_build_view_statement("DROP VIEW `Work`.`Lake`.`Sales`.`V`")
