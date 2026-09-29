"""Compile saved Spark Views from metadata written by the attached Fabric runtime."""

from __future__ import annotations

import json
import re
from copy import deepcopy
from typing import Any, Mapping
from uuid import uuid4

from ..locations import Location
from ..spark import FabricSparkTarget
from ..targets import ItemRef, validate_name

VIEW_PROPERTIES = "xCatalogMetadataVersion=MjAyNDA1,xCatalogTableType=VklFVw=="
_MAX_TEMPLATE_BYTES = 1024 * 1024
_TOP_LEVEL = frozenset({
    "tableType", "storage", "allColumns", "partitionColumnNames", "properties",
    "owner", "createTime", "lastAccessTime", "createVersion", "viewText",
    "unsupportedFeatures", "tracksPartitionsInCatalog", "schemaPreservesCase",
    "ignoredProperties", "viewOriginalText",
})
_FIXED_PROPERTIES = frozenset({
    "view.referredTempFunctionsNames", "view.referredTempVariablesNames",
    "view.referredTempViewNames", "view.catalogAndNamespace.numParts",
    "view.catalogAndNamespace.part.0", "view.catalogAndNamespace.part.1",
    "view.schemaMode", "view.query.out.numCols",
})
_PART = r"`(?:``|[^`])+`"
_OBJECT = r"\.".join([_PART] * 4)
_STATEMENT = re.compile(
    rf"\ACREATE VIEW (?P<object>{_OBJECT}) AS\n(?P<query>.+)\Z",
    re.DOTALL,
)


def parse_build_view_statement(statement: str) -> tuple[str, str]:
    match = _STATEMENT.fullmatch(statement)
    if match is None:
        raise ValueError("View statement does not match Weaver's generated DDL")
    query = match.group("query").removesuffix("\n")
    if not query.strip() or "\x00" in query:
        raise ValueError("View statement has no valid query")
    return match.group("object"), query


def bound_view_paths(resolver, qualified: str) -> tuple[Location, Location, str]:
    """Resolve an exact four-part View to one frozen Lakehouse and a private stage."""
    match = re.fullmatch(_OBJECT, qualified)
    if match is None:
        raise ValueError("View statement does not name a bound Lakehouse object")
    parts = [part[1:-1].replace("``", "`") for part in re.findall(_PART, qualified)]
    workspace, lakehouse, schema, name = parts
    target = FabricSparkTarget(resolver.configuration.workspace, lakehouse)
    if target.namespace != (workspace, lakehouse) or target.qualify(schema, name) != qualified:
        raise ValueError("View statement does not match its bound Lakehouse")
    schema = validate_name(schema, what="schema")
    name = validate_name(name, what="object name")
    root = resolver.lakehouse(ItemRef(target.lakehouse))
    return (
        root.join("Files", f"weaver-view-stage-{uuid4().hex}"),
        root.join("Tables", schema, name),
        schema,
    )


def _profile_error(detail: str) -> ValueError:
    return ValueError(f"View metadata profile: {detail}")


def _validate_template(template: Any) -> None:
    if not isinstance(template, dict) or template.keys() != _TOP_LEVEL:
        raise _profile_error("native fields changed")
    if template["tableType"] != "VIEW" or template["storage"] != {"compressed": False, "properties": {}}:
        raise _profile_error("native View storage changed")
    if (
        template["partitionColumnNames"] != []
        or template["unsupportedFeatures"] != []
        or template["ignoredProperties"] != {}
        or template["tracksPartitionsInCatalog"] is not False
        or template["schemaPreservesCase"] is not True
        or template["lastAccessTime"] != -1
        or not isinstance(template["owner"], str)
        or type(template["createTime"]) is not int
        or not isinstance(template["createVersion"], str)
        or not re.fullmatch(r"\d+(?:\.\d+)+", template["createVersion"])
        or not isinstance(template["viewText"], str)
        or template["viewText"] != template["viewOriginalText"]
    ):
        raise _profile_error("native View fields changed")
    properties = template["properties"]
    columns = template["allColumns"]
    if not isinstance(properties, dict) or not isinstance(columns, list):
        raise _profile_error("native View schema changed")
    expected = _FIXED_PROPERTIES | {
        f"view.query.out.col.{index}" for index in range(len(columns))
    }
    unknown = properties.keys() - expected
    if not all(key.startswith("view.sqlConfig.") and len(key) > len("view.sqlConfig.") for key in unknown):
        raise _profile_error("native View properties changed")
    if not expected <= properties.keys() or not unknown or not all(
        isinstance(value, str) for value in properties.values()
    ):
        raise _profile_error("native View properties changed")
    if (
        properties["view.referredTempFunctionsNames"] != "[]"
        or properties["view.referredTempVariablesNames"] != "[]"
        or properties["view.referredTempViewNames"] != "[]"
        or properties["view.catalogAndNamespace.numParts"] != "2"
        or properties["view.catalogAndNamespace.part.0"] != "spark_catalog"
        or not properties["view.catalogAndNamespace.part.1"]
        or properties["view.schemaMode"] != "COMPENSATION"
        or properties["view.query.out.numCols"] != str(len(columns))
        or len(columns) > 10000
    ):
        raise _profile_error("native View namespace or schema changed")
    for index, column in enumerate(columns):
        if (
            not isinstance(column, dict)
            or column.keys() != {"name", "colType", "nullable", "metadata"}
            or not isinstance(column["name"], str)
            or not column["name"]
            or type(column["nullable"]) is not bool
            or properties[f"view.query.out.col.{index}"] != column["name"]
        ):
            raise _profile_error("native View columns changed")
        try:
            json.loads(column["colType"])
            metadata = json.loads(column["metadata"])
        except (TypeError, ValueError) as exc:
            raise _profile_error("native View column types changed") from exc
        if not isinstance(metadata, dict):
            raise _profile_error("native View column metadata changed")


def load_native_view_template(
    decoded: bytes, *, content_type: str, content_encoding: str, properties: str
) -> dict:
    """Accept only the observed Fabric View file profile and identifying headers."""
    if (
        content_type != "application/json"
        or content_encoding != "deflate"
        or properties != VIEW_PROPERTIES
        or len(decoded) > _MAX_TEMPLATE_BYTES
    ):
        raise _profile_error("native View headers changed")
    try:
        template = json.loads(decoded)
    except (UnicodeDecodeError, ValueError) as exc:
        raise _profile_error("native View JSON is invalid") from exc
    _validate_template(template)
    return template


def _contains_variant(value: Any) -> bool:
    if isinstance(value, str):
        return bool(re.search(r"\bvariant\b", value, re.IGNORECASE))
    if isinstance(value, dict):
        return any(_contains_variant(child) for child in value.values())
    if isinstance(value, list):
        return any(_contains_variant(child) for child in value)
    return False


def compile_view_metadata(
    template: Mapping[str, Any], schema: Mapping[str, Any], query: str, *, now_ms: int
) -> bytes:
    """Build one View from a live same-namespace Fabric template and analysed output."""
    _validate_template(template)
    if not isinstance(query, str) or not query.strip() or "\x00" in query:
        raise ValueError("View output shape requires a nonempty query")
    if type(now_ms) is not int or now_ms < 0:
        raise ValueError("View output shape requires a valid creation time")
    if not isinstance(schema, Mapping) or schema.keys() != {"type", "fields"} or schema["type"] != "struct":
        raise ValueError("View output shape must be a Spark struct")
    fields = schema["fields"]
    if not isinstance(fields, list) or not fields or len(fields) > 10000:
        raise ValueError("View output shape has no usable columns")
    columns = []
    names = set()
    for field in fields:
        if not isinstance(field, dict) or field.keys() != {"name", "type", "nullable", "metadata"}:
            raise ValueError("View output shape contains an unknown column")
        name = field["name"]
        if not isinstance(name, str) or not name or "\x00" in name or name.casefold() in names:
            raise ValueError("View output shape contains a duplicate or invalid name")
        names.add(name.casefold())
        if type(field["nullable"]) is not bool or not isinstance(field["metadata"], dict) or _contains_variant(field["type"]):
            raise ValueError("View output shape contains an unsupported column")
        try:
            column_type = json.dumps(field["type"], separators=(",", ":"), ensure_ascii=False, allow_nan=False)
            metadata = json.dumps(field["metadata"], separators=(",", ":"), ensure_ascii=False, allow_nan=False)
        except (TypeError, ValueError) as exc:
            raise ValueError("View output shape contains invalid column JSON") from exc
        columns.append({"name": name, "colType": column_type, "nullable": field["nullable"], "metadata": metadata})
    result = deepcopy(dict(template))
    result["allColumns"] = columns
    result["viewText"] = result["viewOriginalText"] = query
    result["createTime"] = now_ms
    properties = result["properties"]
    for key in tuple(properties):
        if key.startswith("view.query.out.col."):
            del properties[key]
    properties["view.query.out.numCols"] = str(len(columns))
    for index, column in enumerate(columns):
        properties[f"view.query.out.col.{index}"] = column["name"]
    return json.dumps(result, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
