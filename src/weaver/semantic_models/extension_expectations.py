"""Project explicitly requested native fragments into readback expectations."""

import re

from ..errors import ConfigError
from .compiler import _COMMON, _SCHEMAS, _merge
from .fragments import expression_text, properties, scalar
from .tmdl import object_name

_KINDS = {key.casefold(): key for key in _SCHEMAS}
_DEFAULTS = {
    "measure": "expression",
    "column": "expression",
    "expression": "expression",
    "annotation": "value",
    "tablepermission": "filterExpression",
}
_ENDPOINT = re.compile(r"('(?:[^']|'')*'|[^.\s]+)\.('(?:[^']|'')*'|[^.\s]+)\Z")


def _value(document, node, expected):
    text = node.text.partition(":")[2].strip()
    if expected is bool:
        if not text:
            return True
        if text.casefold() in {"true", "false"}:
            return text.casefold() == "true"
    elif expected is int:
        try:
            return int(text)
        except ValueError:
            pass
    elif expected is str:
        return scalar(text)
    raise ConfigError(
        f"{document.path}:{node.header + 1}: {node.name} requires {expected.__name__}"
    )


def requested_object(document, node, kind=None):
    kind = kind or _KINDS.get(node.kind)
    if kind not in _SCHEMAS:
        return None
    schema = {**_COMMON, **_SCHEMAS[kind]}
    result = (
        {"name": node.name} if node.kind and kind not in {"model", "database"} else {}
    )
    result.update(properties(document, node, {"description"}))
    default = _DEFAULTS.get(node.kind)
    if default and node.value is not None:
        result[default] = expression_text(document, node)
        if kind == "column":
            result["type"] = "calculated"
    if kind == "partition" and node.value is not None:
        result["source"] = {"type": node.value}
    collections = {
        value[0].casefold(): (key, value[0])
        for key, value in schema.items()
        if isinstance(value, tuple)
    }
    for child in node.children:
        if child.kind:
            entry = collections.get(child.kind)
            if entry:
                key, child_kind = entry
                result.setdefault(key, []).append(
                    requested_object(document, child, child_kind)
                )
            continue
        key = next((k for k in schema if k.casefold() == child.name.casefold()), None)
        if key is None:
            continue
        expected = schema[key]
        if kind == "partition" and key == "source":
            source = result.setdefault("source", {})
            if "=" in child.text:
                source["expression"] = expression_text(document, child)
            else:
                source.update(requested_object(document, child, "source"))
        elif kind == "relationship" and key in {"fromColumn", "toColumn"}:
            value = child.text.partition(":")[2].strip()
            endpoint = _ENDPOINT.fullmatch(value)
            if endpoint:
                result[key.replace("Column", "Table")] = object_name(endpoint[1])
                result[key] = object_name(endpoint[2])
        elif isinstance(expected, str):
            result[key] = requested_object(document, child, expected)
        elif isinstance(expected, type):
            result[key] = _value(document, child, expected)
    return result


def requested_fragment(document):
    result = {}
    collections = {
        value[0].casefold(): key
        for key, value in _SCHEMAS["model"].items()
        if isinstance(value, tuple)
    }
    collections["annotation"] = "annotations"
    for node in document.spans:
        if node.parent is not None:
            continue
        value = requested_object(document, node)
        if value is None:
            continue
        if node.kind == "model":
            result = _merge(result, value)
        elif node.kind in collections:
            key = collections[node.kind]
            result = _merge(result, {key: [value]})
    return result
