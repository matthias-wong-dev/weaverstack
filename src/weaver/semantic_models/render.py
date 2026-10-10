"""Render fragments whose complete definition is supplied by Weaver."""

import json
from urllib.parse import quote

from ..errors import ConfigError
from .compiler import _COMMON, _SCHEMAS
from .tmdl import quote_name, text_value


def empty_parts(name):
    return {
        "definition.pbism": b'{"version":"4.2","settings":{}}\n',
        "definition/database.tmdl": f"database {quote_name(name)}\n\tcompatibilityLevel: 1606\n".encode(),
        "definition/model.tmdl": b"model Model\n\tculture: en-US\n\tdefaultPowerBIDataSourceVersion: powerBI_V3\n",
    }


def object_file(kind, name):
    if kind in {"table", "role"}:
        return f"definition/{kind}s/{quote(name, safe='')}.tmdl"
    return {
        "expression": "definition/expressions.tmdl",
        "relationship": "definition/relationships.tmdl",
    }[kind]


def expression_lines(header, expression, prefix, unit="\t", newline="\n"):
    if not isinstance(expression, str):
        raise ConfigError("TMDL expressions require text")
    if any(line.strip() == "```" for line in expression.splitlines()):
        raise ConfigError("An authored expression cannot contain a TMDL closing fence")
    inner = prefix + unit * 2
    return (
        prefix
        + header
        + " = ```"
        + newline
        + "".join(inner + line + newline for line in expression.split("\n"))
        + inner
        + "```"
        + newline
    )


def render_object(kind, value, *, prefix="", unit="\t", newline="\n"):
    name = value.get("name", "Model" if kind == "model" else "")
    header = kind + (" " + quote_name(name) if name else "")
    source = value.get("source", {}) if kind == "partition" else {}
    result = ""
    if "description" in value:
        result += "".join(
            prefix + "/// " + line + newline
            for line in value["description"].split("\n")
        )
    default = {
        "measure": "expression",
        "expression": "expression",
        "annotation": "value",
        "tablePermission": "filterExpression",
    }.get(kind)
    if kind == "column" and "expression" in value:
        default = "expression"
    if default and default in value:
        result += expression_lines(header, value[default], prefix, unit, newline)
    else:
        if kind == "partition":
            source_type = source.get("type")
            if source_type not in {"m", "calculated", "entity"}:
                raise ConfigError(
                    f"Partition {name!r}: unsupported source type {source_type!r}"
                )
            header += " = " + source_type
        result += prefix + header + newline
    child_prefix = prefix + unit
    schema = {**_COMMON, **_SCHEMAS[kind]}
    for key, member in value.items():
        if key in {"name", "description", default, "source", "type", "kind"}:
            continue
        if kind == "relationship" and key in {"fromTable", "toTable"}:
            continue
        child = schema.get(key)
        if isinstance(child, tuple):
            for entry in member:
                result += newline + render_object(
                    child[0], entry, prefix=child_prefix, unit=unit, newline=newline
                )
        elif key == "formatStringDefinition":
            result += expression_lines(
                key, member["expression"], child_prefix, unit, newline
            )
        elif key == "dataAccessOptions":
            result += (
                child_prefix
                + key
                + " = "
                + json.dumps(member, separators=(",", ":"))
                + newline
            )
        else:
            if kind == "relationship" and key in {"fromColumn", "toColumn"}:
                table = value.get(key.replace("Column", "Table"))
                if not table:
                    raise ConfigError(f"Relationship {name!r}: {key} requires a table")
                rendered = quote_name(table) + "." + quote_name(member)
            elif key in {"sortByColumn", "column", "expressionSource"}:
                rendered = quote_name(member)
            else:
                rendered = text_value(member)
            result += f"{child_prefix}{key}: {rendered}{newline}"
    if source:
        if source["type"] == "entity":
            result += child_prefix + "source" + newline
            for key, member in source.items():
                if key != "type":
                    rendered = (
                        quote_name(member)
                        if key == "expressionSource"
                        else text_value(member)
                    )
                    result += child_prefix + unit + key + ": " + rendered + newline
        else:
            result += expression_lines(
                "source", source.get("expression", ""), child_prefix, unit, newline
            )
    return result
