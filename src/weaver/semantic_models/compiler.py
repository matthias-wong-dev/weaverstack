"""Validate and combine the native properties in explicit addon patches."""

from __future__ import annotations

import copy
import hashlib
import json

from ..errors import ConfigError

_NAMED_COLLECTIONS = frozenset(
    {
        "tables",
        "columns",
        "measures",
        "partitions",
        "hierarchies",
        "levels",
        "roles",
        "annotations",
        "relationships",
        "tablePermissions",
        "expressions",
    }
)


def _named_members(value, key):
    if isinstance(value, dict):
        members = []
        for name, properties in value.items():
            if not isinstance(properties, dict):
                raise ConfigError(f"{key}/{name}: expected an object")
            if "name" in properties and properties["name"] != name:
                raise ConfigError(f"{key}/{name}: mapping key conflicts with name")
            members.append({"name": name, **properties})
    elif isinstance(value, list):
        members = value
    else:
        raise ConfigError(f"{key}: expected a named mapping or array")
    names = set()
    for member in members:
        name = member.get("name") if isinstance(member, dict) else None
        if not isinstance(name, str) or not name.strip():
            raise ConfigError(f"{key}: each object requires a nonempty name")
        if name.casefold() in names:
            raise ConfigError(f"{key}: duplicate name {name!r}")
        names.add(name.casefold())
    return members


def _normalise(value, key=""):
    if key in _NAMED_COLLECTIONS:
        return [_normalise(member) for member in _named_members(value, key)]
    if isinstance(value, dict):
        return {name: _normalise(member, name) for name, member in value.items()}
    if isinstance(value, list):
        return [_normalise(member) for member in value]
    return copy.deepcopy(value)


def _merge(base, patch, key=""):
    if isinstance(base, dict) and isinstance(patch, dict):
        result = copy.deepcopy(base)
        for name, value in patch.items():
            result[name] = _merge(result.get(name), value, name)
        return result
    if key in _NAMED_COLLECTIONS and isinstance(base, list) and isinstance(patch, list):
        result = copy.deepcopy(base)
        positions = {
            member["name"].casefold(): index for index, member in enumerate(result)
        }
        for member in patch:
            identity = member["name"].casefold()
            if identity in positions:
                index = positions[identity]
                result[index] = _merge(result[index], member)
            else:
                positions[identity] = len(result)
                result.append(copy.deepcopy(member))
        return result
    return copy.deepcopy(patch)


# Nested strings name object schemas; tuples name collections of objects.
_COMMON = {"name": str, "description": str, "annotations": ("annotation",)}
_SCHEMAS = {
    "database": {"name": str, "id": str, "compatibilityLevel": int, "model": "model"},
    "model": {
        "culture": str,
        "sourceQueryCulture": str,
        "defaultPowerBIDataSourceVersion": str,
        "discourageImplicitMeasures": bool,
        "dataAccessOptions": "dataAccessOptions",
        "defaultMode": str,
        "directLakeBehavior": str,
        "expressions": ("expression",),
        "tables": ("table",),
        "relationships": ("relationship",),
        "roles": ("role",),
    },
    "table": {
        "isHidden": bool,
        "lineageTag": str,
        "dataCategory": str,
        "columns": ("column",),
        "measures": ("measure",),
        "partitions": ("partition",),
        "hierarchies": ("hierarchy",),
        ".dax": str,
        ".source": str,
    },
    "column": {
        "type": str,
        "dataType": str,
        "sourceColumn": str,
        "expression": str,
        "formatString": str,
        "summarizeBy": str,
        "isKey": bool,
        "isHidden": bool,
        "lineageTag": str,
        "sortByColumn": str,
        "displayFolder": str,
        "dataCategory": str,
        "isNullable": bool,
    },
    "measure": {
        "expression": str,
        "formatString": str,
        "displayFolder": str,
        "lineageTag": str,
        "isHidden": bool,
    },
    "partition": {"mode": str, "source": "source"},
    "source": {
        "type": str,
        "expression": str,
        "schemaName": str,
        "entityName": str,
        "expressionSource": str,
    },
    "expression": {"kind": str, "expression": str},
    "hierarchy": {"levels": ("level",), "isHidden": bool, "displayFolder": str},
    "level": {"ordinal": int, "column": str},
    "role": {
        "modelPermission": str,
        "members": ("member",),
        "tablePermissions": ("tablePermission",),
    },
    "member": {"memberName": str, "memberId": str, "identityProvider": str},
    "tablePermission": {"filterExpression": str, "metadataPermission": str},
    "annotation": {"value": str},
    "relationship": {
        "type": str,
        "fromTable": str,
        "fromColumn": str,
        "toTable": str,
        "toColumn": str,
        "fromCardinality": str,
        "toCardinality": str,
        "crossFilteringBehavior": str,
        "isActive": bool,
        "securityFilteringBehavior": str,
        "relyOnReferentialIntegrity": bool,
    },
    "dataAccessOptions": {"legacyRedirects": bool, "returnErrorValuesAsNull": bool},
}
_ROOT_COLLECTIONS = {"tables", "relationships", "roles", "annotations"}


def _validate(value, kind, path):
    if not isinstance(value, dict):
        raise ConfigError(f"{path}: expected an object")
    allowed = {**_COMMON, **_SCHEMAS[kind]}
    for key, member in value.items():
        location = f"{path}/{key}"
        expected = allowed.get(key)
        if expected is None:
            raise ConfigError(
                f"{location}: unsupported semantic-model property or directive"
            )
        if isinstance(expected, tuple):
            if not isinstance(member, list):
                raise ConfigError(f"{location}: expected an array")
            for index, child in enumerate(member):
                label = child.get("name", index) if isinstance(child, dict) else index
                _validate(child, expected[0], f"{location}/{label}")
        elif isinstance(expected, str):
            _validate(member, expected, location)
        elif type(member) is not expected:
            raise ConfigError(f"{location}: expected {expected.__name__}")
        if key == ".dax" and not member.strip():
            raise ConfigError(f"{location}: expression must not be empty")
    if ".dax" in value and "partitions" in value:
        raise ConfigError(f"{path}: .dax and partitions cannot both be authored")


def _addon_patch(addon):
    if not isinstance(addon, dict):
        raise ConfigError("addon: expected an object")
    unknown = set(addon) - {"model", *_ROOT_COLLECTIONS}
    if unknown:
        raise ConfigError(
            f"addon/{sorted(unknown, key=str)[0]}: unsupported property or directive"
        )
    patch = copy.deepcopy(addon.get("model", {}))
    if not isinstance(patch, dict):
        raise ConfigError("addon/model: expected an object")
    for key in _ROOT_COLLECTIONS & addon.keys():
        if key in patch:
            raise ConfigError(f"addon/{key}: also declared under model")
        patch[key] = addon[key]
    patch = _normalise(patch)
    _validate(patch, "model", "addon/model")
    return patch


def _expand_dax(patch, base):
    existing = {t["name"].casefold(): t for t in base.get("tables", [])}
    for table in patch.get("tables", []):
        if ".dax" not in table:
            continue
        partitions = existing.get(table["name"].casefold(), {}).get("partitions", [])
        if partitions and not (
            len(partitions) == 1
            and partitions[0]["name"] == table["name"]
            and partitions[0].get("source", {}).get("type") == "calculated"
        ):
            raise ConfigError(
                f"tables/{table['name']}: .dax cannot replace existing partitions"
            )
        table["partitions"] = [
            {
                "name": table["name"],
                "source": {"type": "calculated", "expression": table.pop(".dax")},
            }
        ]


def escape(value):
    return str(value).replace("~", "~0").replace("/", "~1")


def leaf_properties(value, path="", key=""):
    if isinstance(value, dict) and value:
        return {
            p: v
            for k, child in value.items()
            for p, v in leaf_properties(child, f"{path}/{escape(k)}", k).items()
        }
    if isinstance(value, list) and value:
        return {
            p: v
            for i, child in enumerate(value)
            for p, v in leaf_properties(
                child,
                f"{path}/{escape(child['name'] if key in _NAMED_COLLECTIONS else i)}",
            ).items()
        }
    return {path: value}


def canonical(value, key=""):
    if isinstance(value, dict):
        return {k: canonical(v, k) for k, v in value.items()}
    if isinstance(value, list):
        children = [canonical(v) for v in value]
        return (
            sorted(children, key=lambda v: v["name"].casefold())
            if key in _NAMED_COLLECTIONS
            else children
        )
    return value


def content_signature(value):
    return hashlib.sha256(
        json.dumps(
            canonical(value),
            sort_keys=True,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
