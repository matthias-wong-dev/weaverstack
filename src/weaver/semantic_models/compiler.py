"""Known generated properties, requested-value comparison and signature helpers."""

from __future__ import annotations

import copy
import hashlib
import json

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
        "formatStringDefinition": "formatStringDefinition",
        "expression": str,
        "formatString": str,
        "displayFolder": str,
        "lineageTag": str,
        "isHidden": bool,
    },
    "formatStringDefinition": {"expression": str},
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
