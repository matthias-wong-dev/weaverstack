"""Verify requested edits against observed semantic metadata.

Weaver's requested values are strict: an object it added, hid, excluded,
generated or rebound must read back with that value. Fabric may rewrite what
Weaver passed through, so a difference that is only formatting, quoting, case
or a service-managed annotation is not one. A difference Weaver cannot judge
is returned for a warning rather than raised.
"""

import re
import textwrap

from ..errors import InstallError
from .compiler import _NAMED_COLLECTIONS

# TMSL omits these default-valued relationship properties on readback.
_RELATIONSHIP_DEFAULTS = {
    "fromCardinality": "many",
    "toCardinality": "one",
    "crossFilteringBehavior": "oneDirection",
    "isActive": True,
}
_DEFAULTS = {
    "isHidden": False,
    "isKey": False,
    "isNullable": True,
    "discourageImplicitMeasures": False,
    "defaultMode": "import",
}
_TEXT = frozenset({"expression", "description", "filterExpression", "value"})
_NAMES = frozenset(
    {
        "name",
        "fromTable",
        "fromColumn",
        "toTable",
        "toColumn",
        "sortByColumn",
        "column",
        "expressionSource",
    }
)
_KINDS = {
    "tables": "table",
    "columns": "column",
    "measures": "measure",
    "partitions": "partition",
    "relationships": "relationship",
    "expressions": "expression",
    "roles": "role",
    "annotations": "annotation",
    "tablePermissions": "tablePermission",
    "hierarchies": "hierarchy",
    "levels": "level",
}


def canonical_model(model):
    def normalise(value, key=""):
        if isinstance(value, dict):
            return {k: normalise(v, k) for k, v in value.items()}
        if isinstance(value, list):
            if key in _TEXT and all(isinstance(v, str) for v in value):
                return "\n".join(value)
            values = [normalise(v) for v in value]
            if (
                key in _NAMED_COLLECTIONS
                and values
                and all(
                    isinstance(v, dict) and isinstance(v.get("name"), str)
                    for v in values
                )
            ):
                return sorted(values, key=lambda v: v["name"])
            return values
        return value

    return normalise({k: v for k, v in model.items() if k not in {"name", "id"}})


def object_identity(name):
    """A semantic object name as Fabric may return it, quoted or recased."""

    if not isinstance(name, str):
        return name
    text = name.strip()
    if len(text) > 1 and text[0] == text[-1] == "'":
        text = text[1:-1].replace("''", "'")
    return text.casefold()


def comparable(value, key=""):
    """``value`` with names and text in the form :func:`verify_requested` compares."""

    if isinstance(value, dict):
        return {k: comparable(v, k) for k, v in value.items()}
    if isinstance(value, list):
        if key in _TEXT and all(isinstance(v, str) for v in value):
            return _layout("\n".join(value))
        return [comparable(v) for v in value]
    if isinstance(value, str) and key in _TEXT:
        return _layout(value)
    if isinstance(value, str) and key in _NAMES:
        return object_identity(value)
    return value


def _layout(text):
    lines = [line.rstrip() for line in text.replace("\r\n", "\n").split("\n")]
    return textwrap.dedent("\n".join(lines).replace("\r", "\n")).strip("\n")


def _same(key, wanted, found):
    """Whether two scalars are equal, and if not, whether only layout differs."""

    if wanted == found:
        return True, False
    if not (isinstance(wanted, str) and isinstance(found, str)):
        return False, False
    if key in _NAMES:
        return object_identity(wanted) == object_identity(found), False
    if key in _TEXT:
        if _layout(wanted) == _layout(found):
            return True, False
        return False, re.sub(r"\s+", "", wanted) == re.sub(r"\s+", "", found)
    return False, False


def verify_requested(requested, actual, *, owned=(), absent=()):
    """Raise for a requested value Fabric did not keep; return what it rewrote.

    The result names each path whose difference Weaver cannot judge: native
    content Fabric reformatted or extended, or an annotation it dropped.
    """

    from .compiler import _COMMON, _SCHEMAS, escape

    if not isinstance(actual, dict) or not isinstance(actual.get("model"), dict):
        raise InstallError("Semantic readback has no model object")
    differences = []

    def structure(node, collections):
        for key, nested in collections.items():
            members = node.get(key, [])
            if not isinstance(members, list):
                raise InstallError(f"Semantic readback has invalid {key}")
            names = set()
            for member in members:
                if (
                    not isinstance(member, dict)
                    or not isinstance(member.get("name"), str)
                    or not member["name"]
                ):
                    raise InstallError(f"Semantic readback has an unnamed {key} object")
                name = object_identity(member["name"])
                if name in names:
                    raise InstallError(
                        f"Semantic readback has duplicate {key} object {name!r}"
                    )
                names.add(name)
                structure(member, nested)

    structure(
        actual["model"],
        {
            "tables": {"columns": {}, "measures": {}, "partitions": {}},
            "relationships": {},
            "expressions": {},
        },
    )
    for path in absent:
        node = actual["model"]
        for kind, name in path:
            collection = {
                "table": "tables",
                "column": "columns",
                "measure": "measures",
                "partition": "partitions",
                "relationship": "relationships",
                "expression": "expressions",
            }.get(kind.casefold())
            if collection is None:
                break
            node = next(
                (
                    member
                    for member in node.get(collection, [])
                    if object_identity(member["name"]) == object_identity(name)
                ),
                None,
            )
            if node is None:
                break
        else:
            raise InstallError(f"Semantic readback retains excluded object {path!r}")

    def differs(path, annotation):
        # Nothing reads annotations from the deployed model, so Fabric's copy stands.
        if annotation:
            differences.append(path)
        else:
            raise InstallError(f"Semantic readback differs at {path}")

    def contains(wanted, found, path, annotation=False):
        if isinstance(wanted, dict):
            if not isinstance(found, dict):
                differs(path, annotation)
                return
            calculated = any(
                p.get("source", {}).get("type") == "calculated"
                for p in wanted.get("partitions", [])
            )
            complete = any(
                path == owner or path.startswith(owner + "/") for owner in owned
            )
            if complete:
                kind = (
                    "model"
                    if path == "/model"
                    else _KINDS.get(path.rsplit("/", 2)[-2], "")
                )
                writable = set(_COMMON) | set(_SCHEMAS.get(kind, {}))
                for key in (
                    found.keys() - wanted.keys()
                ) & writable - _NAMED_COLLECTIONS:
                    native_default = _DEFAULTS.get(key)
                    if kind == "relationship":
                        native_default = _RELATIONSHIP_DEFAULTS.get(key)
                    elif key == "type" and kind == "column":
                        native_default = "data"
                    elif key == "summarizeBy":
                        native_default = "default"
                    if found[key] == native_default or found[key] in (
                        None,
                        "",
                        [],
                        {},
                    ):
                        continue
                    # Omitting a property with a known default asks for that
                    # default, and content such as role members is never
                    # Fabric's to add. Another scalar may be a service default.
                    if annotation or (
                        native_default is None
                        and not isinstance(found[key], (list, dict))
                    ):
                        differences.append(f"{path}/{key}")
                    else:
                        raise InstallError(
                            f"Semantic readback retains removed property {path}/{key}"
                        )
                for key in _NAMED_COLLECTIONS.intersection(found.keys()):
                    # Fabric manages annotations of its own.
                    if key == "annotations" or (key == "columns" and calculated):
                        continue
                    expected_names = {
                        object_identity(v["name"]) for v in wanted.get(key, [])
                    }
                    if any(
                        object_identity(v["name"]) not in expected_names
                        for v in found.get(key, [])
                    ):
                        raise InstallError(f"Semantic readback differs at {path}/{key}")
            for key, value in wanted.items():
                default = (
                    _RELATIONSHIP_DEFAULTS.get(key)
                    if "/relationships/" in path
                    else _DEFAULTS.get(key)
                )
                contains(
                    value,
                    found.get(key, default),
                    f"{path}/{escape(key)}",
                    annotation or key == "annotations",
                )
            if calculated and not found.get("columns"):
                raise InstallError(
                    f"Semantic readback has no inferred column schema at {path}"
                )
        elif isinstance(wanted, list):
            if wanted == [] and found is None:
                return
            if found is None and path.rsplit("/", 1)[-1] in _NAMED_COLLECTIONS:
                found = []
            if not isinstance(found, list):
                differs(path, annotation)
                return
            if path.rsplit("/", 1)[-1] in _NAMED_COLLECTIONS:
                indexed = {
                    object_identity(v["name"]): v
                    for v in found
                    if isinstance(v, dict) and isinstance(v.get("name"), str)
                }
                if wanted == [] and found and not annotation:
                    raise InstallError(f"Semantic readback differs at {path}")
                for value in wanted:
                    contains(
                        value,
                        indexed.get(object_identity(value["name"])),
                        f"{path}/{escape(value['name'])}",
                        annotation,
                    )
            elif wanted != found:
                differs(path, annotation)
        else:
            same, layout = _same(path.rsplit("/", 1)[-1], wanted, found)
            if layout:
                differences.append(path)
            elif not same:
                differs(path, annotation)

    contains(canonical_model({"model": requested}), canonical_model(actual), "")
    return tuple(differences)
