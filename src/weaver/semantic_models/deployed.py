"""Verify requested edits against observed semantic metadata."""

from ..errors import InstallError
from .compiler import _NAMED_COLLECTIONS

# TMSL omits these default-valued relationship properties on readback.
_RELATIONSHIP_DEFAULTS = {
    "fromCardinality": "many",
    "toCardinality": "one",
    "crossFilteringBehavior": "oneDirection",
    "isActive": True,
}


def canonical_model(model):
    def normalise(value, key=""):
        if isinstance(value, dict):
            return {k: normalise(v, k) for k, v in value.items()}
        if isinstance(value, list):
            if key in {
                "expression",
                "description",
                "filterExpression",
                "value",
            } and all(isinstance(v, str) for v in value):
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


def verify_requested(requested, actual, *, owned=(), absent=()):
    from .compiler import _COMMON, _SCHEMAS, escape

    if not isinstance(actual, dict) or not isinstance(actual.get("model"), dict):
        raise InstallError("Semantic readback has no model object")

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
                name = member["name"].casefold()
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
            collection = {"table": "tables", "column": "columns"}[kind]
            node = next(
                (
                    member
                    for member in node.get(collection, [])
                    if member["name"].casefold() == name.casefold()
                ),
                None,
            )
            if node is None:
                break
        else:
            raise InstallError(f"Semantic readback retains excluded object {path!r}")
    defaults = {
        "isHidden": False,
        "isKey": False,
        "isNullable": True,
        "discourageImplicitMeasures": False,
    }

    def contains(wanted, found, path):
        if isinstance(wanted, dict):
            if not isinstance(found, dict):
                raise InstallError(f"Semantic readback differs at {path}")
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
                    else {
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
                    }.get(path.rsplit("/", 2)[-2], "")
                )
                writable = set(_COMMON) | set(_SCHEMAS.get(kind, {}))
                for key in (
                    found.keys() - wanted.keys()
                ) & writable - _NAMED_COLLECTIONS:
                    native_default = defaults.get(key)
                    if kind == "relationship":
                        native_default = _RELATIONSHIP_DEFAULTS.get(key)
                    elif key == "type" and kind == "column":
                        native_default = "data"
                    elif key == "summarizeBy":
                        native_default = "default"
                    if found[key] != native_default and found[key] not in (
                        None,
                        "",
                        [],
                        {},
                    ):
                        raise InstallError(
                            f"Semantic readback retains removed property {path}/{key}"
                        )
                for key in _NAMED_COLLECTIONS.intersection(
                    wanted.keys() | found.keys()
                ):
                    if key == "columns" and calculated:
                        continue
                    expected_names = {v["name"].casefold() for v in wanted.get(key, [])}
                    actual_names = {v["name"].casefold() for v in found.get(key, [])}
                    if expected_names != actual_names:
                        raise InstallError(f"Semantic readback differs at {path}/{key}")
            for key, value in wanted.items():
                default = (
                    _RELATIONSHIP_DEFAULTS.get(key)
                    if "/relationships/" in path
                    else defaults.get(key)
                )
                received = found.get(key, default)
                if (
                    key == "name"
                    and isinstance(received, str)
                    and isinstance(value, str)
                ):
                    received, value = received.casefold(), value.casefold()
                contains(value, received, f"{path}/{escape(key)}")
            if calculated and not found.get("columns"):
                raise InstallError(
                    f"Semantic readback has no inferred column schema at {path}"
                )
        elif isinstance(wanted, list):
            if wanted == [] and found is None:
                return
            if not isinstance(found, list):
                raise InstallError(f"Semantic readback differs at {path}")
            if path.rsplit("/", 1)[-1] in _NAMED_COLLECTIONS:
                indexed = {
                    v["name"].casefold(): v
                    for v in found
                    if isinstance(v, dict) and isinstance(v.get("name"), str)
                }
                if wanted == [] and found:
                    raise InstallError(f"Semantic readback differs at {path}")
                for value in wanted:
                    contains(
                        value,
                        indexed.get(value["name"].casefold()),
                        f"{path}/{escape(value['name'])}",
                    )
            elif wanted != found:
                raise InstallError(f"Semantic readback differs at {path}")
        elif wanted != found:
            raise InstallError(f"Semantic readback differs at {path}")

    contains(canonical_model({"model": requested}), canonical_model(actual), "")
