"""Verify the deployed TMSL against the assembled incoming definition."""

from ..errors import InstallError

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
            if key == "expression" and all(isinstance(v, str) for v in value):
                return "\n".join(value)
            values = [normalise(v) for v in value]
            if values and all(
                isinstance(v, dict) and isinstance(v.get("name"), str) for v in values
            ):
                return sorted(values, key=lambda v: v["name"])
            return values
        return value

    return normalise({k: v for k, v in model.items() if k not in {"name", "id"}})


def verify_deployed(expected, actual):
    """Require authored properties and engine-inferred calculated columns."""

    named_collections = {
        "tables",
        "columns",
        "measures",
        "partitions",
        "hierarchies",
        "levels",
        "relationships",
        "roles",
        "tablePermissions",
    }

    def contains(wanted, found, path):
        if isinstance(wanted, dict):
            if not isinstance(found, dict):
                raise InstallError(f"Semantic readback differs at {path}")
            calculated = any(
                p.get("source", {}).get("type") == "calculated"
                for p in wanted.get("partitions", [])
            )
            for key in named_collections.intersection(wanted.keys() | found.keys()):
                if key == "columns" and calculated:
                    continue
                expected_names = {child["name"] for child in wanted.get(key, [])}
                found_names = {child["name"] for child in found.get(key, [])}
                if expected_names != found_names:
                    raise InstallError(f"Semantic readback differs at {path}/{key}")
            for key, value in wanted.items():
                default = (
                    _RELATIONSHIP_DEFAULTS.get(key)
                    if "/relationships/" in path
                    else None
                )
                contains(value, found.get(key, default), f"{path}/{key}")
        elif isinstance(wanted, list):
            if not isinstance(found, list):
                if wanted == [] and found is None:
                    return
                raise InstallError(f"Semantic readback differs at {path}")
            if all(isinstance(v, dict) and "name" in v for v in wanted):
                indexed = {
                    v["name"]: v for v in found if isinstance(v, dict) and "name" in v
                }
                for value in wanted:
                    contains(
                        value, indexed.get(value["name"]), f"{path}/{value['name']}"
                    )
            elif wanted != found:
                raise InstallError(f"Semantic readback differs at {path}")
        elif wanted != found:
            raise InstallError(f"Semantic readback differs at {path}")

    contains(canonical_model(expected), canonical_model(actual), "")
    for table in actual["model"].get("tables", []):
        if any(
            p.get("source", {}).get("type") == "calculated"
            for p in table.get("partitions", [])
        ):
            columns = table.get("columns", [])
            if not columns or any(
                not c.get("name") or not c.get("dataType") for c in columns
            ):
                raise InstallError(
                    f"Semantic readback has no inferred column schema for {table['name']!r}"
                )
