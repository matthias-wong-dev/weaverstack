"""Native Report parts with deployment-only service binding."""

import base64
import hashlib
import json
import re

from .errors import ConfigError, InstallError


def service_reference(binding, schema):
    for field in ("workspace_id", "item_id"):
        value = binding.get(field)
        if not isinstance(value, str) or not re.fullmatch(_GUID, value):
            raise ConfigError(f"Report model binding has invalid {field}: {value!r}")
    connection = {
        "connectionString": f"Data Source=powerbi://api.powerbi.com/v1.0/myorg/{binding['workspace_id']};Initial Catalog={binding['item_id']};"
    }
    if schema.endswith("/1.0.0/schema.json"):
        connection.update(
            pbiServiceModelId=None,
            pbiModelVirtualServerName="sobe_wowvirtualserver",
            pbiModelDatabaseName=binding["item_id"],
            name="EntityDataSource",
            connectionType="pbiServiceXmlaStyleLive",
        )
    elif schema.endswith("/2.0.0/schema.json"):
        connection["connectionString"] += f"semanticModelId={binding['item_id']};"
    else:
        raise ConfigError(f"Unsupported Report definition schema {schema!r}")
    return {"byConnection": connection}


def encode_report(contribution):
    parts = dict(contribution.parts)
    if contribution.model is not None and contribution.binding is None:
        raise ConfigError("Report deployment needs a resolved semantic model binding")
    if contribution.model is not None:
        definition = json.loads(parts["definition.pbir"].decode("utf-8-sig"))
        definition["datasetReference"] = service_reference(
            contribution.binding, definition.get("$schema", "")
        )
        parts["definition.pbir"] = json.dumps(definition, ensure_ascii=False).encode()
    native_format = "PBIR-Legacy" if "report.json" in parts else "PBIR"
    result = {
        "format": native_format,
        "parts": [
            {
                "path": p,
                "payloadType": "InlineBase64",
                "payload": base64.b64encode(b).decode(),
            }
            for p, b in sorted(parts.items())
        ],
    }
    decode_report(result)
    return result


def _decoded_parts(definition):
    parts = {}
    for part in definition["parts"]:
        path = part["path"]
        if (
            not isinstance(path, str)
            or "\\" in path
            or any(p in {"", ".", ".."} or p != p.strip() for p in path.split("/"))
            or path in parts
            or part["payloadType"] != "InlineBase64"
        ):
            raise ValueError("invalid or duplicate Report part")
        parts[path] = base64.b64decode(part["payload"], validate=True)
    return parts


def decode_report(definition):
    try:
        parts = _decoded_parts(definition)
        enhanced = (
            "definition/report.json" in parts and "definition/version.json" in parts
        )
        legacy = "report.json" in parts
        if "definition.pbir" not in parts or enhanced == legacy:
            raise ValueError("expected one complete native Report definition")
        if definition.get("format", "PBIR" if enhanced else "PBIR-Legacy") != (
            "PBIR" if enhanced else "PBIR-Legacy"
        ):
            raise ValueError("Report format disagrees with native parts")
        return parts
    except (KeyError, TypeError, ValueError, AttributeError) as exc:
        raise ConfigError(f"Invalid Report definition: {exc}") from exc


def validate_service_report(definition):
    parts = decode_report(definition)
    try:
        authored = json.loads(parts["definition.pbir"].decode("utf-8-sig"))
        reference = authored["datasetReference"]
        if set(reference) != {"byConnection"} or not reference["byConnection"].get(
            "connectionString"
        ):
            raise ValueError("expected a service-bound Report definition")
    except (KeyError, TypeError, ValueError, AttributeError) as exc:
        raise ConfigError(f"Invalid Report service binding: {exc}") from exc


_NOT_JSON = object()
_GUID = r"[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}"


def _json_value(content):
    try:
        return json.loads(content.decode("utf-8-sig"))
    except (UnicodeDecodeError, ValueError):
        return _NOT_JSON


def part_digest(content):
    """Hash a Report part, a JSON part by its parsed value.

    Fabric rewrites JSON parts without changing their value, for example by
    dropping a final newline.
    """
    value = _json_value(content)
    if value is not _NOT_JSON:
        content = json.dumps(
            value, sort_keys=True, ensure_ascii=False, separators=(",", ":")
        ).encode()
    return hashlib.sha256(content).hexdigest()


def _same_part(expected, actual):
    value = _json_value(expected)
    if value is _NOT_JSON:
        return expected == actual
    return value == _json_value(actual)


def _model_ids(reference):
    """The model IDs a `datasetReference` names, in any form Fabric writes."""

    connection = (
        (reference or {}).get("byConnection") if isinstance(reference, dict) else None
    )
    if not isinstance(connection, dict):
        return set()
    found = re.findall(
        r"(?:^|;)\s*semanticModelId\s*=\s*([0-9a-fA-F-]+)\s*(?=;|$)",
        str(connection.get("connectionString", "")),
        flags=re.IGNORECASE,
    )
    found.append(str(connection.get("pbiModelDatabaseName", "")))
    return {value.casefold() for value in found if re.fullmatch(_GUID, value)}


def _platform(content):
    """`.platform` without the display name and logical ID Fabric assigns."""

    value = _json_value(content)
    if isinstance(value, dict):
        for section, key in (("metadata", "displayName"), ("config", "logicalId")):
            if isinstance(value.get(section), dict):
                value[section].pop(key, None)
    return value


def verify_report(desired, observed, *, binding=None, service_binding=None):
    """Confirm a deployed Report's model binding; return where Fabric's copy differs.

    The binding is Weaver's edit and must match. Every other part is native
    content passed through unchanged, which Fabric may rewrite, so those
    differences are returned rather than raised.
    """

    expected = decode_report(desired)
    try:
        actual = _decoded_parts(observed)
    except (KeyError, TypeError, ValueError, AttributeError) as exc:
        raise InstallError(f"Report readback is invalid: {exc}") from exc
    expected_properties = _json_value(expected.pop("definition.pbir"))
    actual_properties = _json_value(actual.pop("definition.pbir", b""))
    if not isinstance(actual_properties, dict):
        actual_properties = {}
    expected_reference = expected_properties.pop("datasetReference", None)
    actual_reference = actual_properties.pop("datasetReference", None)
    differences = []
    if binding is None:
        wanted, found = _model_ids(expected_reference), _model_ids(actual_reference)
        if wanted and found and not wanted & found:
            raise InstallError("Report binding does not match the authored model")
        if expected_reference != actual_reference:
            differences.append("definition.pbir datasetReference changed")
    else:
        if expected_reference != service_reference(
            binding, expected_properties.get("$schema", "")
        ):
            raise InstallError(
                "Report deployment binding differs from its model identity"
            )
        if not isinstance(service_binding, dict) or any(
            str(service_binding.get(key, "")).casefold() != binding[field].casefold()
            for key, field in (
                ("datasetId", "item_id"),
                ("datasetWorkspaceId", "workspace_id"),
            )
        ):
            raise InstallError(
                "Report service binding does not match the deployed model and workspace"
            )
        # Fabric's binding above is authoritative; the definition only has to
        # agree where it names a model at all.
        named = _model_ids(actual_reference)
        if named and binding["item_id"].casefold() not in named:
            raise InstallError("Report binding does not match the deployed model")
    if expected_properties != actual_properties:
        differences.append("definition.pbir changed")
    for path in sorted(set(expected) | set(actual)):
        if path not in actual:
            differences.append(f"{path} missing")
        elif path not in expected:
            differences.append(f"{path} extra")
        elif path == ".platform":
            if _platform(expected[path]) != _platform(actual[path]):
                differences.append(f"{path} changed")
        elif not _same_part(expected[path], actual[path]):
            differences.append(f"{path} changed")
    return tuple(differences)
