"""Native Report parts with deployment-only service binding."""

import base64
import hashlib
import json
import re

from .errors import ConfigError, InstallError


def service_reference(binding, schema):
    for field in ("workspace_id", "item_id"):
        value = binding.get(field)
        if not isinstance(value, str) or not re.fullmatch(
            r"[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}", value
        ):
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


def decode_report(definition):
    try:
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
_ZERO_ID = "00000000-0000-0000-0000-000000000000"


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


def _first_difference(expected, actual):
    for path in sorted(set(expected) | set(actual)):
        if path not in actual:
            return f"{path} missing"
        if path not in expected:
            return f"{path} extra"
        if not _same_part(expected[path], actual[path]):
            return f"{path} changed"
    return None


def _normalise_platform(expected, actual, report_name):
    """Undo Fabric's ``.platform`` rename to the target and its zero logicalId."""
    if ".platform" not in expected or ".platform" not in actual:
        return
    wanted, seen = _json_value(expected[".platform"]), _json_value(actual[".platform"])
    try:
        if seen["metadata"]["displayName"] != report_name:
            return
        seen["metadata"]["displayName"] = wanted["metadata"]["displayName"]
        if seen.get("config", {}).get("logicalId") == _ZERO_ID:
            seen["config"]["logicalId"] = wanted["config"]["logicalId"]
    except (KeyError, TypeError, AttributeError):
        return
    actual[".platform"] = json.dumps(seen).encode()


def verify_report(
    desired, observed, *, binding=None, service_binding=None, report_name=None
):
    expected, actual = decode_report(desired), decode_report(observed)
    try:
        expected_properties = json.loads(
            expected.pop("definition.pbir").decode("utf-8-sig")
        )
        actual_properties = json.loads(
            actual.pop("definition.pbir").decode("utf-8-sig")
        )
        expected_reference = expected_properties.pop("datasetReference")
        actual_reference = actual_properties.pop("datasetReference")
        if binding is None:
            if expected_reference != actual_reference:
                raise InstallError("Report binding does not match the deployed model")
        else:
            if expected_reference != service_reference(
                binding, expected_properties.get("$schema", "")
            ):
                raise InstallError(
                    "Report deployment binding differs from its model identity"
                )
            if not isinstance(service_binding, dict) or any(
                str(service_binding.get(key, "")).casefold()
                != binding[field].casefold()
                for key, field in (
                    ("datasetId", "item_id"),
                    ("datasetWorkspaceId", "workspace_id"),
                )
            ):
                raise InstallError(
                    "Report service binding does not match the deployed model and workspace"
                )
            if set(actual_reference) != {"byConnection"}:
                raise InstallError(
                    "Report binding readback is not a service connection"
                )
            connection = actual_reference["byConnection"]
            identifiers = re.findall(
                r"(?:^|;)\s*semanticModelId\s*=\s*([0-9a-fA-F-]+)\s*(?=;|$)",
                connection.get("connectionString", ""),
                flags=re.IGNORECASE,
            )
            if not identifiers:
                identifiers = [connection.get("pbiModelDatabaseName", "")]
            if (
                len(identifiers) != 1
                or identifiers[0].casefold() != binding["item_id"].casefold()
            ):
                raise InstallError("Report binding does not match the deployed model")
    except (KeyError, ValueError, TypeError, AttributeError) as exc:
        raise InstallError(f"Report binding readback is invalid: {exc}") from exc
    _normalise_platform(expected, actual, report_name)
    difference = (
        "definition.pbir changed"
        if expected_properties != actual_properties
        else _first_difference(expected, actual)
    )
    if difference:
        raise InstallError(
            f"Report definition readback differs from deployed native parts: {difference}"
        )
