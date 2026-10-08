"""Native Report parts with deployment-only service binding."""

import base64
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


def verify_report(desired, observed):
    expected, actual = decode_report(desired), decode_report(observed)
    for parts in (expected, actual):
        try:
            reference = json.loads(parts["definition.pbir"].decode("utf-8-sig"))[
                "datasetReference"
            ]
        except (KeyError, ValueError, TypeError) as exc:
            raise InstallError(f"Report binding readback is invalid: {exc}") from exc
        if parts is expected:
            binding = reference
        elif reference != binding:
            raise InstallError("Report binding does not match the deployed model")
    # Fabric can reserialize the binding JSON; every other native part stays exact.
    expected_binding = json.loads(expected.pop("definition.pbir").decode("utf-8-sig"))
    actual_binding = json.loads(actual.pop("definition.pbir").decode("utf-8-sig"))
    if expected_binding != actual_binding or expected != actual:
        raise InstallError(
            "Report definition readback differs from deployed native parts"
        )
