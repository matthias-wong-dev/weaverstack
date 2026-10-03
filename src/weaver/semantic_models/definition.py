"""Fabric definition transport for desired TMDL and observed TMSL."""

from __future__ import annotations

import base64
import binascii
import json

from ..errors import ConfigError


def encode_definition(model: dict, *, properties: dict | None = None) -> dict:
    properties = (
        properties if properties is not None else {"version": "4.2", "settings": {}}
    )
    return {
        "format": "TMSL",
        "parts": [
            {
                "path": path,
                "payloadType": "InlineBase64",
                "payload": base64.b64encode(
                    json.dumps(value, ensure_ascii=False, allow_nan=False).encode(
                        "utf-8"
                    )
                ).decode("ascii"),
            }
            for path, value in (("model.bim", model), ("definition.pbism", properties))
        ],
    }


def encode_parts(parts):
    return {
        "format": "TMDL",
        "parts": [
            {
                "path": path,
                "payloadType": "InlineBase64",
                "payload": base64.b64encode(content).decode("ascii"),
            }
            for path, content in sorted(parts.items())
        ],
    }


def decode_parts(definition):
    try:
        if definition["format"] != "TMDL":
            raise ValueError("expected TMDL format")
        parts = {}
        for part in definition["parts"]:
            path = part["path"]
            if (
                not isinstance(path, str)
                or path != path.strip()
                or "\\" in path
                or any(p in {"", ".", ".."} for p in path.split("/"))
                or path in parts
                or part["payloadType"] != "InlineBase64"
            ):
                raise ValueError("invalid or duplicate definition part")
            parts[path] = base64.b64decode(part["payload"], validate=True)
        if "definition.pbism" not in parts or not any(
            p.startswith("definition/") and p.endswith(".tmdl") for p in parts
        ):
            raise ValueError("expected definition.pbism and TMDL parts")
        if "model.bim" in parts:
            raise ValueError("TMDL and TMSL parts cannot be combined")
        return parts
    except (KeyError, TypeError, ValueError, AttributeError, binascii.Error) as exc:
        raise ConfigError(f"Invalid semantic TMDL definition: {exc}") from exc


def decode_model(definition: dict) -> dict:
    try:
        parts = [
            part for part in definition["parts"] if part.get("path") == "model.bim"
        ]
        if len(parts) != 1 or parts[0].get("payloadType") != "InlineBase64":
            raise ValueError("expected one InlineBase64 part")
        model = json.loads(
            base64.b64decode(parts[0]["payload"], validate=True).decode("utf-8-sig")
        )
        if not isinstance(model, dict) or not isinstance(model.get("model"), dict):
            raise ValueError("expected a TMSL model object")
        return model
    except (KeyError, TypeError, ValueError, AttributeError, binascii.Error) as exc:
        raise ConfigError(f"Invalid semantic model.bim definition: {exc}") from exc
