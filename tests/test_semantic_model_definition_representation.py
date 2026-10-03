"""Canonical TMSL is packaged directly as a Fabric item definition."""

import base64
import json

import pytest
from support.weaver_test import weaver_test

from weaver.errors import ConfigError


@weaver_test()
def test_definition_packaging_preserves_tmsl_and_pbism_properties():
    from weaver.semantic_models.definition import decode_model, encode_definition

    model = {"compatibilityLevel": 1606, "model": {"culture": "en-AU", "tables": []}}
    properties = {"version": "4.2", "settings": {"qnaEnabled": False}}
    definition = encode_definition(model, properties=properties)
    assert definition["format"] == "TMSL"
    assert [part["path"] for part in definition["parts"]] == [
        "model.bim",
        "definition.pbism",
    ]
    assert all(part["payloadType"] == "InlineBase64" for part in definition["parts"])
    decoded = [
        json.loads(base64.b64decode(part["payload"], validate=True))
        for part in definition["parts"]
    ]
    assert decoded == [model, properties]
    assert decode_model(definition) == model


@pytest.mark.parametrize(
    "parts",
    [
        [],
        [{"path": "model.bim", "payload": "bad"}],
        [{"path": "model.bim", "payloadType": "InlineBase64", "payload": "e30="}],
    ],
)
@weaver_test()
def test_definition_read_requires_a_valid_canonical_model(parts):
    from weaver.semantic_models.definition import decode_model

    with pytest.raises(ConfigError, match="model.bim"):
        decode_model({"parts": parts})
