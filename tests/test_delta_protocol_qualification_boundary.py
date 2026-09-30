"""Fabric protocol instrumentation uses the production declaration and payload."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest
from support.weaver_test import weaver_test

from weaver.spark import FabricSparkTarget


def _qualification():
    path = (
        Path(__file__).parent / "fabric" / "test_delta_protocol_lakehouse_boundary.py"
    )
    spec = importlib.util.spec_from_file_location("protocol_qualification", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@weaver_test()
@pytest.mark.parametrize("route", ("direct", "spark"))
@pytest.mark.parametrize("case", range(5))
def test_runtime_qualification_generates_its_actual_authored_payload(route, case):
    import json

    module = _qualification()
    name, dtype, expression, _read, _expected, identity = module.CASES[case]
    source = module._source(
        f"{route}_{name}", dtype, expression, identity, name == "explicit_scalar"
    )
    payload = json.loads(
        source.create_ddl(
            destination=FabricSparkTarget("Workspace", "Lakehouse")
        ).content
    )
    assert payload["protocol_minima"] == (
        {"minReaderVersion": 2, "minWriterVersion": 5}
        if name == "explicit_scalar"
        else {"minReaderVersion": 3, "minWriterVersion": 7}
    )
    assert payload["audit_columns"]
    assert (
        payload["identity_column"][0] if identity else payload["identity_column"]
    ) == ("Id" if identity else None)
    compile(module._observation_program([]), "protocol-observer", "exec")


@weaver_test()
def test_runtime_qualification_calls_existing_session_statement_capabilities():
    import ast
    import inspect

    from weaver.sessions import ConsoleSession

    module = _qualification()
    tree = ast.parse(inspect.getsource(module.protocol_estate.__wrapped__))
    methods = {
        node.func.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "weaver_session"
    }
    assert methods
    assert all(callable(getattr(ConsoleSession, method, None)) for method in methods), (
        methods
    )


@weaver_test()
def test_runtime_qualification_accepts_explicit_minima_promoted_by_a_table_feature():
    module = _qualification()
    # Hypothetical observation exercises minimum semantics, not Fabric support.
    seen = {
        "protocol": {
            "minReaderVersion": 3,
            "minWriterVersion": 7,
            "readerFeatures": ["columnMapping", "deletionVectors"],
            "writerFeatures": ["columnMapping", "deletionVectors"],
        },
        "snapshot": {
            "metaData": {"configuration": {"delta.enableDeletionVectors": "true"}}
        },
        "schema": {"fields": [{"name": "Value", "type": "string"}]},
        "rows": [{"V": "hello"}],
        "expected": "hello",
        "dtype": "string",
    }
    module.test_runtime_table_protocol_features_and_readback_match_the_declaration(
        "spark",
        "explicit_scalar",
        (
            {"spark_explicit_scalar": SimpleNamespace(status="succeeded")},
            {"spark_explicit_scalar": seen, "runtime": {"deltalake": "1.6.6"}},
        ),
    )


@weaver_test()
@pytest.mark.parametrize("backing", ["none", "flag_only", "feature_only"])
def test_runtime_qualification_rejects_unexplained_explicit_minima_promotion(backing):
    module = _qualification()
    seen = {
        "protocol": {
            "minReaderVersion": 3,
            "minWriterVersion": 7,
            "readerFeatures": ["columnMapping"],
            "writerFeatures": ["columnMapping"],
        },
        "snapshot": {"metaData": {"configuration": {}}},
        "schema": {"fields": [{"name": "Value", "type": "string"}]},
        "rows": [{"V": "hello"}],
        "expected": "hello",
        "dtype": "string",
    }
    if backing == "flag_only":
        seen["snapshot"]["metaData"]["configuration"]["delta.enableDeletionVectors"] = (
            "true"
        )
    if backing == "feature_only":
        seen["protocol"]["readerFeatures"].append("deletionVectors")
        seen["protocol"]["writerFeatures"].append("deletionVectors")
    with pytest.raises(AssertionError):
        module.test_runtime_table_protocol_features_and_readback_match_the_declaration(
            "spark",
            "explicit_scalar",
            (
                {"spark_explicit_scalar": SimpleNamespace(status="succeeded")},
                {"spark_explicit_scalar": seen, "runtime": {"deltalake": "1.6.6"}},
            ),
        )
