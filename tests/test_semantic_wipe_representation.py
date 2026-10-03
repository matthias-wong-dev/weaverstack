"""Native empty-model reset keeps the target language and removes authored content."""

import copy

import pytest
from support.semantic_models import probe_model
from support.weaver_test import weaver_test

from weaver.semantic_models.definition import decode_parts


@weaver_test()
def test_plain_reset_uses_native_tmdl_and_retains_the_existing_culture():
    from weaver.semantic_models.wipe import reset_definition

    observed = probe_model()
    observed["model"]["culture"] = "fr-FR"
    result = reset_definition("Reporting", observed)
    parts = decode_parts(result["definition"])

    assert b"culture: fr-FR" in parts["definition/model.tmdl"]
    assert not any(path.startswith("definition/tables/") for path in parts)
    assert result["preserve_data_source"] is False
    assert result["retained_sources"] == []


@pytest.mark.parametrize("newline", [b"\n", b"\r\n"], ids=["lf", "crlf"])
@weaver_test()
def test_preserve_uses_one_hidden_columnless_native_source_anchor(newline):
    from support.semantic_models import shared_source_tmdl, source_model

    from weaver.semantic_models.extensions import merge_extensions
    from weaver.semantic_models.render import empty_parts
    from weaver.semantic_models.tmdl import Document
    from weaver.semantic_models.wipe import reset_definition

    native = merge_extensions(
        empty_parts("Reporting"),
        ((shared_source_tmdl().encode(), "extension.tmdl"),),
    ).parts
    observed = source_model()
    native = {path: data.replace(b"\n", newline) for path, data in native.items()}
    connections = [
        {
            "connectivityType": "Automatic",
            "connectionDetails": {"type": "SQL", "path": "source"},
        }
    ]
    result = reset_definition(
        "Reporting",
        observed,
        parts=native,
        preserve_data_source=True,
        connections=connections,
    )
    parts = decode_parts(result["definition"])
    spans = [
        span
        for path, data in parts.items()
        if path.endswith(".tmdl")
        for span in Document(path, data).spans
    ]

    assert [span.name for span in spans if span.kind == "table"] == ["__WeaverSource"]
    assert not any(
        span.kind in {"column", "measure", "relationship", "role"} for span in spans
    )
    assert b"isHidden: true" in parts["definition/tables/__WeaverSource.tmdl"]
    assert result["retained_sources"] == ["Warehouse/Serving"]
    assert result["preserve_data_source"] is True
    assert parts["definition/expressions.tmdl"] == native["definition/expressions.tmdl"]
    assert (
        b"expressionSource: 'Warehouse/Serving'"
        in parts["definition/tables/__WeaverSource.tmdl"]
    )


def _preserved_inputs():
    from support.semantic_models import shared_source_tmdl, source_model

    from weaver.semantic_models.extensions import merge_extensions
    from weaver.semantic_models.render import empty_parts

    parts = merge_extensions(
        empty_parts("Reporting"), ((shared_source_tmdl().encode(), "extension.tmdl"),)
    ).parts
    connections = [
        {
            "connectivityType": "Automatic",
            "connectionDetails": {"type": "SQL", "path": "source"},
        }
    ]
    return source_model(), parts, connections


@pytest.mark.parametrize(
    "case",
    [
        "shared",
        "missing",
        "multiple",
        "non_sql",
        "import",
        "multiple_expressions",
        "missing_expression",
        "missing_native_partition",
    ],
)
@weaver_test()
def test_preserve_refuses_unqualified_or_incomplete_source_configuration(case):
    from weaver.errors import CommandError
    from weaver.semantic_models.wipe import reset_definition

    observed, parts, connections = _preserved_inputs()
    if case == "shared":
        connections[0].update(
            connectivityType="ShareableCloud", id="approved-connection"
        )
    elif case == "missing":
        connections.clear()
    elif case == "multiple":
        connections.append(copy.deepcopy(connections[0]))
    elif case == "non_sql":
        connections[0]["connectionDetails"]["type"] = "Web"
    elif case == "import":
        observed["model"]["tables"][0]["partitions"][0]["mode"] = "import"
    elif case == "multiple_expressions":
        observed["model"]["tables"][1]["partitions"][0]["source"][
            "expressionSource"
        ] = "Other"
    elif case == "missing_expression":
        observed["model"]["expressions"] = []
    elif case == "missing_native_partition":
        parts.pop("definition/tables/Sales.tmdl")
    with pytest.raises(CommandError, match="preserve|source|Source|TMDL"):
        reset_definition(
            "Reporting",
            observed,
            parts=parts,
            preserve_data_source=True,
            connections=connections,
        )


@weaver_test()
def test_preserve_on_a_model_with_no_external_source_is_an_empty_reset():
    from weaver.semantic_models.wipe import reset_definition

    observed = {"model": {"culture": "en-US", "tables": []}}
    result = reset_definition("Reporting", observed, preserve_data_source=True)
    assert result["preserve_data_source"] is True
    assert result["retained_sources"] == []
    assert not any(
        path.startswith("definition/tables/")
        for path in decode_parts(result["definition"])
    )


@pytest.mark.parametrize(
    "fault",
    [
        None,
        "table",
        "column",
        "measure",
        "relationship",
        "role",
        "expression",
        "partition",
        "binding",
    ],
)
@weaver_test()
def test_preserved_readback_requires_only_the_source_shell_and_same_connection(fault):
    from weaver.errors import InstallError
    from weaver.semantic_models.wipe import reset_definition, verify_reset

    original, parts, connections = _preserved_inputs()
    spec = reset_definition(
        "Reporting",
        original,
        parts=parts,
        preserve_data_source=True,
        connections=connections,
    )
    partition = copy.deepcopy(original["model"]["tables"][0]["partitions"][0])
    partition["name"] = "Source"
    shell = {"name": "__WeaverSource", "isHidden": True, "partitions": [partition]}
    actual = {
        "model": {
            "culture": "en-US",
            "tables": [shell],
            "expressions": copy.deepcopy(original["model"]["expressions"]),
        }
    }
    if fault == "table":
        actual["model"]["tables"].append({"name": "Sales"})
    elif fault == "column":
        shell["columns"] = [{"name": "Id"}]
    elif fault == "measure":
        shell["measures"] = [{"name": "Rows", "expression": "1"}]
    elif fault == "relationship":
        actual["model"]["relationships"] = [{"name": "Old"}]
    elif fault == "role":
        actual["model"]["roles"] = [{"name": "Old"}]
    elif fault == "expression":
        actual["model"]["expressions"][0]["expression"] = "changed"
    elif fault == "partition":
        partition["source"]["entityName"] = "Wrong"
    elif fault == "binding":
        connections.clear()
    if fault:
        with pytest.raises(InstallError, match="wipe|source|connection"):
            verify_reset(spec, actual, connections)
    else:
        verify_reset(spec, actual, connections)


@weaver_test()
def test_plain_readback_refuses_residual_content_or_connections():
    from weaver.errors import InstallError
    from weaver.semantic_models.wipe import reset_definition, verify_reset

    spec = reset_definition("Reporting", {"model": {"culture": "en-US"}})
    verify_reset(spec, {"model": {"culture": "en-US"}}, [])
    with pytest.raises(InstallError, match="wipe"):
        verify_reset(
            spec, {"model": {"culture": "en-US", "tables": [{"name": "Old"}]}}, []
        )
    with pytest.raises(InstallError, match="connection"):
        verify_reset(
            spec, {"model": {"culture": "en-US"}}, [{"connectivityType": "Automatic"}]
        )
