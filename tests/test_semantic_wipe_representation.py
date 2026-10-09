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
        ((shared_source_tmdl().encode(), "Reporting.tmdl"),),
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
        empty_parts("Reporting"), ((shared_source_tmdl().encode(), "Reporting.tmdl"),)
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
        "culture",
        "name",
        "visible",
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
    elif fault == "culture":
        actual["model"]["culture"] = "fr-FR"
    elif fault == "name":
        shell["name"] = "Wrong"
    elif fault == "visible":
        shell["isHidden"] = False
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
def test_preserved_readback_tolerates_service_normalisation_and_names_rewrites():
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
    partition["name"] = "'source'"
    expressions = copy.deepcopy(original["model"]["expressions"])
    text = expressions[0]["expression"]
    if isinstance(text, list):
        text = "\n".join(text)
    expressions[0]["expression"] = text.replace(", ", ",")
    expressions[0]["lineageTag"] = "assigned-by-fabric"
    actual = {
        "model": {
            "culture": "EN-us",
            "tables": [
                {
                    "name": "'__weaversource'",
                    "isHidden": True,
                    "partitions": [partition],
                }
            ],
            "expressions": expressions,
        }
    }
    listed = [{**connections[0], "displayName": "Assigned", "privacyLevel": "None"}]

    differences = verify_reset(spec, actual, listed)

    assert differences == ("/model/expressions/Warehouse~1Serving/expression",)


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


class _WipeProjection(dict):
    def __init__(self, values, fields):
        super().__init__(values)
        self.fields = fields

    def get(self, key, default=None):
        assert key in self.fields, f"Readback inspected non-wipe field {key}"
        return super().get(key, default)


@pytest.mark.parametrize("preserve", [False, True])
@weaver_test()
def test_readback_projects_only_the_wipe_owned_model_and_source_shell(preserve):
    from weaver.semantic_models.wipe import reset_definition, verify_reset

    original, parts, connections = _preserved_inputs()
    spec = reset_definition(
        "Reporting",
        original,
        parts=parts,
        preserve_data_source=preserve,
        connections=connections,
    )
    model = copy.deepcopy(spec["expected"])
    model["tables"] = [
        _WipeProjection(
            table, {"name", "isHidden", "columns", "measures", "partitions"}
        )
        for table in model["tables"]
    ]
    verify_reset(
        spec,
        {"model": _WipeProjection(model, {"culture", "tables", "expressions"})},
        connections if preserve else [],
    )


def _import_inputs():
    from weaver.semantic_models.extensions import merge_extensions
    from weaver.semantic_models.render import empty_parts

    expression = (
        'let\n    Source = Sql.Database("server", "Catalogue"),\n'
        '    Navigation = Source{[Schema="_", Item="TableDictionary"]}[Data]\n'
        "in\n    Navigation"
    )
    partition = {
        "name": "Objects",
        "mode": "import",
        "source": {"type": "m", "expression": expression},
    }
    observed = {
        "model": {
            "culture": "en-US",
            "tables": [
                {
                    "name": "Objects",
                    "columns": [{"name": "Item name", "dataType": "string"}],
                    "partitions": [partition],
                },
            ],
        }
    }
    text = (
        "table Objects\n\tcolumn 'Item name'\n\t\tdataType: string\n"
        "\tpartition Objects = m\n\t\tmode: import\n\t\tsource =\n"
        + "".join("\t\t\t" + line + "\n" for line in expression.splitlines())
    )
    parts = merge_extensions(
        empty_parts("Reporting"), ((text.encode(), "source.tmdl"),)
    ).parts
    connections = [
        {
            "id": "12345678-1234-1234-1234-123456789abc",
            "connectivityType": "ShareableCloud",
            "connectionDetails": {"type": "SQL", "path": "server;Catalogue"},
        }
    ]
    return observed, parts, connections


@weaver_test()
def test_explicit_sql_import_retains_native_m_and_binding_in_columnless_shell():
    from weaver.semantic_models.wipe import reset_definition, verify_reset

    observed, native, connections = _import_inputs()
    result = reset_definition(
        "Reporting",
        observed,
        parts=native,
        preserve_data_source=True,
        connections=connections,
    )
    parts = decode_parts(result["definition"])
    text = parts["definition/tables/__WeaverSource.tmdl"]
    assert b"partition 'Source' = m" in text
    assert b"mode: import" in text
    assert b'Source = Sql.Database("server", "Catalogue")' in text
    assert b'Navigation = Source{[Schema="_", Item="TableDictionary"]}[Data]' in text
    assert b"column " not in text
    assert (
        result["expected"]["tables"][0]["partitions"][0]["source"]
        == observed["model"]["tables"][0]["partitions"][0]["source"]
    )
    verify_reset(result, {"model": result["expected"]}, connections)


@pytest.mark.parametrize(
    "fault",
    [
        "unbound",
        "automatic",
        "gateway",
        "non_sql",
        "multiple_connections",
        "wrong_path",
        "direct_query",
        "mixed_mode",
        "transformed_m",
        "escaped_identity",
        "shared_expression",
        "legacy_datasource",
        "missing_native",
    ],
)
@weaver_test()
def test_explicit_import_refuses_unsupported_sources_before_mutation(fault):
    from test_semantic_wipe_cycle import setup

    from weaver import wipe
    from weaver.errors import CommandError
    from weaver.semantic_models.definition import encode_parts

    original, parts, connections = _import_inputs()
    partition = original["model"]["tables"][0]["partitions"][0]
    if fault == "unbound":
        connections[0].pop("id")
    elif fault == "automatic":
        connections[0]["connectivityType"] = "Automatic"
    elif fault == "gateway":
        connections[0]["connectivityType"] = "OnPremisesGateway"
    elif fault == "non_sql":
        connections[0]["connectionDetails"]["type"] = "Web"
    elif fault == "multiple_connections":
        connections.append(copy.deepcopy(connections[0]))
    elif fault == "wrong_path":
        connections[0]["connectionDetails"]["path"] = "other;Catalogue"
    elif fault == "direct_query":
        partition["mode"] = "directQuery"
    elif fault == "mixed_mode":
        other = copy.deepcopy(partition)
        other["mode"] = "directLake"
        original["model"]["tables"][0]["partitions"].append(other)
    elif fault == "transformed_m":
        partition["source"]["expression"] += " & OtherSource"
    elif fault == "escaped_identity":
        partition["source"]["expression"] = partition["source"]["expression"].replace(
            '"server"', '"s#(lf)erver"'
        )
    elif fault == "shared_expression":
        original["model"]["expressions"] = [
            {"name": "Other", "kind": "m", "expression": "1"}
        ]
    elif fault == "legacy_datasource":
        original["model"]["dataSources"] = [{"name": "Other"}]
    elif fault == "missing_native":
        parts = {
            p: v for p, v in parts.items() if not p.startswith("definition/tables/")
        }
    session, client = setup()
    client.before, client.native, client.connections = (
        original,
        encode_parts(parts),
        connections,
    )
    with pytest.raises(CommandError, match="preserve-data-source|Source TMDL"):
        wipe(
            ("Warehouse/Reporting", "SemanticModel/Reporting"),
            session=session,
            preserve_data_source=True,
        )
    assert not client.updated
    assert not any(call.kind == "execute_mutation" for call in session.calls)


@pytest.mark.parametrize("changed_binding", [False, True])
@weaver_test()
def test_public_import_wipe_requires_the_same_explicit_binding_after_update(
    changed_binding,
):
    from test_semantic_wipe_cycle import setup

    from weaver import wipe
    from weaver.semantic_models.definition import encode_parts
    from weaver.semantic_models.wipe import reset_definition

    original, parts, connections = _import_inputs()
    session, client = setup()
    client.before, client.native, client.connections = (
        original,
        encode_parts(parts),
        connections,
    )
    spec = reset_definition(
        "Reporting",
        original,
        parts=parts,
        preserve_data_source=True,
        connections=connections,
    )
    client.after = {"model": spec["expected"]}
    if changed_binding:
        from weaver.errors import CommandError

        update = client.update_definition

        def lose_binding(*args, **kwargs):
            result = update(*args, **kwargs)
            client.connections = []
            return result

        client.update_definition = lose_binding
        with pytest.raises(CommandError, match="wipe did not complete.*connection"):
            wipe("SemanticModel/Reporting", session=session, preserve_data_source=True)
    else:
        result = wipe(
            "SemanticModel/Reporting", session=session, preserve_data_source=True
        )
        assert result.emptied == ("SemanticModel/Reporting",)
        assert client.connections == connections
    assert len([c for c in client.calls if c[0] == "update"]) == 1
