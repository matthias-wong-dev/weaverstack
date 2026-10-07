"""Load binds a model's data sources to the connection that reaches them."""

import json

import pytest
from support.weaver_test import weaver_test
from test_semantic_model_rest_boundary import Client, response

from weaver.fabric.semantic_model import (
    ConnectionBindingError,
    SemanticModelClient,
    _refresh_errors,
)

SERVER = "abc.datawarehouse.fabric.microsoft.com"


def source(database, *, bound=False):
    value = {
        "datasourceType": "Sql",
        "connectionDetails": {"server": SERVER, "database": database},
    }
    if bound:
        value.update(datasourceId="connection-id", gatewayId="gateway-id")
    return value


def connection(identifier, path, *, kind="ShareableCloud", name=None):
    return {
        "id": identifier,
        "displayName": name or identifier,
        "connectivityType": kind,
        "connectionDetails": {"type": "SQL", "path": path},
    }


def scripted(sources, connections=None):
    power_bi = Client(response({"value": sources}))
    power_bi.responses[0].datasources = True
    fabric = Client(
        *(() if connections is None else (response({"value": connections}),)),
        response({}),
    )
    return SemanticModelClient("ws", "model", fabric=fabric, power_bi=power_bi), fabric


@weaver_test()
def test_unbound_source_binds_to_the_connection_with_its_path():
    model, fabric = scripted(
        [source("DEV_Curated")],
        [
            connection("prod", f"{SERVER};Curated"),
            connection("dev", f"{SERVER.upper()};dev_curated"),
            connection("personal", f"{SERVER};DEV_Curated", kind="PersonalCloud"),
        ],
    )
    assert model.bind_data_sources(label="SemanticModel/Sales") == (
        f"{SERVER};DEV_Curated",
    )
    (_, (method, path, request)) = fabric.calls
    assert (method, path) == (
        "POST",
        "workspaces/ws/semanticModels/model/bindConnection",
    )
    assert request["payload"] == {
        "connectionBinding": {
            "id": "dev",
            "connectivityType": "ShareableCloud",
            "connectionDetails": {"type": "SQL", "path": f"{SERVER};DEV_Curated"},
        }
    }
    assert request["retry_transient"] is False


@weaver_test()
def test_bound_sources_need_no_connection_lookup():
    model, fabric = scripted([source("Curated", bound=True)])
    assert model.bind_data_sources(label="SemanticModel/Sales") == ()
    assert not fabric.calls


@pytest.mark.parametrize(
    "connections,message",
    [
        ([], "no cloud connection reaches it. Create a SQL cloud connection"),
        (
            [
                connection("a", f"{SERVER};DEV_Curated", name="First"),
                connection("b", f"{SERVER};DEV_Curated", name="Second"),
            ],
            "several connections reach it: First, Second. Keep one.",
        ),
    ],
)
@weaver_test()
def test_unbindable_source_fails_in_one_line(connections, message):
    model, fabric = scripted([source("DEV_Curated")], connections)
    with pytest.raises(ConnectionBindingError) as failed:
        model.bind_data_sources(label="SemanticModel/Sales")
    text = str(failed.value)
    assert text.startswith(
        f"SemanticModel/Sales reads database DEV_Curated on {SERVER}"
    )
    assert message in text and "\n" not in text
    assert all(call[0] == "GET" for call in fabric.calls)


@weaver_test()
def test_refresh_failure_reads_as_one_message_per_cause():
    cause = (
        "We cannot refresh this semantic model because this semantic model uses a "
        "default data connection without explicit connection credentials."
    )
    wrapped = json.dumps(
        {
            "error": {
                "code": "Premium_ASWL_Error",
                "pbi.error": {
                    "code": "Premium_ASWL_Error",
                    "details": [
                        {
                            "code": "Premium_ASWL_Error_Details_Label",
                            "detail": {"type": 1, "value": cause},
                        }
                    ],
                },
            }
        }
    )
    body = {
        "status": "Failed",
        "messages": [{"type": "Error", "message": wrapped}] * 9
        + [
            {
                "type": "Error",
                "message": "The current operation was cancelled because another "
                "operation in the transaction failed.",
            }
        ],
        "serviceExceptionJson": wrapped,
    }
    assert _refresh_errors(body) == [cause, "(Premium_ASWL_Error)"]


@pytest.mark.parametrize("bind", [False, True])
@weaver_test()
def test_build_binds_after_deploying_only_when_asked(tmp_path, bind):
    from dataclasses import replace
    from types import SimpleNamespace

    from test_semantic_model_build_cycle import DefinitionClient, prepared

    from weaver.build_bundle.executors.semantic import SemanticModelExecutor
    from weaver.build_bundle.semantic import semantic_stage
    from weaver.declaration.model import WeaverItemId

    _, repository, bindings, _, _ = prepared(tmp_path)
    item = WeaverItemId.parse("SemanticModel/Reporting")
    repository = replace(
        repository,
        semantic_models={
            item: replace(repository.semantic_models[item], bind_data_sources=bind)
        },
    )
    target = bindings.by_item[item].to_bound_target()
    stage = semantic_stage(repository, item, target)
    (payload,) = stage.payloads.values()
    client = DefinitionClient()
    context = SimpleNamespace(
        target=SimpleNamespace(bound=target), semantic_model=lambda bound: client
    )
    SemanticModelExecutor().execute(None, payload, context)
    assert [call[0] for call in client.calls] == ["update_definition"] + (
        ["bind_data_sources"] if bind else []
    )
    assert (
        repository.semantic_models[item].signature
        == replace(
            repository.semantic_models[item], bind_data_sources=not bind
        ).signature
    )
