"""Load binds a model's data sources to the connection that reaches them."""

import json
from types import SimpleNamespace

import pytest
from support.weaver_test import weaver_test
from test_semantic_model_rest_boundary import Client, response

from weaver.fabric.client import FabricError
from weaver.fabric.semantic_model import (
    ConnectionBindingError,
    DataSourceBinding,
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
    assert model.bind_data_sources() == DataSourceBinding(
        bound=(f"{SERVER};DEV_Curated",)
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
    assert model.bind_data_sources() == DataSourceBinding()
    assert not fabric.calls


@weaver_test()
def test_a_source_no_connection_reaches_keeps_its_own_and_is_named():
    model, fabric = scripted([source("DEV_Curated")], [])
    assert model.bind_data_sources() == DataSourceBinding(
        unreached=(f"{SERVER};DEV_Curated",)
    )
    assert all(call[0] == "GET" for call in fabric.calls)


@weaver_test()
def test_a_source_several_connections_reach_fails_in_one_line():
    connections = [
        connection("a", f"{SERVER};DEV_Curated", name="First"),
        connection("b", f"{SERVER};DEV_Curated", name="Second"),
    ]
    message = "several connections reach it: First, Second. Keep one."
    model, fabric = scripted([source("DEV_Curated")], connections)
    with pytest.raises(ConnectionBindingError) as failed:
        model.bind_data_sources()
    text = str(failed.value)
    assert text.startswith(f"The model reads database DEV_Curated on {SERVER}")
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
    ) + ["invalid_measures"]
    assert (
        repository.semantic_models[item].signature
        == replace(
            repository.semantic_models[item], bind_data_sources=not bind
        ).signature
    )


DEFAULT_CONNECTION = {
    "status": "Failed",
    "serviceExceptionJson": json.dumps(
        {
            "errorCode": "Premium_ASWL_Error",
            "errorDescription": "This semantic model uses a default data connection "
            "without explicit connection credentials.",
        }
    ),
}


@weaver_test()
def test_a_connection_failure_names_the_path_no_connection_reaches():
    from weaver.fabric.semantic_model import SemanticRefreshError

    request_id = "11111111-2222-3333-4444-555555555555"
    power_bi = Client(
        response({}, 202, {"x-ms-request-id": request_id}),
        response(DEFAULT_CONNECTION),
    )
    model = SemanticModelClient("ws", "model", fabric=Client(), power_bi=power_bi)
    with pytest.raises(SemanticRefreshError) as failed:
        model.refresh(timeout=10, unreached=(f"{SERVER};DEV_Landing",))
    message = str(failed.value)
    assert message.endswith(
        f"No connection has the path {SERVER};DEV_Landing. Create a connection "
        "for it in Fabric, then load again."
    )
    assert "connection owner" not in message


@weaver_test()
def test_load_warns_before_refreshing_a_model_with_a_source_no_connection_reaches():
    from test_semantic_model_build_cycle import session_for
    from test_semantic_model_load_cycle import (
        ITEM,
        MODEL_ID,
        REQUEST_ID,
        WORKSPACE_ID,
        answer_installed,
        installed_rows,
    )

    import weaver
    from weaver.errors import LoadError

    datasources = response({"value": [source("DEV_Landing")]})
    datasources.datasources = True
    power_bi = Client(
        datasources,
        response({}, 202, {"x-ms-request-id": REQUEST_ID}),
        response(DEFAULT_CONNECTION),
    )
    fabric = Client(response({"value": [connection("dev", f"{SERVER};DEV_Curated")]}))
    model = SemanticModelClient(
        WORKSPACE_ID, MODEL_ID, fabric=fabric, power_bi=power_bi
    )
    session = session_for()
    session.answer_semantic_model(WORKSPACE_ID, MODEL_ID, model)
    answer_installed(session, installed_rows())
    warned = []
    session.warn = lambda message: warned.append((message, list(power_bi.calls)))
    with session:
        with pytest.raises(LoadError) as failed:
            weaver.load(str(ITEM), session=session)

    path = f"{SERVER};DEV_Landing"
    ((message, calls),) = warned
    assert message == (
        f"SemanticModel/Reporting reads {path}, and no connection has that path. "
        "If the refresh fails, create a connection for it."
    )
    assert not [call for call in calls if call[0] == "POST"]
    assert f"No connection has the path {path}." in str(failed.value)
    assert not [call for call in fabric.calls if call[0] == "POST"]


@weaver_test()
@pytest.mark.parametrize(
    "answer, outcome",
    [([], "passed"), (FabricError("401 Unauthorized", status_code=401), "not run")],
)
def test_deployment_reports_whether_its_measures_were_checked(answer, outcome):
    from weaver.build_bundle.executors.semantic import require_valid_measures

    def invalid_measures():
        if isinstance(answer, Exception):
            raise answer
        return answer

    client = SimpleNamespace(invalid_measures=invalid_measures)
    assert require_valid_measures(client, "SemanticModel/Sales").startswith(outcome)


@weaver_test()
def test_deployment_with_an_invalid_measure_fails_naming_it():
    from weaver.build_bundle.executors.semantic import require_valid_measures
    from weaver.errors import InstallError

    client = SimpleNamespace(
        invalid_measures=lambda: [
            ("Sales", "Margin", "Column 'Cost' in table 'Sales' cannot be found.")
        ]
    )
    with pytest.raises(InstallError) as raised:
        require_valid_measures(client, "SemanticModel/Sales")
    assert str(raised.value) == (
        "SemanticModel/Sales: 1 measure cannot be evaluated after deployment. "
        "'Sales'[Margin]: Column 'Cost' in table 'Sales' cannot be found. Fix the "
        "DAX or restore what it references, then build again."
    )


@weaver_test()
def test_build_says_what_it_bound_and_what_it_could_not_check():
    from weaver.build_bundle.report import ActionResult
    from weaver.operations.build import _present_semantic_outcomes

    deployed = ActionResult(
        action_id="semantic_model-Reporting",
        resource_node_id="SemanticModel/Reporting",
        target_id="Reporting",
        executor="semantic_model",
        status="succeeded",
        details={
            "data_sources": {
                "bound": [f"{SERVER};Serving"],
                "unreached": [f"{SERVER};Landing"],
            },
            "measure_check": "not run: 401 Unauthorized",
        },
    )
    seen = SimpleNamespace(reported=[], warned=[])
    session = SimpleNamespace(report=seen.reported.extend, warn=seen.warned.append)
    _present_semantic_outcomes(
        SimpleNamespace(action_results=lambda: [deployed]), session
    )
    assert seen.reported == [
        f"SemanticModel/Reporting data sources bound: {SERVER};Serving"
    ]
    assert seen.warned == [
        f"SemanticModel/Reporting reads {SERVER};Landing, and no connection has "
        "that path. Create a connection for it in Fabric before refreshing.",
        "SemanticModel/Reporting measures were not checked: 401 Unauthorized",
    ]


@weaver_test()
def test_build_warns_once_per_item_about_what_fabric_rewrote():
    from weaver.build_bundle.report import ActionResult
    from weaver.operations.build import _present_semantic_outcomes

    readback = ActionResult(
        action_id="report_readback-Executive",
        resource_node_id="Report/Executive/artifact:Definition/Executive.Report",
        target_id="Executive",
        executor="report_readback",
        status="succeeded",
        details={
            "readback_differences": [
                "definition/report.json changed",
                "definition/pages/extra.json extra",
                ".platform changed",
                "StaticResources/theme.json missing",
            ]
        },
    )
    seen = SimpleNamespace(reported=[], warned=[])
    session = SimpleNamespace(report=seen.reported.extend, warn=seen.warned.append)
    _present_semantic_outcomes(
        SimpleNamespace(action_results=lambda: [readback]), session
    )
    assert seen.warned == [
        "Report/Executive: Fabric's copy differs from what Build deployed at "
        "definition/report.json changed, definition/pages/extra.json extra, "
        ".platform changed and 1 more. Fabric accepted the deployment, so Build "
        "trusted it."
    ]
    assert not seen.reported


@weaver_test()
def test_a_readback_fabric_does_not_return_leaves_the_item_uncertified():
    from weaver.build_bundle.executors.base import read_back
    from weaver.errors import InstallError

    def unavailable():
        raise FabricError("getDefinition returned 500")

    with pytest.raises(InstallError) as raised:
        read_back("Report/Executive", unavailable)
    assert str(raised.value) == (
        "Report/Executive was deployed, but Fabric did not return its definition "
        "to confirm it: getDefinition returned 500. Build Report/Executive again "
        "to certify it."
    )
