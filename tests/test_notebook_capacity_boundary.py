"""Native capacity control reads Azure state without an Azure CLI process."""

from types import SimpleNamespace

from support.weaver_test import weaver_test

import weaver


@weaver_test()
def test_notebook_capacity_returns_the_service_state_without_a_cli():
    calls = []

    class Capacities:
        def get(self, *, resource_group_name, capacity_name):
            calls.append((resource_group_name, capacity_name))
            return SimpleNamespace(
                as_dict=lambda: {
                    "properties": {"state": "Paused"},
                    "sku": {"name": "F2"},
                }
            )

    result = weaver.capacity(
        "status",
        resource_group="parcel-development",
        capacity_name="parceldev",
        subscription_id="00000000-0000-0000-0000-000000000000",
        client=SimpleNamespace(fabric_capacities=Capacities()),
    )
    assert calls == [("parcel-development", "parceldev")]
    assert result.state == "Paused"
    assert not result.running
    assert str(result) == "parceldev: Paused, F2"


@weaver_test()
def test_notebook_capacity_waits_for_the_operation_then_reads_back_state():
    for action, expected in [("resume", "Active"), ("suspend", "Paused")]:
        calls = []

        class Capacities:
            def begin_resume(self, **kwargs):
                calls.append("begin")
                return SimpleNamespace(result=lambda: calls.append("completed"))

            begin_suspend = begin_resume

            def get(self, **kwargs):
                calls.append("get")
                return SimpleNamespace(
                    as_dict=lambda: {
                        "properties": {"state": expected},
                        "sku": {"name": "F2"},
                    }
                )

        result = weaver.capacity(
            action,
            resource_group="parcel-development",
            capacity_name="parceldev",
            subscription_id="00000000-0000-0000-0000-000000000000",
            client=SimpleNamespace(fabric_capacities=Capacities()),
        )
        assert calls == ["begin", "completed", "get"]
        assert result.state == expected


@weaver_test()
def test_notebook_capacity_uses_the_azure_sdk_with_the_callers_credential(monkeypatch):
    import azure.mgmt.fabric
    from azure.mgmt.fabric.models import FabricCapacity

    calls = []
    credential = SimpleNamespace(get_token=lambda *args: None)
    subscription = "00000000-0000-0000-0000-000000000000"
    model = FabricCapacity(
        {
            "properties": {"state": "Active"},
            "sku": {"name": "F2", "tier": "Fabric"},
            "location": "australiaeast",
        }
    )

    def create(credential, subscription_id):
        calls.append((credential, subscription_id))
        return SimpleNamespace(
            fabric_capacities=SimpleNamespace(get=lambda **kwargs: model),
            close=lambda: calls.append("closed"),
        )

    monkeypatch.setattr(azure.mgmt.fabric, "FabricMgmtClient", create)
    result = weaver.capacity(
        "status",
        resource_group="parcel-development",
        capacity_name="parceldev",
        subscription_id=subscription,
        credential=credential,
    )
    assert result.state == "Active"
    assert calls == [(credential, subscription), "closed"]


@weaver_test()
def test_notebook_capacity_rejects_invalid_requests_before_the_sdk(monkeypatch):
    import pytest

    from weaver.fabric.capacity import CapacityError

    monkeypatch.delenv("FABRIC_SUBSCRIPTION_ID", raising=False)
    base = dict(
        action="status",
        resource_group="parcel-development",
        capacity_name="parceldev",
        subscription_id="00000000-0000-0000-0000-000000000000",
    )

    def forbidden(**kwargs):
        raise AssertionError("invalid requests must not reach Azure")

    client = SimpleNamespace(fabric_capacities=SimpleNamespace(get=forbidden))
    for override in [
        dict(action="delete"),
        dict(resource_group=""),
        dict(capacity_name=""),
        dict(subscription_id="bad-id"),
        dict(subscription_id=None),
    ]:
        with pytest.raises(CapacityError):
            weaver.capacity(**{**base, **override}, client=client)


@weaver_test()
def test_notebook_capacity_uses_the_subscription_environment_without_rewriting_it(
    monkeypatch,
):
    import azure.mgmt.fabric

    subscription = "ABCDEF01-2345-6789-ABCD-0123456789AB"
    monkeypatch.setenv("FABRIC_SUBSCRIPTION_ID", subscription)
    selected = []

    def create(credential, subscription_id):
        selected.append(subscription_id)
        return SimpleNamespace(
            fabric_capacities=SimpleNamespace(
                get=lambda **kwargs: SimpleNamespace(
                    as_dict=lambda: {"properties": {"state": "Active"}}
                )
            ),
            close=lambda: None,
        )

    monkeypatch.setattr(azure.mgmt.fabric, "FabricMgmtClient", create)
    weaver.capacity(
        "status",
        resource_group="parcel-development",
        capacity_name="parceldev",
        credential=SimpleNamespace(get_token=lambda *args: None),
    )
    assert selected == [subscription]


@weaver_test()
def test_notebook_capacity_respects_the_library_credential_choice(monkeypatch):
    import azure.mgmt.fabric

    import weaver.fabric.auth

    chosen = SimpleNamespace(get_token=lambda *args: None)
    selected = []
    monkeypatch.setattr(weaver.fabric.auth, "credential", lambda: chosen)

    def create(credential, subscription_id):
        selected.append(credential)
        return SimpleNamespace(
            fabric_capacities=SimpleNamespace(
                get=lambda **kwargs: SimpleNamespace(
                    as_dict=lambda: {"properties": {"state": "Active"}}
                )
            ),
            close=lambda: None,
        )

    monkeypatch.setattr(azure.mgmt.fabric, "FabricMgmtClient", create)
    weaver.capacity(
        "status",
        resource_group="parcel-development",
        capacity_name="parceldev",
        subscription_id="00000000-0000-0000-0000-000000000000",
    )
    assert selected == [chosen]


@weaver_test()
def test_notebook_capacity_sdk_is_an_installed_runtime_dependency():
    import tomllib
    from pathlib import Path

    project = tomllib.loads((Path(__file__).parents[1] / "pyproject.toml").read_text())
    assert any(
        value.startswith("azure-mgmt-fabric")
        for value in project["project"]["dependencies"]
    )


@weaver_test()
def test_notebook_capacity_exercises_the_real_sdk_request_and_deserialiser():
    import json

    from azure.core.credentials import AccessToken
    from azure.core.pipeline.transport import HttpResponse, HttpTransport
    from azure.mgmt.fabric import FabricMgmtClient

    requests = []
    scopes = []

    class Response(HttpResponse):
        def __init__(self, request):
            super().__init__(request, None)
            self.status_code = 200
            self.headers = {"Content-Type": "application/json"}

        def json(self):
            return json.loads(self.body())

        def body(self):
            return json.dumps(
                {
                    "properties": {"state": "Active"},
                    "sku": {"name": "F2", "tier": "Fabric"},
                    "location": "australiaeast",
                }
            ).encode()

    class Transport(HttpTransport):
        def open(self):
            pass

        def close(self):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def send(self, request, **kwargs):
            requests.append((request.method, request.url))
            return Response(request)

    class Credential:
        def get_token(self, *requested_scopes, **kwargs):
            scopes.extend(requested_scopes)
            return AccessToken("test", 9999999999)

    subscription = "00000000-0000-0000-0000-000000000000"
    with FabricMgmtClient(Credential(), subscription, transport=Transport()) as sdk:
        result = weaver.capacity(
            "status",
            resource_group="parcel-development",
            capacity_name="parceldev",
            subscription_id=subscription,
            client=sdk,
        )
    assert result.state == "Active"
    assert result.sku == "F2"
    assert scopes == ["https://management.azure.com/.default"]
    assert requests == [
        (
            "GET",
            f"https://management.azure.com/subscriptions/{subscription}/resourceGroups/parcel-development/providers/Microsoft.Fabric/capacities/parceldev?api-version=2023-11-01",
        )
    ]


@weaver_test()
def test_notebook_capacity_translates_azure_failures_to_weaver_errors():
    import pytest
    from azure.core.exceptions import ClientAuthenticationError

    def failed(**kwargs):
        raise ClientAuthenticationError(message="Azure authentication failed")

    with pytest.raises(weaver.WeaverError, match="Azure authentication failed"):
        weaver.capacity(
            "status",
            resource_group="parcel-development",
            capacity_name="parceldev",
            subscription_id="00000000-0000-0000-0000-000000000000",
            client=SimpleNamespace(fabric_capacities=SimpleNamespace(get=failed)),
        )
