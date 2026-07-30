"""What Fabric does that a filesystem cannot: shortcuts, and endpoint refresh."""

from __future__ import annotations

import pytest

from weaver.errors import CommandError
from weaver.fabric.client import FabricError
from weaver.fabric.endpoints import refresh_sql_endpoint_metadata, sql_endpoint_id
from weaver.fabric.resources import LAKEHOUSE, Item
from weaver.fabric.shortcuts import create_shortcut, delete_shortcut


def _lakehouse(name: str, item_id: str) -> Item:
    return Item(id=item_id, name=name, type=LAKEHOUSE, workspace_id="ws1")


class _Response:
    def __init__(self, status_code: int, headers=None):
        self.status_code = status_code
        self.headers = headers or {}


class _Client:
    """A Fabric client that records calls and answers with staged payloads."""

    def __init__(self, *, responses=None, json_by_path=None):
        self.calls: list[tuple[str, str, object]] = []
        self.responses = list(responses or [])
        self.json_by_path = json_by_path or {}

    def request(self, method, path, *, payload=None, expected=(200, 201, 202)):
        self.calls.append((method, path, payload))
        if self.responses:
            return self.responses.pop(0)
        return _Response(200)

    def get_json(self, path):
        self.calls.append(("GET", path, None))
        return self.json_by_path[path]


# --- shortcuts ----------------------------------------------------------------


def test_a_shortcut_is_replaced_rather_than_created_strictly():
    client = _Client()

    details = create_shortcut(
        _lakehouse("Curated", "dest1"),
        path="Tables/Sales",
        name="Landed",
        source=_lakehouse("Raw", "src1"),
        source_path="Tables/Sales/Customer",
        client=client,
    )

    methods = [method for method, _path, _payload in client.calls]
    assert methods == ["DELETE", "POST"]
    _method, path, payload = client.calls[1]
    assert path == "workspaces/ws1/items/dest1/shortcuts"
    assert payload == {
        "path": "Tables/Sales",
        "name": "Landed",
        "target": {
            "oneLake": {
                "workspaceId": "ws1",
                "itemId": "src1",
                "path": "Tables/Sales/Customer",
            }
        },
    }
    assert details["shortcut"] == "Tables/Sales/Landed"


def test_a_shortcut_path_is_escaped_into_one_url_segment():
    client = _Client()

    delete_shortcut(
        _lakehouse("Curated", "dest1"), path="Tables/Sales", name="Landed", client=client
    )

    _method, path, _payload = client.calls[0]
    assert path == "workspaces/ws1/items/dest1/shortcuts/Tables%2FSales/Landed"


def test_removing_an_absent_shortcut_is_the_intended_state_not_a_fault():
    client = _Client(responses=[_Response(404)])

    delete_shortcut(
        _lakehouse("Curated", "dest1"), path="Tables/Sales", name="Landed", client=client
    )

    assert client.calls[0][0] == "DELETE"


# --- endpoint refresh ---------------------------------------------------------

_LAKEHOUSE_PATH = "workspaces/ws1/lakehouses/lh1"


def test_the_endpoint_id_comes_from_the_lakehouses_own_properties():
    client = _Client(
        json_by_path={
            _LAKEHOUSE_PATH: {"properties": {"sqlEndpointProperties": {"id": "ep1"}}}
        }
    )

    assert sql_endpoint_id(_lakehouse("Raw", "lh1"), client=client) == "ep1"


def test_a_lakehouse_without_an_endpoint_yet_fails_rather_than_silently_skipping():
    client = _Client(json_by_path={_LAKEHOUSE_PATH: {"properties": {}}})

    with pytest.raises(CommandError, match="no SQL analytics endpoint yet"):
        sql_endpoint_id(_lakehouse("Raw", "lh1"), client=client)


def test_an_accepted_refresh_returns_only_once_the_operation_has_settled():
    """The barrier is the point: returning on 202 would put it in the wrong place."""

    client = _Client(
        responses=[_Response(202, {"Operation-Location": "operations/op1"})],
        json_by_path={
            _LAKEHOUSE_PATH: {"properties": {"sqlEndpointProperties": {"id": "ep1"}}},
            "operations/op1": {"status": "Succeeded"},
        },
    )

    details = refresh_sql_endpoint_metadata(
        _lakehouse("Raw", "lh1"), client=client, poll_interval=0
    )

    assert details == {
        "lakehouse": "Raw",
        "sql_endpoint_id": "ep1",
        "state": "Succeeded",
    }
    assert ("GET", "operations/op1", None) in client.calls


def test_a_refresh_that_settles_as_failed_fails_the_action():
    client = _Client(
        responses=[_Response(202, {"Location": "operations/op1"})],
        json_by_path={
            _LAKEHOUSE_PATH: {"properties": {"sqlEndpointProperties": {"id": "ep1"}}},
            "operations/op1": {"status": "Failed"},
        },
    )

    with pytest.raises(FabricError, match="finished as 'Failed'"):
        refresh_sql_endpoint_metadata(
            _lakehouse("Raw", "lh1"), client=client, poll_interval=0
        )


def test_an_accepted_refresh_with_nothing_to_poll_is_a_fault():
    client = _Client(
        responses=[_Response(202)],
        json_by_path={
            _LAKEHOUSE_PATH: {"properties": {"sqlEndpointProperties": {"id": "ep1"}}}
        },
    )

    with pytest.raises(FabricError, match="no operation to poll"):
        refresh_sql_endpoint_metadata(_lakehouse("Raw", "lh1"), client=client)


def test_a_synchronous_refresh_needs_no_polling_at_all():
    client = _Client(
        responses=[_Response(200)],
        json_by_path={
            _LAKEHOUSE_PATH: {"properties": {"sqlEndpointProperties": {"id": "ep1"}}}
        },
    )

    details = refresh_sql_endpoint_metadata(_lakehouse("Raw", "lh1"), client=client)

    assert details["state"] == "succeeded"
    assert not any(method == "GET" and "operations" in path for method, path, _ in client.calls)
