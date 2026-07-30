"""What Fabric does that a filesystem cannot: shortcuts, and endpoint refresh."""

from __future__ import annotations

import pytest

from weaver.errors import CommandError
from weaver.fabric.client import FabricError
from weaver.fabric.resources import SQL_ENDPOINT, refresh_sql_endpoint_metadata
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
    # Creating one shortcut is documented as synchronous; the status says whether
    # Fabric honoured that, and distinguishes it from the destination Lakehouse
    # not yet having registered the shortcut as a table.
    assert details["status"] == 200


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
