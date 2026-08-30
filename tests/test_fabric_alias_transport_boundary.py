"""What Fabric does that a filesystem cannot: shortcuts, and endpoint refresh."""

from __future__ import annotations

import pytest
from support.weaver_test import weaver_test

from weaver.errors import CommandError
from weaver.fabric.client import FabricError
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
            staged = self.responses.pop(0)
            if isinstance(staged, Exception):
                raise staged
            return staged
        return _Response(200)

    def get_json(self, path):
        self.calls.append(("GET", path, None))
        return self.json_by_path[path]


# --- shortcuts ----------------------------------------------------------------


@weaver_test()
def test_a_shortcut_is_overwritten_rather_than_created_strictly():
    """One request, whether or not a shortcut of that name is already there.

    ``CreateOrOverwrite`` is what makes a build re-runnable over its own
    pointers. Fabric holds a deleted shortcut's name for up to thirty-five
    seconds afterwards, and an overwrite never meets it.
    """

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
    assert methods == ["POST"]
    _method, path, payload = client.calls[0]
    assert path == (
        "workspaces/ws1/items/dest1/shortcuts?shortcutConflictPolicy=CreateOrOverwrite"
    )
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
    assert details["path"] == "Tables/Sales/Landed"
    # Creating one shortcut is documented as synchronous; the status says whether
    # Fabric honoured that, and distinguishes it from the destination Lakehouse
    # not yet having registered the shortcut as a table.
    assert details["status"] == 200


@weaver_test()
def test_a_source_published_a_moment_later_is_waited_for(monkeypatch):
    """One build can create a thing and point at it.

    Fabric validates a shortcut's target, and a Warehouse publishes a table to
    OneLake shortly after creating it in its own catalogue, so the create can
    arrive before there is anything to point at.
    """

    import weaver.fabric.shortcuts as shortcuts

    slept: list[float] = []
    monkeypatch.setattr(shortcuts.time, "sleep", slept.append)
    client = _Client(
        responses=[
            FabricError("400: RequestBodyValidationFailed: Target path doesn't exist"),
            _Response(201),
        ]
    )

    details = create_shortcut(
        _lakehouse("Curated", "dest1"),
        path="Tables/_",
        name="Bookmark",
        source=_lakehouse("Weaver", "src1"),
        source_path="Tables/_/Bookmark",
        client=client,
    )

    assert details["status"] == 201
    assert slept == [shortcuts.SOURCE_POLL_INTERVAL]


@weaver_test()
def test_a_source_that_never_appears_still_fails(monkeypatch):
    """Bounded, so a target that is absent is a failure and not a hang."""

    import weaver.fabric.shortcuts as shortcuts

    monkeypatch.setattr(shortcuts, "SOURCE_TIMEOUT", 0.0)
    monkeypatch.setattr(shortcuts.time, "sleep", lambda _seconds: None)
    client = _Client(
        responses=[
            FabricError("400: RequestBodyValidationFailed: Target path doesn't exist"),
        ]
    )

    with pytest.raises(CommandError) as raised:
        create_shortcut(
            _lakehouse("Curated", "dest1"),
            path="Tables/_",
            name="Bookmark",
            source=_lakehouse("Weaver", "src1"),
            source_path="Tables/_/Bookmark",
            client=client,
        )

    assert "did not appear in OneLake" in str(raised.value)


@weaver_test()
def test_a_shortcut_path_is_escaped_into_one_url_segment():
    client = _Client()

    delete_shortcut(
        _lakehouse("Curated", "dest1"),
        path="Tables/Sales",
        name="Landed",
        client=client,
    )

    _method, path, _payload = client.calls[0]
    assert path == "workspaces/ws1/items/dest1/shortcuts/Tables%2FSales/Landed"


@weaver_test()
def test_removing_an_absent_shortcut_is_the_intended_state_not_a_fault():
    client = _Client(responses=[_Response(404)])

    delete_shortcut(
        _lakehouse("Curated", "dest1"),
        path="Tables/Sales",
        name="Landed",
        client=client,
    )

    assert client.calls[0][0] == "DELETE"
