"""What Fabric does that a filesystem cannot: shortcuts, and endpoint refresh."""

from __future__ import annotations

import pytest
from support.weaver_test import weaver_test

from weaver.errors import CommandError
from weaver.fabric.client import FabricError
from weaver.fabric.resources import LAKEHOUSE, Item
from weaver.fabric.shortcuts import ShortcutRequest, create_shortcuts, delete_shortcut

BULK = (
    "workspaces/ws1/items/dest1/shortcuts/bulkCreate"
    "?shortcutConflictPolicy=CreateOrOverwrite"
)

#: What Fabric reports for a source it has not published to OneLake yet.
MISSING = {
    "errorCode": "RequestBodyValidationFailed",
    "message": "Target path doesn't exist",
}


def _lakehouse(name: str, item_id: str) -> Item:
    return Item(id=item_id, name=name, type=LAKEHOUSE, workspace_id="ws1")


def _request(name: str, source_path: str, *, path: str = "Tables/Sales"):
    return ShortcutRequest(
        path=path,
        name=name,
        source=_lakehouse("Raw", "src1"),
        source_path=source_path,
    )


class _Response:
    def __init__(self, status_code: int, headers=None, body=None):
        self.status_code = status_code
        self.headers = headers or {}
        self._body = body
        self.content = b"{}" if body is not None else b""

    def json(self):
        return self._body


def _members(*outcomes) -> dict:
    """A bulk response body: one member per (path, name, error-or-None)."""

    return {
        "value": [
            {
                "request": {"path": path, "name": name},
                "status": "Failed" if error else "Succeeded",
                **({"error": error} if error else {"result": {}}),
            }
            for path, name, error in outcomes
        ]
    }


class _Client:
    """A Fabric client that records calls and answers with staged payloads."""

    def __init__(self, *, responses=None, json_by_path=None):
        self.calls: list[tuple[str, str, object]] = []
        self.responses = list(responses or [])
        self.json_by_path = json_by_path or {}
        self.waited = 0

    def request(self, method, path, *, payload=None, expected=(200, 201, 202)):
        self.calls.append((method, path, payload))
        if self.responses:
            staged = self.responses.pop(0)
            if isinstance(staged, Exception):
                raise staged
            return staged
        return _Response(200)

    def wait_for_operation(self, response):
        self.waited += 1
        return {"status": "Succeeded"}

    def get_json(self, path):
        self.calls.append(("GET", path, None))
        return self.json_by_path[path]


# --- shortcuts ----------------------------------------------------------------


@weaver_test()
def test_shortcuts_are_overwritten_rather_than_created_strictly():
    """One request for the whole batch, whatever names are already there.

    ``CreateOrOverwrite`` is what makes a build re-runnable over its own
    pointers. Fabric holds a deleted shortcut's name for up to thirty-five
    seconds afterwards, and an overwrite never meets it.
    """

    client = _Client(
        responses=[
            _Response(
                200,
                body=_members(
                    ("Tables/Sales", "Landed", None),
                    ("Tables/Sales", "Second", None),
                ),
            )
        ]
    )

    result = create_shortcuts(
        _lakehouse("Curated", "dest1"),
        [
            _request("Landed", "Tables/Sales/Customer"),
            _request("Second", "Tables/Sales/Region"),
        ],
        client=client,
    )

    assert [method for method, _path, _payload in client.calls] == ["POST"]
    _method, path, payload = client.calls[0]
    assert path == BULK
    assert payload == {
        "createShortcutRequests": [
            {
                "path": "Tables/Sales",
                "name": "Landed",
                "target": {
                    "oneLake": {
                        "workspaceId": "ws1",
                        "itemId": "src1",
                        "path": "Tables/Sales/Customer",
                    }
                },
            },
            {
                "path": "Tables/Sales",
                "name": "Second",
                "target": {
                    "oneLake": {
                        "workspaceId": "ws1",
                        "itemId": "src1",
                        "path": "Tables/Sales/Region",
                    }
                },
            },
        ]
    }
    assert result.calls == 1
    assert [detail["path"] for detail in result.created] == [
        "Tables/Sales/Landed",
        "Tables/Sales/Second",
    ]


@weaver_test()
def test_the_outcomes_are_read_from_the_operation_result():
    """Bulk creation is long-running, so a 202 carries its outcomes elsewhere.

    The body of a 202 says nothing about the members. They are at the
    operation's result address, once it settles.
    """

    client = _Client(
        responses=[
            _Response(202, headers={"x-ms-operation-id": "op1"}),
            _Response(200, body=_members(("Tables/Sales", "Landed", None))),
        ]
    )

    result = create_shortcuts(
        _lakehouse("Curated", "dest1"),
        [_request("Landed", "Tables/Sales/Customer")],
        client=client,
    )

    assert client.waited == 1
    assert client.calls[1][:2] == ("GET", "operations/op1/result")
    assert result.created[0]["path"] == "Tables/Sales/Landed"


@weaver_test()
def test_a_source_published_a_moment_later_is_waited_for(monkeypatch):
    """One build can create a thing and point at it.

    Fabric validates a shortcut's target, and a Warehouse publishes a table to
    OneLake shortly after creating it in its own catalogue, so the create can
    arrive before there is anything to point at. Only the member that failed is
    sent again: what succeeded is already made.
    """

    import weaver.fabric.shortcuts as shortcuts

    slept: list[float] = []
    monkeypatch.setattr(shortcuts.time, "sleep", slept.append)
    client = _Client(
        responses=[
            _Response(
                200,
                body=_members(
                    ("Tables/_", "Landed", None),
                    ("Tables/_", "Bookmark", MISSING),
                ),
            ),
            _Response(200, body=_members(("Tables/_", "Bookmark", None))),
        ]
    )

    result = create_shortcuts(
        _lakehouse("Curated", "dest1"),
        [
            _request("Landed", "Tables/Sales/Customer", path="Tables/_"),
            _request("Bookmark", "Tables/_/Bookmark", path="Tables/_"),
        ],
        client=client,
    )

    assert slept == [shortcuts.SOURCE_POLL_INTERVAL]
    assert result.calls == 2
    # Only the member that failed is resent; the one that succeeded is not.
    second = client.calls[1][2]["createShortcutRequests"]
    assert [each["name"] for each in second] == ["Bookmark"]
    # Both are reported, in the order they were requested.
    assert [detail["path"] for detail in result.created] == [
        "Tables/_/Landed",
        "Tables/_/Bookmark",
    ]


@weaver_test()
def test_a_source_that_never_appears_still_fails(monkeypatch):
    """One deadline covers the batch, so an absent target fails and does not hang."""

    import weaver.fabric.shortcuts as shortcuts

    monkeypatch.setattr(shortcuts, "SOURCE_TIMEOUT", 0.0)
    monkeypatch.setattr(shortcuts.time, "sleep", lambda _seconds: None)
    client = _Client(
        responses=[_Response(200, body=_members(("Tables/_", "Bookmark", MISSING)))]
    )

    with pytest.raises(CommandError) as raised:
        create_shortcuts(
            _lakehouse("Curated", "dest1"),
            [_request("Bookmark", "Tables/_/Bookmark", path="Tables/_")],
            client=client,
        )

    assert "Tables/_/Bookmark" in str(raised.value)
    assert "did not appear in OneLake" in str(raised.value)


@weaver_test()
def test_an_occupied_path_is_reported_rather_than_retried():
    """Something that is not a shortcut standing at the name is not a wait."""

    client = _Client(
        responses=[
            _Response(
                200,
                body=_members(
                    (
                        "Tables/Sales",
                        "Landed",
                        {"errorCode": "NameConflictError", "message": "occupied"},
                    )
                ),
            )
        ]
    )

    with pytest.raises(CommandError) as raised:
        create_shortcuts(
            _lakehouse("Curated", "dest1"),
            [_request("Landed", "Tables/Sales/Customer")],
            client=client,
        )

    assert "already holds something at Tables/Sales/Landed" in str(raised.value)
    assert client.calls[0][1] == BULK


@weaver_test()
def test_every_member_outcome_is_inspected():
    """A member that failed for an unrecognised reason fails the batch.

    A bulk request answers 200 with failures inside it, so reading only the
    status code would take a batch that created nothing for a success.
    """

    client = _Client(
        responses=[
            _Response(
                200,
                body=_members(
                    ("Tables/Sales", "Landed", None),
                    (
                        "Tables/Sales",
                        "Second",
                        {"errorCode": "UnknownError", "message": "no"},
                    ),
                ),
            )
        ]
    )

    with pytest.raises(CommandError) as raised:
        create_shortcuts(
            _lakehouse("Curated", "dest1"),
            [
                _request("Landed", "Tables/Sales/Customer"),
                _request("Second", "Tables/Sales/Region"),
            ],
            client=client,
        )

    assert "Tables/Sales/Second" in str(raised.value)
    assert "UnknownError" in str(raised.value)


@weaver_test()
def test_a_member_fabric_reports_nothing_for_is_not_assumed_created():
    """An outcome Fabric omits says nothing, and silence is not success."""

    client = _Client(
        responses=[_Response(200, body=_members(("Tables/Sales", "Landed", None)))]
    )

    with pytest.raises(CommandError) as raised:
        create_shortcuts(
            _lakehouse("Curated", "dest1"),
            [
                _request("Landed", "Tables/Sales/Customer"),
                _request("Second", "Tables/Sales/Region"),
            ],
            client=client,
        )

    assert "no outcome for the shortcut Tables/Sales/Second" in str(raised.value)


@weaver_test()
def test_a_refused_batch_names_what_it_could_not_create():
    """Fabric refusing the request means no member has an outcome at all."""

    client = _Client(responses=[FabricError("429: too many requests")])

    with pytest.raises(CommandError) as raised:
        create_shortcuts(
            _lakehouse("Curated", "dest1"),
            [_request("Landed", "Tables/Sales/Customer")],
            client=client,
        )

    assert "could not create 1 shortcut(s) in Curated" in str(raised.value)


@weaver_test()
def test_an_empty_batch_sends_nothing():
    """Nothing declared is nothing to create, and no crossing to pay for."""

    client = _Client()

    result = create_shortcuts(_lakehouse("Curated", "dest1"), [], client=client)

    assert client.calls == []
    assert result.created == () and result.calls == 0


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
