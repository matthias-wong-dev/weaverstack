"""Livy session plumbing that needs no tenant.

The parts a Fabric session leans on — how a session URL is formed, and how a
returned value is told from printed output — verified without a workspace.
"""

from __future__ import annotations

from weaver import FabricWorkspace
from weaver.fabric.livy import (
    RESULT_PREFIX,
    LivySessionInfo,
    StatementResult,
    _payload,
    list_livy_sessions,
    list_workspace_livy_sessions,
    sessions_url,
)


def test_the_sessions_url_names_workspace_and_lakehouse():
    url = sessions_url("ws-id", "lh-id")
    assert "/workspaces/ws-id/lakehouses/lh-id/livyapi/" in url
    assert url.endswith("/sessions")


def test_a_returned_value_is_told_from_printed_output():
    text = f"some log line\n{RESULT_PREFIX}" + '{"removed": 2}\n' + "another line"
    assert _payload(text) == {"removed": 2}


def test_output_with_no_returned_value():
    assert _payload("just logging\n") is None
    assert StatementResult(text="x").returned is False


def test_the_last_returned_value_wins():
    text = f"{RESULT_PREFIX}" + '{"n": 1}\n' + f"{RESULT_PREFIX}" + '{"n": 2}\n'
    assert _payload(text) == {"n": 2}


def test_malformed_json_is_not_a_result():
    assert _payload(f"{RESULT_PREFIX}not json\n") is None


class _CollectionClient:
    api_base_url = "https://fabric.example/v1"

    def paged(self, path):
        assert path == "workspaces/ws-id/items?type=Lakehouse"
        return [
            {"id": "lh-2", "displayName": "Sales", "type": "Lakehouse"},
            {"id": "lh-1", "displayName": "Control", "type": "Lakehouse"},
        ]

    def get_json(self, url):
        if url.endswith("/lh-1/livyapi/versions/2023-12-01/sessions"):
            return {"items": [{
                "id": "11", "name": "weaver", "schedulerState": "Ended",
                "pluginState": "Ended", "livyState": "killed",
                "cancellationReason": "cancelled by user",
            }]}
        return {"items": [{
            "id": "22", "name": "notebook", "schedulerState": "Scheduled",
            "pluginState": "Queued", "livyState": "not_started",
            "submitterId": "person-id", "submitterName": "builder@example.invalid",
            "artifactId": "lh-2",
            "submittedAt": "2026-07-27T01:02:03Z", "tags": ["interactive"],
        }]}


def test_livy_collection_preserves_scheduler_details():
    sessions = list_livy_sessions("ws-id", "lh-2", client=_CollectionClient())

    assert sessions == (LivySessionInfo(
        id="22", name="notebook", scheduler_state="Scheduled",
        plugin_state="Queued", livy_state="not_started",
        submitter_id="person-id", submitter_name="builder@example.invalid",
        artifact_id="lh-2",
        submitted_at="2026-07-27T01:02:03Z", tags=("interactive",),
    ),)
    assert sessions[0].active is True


def test_workspace_session_listing_finds_other_lakehouses_and_can_filter_ended(
    monkeypatch,
):
    workspace = FabricWorkspace(workspace="Analytics", weaver_lakehouse="Control")
    client = _CollectionClient()

    # Keep the test pure: the fake resolver already knows the workspace identity.
    from weaver.fabric.resources import Workspace

    class _Resolver:
        def __init__(self, workspace, client):
            self.workspace = Workspace("ws-id", workspace.workspace)

    monkeypatch.setattr("weaver.fabric.resolution.FabricResolver", _Resolver)
    active = list_workspace_livy_sessions(workspace, client=client, active_only=True)

    assert [(entry.lakehouse_name, entry.session.id) for entry in active] == [
        ("Sales", "22")
    ]


# --- closing releases the capacity slot ---------------------------------------
#
# A capacity has a limit on concurrent Spark sessions, often one, and `DELETE`
# returns when the request is accepted rather than when the slot is free. A close
# that did not wait would let a caller ask for the next session while the previous
# still held the only slot — which is how a long Fabric run ends with a session
# that never reaches `idle`.


class _Api:
    """Records calls, and reports a session that takes a while to die.

    ``states`` are the answers to successive ``GET``s; ``None`` stands for the
    404 Fabric gives once the session is gone. The last answer repeats, so a
    session that never admits it has died can be modelled.
    """

    def __init__(self, states):
        self.states = list(states)
        self.calls = []

    def __call__(self, method, url, token, payload=None, expected=()):
        self.calls.append((method, url))
        if method == "DELETE":
            return {}
        state = self.states[0] if len(self.states) == 1 else self.states.pop(0)
        return {} if state is None else {"state": state}


def _closing_session(api, monkeypatch, **kwargs):
    from weaver.fabric import livy

    monkeypatch.setattr(livy, "_call", api)
    session = livy.LivySession("ws", "lh", token="t", poll_interval=0, **kwargs)
    session.session_url = f"{session.base}/7"
    return session


def test_close_waits_for_the_session_to_report_itself_gone(monkeypatch):
    api = _Api(["shutting_down", "shutting_down", "dead"])

    _closing_session(api, monkeypatch).close()

    assert api.calls[0][0] == "DELETE"
    # It kept asking until the session said it had gone, rather than assuming.
    assert [method for method, _ in api.calls] == ["DELETE", "GET", "GET", "GET"]


def test_close_stops_as_soon_as_the_session_is_no_longer_there(monkeypatch):
    """A 404 is the answer, not a failure: the slot is free."""

    api = _Api([None])
    _closing_session(api, monkeypatch).close()
    assert [method for method, _ in api.calls] == ["DELETE", "GET"]


def test_a_close_that_cannot_be_confirmed_warns_rather_than_raises(monkeypatch, capsys):
    """The session is abandoned either way, and a teardown must not mask a result."""

    api = _Api(["shutting_down"])  # a session that never admits it has gone

    _closing_session(api, monkeypatch).close(timeout=0.01)

    assert "did not report itself released" in capsys.readouterr().out


def test_closing_twice_is_harmless(monkeypatch):
    api = _Api([None])
    session = _closing_session(api, monkeypatch)
    session.close()
    before = len(api.calls)
    session.close()
    assert len(api.calls) == before
