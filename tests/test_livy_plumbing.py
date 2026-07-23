"""Livy session plumbing that needs no tenant.

The parts a Fabric session leans on — how a session URL is formed, and how a
returned value is told from printed output — verified without a workspace.
"""

from __future__ import annotations

from weaver.fabric.livy import RESULT_PREFIX, StatementResult, _payload, sessions_url


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
