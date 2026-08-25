"""When a Fabric REST call is repeated, and when it is not.

Fabric refuses a call it is too busy to take, and a build that treated that as a
failure would abandon work over a few seconds of throttling. A 503 on a shortcut
delete failed a whole install before this was here.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from support.weaver_test import weaver_test

from weaver.fabric.client import FabricClient, FabricError


def _response(status: int, *, headers=None):
    return SimpleNamespace(
        status_code=status, headers=headers or {}, text="", json=lambda: {}
    )


def _client(monkeypatch, answers, *, slept=None):
    """A client whose transport returns ``answers`` in order."""

    remaining = list(answers)
    sent = []

    def send(method, url, **kwargs):
        sent.append((method, url))
        return remaining.pop(0)

    monkeypatch.setattr("weaver.fabric.client.send", send)
    if slept is not None:
        monkeypatch.setattr("weaver.fabric.client.time.sleep", slept.append)
    return FabricClient(token="token"), sent


@weaver_test()
def test_a_refused_call_is_repeated_until_fabric_takes_it(monkeypatch):
    """503 means the request was not acted on, so repeating it is safe."""

    slept: list = []
    client, sent = _client(
        monkeypatch, [_response(503), _response(503), _response(200)], slept=slept
    )

    response = client.request("DELETE", "workspaces/w/items/i/shortcuts/Tables%2F_/Log")

    assert response.status_code == 200
    assert len(sent) == 3
    # A widening gap, so a busy capacity is not hammered.
    assert slept == [2.0, 4.0]


@weaver_test()
def test_the_delay_fabric_asked_for_is_the_one_taken(monkeypatch):
    slept: list = []
    client, _sent = _client(
        monkeypatch,
        [_response(429, headers={"Retry-After": "7"}), _response(200)],
        slept=slept,
    )

    client.request("GET", "workspaces/w", expected=(200,))

    assert slept == [7.0]


@weaver_test()
def test_a_call_fabric_answered_is_not_repeated(monkeypatch):
    """404 is an answer. Repeating it would only be slower."""

    client, sent = _client(monkeypatch, [_response(404)])

    with pytest.raises(FabricError) as raised:
        client.request("GET", "workspaces/w/items/missing", expected=(200,))

    assert raised.value.status_code == 404
    assert len(sent) == 1


@weaver_test()
def test_a_refusal_that_never_clears_is_reported_with_its_status(monkeypatch):
    slept: list = []
    client, sent = _client(monkeypatch, [_response(503)] * 4, slept=slept)

    with pytest.raises(FabricError) as raised:
        client.request("POST", "workspaces/w/items", expected=(201,))

    assert raised.value.status_code == 503
    assert len(sent) == 4
