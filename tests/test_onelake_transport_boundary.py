"""OneLake DFS requests: which URL each call reaches, and when one is repeated.

The transport is faked at ``requests.request``, so the client's own addressing
and retry policy run unchanged.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from support.weaver_test import weaver_test

from weaver.fabric import client
from weaver.fabric.onelake import OneLakeDfsClient, onelake_url, parse_onelake
from weaver.locations import Location
from weaver.store import StoreError

GUID = "3fa85f64-5717-4562-b3fc-2c963f66afa6"


def _response(status: int, *, headers=None, body=None):
    return SimpleNamespace(
        status_code=status,
        headers=headers or {},
        text="",
        content=b"{}" if body is not None else b"",
        json=lambda: body or {},
    )


def _transport(monkeypatch, answers):
    """Answer each request in turn; an exception in ``answers`` is raised."""

    import requests

    remaining = list(answers)
    sent: list[tuple[str, str]] = []
    slept: list[float] = []

    def request(method, url, **kwargs):
        sent.append((method, url))
        answer = remaining.pop(0)
        if isinstance(answer, BaseException):
            raise answer
        return answer

    monkeypatch.setattr(requests, "request", request)
    monkeypatch.setattr(client.time, "sleep", slept.append)
    return sent, slept


def _file() -> Location:
    return Location(onelake_url("ws", "Sales", "Files/x.csv"))


def _unsent():
    import requests
    from urllib3.exceptions import NewConnectionError

    return requests.exceptions.ConnectionError(
        NewConnectionError(None, "connection refused")
    )


def _ambiguous():
    import requests

    return requests.exceptions.ConnectionError("connection reset")


# --- addressing -----------------------------------------------------------------


@pytest.mark.parametrize("item,segment", [("Sales", "Sales.Lakehouse"), (GUID, GUID)])
@weaver_test()
def test_render_parse_render_keeps_the_artifact_segment(item, segment):
    url = onelake_url("ws", item, "Tables/Sales/Customer")
    parsed = parse_onelake(Location(url))

    assert parsed.segment == segment
    assert parsed.url() == url


@weaver_test()
def test_a_listed_location_reads_back_unchanged(monkeypatch):
    listing = {"paths": [{"name": "Sales.Lakehouse/Files/a.csv"}]}
    sent, _slept = _transport(
        monkeypatch, [_response(200, body=listing), _response(200)]
    )
    store = OneLakeDfsClient(token="token")

    entry = store.list(Location(onelake_url("ws", "Sales", "Files")))[0]
    store.read(entry.location)

    assert entry.location.value.endswith("/ws/Sales.Lakehouse/Files/a.csv")
    assert sent[1] == (
        "GET",
        "https://onelake.dfs.fabric.microsoft.com/ws/Sales.Lakehouse/Files/a.csv",
    )
    assert not any(".Lakehouse.Lakehouse" in url for _method, url in sent)


@weaver_test()
def test_a_listed_directory_lists_again_at_the_same_segment(monkeypatch):
    first = {"paths": [{"name": "Sales.Lakehouse/Files/in", "isDirectory": "true"}]}
    sent, _slept = _transport(
        monkeypatch, [_response(200, body=first), _response(200, body={"paths": []})]
    )
    store = OneLakeDfsClient(token="token")

    directory = store.list(Location(onelake_url("ws", "Sales", "Files")))[0]
    store.list(directory.location)

    assert "directory=Sales.Lakehouse%2FFiles%2Fin" in sent[1][1]
    assert not any(".Lakehouse.Lakehouse" in url for _method, url in sent)


# --- reads ----------------------------------------------------------------------


@weaver_test()
def test_a_read_is_repeated_after_a_refused_connection(monkeypatch):
    sent, slept = _transport(monkeypatch, [_ambiguous(), _response(200)])

    OneLakeDfsClient(token="token").read(_file())

    assert len(sent) == 2
    assert slept == [client.CONNECTION_BACKOFF]


@weaver_test()
def test_a_read_is_repeated_after_a_transient_status(monkeypatch):
    sent, slept = _transport(monkeypatch, [_response(503), _response(200)])

    OneLakeDfsClient(token="token").read(_file())

    assert len(sent) == 2
    assert slept == [client.CONNECTION_BACKOFF]


@weaver_test()
def test_a_read_waits_as_long_as_onelake_asked(monkeypatch):
    _sent, slept = _transport(
        monkeypatch,
        [_response(429, headers={"Retry-After": "7"}), _response(200, body={})],
    )

    OneLakeDfsClient(token="token").exists(_file())

    assert slept == [7.0]


@weaver_test()
def test_a_read_that_stays_refused_stops_at_the_attempt_bound(monkeypatch):
    sent, _slept = _transport(
        monkeypatch, [_response(503)] * client.CONNECTION_ATTEMPTS
    )

    with pytest.raises(StoreError, match="returned 503") as raised:
        OneLakeDfsClient(token="token").read(_file())

    assert raised.value.executor == "OneLake"
    assert len(sent) == client.CONNECTION_ATTEMPTS


@weaver_test()
def test_an_unreachable_read_stops_at_the_attempt_bound(monkeypatch):
    sent, _slept = _transport(monkeypatch, [_ambiguous()] * client.CONNECTION_ATTEMPTS)

    with pytest.raises(StoreError, match="could not be reached") as raised:
        OneLakeDfsClient(token="token").read(_file())

    assert raised.value.executor == "OneLake"
    assert len(sent) == client.CONNECTION_ATTEMPTS


@pytest.mark.parametrize("status", [400, 401, 403, 404])
@weaver_test()
def test_a_read_onelake_answered_is_not_repeated(monkeypatch, status):
    sent, _slept = _transport(monkeypatch, [_response(status)])

    with pytest.raises(StoreError, match=f"returned {status}"):
        OneLakeDfsClient(token="token").read(_file())

    assert len(sent) == 1


# --- mutations ------------------------------------------------------------------


@weaver_test()
def test_a_mutation_that_never_left_is_sent_again(monkeypatch):
    sent, _slept = _transport(monkeypatch, [_unsent(), _response(200)])

    OneLakeDfsClient(token="token").delete(_file())

    assert [method for method, _url in sent] == ["DELETE", "DELETE"]


@weaver_test()
def test_a_mutation_that_may_have_arrived_is_not_sent_again(monkeypatch):
    sent, _slept = _transport(monkeypatch, [_ambiguous()])

    with pytest.raises(StoreError, match="could not be reached") as raised:
        OneLakeDfsClient(token="token").make_directory(_file())

    assert raised.value.executor == "OneLake"
    assert len(sent) == 1


@pytest.mark.parametrize("status", [502, 503, 504])
@weaver_test()
def test_a_mutation_refused_with_a_transient_status_is_not_sent_again(
    monkeypatch, status
):
    sent, _slept = _transport(monkeypatch, [_response(status)])

    with pytest.raises(StoreError, match=f"returned {status}"):
        OneLakeDfsClient(token="token").delete(_file())

    assert len(sent) == 1
