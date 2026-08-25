"""Holding a credential rather than a token string.

The bug these cover is invisible in a short run and fatal in a long one: a client
that snapshots the bearer keeps sending it until the API answers ``401``, and the
Azure CLI's own cache means the string may already be nearly spent when it
arrives. So the tests are about time passing, which is the one thing the
previous shape never modelled.
"""

from __future__ import annotations

from support.weaver_test import weaver_test

from weaver.fabric.auth import TokenProvider, token_source
from weaver.fabric.client import FabricClient
from weaver.fabric.livy import LivySession
from weaver.fabric.onelake import OneLakeDfsClient


class _Acquired:
    def __init__(self, token: str, expires_on: float):
        self.token = token
        self.expires_on = expires_on


class _Credential:
    """A credential that hands out a new token each time it is asked."""

    def __init__(self, *, lifetime: float = 3600.0, now=None):
        self.lifetime = lifetime
        self.calls = 0
        self._now = now

    def get_token(self, scope):
        self.calls += 1
        return _Acquired(f"token-{self.calls}", self._now() + self.lifetime)


def _clock(start: float = 1_000_000.0):
    """A movable clock, so a test can age a token without sleeping."""

    state = {"now": start}

    def now() -> float:
        return state["now"]

    def advance(seconds: float) -> None:
        state["now"] += seconds

    return now, advance


def _provider(monkeypatch, *, lifetime=3600.0, margin=300.0):
    now, advance = _clock()
    monkeypatch.setattr("time.time", now)
    credential = _Credential(lifetime=lifetime, now=now)
    return TokenProvider("scope", credential, margin=margin), credential, advance


# --- the provider -------------------------------------------------------------


@weaver_test()
def test_a_token_is_reused_while_it_is_comfortably_valid(monkeypatch):
    provider, credential, advance = _provider(monkeypatch)

    first = provider()
    advance(60)
    second = provider()

    assert first == second == "token-1"
    assert credential.calls == 1


@weaver_test()
def test_a_token_is_renewed_before_it_expires_not_after(monkeypatch):
    """Renewed inside the margin, so a call in flight still carries a valid one."""

    provider, credential, advance = _provider(monkeypatch, lifetime=3600, margin=300)

    assert provider() == "token-1"
    advance(3600 - 300 - 1)  # a second short of the margin
    assert provider() == "token-1"
    advance(2)  # now inside it
    assert provider() == "token-2"
    assert credential.calls == 2


@weaver_test()
def test_a_nearly_spent_token_is_renewed_on_first_use(monkeypatch):
    """The CLI's cache can hand over a token with minutes left, not an hour."""

    provider, credential, advance = _provider(monkeypatch, lifetime=120, margin=300)

    provider()
    provider()

    # Already inside the margin when it arrived, so every use renews.
    assert credential.calls == 2


@weaver_test()
def test_the_credential_is_built_once_and_kept(monkeypatch):
    """Rebuilding it per call would shell out to `az` every request."""

    provider, credential, advance = _provider(monkeypatch)

    for _ in range(5):
        provider()

    assert credential.calls == 1
    assert provider._credential() is credential


# --- what a caller may supply -------------------------------------------------


@weaver_test()
def test_a_supplied_string_is_honoured_exactly(monkeypatch):
    """The caller owns it, and its lifetime, a Fabric session passes one on."""

    source = token_source("fixed", scope="scope")

    assert source() == "fixed"
    assert source() == "fixed"


@weaver_test()
def test_a_supplied_callable_is_asked_every_time():
    """A caller with its own refresh keeps it."""

    answers = iter(["one", "two", "three"])
    source = token_source(lambda: next(answers), scope="scope")

    assert [source(), source(), source()] == ["one", "two", "three"]


# --- the clients that hold one ------------------------------------------------


@weaver_test()
def test_every_fabric_client_reads_its_token_per_request(monkeypatch):
    """The three that used to cache the string permanently.

    Each is long-lived, a REST client across a build, a DFS client across a
    push, a Livy session across a whole suite, so each has to ask again.
    """

    answers = iter([f"token-{index}" for index in range(1, 10)])
    monkeypatch.setattr(
        "weaver.fabric.auth.TokenProvider.__call__", lambda self: next(answers)
    )

    for client in (
        FabricClient(),
        OneLakeDfsClient(),
        LivySession("workspace", "lakehouse"),
    ):
        first, second = client.token, client.token
        assert first != second, f"{type(client).__name__} cached its token"


@weaver_test()
def test_a_livy_session_given_a_token_keeps_using_that_one():
    """An explicitly supplied token is the caller's to manage."""

    session = LivySession("workspace", "lakehouse", token="supplied")

    assert session.token == "supplied"
    assert session.token == "supplied"
