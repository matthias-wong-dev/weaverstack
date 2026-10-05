"""What a Livy session does when Fabric refuses, ends or loses a statement.

Fabric is doubled at the HTTP boundary: sessions start, statements are accepted
and finish, and a session Fabric has ended refuses statements as Fabric does,
with a 400 naming its terminal state. The ``LivySession`` and the
``ConsoleSession`` above it are the real ones.
"""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest
import requests
from support.weaver_test import weaver_test

from weaver.errors import OutcomeUnknown
from weaver.fabric import livy
from weaver.fabric.livy import (
    RESULT_PREFIX,
    LivyOutcomeUnknown,
    LivyRefused,
    LivySession,
    LivySessionEnded,
)
from weaver.sessions.console import ConsoleScope, ConsoleSession
from weaver.sessions.resources import ResourceState
from weaver.workspaces import Workspace

BASE = livy.sessions_url("ws", "lh")


class _Response:
    def __init__(self, status: int, body: dict | None = None) -> None:
        self.status_code = status
        self._body = body or {}
        self.content = json.dumps(self._body).encode()
        self.text = self.content.decode()
        self.headers: dict = {}

    def json(self):
        return self._body


class Fabric:
    """Fabric's Livy API as a client meets it."""

    def __init__(self) -> None:
        self.sessions: list[str] = []
        self.ended: set[str] = set()
        self.statements: list[tuple[str, str]] = []
        #: Each refusal is answered once, in order, to the next statement POST.
        self.refusals: list[_Response] = []
        self.lose_next_response = False
        self.end_while_running = False

    def send(self, method, url, data=None, **kwargs):
        if method == "POST" and url == BASE:
            self.sessions.append(str(len(self.sessions) + 1))
            return _Response(201, {"id": self.sessions[-1]})
        session = url[len(BASE) + 1 :].split("/")[0]
        if method == "GET" and url == f"{BASE}/{session}":
            return _Response(200, {"state": "idle"})
        if method == "POST" and url.endswith("/statements"):
            if self.refusals:
                return self.refusals.pop(0)
            if session in self.ended:
                return _Response(
                    400,
                    {
                        "message": f"Session {session} is in a terminal state. "
                        "Scheduler state : Ended. Livy state : dead."
                    },
                )
            self.statements.append((session, json.loads(data)["code"]))
            if self.lose_next_response:
                self.lose_next_response = False
                raise requests.exceptions.ReadTimeout("the response was lost")
            return _Response(201, {"id": len(self.statements)})
        if method == "GET" and "/statements/" in url:
            if self.end_while_running:
                return _Response(404, {"message": "Session not found"})
            return _Response(
                200,
                {
                    "state": "available",
                    "output": {
                        "status": "ok",
                        "data": {"text/plain": f"{RESULT_PREFIX}{json.dumps('ran')}"},
                    },
                },
            )
        raise AssertionError(f"unexpected {method} {url}")

    def run_on(self, session: str) -> list[str]:
        return [code for one, code in self.statements if one == session]


@pytest.fixture
def fabric(monkeypatch):
    fabric = Fabric()
    monkeypatch.setattr(livy, "send", fabric.send)
    monkeypatch.setattr(livy.time, "sleep", lambda seconds: None)
    return fabric


def _started(**kwargs) -> LivySession:
    session = LivySession("ws", "lh", token="t", poll_interval=0, **kwargs)
    session.start()
    return session


# --- a session Fabric ended while idle -------------------------------------------


@weaver_test()
def test_a_statement_for_an_ended_session_runs_once_on_a_new_one(fabric):
    """Fabric refused it before accepting it, so it is sent once more, even for a
    submission that is never resent."""

    session = _started()
    fabric.ended.add("1")

    assert session.run("load()", retry_submission=False).payload == "ran"

    assert fabric.sessions == ["1", "2"]
    assert fabric.run_on("1") == []
    assert fabric.run_on("2") == ["load()"]


@weaver_test()
def test_a_new_session_is_bootstrapped_as_the_ended_one_was(fabric):
    session = _started(
        bootstrap="def emit(value): ...", weaver_bootstrap="import weaver"
    )
    session.ensure_weaver()
    fabric.ended.add("1")

    session.run("load()")

    assert fabric.run_on("2") == ["def emit(value): ...", "import weaver", "load()"]


@weaver_test()
def test_a_replacement_is_reported(fabric):
    session = _started()
    restarts = []
    session.restarted = lambda: restarts.append(1)
    fabric.ended.add("1")

    session.run("load()")

    assert restarts == [1]


@weaver_test()
def test_a_replacement_fabric_also_ends_is_not_replaced_again(fabric):
    session = _started()
    fabric.ended.update({"1", "2"})

    with pytest.raises(LivySessionEnded):
        session.run("load()")

    assert fabric.sessions == ["1", "2"]
    assert fabric.statements == []


# --- a statement Fabric may have accepted ----------------------------------------


@weaver_test()
def test_a_lost_response_to_a_submission_is_never_resent(fabric):
    session = _started()
    fabric.lose_next_response = True

    with pytest.raises(LivyOutcomeUnknown):
        session.run("load()", retry_submission=False)

    assert fabric.run_on("1") == ["load()"]
    assert fabric.sessions == ["1"]


@weaver_test()
def test_a_statement_whose_session_ends_while_it_runs_is_unknown(fabric):
    session = _started()
    fabric.end_while_running = True

    with pytest.raises(LivyOutcomeUnknown, match="was accepted"):
        session.run("load()")

    assert fabric.run_on("1") == ["load()"]
    assert fabric.sessions == ["1"]


# --- what the Session does with each ---------------------------------------------


@pytest.fixture
def desktop(fabric, monkeypatch):
    monkeypatch.setattr(
        ConsoleScope, "resolver", property(lambda self: SimpleNamespace(workspace=None))
    )
    session = ConsoleSession(
        workspace=Workspace(workspace="Analytics", environment="weaver"),
        livy=_started(),
    )
    with session:
        yield session, session.scope()


@weaver_test()
def test_a_refused_statement_leaves_the_session_for_the_next(desktop, fabric):
    """Fabric refused one request; the session it was for is still up."""

    session, scope = desktop
    fabric.refusals.append(
        _Response(400, {"message": "Request body cannot exceed 4194304 bytes"})
    )

    with pytest.raises(LivyRefused, match="4194304"):
        session.execute_spark_sql("SELECT 1")

    assert scope.livy.state is ResourceState.READY
    session.execute_spark_sql("SELECT 2")
    assert fabric.sessions == ["1"]


@weaver_test()
def test_a_session_fabric_ended_is_replaced_inside_the_session(desktop, fabric):
    session, scope = desktop
    fabric.ended.add("1")

    session.execute_spark_sql("SELECT 1")

    assert scope.livy.state is ResourceState.READY
    assert len(fabric.run_on("2")) == 1


@weaver_test()
def test_a_lost_response_fails_the_session(desktop, fabric):
    session, scope = desktop
    fabric.lose_next_response = True

    with pytest.raises(OutcomeUnknown):
        session.execute_spark_sql("SELECT 1")

    assert scope.livy.state is ResourceState.FAILED
