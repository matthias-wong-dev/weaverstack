"""What a Livy session does when Fabric refuses, ends or loses a statement.

Fabric is doubled at the HTTP boundary: sessions start, statements are accepted
and finish, and a session Fabric has ended refuses statements as Fabric does,
with a 400 naming its terminal state. The ``LivySession`` and the
``ConsoleSession`` above it are the real ones.
"""

from __future__ import annotations

import json
import threading
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
            return _Response(
                200, {"state": "dead" if session in self.ended else "idle"}
            )
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
    fabric.ended.add("1")
    fabric.refusals.append(_Response(400, {"message": "Session is gone"}))
    session.restarted = lambda: fabric.ended.add("2")

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
def test_an_ended_session_is_read_from_its_state_not_the_refusals_wording(
    desktop, fabric
):
    session, scope = desktop
    fabric.ended.add("1")
    fabric.refusals.append(_Response(400, {"message": "Session is gone"}))

    session.execute_spark_sql("SELECT 1")

    assert fabric.sessions == ["1", "2"]
    assert len(fabric.run_on("2")) == 1


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


# --- callers on other threads ---------------------------------------------------


class Sessions:
    """Fabric's Livy API for callers on several threads.

    Statement ids are numbered per session, as Fabric's are, so statement 1 of an
    ended session and statement 1 of its replacement have the same id. ``on_post``
    runs inside a statement POST, after Fabric has accepted or refused it, which
    is where a test holds one caller while another acts.
    """

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.ended: set[str] = set()
        self.sessions: list[str] = []
        self.statements: dict[str, list[str]] = {}
        self.on_post = lambda session, code, accepted: None

    def send(self, method, url, data=None, **kwargs):
        if method == "POST" and url == BASE:
            with self.lock:
                self.sessions.append(str(len(self.sessions) + 1))
                session = self.sessions[-1]
                self.statements[session] = []
            return _Response(201, {"id": session})
        session = url[len(BASE) + 1 :].split("/")[0]
        if method == "GET" and url == f"{BASE}/{session}":
            return _Response(
                200, {"state": "dead" if session in self.ended else "idle"}
            )
        if method == "POST" and url.endswith("/statements"):
            code = json.loads(data)["code"]
            with self.lock:
                accepted = session not in self.ended
                if accepted:
                    self.statements[session].append(code)
                    number = len(self.statements[session])
            self.on_post(session, code, accepted)
            if not accepted:
                return _Response(
                    400,
                    {
                        "message": f"Session {session} is in a terminal state. "
                        "Scheduler state : Ended. Livy state : dead."
                    },
                )
            return _Response(201, {"id": number})
        if method == "GET" and "/statements/" in url:
            number = int(url.rsplit("/", 1)[-1])
            with self.lock:
                code = self.statements[session][number - 1]
            ran = f"{session}:{code}"
            return _Response(
                200,
                {
                    "state": "available",
                    "output": {
                        "status": "ok",
                        "data": {"text/plain": f"{RESULT_PREFIX}{json.dumps(ran)}"},
                    },
                },
            )
        raise AssertionError(f"unexpected {method} {url}")


@pytest.fixture
def sessions(monkeypatch):
    fabric = Sessions()
    monkeypatch.setattr(livy, "send", fabric.send)
    monkeypatch.setattr(livy.time, "sleep", lambda seconds: None)
    return fabric


def _in_thread(call) -> tuple[threading.Thread, list]:
    outcome: list = []

    def run():
        try:
            outcome.append(call())
        except BaseException as exc:  # noqa: BLE001 - asserted by the caller
            outcome.append(exc)

    thread = threading.Thread(target=run)
    thread.start()
    return thread, outcome


@weaver_test()
def test_a_statement_is_followed_on_the_session_that_accepted_it(sessions):
    """Session 1 accepts the first caller's statement and then ends. Before the
    first caller learns the statement's id, a second caller finds session 1
    ended and replaces it. The first statement is still session 1's."""

    session = _started()
    accepted, replaced = threading.Event(), threading.Event()

    def on_post(at, code, was_accepted):
        if code == "first" and was_accepted:
            sessions.ended.add(at)
            accepted.set()
            assert replaced.wait(5), "the second caller never replaced the session"

    sessions.on_post = on_post
    session.restarted = replaced.set

    first, first_outcome = _in_thread(lambda: session.run("first"))
    assert accepted.wait(5)
    second = session.run("second")
    first.join(5)

    assert second.payload == "2:second"
    (first_result,) = first_outcome
    assert first_result.payload == "1:first"
    assert sessions.statements == {"1": ["first"], "2": ["second"]}


@weaver_test()
def test_no_caller_submits_into_a_replacement_before_it_is_bootstrapped(sessions):
    """While one caller bootstraps the replacement, another caller's statement
    waits for it rather than running ahead of the bootstrap."""

    session = _started(
        bootstrap="def emit(value): ...", weaver_bootstrap="import weaver"
    )
    session.ensure_weaver()
    sessions.ended.add("1")
    bootstrapping, attempted = threading.Event(), threading.Event()
    third: list = []

    def on_post(at, code, was_accepted):
        if code == "third":
            attempted.set()
        elif at == "2" and code == "def emit(value): ...":
            bootstrapping.set()
            # Without the fix the replacement is already published, and the
            # third caller submits into it here.
            attempted.wait(1)

    sessions.on_post = on_post

    def third_caller():
        assert bootstrapping.wait(5)
        third.append(_in_thread(lambda: session.run("third")))

    starter = threading.Thread(target=third_caller)
    starter.start()
    assert session.run("second").payload == "2:second"
    starter.join(5)
    thread, outcome = third[0]
    thread.join(5)

    assert outcome[0].payload == "2:third"
    bootstrap, *after = sessions.statements["2"]
    assert bootstrap == "def emit(value): ..."
    assert after[0] == "import weaver"
    assert sorted(after[1:]) == ["second", "third"]
    assert sessions.sessions == ["1", "2"]
