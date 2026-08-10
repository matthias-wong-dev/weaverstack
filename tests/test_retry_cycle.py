"""Fix the file, press Enter, and the Task runs again in the same Session.

The interaction this exists for:

.. code-block:: text

    Error installing Warehouse/Reporting/Sales.CustomerRevenue

    Source: Warehouse/Reporting/Sales.CustomerRevenue.sql

    Incorrect syntax near 'from'.

    Fix the file and press Enter to retry, or 'q' to give up.

Two claims, and the second is the one that makes it worth having:

**Retry is the whole Task from the beginning.** Fresh repository parse, fresh
observation of the estate, fresh plan. Nothing resumes inside a stale bundle or
graph, because a stale bundle describes the repository as it was before the
edit that prompted the retry.

**The Session is not part of what gets rebuilt.** A SQL syntax error is a Task
failure, not a resource failure, so the credential, resolver, item cache and
Livy session are all still healthy and still warm. Retrying through a cold start
would cost a minute per fix and defeat the point.
"""

from __future__ import annotations

import argparse

import pytest

from weaver.errors import CommandError


def _cli():
    """The command module, not the ``main`` function of the same dotted name.

    ``weaver_cli.main`` is both, and attribute access finds the function — so
    the module is asked for by name, exactly as the load tests do.
    """

    import sys

    import weaver_cli.main  # noqa: F401 - imported for its effect on sys.modules

    return sys.modules["weaver_cli.main"]


def _until_fixed(args, attempt):
    return _cli()._until_fixed(args, attempt)


class _Terminal:
    """Somebody at a keyboard, answering a fixed script."""

    def __init__(self, monkeypatch, answers=()):
        self.answers = list(answers)
        self.asked = []
        monkeypatch.setattr(_cli(), "_can_ask", lambda: True)
        monkeypatch.setattr("builtins.input", self._input)

    def _input(self, prompt=""):
        self.asked.append(prompt)
        if not self.answers:
            raise EOFError
        return self.answers.pop(0)


@pytest.fixture
def args(monkeypatch):
    """A parsed command with no workspace worth resolving."""

    monkeypatch.setattr(_cli(), "_resolve_workspace", lambda args: None)
    return argparse.Namespace(session=None)


def attempts(*statuses):
    """A Task that returns each status in turn, recording how often it ran."""

    remaining = list(statuses)
    calls = []

    def attempt():
        calls.append(len(calls) + 1)
        return remaining.pop(0) if remaining else 0

    attempt.calls = calls
    return attempt


# --- the loop -----------------------------------------------------------------


def test_a_task_that_succeeds_is_never_asked_about(args, monkeypatch):
    terminal = _Terminal(monkeypatch)
    attempt = attempts(0)

    assert _until_fixed(args, attempt) == 0
    assert len(attempt.calls) == 1
    assert terminal.asked == []


def test_a_failure_offers_a_retry_and_runs_the_whole_task_again(args, monkeypatch):
    terminal = _Terminal(monkeypatch, answers=[""])
    attempt = attempts(1, 0)

    assert _until_fixed(args, attempt) == 0
    assert len(attempt.calls) == 2
    assert len(terminal.asked) == 1


def test_retrying_repeatedly_is_allowed(args, monkeypatch):
    """A developer fixes one thing and finds the next. That is ordinary."""

    _Terminal(monkeypatch, answers=["", "", ""])
    attempt = attempts(1, 1, 1, 0)

    assert _until_fixed(args, attempt) == 0
    assert len(attempt.calls) == 4


def test_the_prompt_says_how_to_decline(args, monkeypatch):
    """An interaction that only offers "try again" is a trap."""

    terminal = _Terminal(monkeypatch, answers=["q"])

    _until_fixed(args, attempts(1))

    assert "q" in terminal.asked[0]
    assert "retry" in terminal.asked[0]


@pytest.mark.parametrize("answer", ["q", "quit", "n", "no", "exit", "Q", " no "])
def test_declining_returns_the_failure(args, monkeypatch, answer):
    _Terminal(monkeypatch, answers=[answer])
    attempt = attempts(1)

    assert _until_fixed(args, attempt) == 1
    assert len(attempt.calls) == 1


def test_an_end_of_input_declines_rather_than_looping(args, monkeypatch):
    """Ctrl-D at the prompt is the operator declining, not a failure of its own."""

    _Terminal(monkeypatch, answers=[])

    assert _until_fixed(args, attempts(1)) == 1


def test_an_interrupt_at_the_prompt_declines(args, monkeypatch):
    """Ctrl-C remains an operator interrupt. It is not a failed retry."""

    monkeypatch.setattr(_cli(), "_can_ask", lambda: True)

    def interrupted(prompt=""):
        raise KeyboardInterrupt

    monkeypatch.setattr("builtins.input", interrupted)

    assert _until_fixed(args, attempts(1)) == 1


# --- non-interactive ----------------------------------------------------------


def test_without_a_terminal_nothing_is_ever_asked(args, monkeypatch):
    """A pipeline gets the first answer, not a prompt it cannot answer."""

    monkeypatch.setattr(_cli(), "_can_ask", lambda: False)
    asked = []
    monkeypatch.setattr("builtins.input", lambda prompt="": asked.append(prompt))
    attempt = attempts(1)

    assert _until_fixed(args, attempt) == 1
    assert len(attempt.calls) == 1
    assert asked == []


def test_a_non_interactive_run_opens_no_session_of_its_own(monkeypatch):
    """Nothing is retried, so nothing needs holding open — and resolving a
    workspace to hold it would make a failure happen in a new place."""

    monkeypatch.setattr(_cli(), "_can_ask", lambda: False)

    def refuse(args):
        raise AssertionError("a non-interactive run resolved a workspace to retry with")

    monkeypatch.setattr(_cli(), "_resolve_workspace", refuse)

    assert _until_fixed(argparse.Namespace(session=None), attempts(1)) == 1


# --- the Session survives -----------------------------------------------------


class _Session:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


def test_every_attempt_runs_in_one_session(args, monkeypatch):
    """The reason retry is worth having: no cold start between fixes."""

    _Terminal(monkeypatch, answers=["", ""])
    seen = []

    def attempt():
        seen.append(args.session)
        return 0 if len(seen) == 3 else 1

    _until_fixed(args, attempt)

    assert len(seen) == 3
    assert len({id(session) for session in seen}) == 1
    assert seen[0] is not None


def test_a_borrowed_session_is_used_and_left_open(monkeypatch):
    """Inside `weaver session`, the console owns the Session and outlives the
    command that failed in it."""

    monkeypatch.setattr(_cli(), "_resolve_workspace", lambda args: None)
    _Terminal(monkeypatch, answers=[""])
    session = _Session()
    args = argparse.Namespace(session=session)
    seen = []

    def attempt():
        seen.append(args.session)
        return 0 if len(seen) == 2 else 1

    _until_fixed(args, attempt)

    assert seen == [session, session]
    assert not session.closed


def test_a_task_failure_is_not_a_resource_failure(args, monkeypatch):
    """A SQL syntax error, a Python import error, a bad declaration: none of
    them is a reason to throw away a Livy session that is still up.

    Asserted through the loop because that is where it would be lost — an
    implementation that reopened the Session per attempt would pass every other
    test here and still cost a cold start per fix.
    """

    _Terminal(monkeypatch, answers=[""])
    opened = []

    from weaver.session.console import ConsoleSession

    original = ConsoleSession.__init__

    def counted(self, *a, **kw):
        opened.append(self)
        original(self, *a, **kw)

    monkeypatch.setattr(ConsoleSession, "__init__", counted)

    attempt = attempts(1, 0)
    _until_fixed(args, attempt)

    assert len(attempt.calls) == 2
    assert len(opened) == 1, "the Session was reopened between attempts"


# --- what a retried Task re-reads ---------------------------------------------


def test_a_retry_re_runs_the_task_rather_than_resuming_inside_it(args, monkeypatch):
    """The Task is a callable that is simply called again.

    Which is the whole design: there is no resume point to get wrong, because
    `weaver.build` re-reads the repository, re-observes the estate and rebuilds
    the bundle every time it is invoked. This pins that the loop holds no state
    of its own between attempts.
    """

    _Terminal(monkeypatch, answers=[""])
    inputs = ["broken", "fixed"]
    read = []

    def attempt():
        # Standing in for the repository parse each attempt performs.
        current = inputs[len(read)]
        read.append(current)
        return 1 if current == "broken" else 0

    assert _until_fixed(args, attempt) == 0
    assert read == ["broken", "fixed"]


def test_a_raised_weaver_error_is_not_swallowed_by_the_loop(args, monkeypatch):
    """A command that raises rather than returning a status keeps raising.

    The loop retries *reported* failures. Turning an exception into a prompt
    would put a retry in front of errors that no edit can fix.
    """

    _Terminal(monkeypatch, answers=[""])

    def attempt():
        raise CommandError("no such Lakehouse")

    with pytest.raises(CommandError):
        _until_fixed(args, attempt)
