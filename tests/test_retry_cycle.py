"""Fix the file, press Enter, and the Task runs again in the same Session.

The interaction this exists for:

.. code-block:: text

    Error installing Warehouse/Reporting/Sales.CustomerRevenue

    Source: Warehouse/Reporting/Sales.CustomerRevenue.sql

    Incorrect syntax near 'from'.

    Enter to retry, Esc to exit.

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
import os
from pathlib import Path

import pytest
from support.weaver_test import weaver_test

from weaver.errors import CommandError

#: The checkout's source root, for the child process a pty test forks.
_SRC = Path(__file__).resolve().parents[1] / "src"


def _cli():
    """The command module, not the ``main`` function of the same dotted name.

    ``weaver_cli.main`` is both, and attribute access finds the function, so
    the module is asked for by name, exactly as the load tests do.
    """

    import sys

    import weaver_cli.main  # noqa: F401 - imported for its effect on sys.modules

    return sys.modules["weaver_cli.main"]


def _until_fixed(args, attempt):
    return _cli()._until_fixed(args, attempt)


ENTER = "\r"
ESC = "\x1b"


class _Terminal:
    """Somebody at a keyboard, pressing a fixed script of keys.

    Doubled at ``_read_key`` rather than at ``input``: the whole reason the
    prompt reads one key is that ``input()`` cannot see Esc, so a test that
    doubled ``input`` would be testing an interaction the product no longer has.
    """

    def __init__(self, monkeypatch, keys=()):
        self.keys = list(keys)
        self.presses = 0
        monkeypatch.setattr(_cli(), "_can_ask", lambda: True)
        monkeypatch.setattr(_cli(), "_read_key", self._read_key)

    def _read_key(self):
        self.presses += 1
        if not self.keys:
            raise EOFError
        return self.keys.pop(0)


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


@weaver_test()
def test_a_task_that_succeeds_is_never_asked_about(args, monkeypatch):
    terminal = _Terminal(monkeypatch)
    attempt = attempts(0)

    assert _until_fixed(args, attempt) == 0
    assert len(attempt.calls) == 1
    assert terminal.presses == 0


@weaver_test()
def test_a_failure_offers_a_retry_and_runs_the_whole_task_again(args, monkeypatch):
    terminal = _Terminal(monkeypatch, keys=[ENTER])
    attempt = attempts(1, 0)

    assert _until_fixed(args, attempt) == 0
    assert len(attempt.calls) == 2
    assert terminal.presses == 1


@weaver_test()
def test_retrying_repeatedly_is_allowed(args, monkeypatch):
    """A developer fixes one thing and finds the next. That is ordinary."""

    _Terminal(monkeypatch, keys=[ENTER, ENTER, ENTER])
    attempt = attempts(1, 1, 1, 0)

    assert _until_fixed(args, attempt) == 0
    assert len(attempt.calls) == 4


@weaver_test()
def test_the_prompt_says_how_to_leave(args, monkeypatch, capsys):
    """An interaction that only offers "try again" is a trap."""

    _Terminal(monkeypatch, keys=[ESC])

    _until_fixed(args, attempts(1))

    printed = capsys.readouterr().err
    assert "Enter to retry" in printed
    assert "Esc to exit" in printed
    # It offers; it does not instruct. The error above it has already said what
    # went wrong, and a build failure has already named the file to open.
    assert "Fix" not in printed


@pytest.mark.parametrize("key", [ESC, "\x03", "\x04"])
@weaver_test()
def test_leaving_returns_the_failure(args, monkeypatch, key):
    """Esc leaves; Ctrl-C and Ctrl-D are the operator declining too."""

    _Terminal(monkeypatch, keys=[key])
    attempt = attempts(1)

    assert _until_fixed(args, attempt) == 1
    assert len(attempt.calls) == 1


@weaver_test()
def test_a_stray_key_decides_nothing(args, monkeypatch):
    """Somebody tabbing back to the terminal and hitting a key has not
    answered, so the prompt keeps waiting rather than guessing."""

    terminal = _Terminal(monkeypatch, keys=["x", "\t", ENTER])
    attempt = attempts(1, 0)

    assert _until_fixed(args, attempt) == 0
    assert terminal.presses == 3
    assert len(attempt.calls) == 2


@weaver_test()
def test_an_end_of_input_declines_rather_than_looping(args, monkeypatch):
    """Ctrl-D at the prompt is the operator declining, not a failure of its own."""

    _Terminal(monkeypatch, keys=[])

    assert _until_fixed(args, attempts(1)) == 1


@weaver_test()
def test_an_interrupt_at_the_prompt_declines(args, monkeypatch):
    """Ctrl-C remains an operator interrupt. It is not a failed retry."""

    monkeypatch.setattr(_cli(), "_can_ask", lambda: True)

    def interrupted():
        raise KeyboardInterrupt

    monkeypatch.setattr(_cli(), "_read_key", interrupted)

    assert _until_fixed(args, attempts(1)) == 1


# --- non-interactive ----------------------------------------------------------


@weaver_test()
def test_without_a_terminal_nothing_is_ever_asked(args, monkeypatch):
    """A pipeline gets the first answer, not a prompt it cannot answer."""

    monkeypatch.setattr(_cli(), "_can_ask", lambda: False)
    pressed = []
    monkeypatch.setattr(_cli(), "_read_key", lambda: pressed.append(1) or ENTER)
    attempt = attempts(1)

    assert _until_fixed(args, attempt) == 1
    assert len(attempt.calls) == 1
    assert pressed == []


@weaver_test()
def test_a_non_interactive_run_opens_no_session_of_its_own(monkeypatch):
    """Nothing is retried, so nothing needs holding open, and resolving a
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


@weaver_test()
def test_every_attempt_runs_in_one_session(args, monkeypatch):
    """The reason retry is worth having: no cold start between fixes."""

    _Terminal(monkeypatch, keys=[ENTER, ENTER])
    seen = []

    def attempt():
        seen.append(args.session)
        return 0 if len(seen) == 3 else 1

    _until_fixed(args, attempt)

    assert len(seen) == 3
    assert len({id(session) for session in seen}) == 1
    assert seen[0] is not None


@weaver_test()
def test_a_borrowed_session_is_used_and_left_open(monkeypatch):
    """Inside `weaver session`, the console owns the Session and outlives the
    command that failed in it."""

    monkeypatch.setattr(_cli(), "_resolve_workspace", lambda args: None)
    _Terminal(monkeypatch, keys=[ENTER])
    session = _Session()
    args = argparse.Namespace(session=session)
    seen = []

    def attempt():
        seen.append(args.session)
        return 0 if len(seen) == 2 else 1

    _until_fixed(args, attempt)

    assert seen == [session, session]
    assert not session.closed


@weaver_test()
def test_a_task_failure_is_not_a_resource_failure(args, monkeypatch):
    """A SQL syntax error, a Python import error, a bad declaration: none of
    them is a reason to throw away a Livy session that is still up.

    Asserted through the loop because that is where it would be lost, an
    implementation that reopened the Session per attempt would pass every other
    test here and still cost a cold start per fix.
    """

    _Terminal(monkeypatch, keys=[ENTER])
    opened = []

    from weaver.sessions.console import ConsoleSession

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


@weaver_test()
def test_a_retry_re_runs_the_task_rather_than_resuming_inside_it(args, monkeypatch):
    """The Task is a callable that is simply called again.

    Which is the whole design: there is no resume point to get wrong, because
    `weaver.build` re-reads the repository, re-observes the estate and rebuilds
    the bundle every time it is invoked. This pins that the loop holds no state
    of its own between attempts.
    """

    _Terminal(monkeypatch, keys=[ENTER])
    inputs = ["broken", "fixed"]
    read = []

    def attempt():
        # Standing in for the repository parse each attempt performs.
        current = inputs[len(read)]
        read.append(current)
        return 1 if current == "broken" else 0

    assert _until_fixed(args, attempt) == 0
    assert read == ["broken", "fixed"]


@weaver_test()
def test_a_raised_weaver_error_is_not_swallowed_by_the_loop(args, monkeypatch):
    """A command that raises rather than returning a status keeps raising.

    The loop retries reported failures. Turning an exception into a prompt
    would put a retry in front of errors that no edit can fix.
    """

    _Terminal(monkeypatch, keys=[ENTER])

    def attempt():
        raise CommandError("no such Lakehouse")

    with pytest.raises(CommandError):
        _until_fixed(args, attempt)


# --- one prompt, for every Task that offers a retry ---------------------------


@weaver_test()
def test_the_prompt_instructs_nothing(args, monkeypatch, capsys):
    """A build failure has already printed `Source: …`; telling somebody who
    just read it to fix the file is telling them the obvious. And a load whose
    upstream is empty would be told to edit a file that is not the problem."""

    _Terminal(monkeypatch, keys=[ESC])

    _until_fixed(args, attempts(1))

    assert capsys.readouterr().err.strip().endswith("Enter to retry, Esc to exit.")


@weaver_test()
def test_every_retryable_command_offers_the_same_prompt():
    """Read off the wiring, so a fourth command cannot acquire wording of its
    own without somebody deciding to give it one.

    ``test`` is not among them. A validation that failed or could not be
    evaluated is a finding the report carries, so there is nothing for a retry
    to fix and nothing to be asked about.
    """

    import inspect

    source = inspect.getsource(_cli())

    for command in ("_build_once", "_load_once"):
        assert f"_until_fixed(args, lambda: {command}(args))" in source
    assert "_retry_until_fixed(lambda: _check_once(args))" in source
    assert "_until_fixed(args, lambda: _test_once(args))" not in source


# --- the keyboard itself ------------------------------------------------------
#
# Everything above doubles `_read_key`, which is right for the loop's logic and
# useless for the reading. What a terminal actually delivers is only observable
# through a terminal, and this got written wrongly twice: once returning a bare
# Esc for an arrow key, and once reading through `sys.stdin`, whose buffering
# swallows the rest of an escape sequence so `select` cannot see it. Both made
# every arrow key mean exit.


@pytest.mark.skipif(not hasattr(os, "fork"), reason="needs a POSIX pty")
@pytest.mark.parametrize(
    "sent, expected",
    [
        (b"\r", "ENTER"),
        (b"\n", "ENTER"),
        (b"\x1b", "ESC"),
        (b"x", "'x'"),
        # Sequences, which share their first byte with Esc and must not be it.
        (b"\x1b[A", r"'\x1b[A'"),
        (b"\x1b[D", r"'\x1b[D'"),
        (b"\x1bOP", r"'\x1bOP'"),
    ],
)
@weaver_test()
def test_one_keypress_is_read_as_itself(sent, expected):
    import pty
    import sys
    import time

    probe = (
        "import sys;"
        f"sys.path.insert(0, {str(_SRC)!r});"
        "from weaver_cli.main import _read_key, ESC;"
        "key = _read_key();"
        'name = {"\\r": "ENTER", "\\n": "ENTER", ESC: "ESC"}.get(key, repr(key));'
        'sys.stderr.write("GOT:" + name + "\\n");'
        "sys.stderr.flush()"
    )

    pid, descriptor = pty.fork()
    if pid == 0:  # the child is the terminal session
        # Between fork and exec the child is a copy of this pytest process, so
        # an exec that failed would leave a second pytest running the rest of
        # the suite concurrently, against the same temporary directories, and
        # failing tests with nothing to do with this one. `os._exit` skips every
        # handler and cannot be caught.
        try:
            os.execv(sys.executable, [sys.executable, "-c", probe])
        finally:
            os._exit(127)

    time.sleep(0.35)
    os.write(descriptor, sent)
    time.sleep(0.35)
    received = b""
    try:
        while b"GOT:" not in received:
            chunk = os.read(descriptor, 1024)
            if not chunk:
                break
            received += chunk
    except OSError:
        pass
    os.waitpid(pid, 0)

    answer = [
        line.strip()
        for line in received.decode(errors="replace").splitlines()
        if "GOT:" in line
    ]
    assert answer == [f"GOT:{expected}"]


# --- the boundary that makes a build retryable --------------------------------


@weaver_test()
def test_a_repository_the_parse_rejects_is_offered_another_attempt(monkeypatch, capsys):
    """What the loop is for, joined to the command that feeds it.

    The mechanism is proved above with synthetic attempts. This is the one case
    that carries a real build into it: a repository the parse rejected becomes a
    failed attempt, and the prompt reads one key before the command ends.
    """

    import weaver
    from weaver.errors import DiscoveryError
    from weaver.workspaces import Workspace

    monkeypatch.setattr(
        _cli(), "_resolve_workspace", lambda args: Workspace(workspace="Analytics")
    )
    terminal = _Terminal(monkeypatch, keys=[ESC])

    def refuse(*arguments, **keywords):
        raise DiscoveryError("Lakehouse/Sales/Tables/Sales__Order.py: refused")

    monkeypatch.setattr(weaver, "build", refuse)
    parsed = _cli().build_parser().parse_args(["build", "."])
    parsed.session = _Session()

    assert _cli().handle_build(parsed) == 1
    assert terminal.presses == 1
    assert "Sales__Order.py: refused" in capsys.readouterr().err


# --- what is not offered a retry ----------------------------------------------


@pytest.mark.parametrize("status", ["failed", "invalid"])
@weaver_test()
def test_a_validation_report_is_never_offered_a_retry(monkeypatch, capsys, status):
    """A Test that failed, and one that could not run, are both findings.

    Retry is for a Task that failed and an edit can fix. A validation reporting
    what it found is the command working, so `weaver test` runs once, exits
    zero, and whatever was pasted after it still runs.
    """

    import weaver
    from weaver.runtime.validation_result import TestResult
    from weaver.test_report import ValidationNodeReport, ValidationRunReport
    from weaver.workspaces import Workspace

    result = (
        TestResult(missing_count=2)
        if status == "failed"
        else TestResult.failed_to_run("not installed")
    )
    report = ValidationRunReport(
        status=status,
        nodes=(
            ValidationNodeReport(
                logical_id="Lakehouse/Sales/Sales.OrdersReconcile",
                kind="Test",
                physical_target="Lakehouse/Sales_LH",
                primitive_kind="python_validation",
                dispatch_location="tests/x.py",
                status=status,
                executed=True,
                result=result,
            ),
        ),
    )
    runs = []

    monkeypatch.setattr(
        _cli(), "_resolve_workspace", lambda args: Workspace(workspace="Analytics")
    )
    terminal = _Terminal(monkeypatch, keys=[ENTER])
    monkeypatch.setattr(weaver, "test", lambda *a, **k: runs.append(1) or report)
    parsed = _cli().build_parser().parse_args(["test", "Lakehouse/Sales"])
    parsed.session = _Session()

    assert _cli().handle_test(parsed) == 0
    assert runs == [1]
    assert terminal.presses == 0
