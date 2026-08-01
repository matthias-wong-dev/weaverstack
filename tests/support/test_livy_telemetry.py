"""The Livy ledger, tested where it belongs: in pure Python, with no Fabric.

Instrumentation that only runs under `-m fabric` is instrumentation nobody
checks, and a miscounting ledger is worse than none — it would report a
reduction in round trips that had not happened. So the counting is proved here
and the workspace only has to supply real statements.
"""

from __future__ import annotations

import pytest
from .livy_telemetry import OUTSIDE_A_TEST, CountedLivySession, LivyLedger


class FakeStatement:
    def __init__(self, text: str = "") -> None:
        self.text = text


class FakeSession:
    """Stands in for a Fabric session: records bodies, returns fixed output."""

    def __init__(self, text: str = "", error: Exception | None = None) -> None:
        self.bodies: list[str] = []
        self.kwargs: list[dict] = []
        self._text = text
        self._error = error
        self.closed = False

    def run(self, code: str, **kwargs) -> FakeStatement:
        self.bodies.append(code)
        self.kwargs.append(kwargs)
        if self._error is not None:
            raise self._error
        return FakeStatement(self._text)

    def close(self) -> None:
        self.closed = True


def test_a_ledger_with_no_calls_reports_nothing():
    """A run that never reached Fabric must not print an empty transport banner."""

    assert LivyLedger().report() == []


def test_every_submission_is_counted_against_the_running_test():
    ledger = LivyLedger()
    session = CountedLivySession(FakeSession(), ledger)

    ledger.nodeid = "tests/fabric/test_x.py::test_one"
    session.run("emit(1)\n", label="query")
    session.run("emit(2)\n", label="query")
    ledger.nodeid = "tests/fabric/test_x.py::test_two"
    session.run("emit(3)\n", label="install")

    assert len(ledger.calls) == 3
    assert dict((nodeid, n) for nodeid, n, _s in ledger.by_test()) == {
        "tests/fabric/test_x.py::test_one": 2,
        "tests/fabric/test_x.py::test_two": 1,
    }
    assert dict((label, n) for label, n, _s in ledger.by_label()) == {
        "query": 2,
        "install": 1,
    }


def test_a_call_made_outside_a_test_is_still_counted():
    """Session-scoped fixtures submit statements too, and they are not free."""

    ledger = LivyLedger()
    CountedLivySession(FakeSession(), ledger).run("emit(1)\n", label="seed")

    assert [nodeid for nodeid, _n, _s in ledger.by_test()] == [OUTSIDE_A_TEST]


def test_the_body_and_its_output_reach_the_session_unchanged():
    """The proxy measures the call; it must not alter what is run or returned."""

    inner = FakeSession(text="__weaver_result__{}")
    session = CountedLivySession(inner, LivyLedger())

    result = session.run("emit({})\n", label="query", timeout=30.0)

    assert inner.bodies == ["emit({})\n"]
    assert inner.kwargs == [{"timeout": 30.0}]  # `label` is the ledger's, not Livy's
    assert result.text == "__weaver_result__{}"


def test_a_failing_statement_is_counted_and_still_raises():
    """A round trip that errored was still paid for, and must not be swallowed."""

    ledger = LivyLedger()
    session = CountedLivySession(FakeSession(error=RuntimeError("boom")), ledger)

    with pytest.raises(RuntimeError, match="boom"):
        session.run("1 / 0\n", label="probe")

    assert len(ledger.calls) == 1
    assert ledger.calls[0].failed is True
    assert ledger.calls[0].label == "probe"


def test_returned_output_size_is_recorded():
    """A consolidated payload trades call count for output size; show both."""

    ledger = LivyLedger()
    session = CountedLivySession(FakeSession(text="x" * 40), ledger)

    session.run("emit(1)\n", label="query")

    assert ledger.calls[0].returned_bytes == 40
    assert ledger.calls[0].code_bytes == len("emit(1)\n")


def test_everything_but_run_passes_through_to_the_real_session():
    """The proxy is transparent: fixtures set attributes and close the session."""

    inner = FakeSession()
    session = CountedLivySession(inner, LivyLedger())

    session.weaver_startup_seconds = 12.5
    session.close()

    assert inner.weaver_startup_seconds == 12.5
    assert inner.closed is True


def test_the_report_names_calls_elapsed_and_the_worst_caller():
    ledger = LivyLedger()
    ledger.startup_seconds = 30.0
    ledger.nodeid = "tests/fabric/test_x.py::test_expensive"
    ledger.record(label="install", seconds=8.0, code_bytes=10, returned_bytes=5)
    ledger.nodeid = "tests/fabric/test_x.py::test_cheap"
    ledger.record(label="query", seconds=1.0, code_bytes=10, returned_bytes=5)

    report = "\n".join(ledger.report())

    assert "Livy calls: 2" in report
    assert "Livy elapsed: 9.0s" in report
    assert "30.0s session startup" in report
    assert "install: 1 calls / 8.0s" in report
    # Ordered by cost, so the line worth acting on is the first one read.
    assert report.index("test_expensive") < report.index("test_cheap")
