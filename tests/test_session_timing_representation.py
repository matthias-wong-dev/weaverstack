"""Task / Step / Sub-step: what was waited for, and how long.

The hierarchy is fixed at three levels and an error is not a fourth:

.. code-block:: text

    Task
      Step
        Sub-step
          diagnostic/error

An error is *content* attached to whichever of the three failed. Adding a level
for it would mean a reader had to know whether a failure was a place in the
tree or a thing that happened at one.

Two ledgers, deliberately, and neither derivable from the other. This one says
a Step took eight seconds. :class:`~weaver.session.telemetry.SessionTelemetry`
says the eight seconds were four Livy submissions and a token acquisition. A
suite that spent nine minutes in Livy startup and one in execution has a
different problem from one that spent ten in execution, and a single number
cannot tell them apart.

Durations themselves are never asserted. A test that pinned one would be a test
that fails on a slow machine and teaches everyone to widen the bound.
"""

from __future__ import annotations

import io
import os

import pytest

from weaver.session import ConsoleSession
from weaver.session.base import STEP, SUBSTEP, TASK


@pytest.fixture
def session():
    with ConsoleSession(progress=False) as opened:
        yield opened


def names(session):
    return [frame.name for frame in session.timings]


# --- the hierarchy ------------------------------------------------------------


def test_a_frame_records_what_it_cost(session):
    with session.task("Build"):
        pass

    (frame,) = session.timings
    assert frame.kind == TASK
    assert frame.elapsed is not None
    assert frame.elapsed >= 0


def test_an_open_frame_has_an_age_rather_than_an_elapsed_time(session):
    """The only honest way to hold it: work that is still running has no
    duration, it has an age."""

    with session.task("Build") as frame:
        assert frame.elapsed is None
        assert frame.age >= 0

    assert frame.elapsed is not None


def test_frames_nest_and_carry_their_depth(session):
    with session.task("Build"):
        with session.step("Install"):
            with session.substep("Sales.Customer"):
                pass

    depths = {frame.name: frame.depth for frame in session.timings}
    kinds = {frame.name: frame.kind for frame in session.timings}
    assert depths == {"Build": 0, "Install": 1, "Sales.Customer": 2}
    assert kinds["Install"] == STEP
    assert kinds["Sales.Customer"] == SUBSTEP


def test_a_child_closes_before_its_parent(session):
    with session.task("Build"):
        with session.step("Install"):
            pass

    assert names(session) == ["Install", "Build"]


def test_nothing_is_left_open_after_a_task(session):
    with session.task("Build"):
        with session.step("Install"):
            pass

    assert session.frames == ()


# --- failure ------------------------------------------------------------------


def test_a_failure_closes_every_frame_it_unwound(session):
    """The frame that fails is not the only one that stops."""

    with pytest.raises(RuntimeError):
        with session.task("Load"):
            with session.step("Execute"):
                with session.substep("DWG.Customer"):
                    raise RuntimeError("boom")

    assert session.frames == ()
    assert names(session) == ["DWG.Customer", "Execute", "Load"]
    assert all(frame.elapsed is not None for frame in session.timings)
    assert all(frame.failed for frame in session.timings)


def test_an_interrupt_closes_its_frames_and_travels_on(session):
    """Ctrl-C is the operator saying stop. The timing of cancelled work is
    still the timing of work that happened."""

    with pytest.raises(KeyboardInterrupt):
        with session.task("Load"):
            with session.step("Execute"):
                raise KeyboardInterrupt

    assert session.frames == ()
    assert names(session) == ["Execute", "Load"]


def test_a_frame_can_be_failed_from_inside_without_an_exception(session):
    """A run node's failure is data, not an exception — and it still reads as
    a failure in the timings."""

    with session.task("Load"):
        with session.substep("DWG.Customer") as frame:
            frame.failed = True

    failed = {frame.name: frame.failed for frame in session.timings}
    assert failed == {"DWG.Customer": True, "Load": False}


# --- what a reader gets -------------------------------------------------------


def test_a_frame_serialises_for_a_log_or_a_report(session):
    with session.task("Build", "My Workspace"):
        pass

    (mapping,) = [frame.to_mapping() for frame in session.timings]
    assert mapping["kind"] == TASK
    assert mapping["name"] == "Build"
    assert mapping["detail"] == "My Workspace"
    assert mapping["depth"] == 0
    assert isinstance(mapping["seconds"], float)


def test_the_console_writes_a_tree_a_person_can_read():
    out = io.StringIO()
    with ConsoleSession(progress=out) as session:
        with session.task("Build"):
            with session.step("Install Lakehouse/Sales"):
                with session.substep("Sales.Customer"):
                    pass

    printed = out.getvalue()
    lines = [line for line in printed.splitlines() if line.strip()]
    # Children above their parent, with the parent's own total underneath — a
    # roll-up, the way `du` reads.
    assert lines[0] == "Build"
    assert lines[1].strip().startswith("Sales.Customer")
    assert lines[2].strip().startswith("Install Lakehouse/Sales")
    assert lines[3].startswith("✓ Build")
    assert all(line.rstrip().endswith("s") for line in lines[1:])


def test_a_failed_task_is_marked_as_one():
    out = io.StringIO()
    with ConsoleSession(progress=out) as session:
        with pytest.raises(RuntimeError):
            with session.task("Build"):
                raise RuntimeError("boom")

    assert "✗ Build" in out.getvalue()


def test_progress_can_be_silenced_entirely():
    """A library caller is not a console, and must not be printed at."""

    with ConsoleSession(progress=False) as session:
        with session.task("Build"):
            pass

        assert session.timings, "silencing output must not stop recording"


def test_progress_never_reaches_stdout(capsys):
    """stdout is the command's answer, and several commands emit JSON on it."""

    with ConsoleSession() as session:
        with session.task("Build"):
            pass

    captured = capsys.readouterr()
    assert captured.out == ""
    assert "Build" in captured.err


# --- what is happening now ----------------------------------------------------
#
# A completed frame says what a wait cost. It cannot say a wait is *underway* —
# and the frames that most need saying so are the slow ones, where recording
# completions alone means a Task heading followed by two minutes of silence.


class _Tty(io.StringIO):
    """A stream that says it can have a line rewritten in it."""

    def isatty(self) -> bool:
        return True


def _screen(raw: str) -> list[str]:
    """What a terminal ends up showing, once carriage returns have overwritten."""

    lines = []
    for chunk in raw.split("\n"):
        line = ""
        for part in chunk.split("\r"):
            line = part + line[len(part) :]
        lines.append(line.rstrip())
    return lines


def test_an_open_frame_is_named_while_it_is_still_running():
    """The point of the feature: a slow Step is visible during the wait."""

    out = _Tty()
    with ConsoleSession(progress=out) as session:
        with session.task("Wipe"):
            with session.step("Unbind catalogue claims"):
                during = out.getvalue()

    assert "⋯ Unbind catalogue claims" in during


def test_the_live_line_reports_the_innermost_open_frame():
    """A Task names the command, which the heading already said. The useful
    answer to "what is it doing" is the smallest thing in flight."""

    out = _Tty()
    with ConsoleSession(progress=out) as session:
        with session.task("Load"):
            with session.step("Execute"):
                with session.substep("Lakehouse/Sales/DWG.Customer"):
                    during = out.getvalue()

    latest = during.rsplit("\r", 2)[-1]
    assert latest.startswith("⋯")
    assert latest.split()[1] == "Lakehouse/Sales/DWG.Customer"


def test_the_live_line_is_erased_and_leaves_no_trace_in_the_transcript():
    """It is a thing on a screen, not a thing in a log."""

    out = _Tty()
    with ConsoleSession(progress=out) as session:
        with session.task("Wipe"):
            with session.step("Unbind catalogue claims"):
                pass

    lines = [line for line in _screen(out.getvalue()) if line]
    assert [line.split()[0] for line in lines] == ["Wipe", "Unbind", "✓"]
    assert [line.split()[-1] for line in lines[1:]] == ["0.0s", "0.0s"]


def test_durations_line_up_however_long_the_names_are(monkeypatch):
    """A long object name must not shove its own duration out of the column.

    ``Warehouse/Reporting/Reporting.CustomerRevenuePresent`` at Sub-step depth
    runs past a fixed fifty-two-character column, and the duration that follows
    lands wherever the name happened to end — which loses the alignment that
    makes a column of durations scannable at all.
    """

    import shutil

    monkeypatch.setattr(
        shutil, "get_terminal_size", lambda *a: os.terminal_size((100, 24))
    )
    out = _Tty()
    with ConsoleSession(progress=out) as session:
        with session.task("Test"):
            with session.step("Execute"):
                with session.substep("Reporting.CustomerRevenuePresent"):
                    pass
                with session.substep("Short"):
                    pass

    timed = [line for line in _screen(out.getvalue()) if line.endswith("s")]
    assert len({len(line) for line in timed}) == 1, timed


def test_the_column_never_narrows_below_its_floor(monkeypatch):
    """A very narrow terminal gets a wrapped line rather than a squashed one."""

    import shutil

    monkeypatch.setattr(
        shutil, "get_terminal_size", lambda *a: os.terminal_size((20, 24))
    )
    session = ConsoleSession(progress=_Tty())
    assert session._width() == ConsoleSession.PROGRESS_WIDTH
    session.close()


def test_the_elapsed_figure_moves_while_nothing_else_happens():
    """Without a ticker the line is painted only when some other frame opens or
    closes — which, for the long waits that most need it, is never."""

    import time

    out = _Tty()
    with ConsoleSession(progress=out) as session:
        session.PROGRESS_TICK = 0.05
        with session.task("Wipe"):
            with session.step("Unbind catalogue claims"):
                time.sleep(0.3)
                repaints = out.getvalue().count("⋯ Unbind catalogue claims")

    assert repaints > 1


def test_a_stream_that_cannot_be_rewritten_gets_the_completed_lines_only():
    """Piped, redirected or captured: a log file and a test transcript are
    exactly what they were before any of this existed."""

    out = io.StringIO()
    with ConsoleSession(progress=out) as session:
        with session.task("Wipe"):
            with session.step("Unbind catalogue claims"):
                pass

    printed = out.getvalue()
    assert "⋯" not in printed
    assert "\r" not in printed
    assert "Unbind catalogue claims" in printed


def test_closing_the_session_takes_the_live_line_down():
    out = _Tty()
    session = ConsoleSession(progress=out)
    session.task_started("Wipe")
    session.step_started("Unbind catalogue claims")
    session.close()

    assert _screen(out.getvalue())[-1] == ""


# --- durable evidence ---------------------------------------------------------


def test_a_run_s_timings_ride_its_completion_document(session):
    """Not a file of their own. The evidence folder already says what a task
    intended and what each step did; how long a step took is a property of
    that step, not a second kind of record."""

    from weaver.load import _completion_document
    from weaver.load_report import LoadRunReport

    with session.task("Load"):
        with session.step("Execute"):
            with session.substep("Lakehouse/Sales/DWG.Customer"):
                pass

    document = _completion_document(
        LoadRunReport(
            requested=("Lakehouse/Sales",),
            status="succeeded",
            dry_run=False,
            fault_tolerant=False,
        ),
        timings=session.timings,
    )

    assert [entry["name"] for entry in document["timings"]] == [
        "Lakehouse/Sales/DWG.Customer",
        "Execute",
        "Load",
    ]
    assert all(entry["seconds"] is not None for entry in document["timings"])


def test_a_completion_document_without_timings_still_has_the_key(session):
    """A caller that recorded none says so, rather than omitting the field and
    making every reader handle its absence."""

    from weaver.load import _completion_document
    from weaver.load_report import LoadRunReport

    document = _completion_document(
        LoadRunReport(
            requested=(), status="succeeded", dry_run=True, fault_tolerant=False
        )
    )

    assert document["timings"] == []


# --- the two ledgers are separate ---------------------------------------------


def test_logical_timing_and_transport_timing_are_kept_apart(session):
    """Neither can be derived from the other, so neither is folded into it."""

    with session.task("Build"):
        with session.telemetry.timing("livy.submit"):
            pass

    assert [frame.name for frame in session.timings] == ["Build"]
    assert "livy.submit" in session.telemetry.measures
    assert "Build" not in session.telemetry.measures


def test_a_child_line_says_what_it_is_doing_not_only_what_to():
    """Children print above their parent, so a bare object name arrives before
    the Step it belongs to and reads as a stray line under the Task.

    This is the roll-up's one cost, and the fix is in the naming rather than in
    the layout: a line that carries its own verb needs no heading above it.
    """

    out = _Tty()
    with ConsoleSession(progress=out) as session:
        with session.task("Build"):
            with session.step("Read target inventories"):
                with session.substep("Read Warehouse/Reporting inventory"):
                    pass

    lines = [line for line in _screen(out.getvalue()) if line.strip()]
    first_child = lines[1]
    assert first_child.strip().startswith("Read Warehouse/Reporting")
