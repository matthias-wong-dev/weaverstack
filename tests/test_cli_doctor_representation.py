"""What `weaver doctor` shows, and what it exits with.

An ordinary connectivity failure is a sentence and a next action. A traceback
would say where in Weaver the call was made, which is not the part anybody can
act on.

The exit code is what a script reads, so a failed check is non-zero.
"""

from __future__ import annotations

import pytest
from support.weaver_test import weaver_test

from weaver.operations.doctor import FAILED, OK, Check, DoctorReport
from weaver_cli.doctor import render
from weaver_cli.main import build_parser


def _parse(*argv):
    return build_parser().parse_args(["doctor", *argv])


@weaver_test()
def test_doctor_takes_a_workspace_or_a_configuration():
    assert _parse("--workspace", "Weaver Example").workspace == "Weaver Example"
    assert _parse("--workspace-config", "w.yml").workspace_config == "w.yml"
    assert _parse().workspace is None


@weaver_test()
def test_doctor_names_no_environment():
    """It checks crossings; which Environment a run attaches is a run's business."""

    with pytest.raises(SystemExit):
        _parse("--environment", "Weaver")


@weaver_test()
def test_the_help_says_what_it_checks_and_what_that_costs():
    text = _doctor_help()

    assert "Check that Weaver can reach Microsoft Fabric." in text
    assert "TDS for a Warehouse, OneLake and Livy for a Lakehouse." in text
    assert "starts a Fabric Spark session" in text


def _command_line():
    """The CLI module. ``weaver_cli.main`` also names a function, so import it."""

    import importlib

    return importlib.import_module("weaver_cli.main")


def _doctor_help() -> str:
    import contextlib
    import io

    parser = build_parser()
    doctor = parser._subparsers._group_actions[0].choices["doctor"]
    stream = io.StringIO()
    with contextlib.redirect_stdout(stream):
        doctor.print_help()
    return stream.getvalue()


@weaver_test()
def test_the_help_separates_doctor_from_check():
    """Two verbs that both mean 'is this alright' are otherwise guessed at.

    The command list is where a reader meets them together, so it is where the
    difference has to be legible: one reaches Fabric, the other reads a
    repository and does not.
    """

    listed = build_parser().format_help()

    assert "doctor" in listed and "check" in listed
    assert "Check that Weaver can reach Microsoft Fabric." in listed
    assert "Check your repository source without contacting Fabric." in listed


@weaver_test()
def test_every_check_lines_up_in_one_column(capsys):
    render(
        DoctorReport(
            checks=(
                Check("Fabric REST", OK),
                Check("Workspace Weaver Example", OK),
                Check("Warehouse/Catalogue TDS", OK),
            ),
            workspace="Weaver Example",
        )
    )

    shown = capsys.readouterr().out
    lines = [line for line in shown.splitlines() if line.endswith("OK")]
    assert len({line.index("OK") for line in lines}) == 1
    assert not [line for line in lines if "...." in line]
    assert shown.rstrip().endswith("Everything checked is reachable.")


@weaver_test()
def test_a_failure_gives_the_reason_and_the_next_step(capsys):
    render(
        DoctorReport(
            checks=(
                Check("Fabric REST", OK),
                Check(
                    "Spark session",
                    FAILED,
                    detail="A Spark session could not be started.",
                    remedy="Check that the capacity is running.",
                ),
            ),
            workspace="Weaver Example",
        )
    )

    shown = capsys.readouterr()
    assert "Spark session                     FAILED" in shown.out
    assert "Spark session failed." in shown.err
    assert "A Spark session could not be started." in shown.err
    assert "Check that the capacity is running." in shown.err


@weaver_test()
def test_no_traceback_for_an_ordinary_failure(capsys):
    render(DoctorReport(checks=(Check("Fabric REST", FAILED, detail="no route"),)))

    shown = capsys.readouterr()
    assert "Traceback" not in shown.err
    assert "weaver/" not in shown.err


# --- the command ---------------------------------------------------------------


class _Report:
    def __init__(self, succeeded):
        self.succeeded = succeeded
        self.checks = (Check("Fabric REST", OK if succeeded else FAILED),)
        self.workspace = None
        self.failures = () if succeeded else self.checks

    def to_mapping(self):
        return {"succeeded": self.succeeded, "checks": [], "workspace": None}


@pytest.fixture
def answering(monkeypatch):
    """Doctor's own answer, without asking Fabric anything."""

    calls: list[dict] = []

    def doctor(**kwargs):
        calls.append(kwargs)
        return _Report(succeeded=True)

    monkeypatch.setattr("weaver.operations.doctor.doctor", doctor)
    monkeypatch.setattr(_command_line(), "_prefer_desktop_credential", lambda: None)
    return calls


@weaver_test()
def test_the_command_passes_what_it_was_given(answering):
    from weaver_cli.main import main

    assert main(["doctor", "--workspace", "Weaver Example"]) == 0
    assert answering[0]["workspace"] == "Weaver Example"


@weaver_test()
def test_a_failed_check_exits_non_zero(monkeypatch, capsys):
    from weaver_cli.main import main

    monkeypatch.setattr(
        "weaver.operations.doctor.doctor", lambda **_: _Report(succeeded=False)
    )
    monkeypatch.setattr(_command_line(), "_prefer_desktop_credential", lambda: None)

    assert main(["doctor"]) == 1


@weaver_test()
def test_the_json_form_is_the_whole_result(answering, capsys):
    import json

    from weaver_cli.main import main

    main(["doctor", "--json"])

    assert json.loads(capsys.readouterr().out) == {
        "succeeded": True,
        "checks": [],
        "workspace": None,
    }
