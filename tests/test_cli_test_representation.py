"""``weaver test`` at the command line: what it parses, and what it prints.

The CLI owns no semantics — it adapts arguments to :func:`weaver.test` and
renders what comes back — so these assert exactly that boundary. The public
operation is faked, because what the operation *does* is proved where it is
implemented and re-proving it here would put two copies of the claim in the
suite.

The verdict is the exit code and the evidence is the output, which is what makes
``weaver test`` usable in a pipeline.
"""

from __future__ import annotations

import importlib

import json

import pytest

import weaver
from weaver.runtime.validation_result import AssumptionResult, TestResult
from weaver.test_report import (
    FAILED,
    INVALID,
    PASSED,
    PLANNED,
    ValidationNodeReport,
    ValidationRunReport,
)
from weaver_cli import main


def _node(name, kind, status, result=None, diagnostics=None):
    return ValidationNodeReport(
        logical_id=f"Lakehouse/Sales/{name}",
        kind=kind,
        physical_target="Lakehouse/Sales_LH",
        primitive_kind="python_validation",
        dispatch_location="Lakehouse/Sales/file:_/Load/tests/x.py",
        status=status,
        executed=status != PLANNED,
        result=result,
        diagnostics=diagnostics,
    )


@pytest.fixture
def captured(monkeypatch, desktop_credential):
    """Whatever the CLI asked the operation for, and a report to give back."""

    seen: dict = {}
    report = ValidationRunReport(
        status=PASSED,
        nodes=(_node("Sales.OrdersReconcile", "Test", PASSED, TestResult()),),
    )

    def fake(targets, **kwargs):
        seen["targets"] = targets
        seen.update(kwargs)
        return seen.get("report", report)

    monkeypatch.setattr(weaver, "test", fake)
    # The desktop preflight resolves each named target over REST before the
    # operation runs. What this file claims is what the CLI *parses* and hands
    # on, so the crossing is stubbed rather than made.
    # `weaver_cli.main` is also the name of the entry point function, so the
    # module is imported rather than reached by dotted string.
    monkeypatch.setattr(
        importlib.import_module("weaver_cli.main"),
        "_refuse_absent_targets",
        lambda *_a, **_k: None,
    )
    return seen


def _run(*args, workspace="Demo"):
    return main(
        [
            "test",
            *args,
            "--workspace",
            workspace,
            "--catalogue",
            "Lakehouse/Weaver",
        ]
    )


# --- what it parses -----------------------------------------------------------


def test_targets_are_passed_through(captured, capsys):
    _run("Lakehouse/Sales", "Warehouse/Reporting")

    assert captured["targets"] == ["Lakehouse/Sales", "Warehouse/Reporting"]


def test_name_selects_one_installed_validation(captured, capsys):
    _run("Lakehouse/Sales", "--name", "Sales.OrdersReconcile")

    assert captured["name"] == "Sales.OrdersReconcile"
    assert captured["file"] is None


def test_file_runs_a_source_file(captured, capsys):
    _run("Lakehouse/Sales", "--file", "tests/Sales.X.sql")

    assert captured["file"] == "tests/Sales.X.sql"
    assert captured["name"] is None


def test_name_and_file_are_mutually_exclusive(capsys):
    """argparse refuses it, so no request that meant both can reach the API."""

    with pytest.raises(SystemExit) as exit_info:
        _run("Lakehouse/Sales", "--name", "Sales.X", "--file", "x.sql")

    assert exit_info.value.code == 2
    assert "not allowed with" in capsys.readouterr().err


def test_dry_run_is_passed_through(captured, capsys):
    _run("Lakehouse/Sales", "--dry-run")

    assert captured["dry_run"] is True


# --- the verdict --------------------------------------------------------------


def test_a_passing_run_exits_zero(captured, capsys):
    assert _run("Lakehouse/Sales") == 0


def test_a_failing_run_exits_non_zero(captured, capsys):
    captured["report"] = ValidationRunReport(
        status=FAILED,
        nodes=(
            _node(
                "Sales.OrdersReconcile",
                "Test",
                FAILED,
                TestResult(missing_count=2, unexpected_count=1),
            ),
        ),
    )

    assert _run("Lakehouse/Sales") == 1


def test_a_run_that_could_not_answer_exits_non_zero(captured, capsys):
    captured["report"] = ValidationRunReport(
        status=INVALID,
        nodes=(
            _node(
                "Sales.OrdersReconcile",
                "Test",
                INVALID,
                TestResult.failed_to_run("not installed"),
            ),
        ),
    )

    assert _run("Lakehouse/Sales") == 1


# --- what it prints -----------------------------------------------------------


def test_the_counts_are_rendered_per_validation(captured, capsys):
    captured["report"] = ValidationRunReport(
        status=FAILED,
        nodes=(
            _node(
                "Sales.OrdersReconcile",
                "Test",
                FAILED,
                TestResult(missing_count=2, unexpected_count=1),
            ),
            _node(
                "Sales.NoOrphans",
                "Assumption",
                FAILED,
                AssumptionResult(violation_count=4),
            ),
        ),
    )
    _run("Lakehouse/Sales")

    printed = capsys.readouterr().out
    assert "2 missing, 1 unexpected" in printed
    assert "4 violation(s)" in printed
    assert "0 passed, 2 failed, 0 could not run" in printed


def test_a_targeted_run_prints_its_evidence(captured, capsys):
    captured["report"] = ValidationRunReport(
        status=FAILED,
        nodes=(
            _node(
                "Sales.OrdersReconcile",
                "Test",
                FAILED,
                TestResult(missing_count=1),
                diagnostics=({"_weaver_side": "expected", "_weaver_sk": 1, "Id": 7},),
            ),
        ),
    )
    _run("Lakehouse/Sales", "--name", "Sales.OrdersReconcile")

    printed = capsys.readouterr().out
    assert "_weaver_side" in printed
    assert "'Id': 7" in printed


def test_a_whole_target_run_prints_no_rows(captured, capsys):
    """There are none to print — the run never asked for them."""

    _run("Lakehouse/Sales")

    assert "_weaver_side" not in capsys.readouterr().out


def test_json_emits_the_whole_report(captured, capsys):
    captured["report"] = ValidationRunReport(
        status=FAILED,
        nodes=(
            _node(
                "Sales.OrdersReconcile",
                "Test",
                FAILED,
                TestResult(missing_count=2, unexpected_count=1),
            ),
        ),
    )
    _run("Lakehouse/Sales", "--json")

    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == FAILED
    assert payload["totals"]["failed"] == 1
    assert payload["nodes"][0]["missing_count"] == 2


def test_json_carries_no_diagnostic_rows(captured, capsys):
    captured["report"] = ValidationRunReport(
        status=FAILED,
        nodes=(
            _node(
                "Sales.OrdersReconcile",
                "Test",
                FAILED,
                TestResult(missing_count=1),
                diagnostics=({"_weaver_sk": 1, "Id": 7},),
            ),
        ),
    )
    _run("Lakehouse/Sales", "--json")

    assert "_weaver_sk" not in capsys.readouterr().out


def test_the_task_log_is_pointed_at(captured, capsys):
    captured["report"] = ValidationRunReport(
        status=PASSED,
        nodes=(_node("Sales.OrdersReconcile", "Test", PASSED, TestResult()),),
        task_log="abfss://Weaver/Files/_/Log/task_date=2026-08-08/…",
    )
    _run("Lakehouse/Sales")

    assert "Logs:" in capsys.readouterr().out


def test_a_onelake_task_log_is_offered_as_a_link_that_opens(captured, capsys):
    """The DFS address is where Weaver wrote it; the portal is where you read it.

    Pasting a ``onelake.dfs`` URL into a browser gets an authentication error,
    so a line that looks like a helpful link and is not is worse than the raw
    path — a reader tries it, it fails, and they stop trusting the line.
    """

    folder = (
        "https://onelake.dfs.fabric.microsoft.com/ws-id/item-id"
        "/Files/_/Log/task_date=2026-08-08/x.json"
    )
    captured["report"] = ValidationRunReport(
        status=PASSED,
        nodes=(_node("Sales.OrdersReconcile", "Test", PASSED, TestResult()),),
        task_log=folder,
    )
    _run("Lakehouse/Sales")

    printed = capsys.readouterr().out
    assert "app.fabric.microsoft.com/groups/ws-id/lakehouses/item-id" in printed
    assert "selectedPath=Files%2F_%2FLog%2Ftask_date%3D2026-08-08%2Fx.json" in printed
    assert "onelake.dfs" not in printed


# --- dry run and strict reach file mode too ------------------------------------


def test_dry_run_is_passed_through_with_file(captured, capsys):
    """`--file --dry-run` must mean what `--dry-run` means everywhere else."""

    _run("Lakehouse/Sales", "--file", "tests/Sales.X.sql", "--dry-run")

    assert captured["dry_run"] is True
    assert captured["file"] == "tests/Sales.X.sql"


def test_a_planned_file_run_exits_zero(captured, capsys):
    captured["report"] = ValidationRunReport(
        status=PLANNED,
        nodes=(_node("Sales.OrdersReconcile", "Test", PLANNED),),
    )

    assert _run("Lakehouse/Sales", "--file", "tests/Sales.X.sql", "--dry-run") == 0


def test_a_failing_file_run_exits_non_zero(captured, capsys):
    captured["report"] = ValidationRunReport(
        status=FAILED,
        nodes=(
            _node(
                "Sales.OrdersReconcile",
                "Test",
                FAILED,
                TestResult(missing_count=1, unexpected_count=1),
            ),
        ),
    )

    assert _run("Lakehouse/Sales", "--file", "tests/Sales.X.sql") == 1


def test_a_planned_installed_run_also_exits_zero(captured, capsys):
    """A dry run dispatched nothing, so there is nothing it can have got wrong."""

    captured["report"] = ValidationRunReport(
        status=PLANNED,
        nodes=(
            _node("Sales.OrdersReconcile", "Test", PLANNED),
            _node("Sales.NoOrphans", "Assumption", PLANNED),
        ),
    )

    assert _run("Lakehouse/Sales", "--dry-run") == 0


# --- evidence crossing the desktop-to-Fabric boundary --------------------------
