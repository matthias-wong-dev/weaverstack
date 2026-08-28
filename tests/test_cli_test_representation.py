"""``weaver test`` at the command line: what it parses, and what it prints.

The CLI owns no semantics. It adapts arguments to :func:`weaver.test` and
renders what comes back, so these assert exactly that boundary. The public
operation is faked, because what the operation does is proved where it is
implemented and re-proving it here would put two copies of the claim in the
suite.

The verdict is the exit code and the evidence is the output, which is what makes
``weaver test`` usable in a pipeline.
"""

from __future__ import annotations

import json

import pytest
from support.weaver_test import weaver_test

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


def _node(name, kind, status, result=None, diagnostics=None, messages=()):
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
        messages=messages,
    )


@pytest.fixture
def captured(monkeypatch, desktop_credential):
    """Whatever the CLI asked the operation for, and a report to give back."""

    seen: dict = {}
    report = ValidationRunReport(
        status=PASSED,
        nodes=(_node("Sales.OrdersReconcile", "Test", PASSED, TestResult()),),
    )

    def fake(items, **kwargs):
        seen["items"] = items
        seen.update(kwargs)
        return seen.get("report", report)

    monkeypatch.setattr(weaver, "test", fake)
    return seen


def _run(*args, workspace="Demo"):
    return main(
        [
            "test",
            *args,
            "--workspace",
            workspace,
            "--catalogue",
            "Warehouse/Weaver",
        ]
    )


# --- what it parses -----------------------------------------------------------


@weaver_test()
def test_items_are_passed_through(captured, capsys):
    _run("--item", "Lakehouse/Sales", "--item", "Warehouse/Reporting")

    assert captured["items"] == ["Lakehouse/Sales", "Warehouse/Reporting"]


@weaver_test()
def test_name_selects_one_installed_validation(captured, capsys):
    _run("--item", "Lakehouse/Sales", "--name", "Sales.OrdersReconcile")

    assert captured["name"] == "Sales.OrdersReconcile"
    assert captured["file"] is None


@weaver_test()
def test_file_runs_a_source_file(captured, capsys):
    _run("--item", "Lakehouse/Sales", "--file", "tests/Sales.X.sql")

    assert captured["file"] == "tests/Sales.X.sql"
    assert captured["name"] is None


@weaver_test()
def test_name_and_file_are_mutually_exclusive(capsys):
    """argparse refuses it, so no request that meant both can reach the API."""

    with pytest.raises(SystemExit) as exit_info:
        _run("--item", "Lakehouse/Sales", "--name", "Sales.X", "--file", "x.sql")

    assert exit_info.value.code == 2
    assert "not allowed with" in capsys.readouterr().err


@weaver_test()
def test_dry_run_is_passed_through(captured, capsys):
    _run("--item", "Lakehouse/Sales", "--dry-run")

    assert captured["dry_run"] is True


# --- the verdict --------------------------------------------------------------


@weaver_test()
def test_a_passing_run_exits_zero(captured, capsys):
    assert _run("--item", "Lakehouse/Sales") == 0


@weaver_test()
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

    assert _run("--item", "Lakehouse/Sales") == 1


@weaver_test()
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

    assert _run("--item", "Lakehouse/Sales") == 1


# --- what it prints -----------------------------------------------------------


@weaver_test()
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
    _run("--item", "Lakehouse/Sales")

    printed = capsys.readouterr().out
    assert "2 missing, 1 unexpected" in printed
    assert "4 violation(s)" in printed
    assert "0 passed, 2 failed, 0 could not run" in printed


@weaver_test()
def test_an_invalid_validation_prints_its_error_without_counts(captured, capsys):
    captured["report"] = ValidationRunReport(
        status=INVALID,
        nodes=(
            _node(
                "Sales.OrdersReconcile",
                "Test",
                INVALID,
                TestResult.failed_to_run("not installed"),
                messages=("not installed",),
            ),
        ),
    )

    assert _run("--item", "Lakehouse/Sales") == 1

    printed = capsys.readouterr().out
    assert "not installed" in printed
    assert "missing" not in printed


@weaver_test()
def test_a_generic_dispatch_failure_does_not_break_test_rendering(captured, capsys):
    from weaver.run.result import RunFailure

    captured["report"] = ValidationRunReport(
        status=INVALID,
        nodes=(
            _node(
                "Sales.OrdersReconcile",
                "Test",
                INVALID,
                RunFailure("LivyError: session unavailable"),
                messages=("LivyError: session unavailable",),
            ),
        ),
    )

    assert _run("--item", "Lakehouse/Sales") == 1

    printed = capsys.readouterr().out
    assert "LivyError: session unavailable" in printed
    assert "missing" not in printed


@weaver_test()
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
    _run("--item", "Lakehouse/Sales", "--name", "Sales.OrdersReconcile")

    printed = capsys.readouterr().out
    assert "_weaver_side" in printed
    assert "'Id': 7" in printed


@weaver_test()
def test_a_whole_target_run_prints_no_rows(captured, capsys):
    """There are none to print, the run never asked for them."""

    _run("--item", "Lakehouse/Sales")

    assert "_weaver_side" not in capsys.readouterr().out


@weaver_test()
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
    _run("--item", "Lakehouse/Sales", "--json")

    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == FAILED
    assert payload["totals"]["failed"] == 1
    assert payload["nodes"][0]["missing_count"] == 2


@weaver_test()
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
    _run("--item", "Lakehouse/Sales", "--json")

    assert "_weaver_sk" not in capsys.readouterr().out


@weaver_test()
def test_the_workflow_is_pointed_at(captured, capsys):
    """A run's evidence is `_.Log` rows correlated by Workflow ID.

    So a finished command gives the identity to select on,
    not a place to browse to.
    """

    captured["report"] = ValidationRunReport(
        status=PASSED,
        nodes=(_node("Sales.OrdersReconcile", "Test", PASSED, TestResult()),),
        workflow_id="0f8b2c1d",
    )
    _run("--item", "Lakehouse/Sales")

    assert "Workflow: 0f8b2c1d" in capsys.readouterr().out


# --- dry run and strict reach file mode too ------------------------------------


@weaver_test()
def test_dry_run_is_passed_through_with_file(captured, capsys):
    """`--file --dry-run` must mean what `--dry-run` means everywhere else."""

    _run("--item", "Lakehouse/Sales", "--file", "tests/Sales.X.sql", "--dry-run")

    assert captured["dry_run"] is True
    assert captured["file"] == "tests/Sales.X.sql"


@weaver_test()
def test_a_planned_file_run_exits_zero(captured, capsys):
    captured["report"] = ValidationRunReport(
        status=PLANNED,
        nodes=(_node("Sales.OrdersReconcile", "Test", PLANNED),),
    )

    assert (
        _run("--item", "Lakehouse/Sales", "--file", "tests/Sales.X.sql", "--dry-run")
        == 0
    )


@weaver_test()
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

    assert _run("--item", "Lakehouse/Sales", "--file", "tests/Sales.X.sql") == 1


@weaver_test()
def test_a_planned_installed_run_also_exits_zero(captured, capsys):
    """A dry run dispatched nothing, so there is nothing it can have got wrong."""

    captured["report"] = ValidationRunReport(
        status=PLANNED,
        nodes=(
            _node("Sales.OrdersReconcile", "Test", PLANNED),
            _node("Sales.NoOrphans", "Assumption", PLANNED),
        ),
    )

    assert _run("--item", "Lakehouse/Sales", "--dry-run") == 0


# --- evidence crossing the desktop-to-Fabric boundary --------------------------
