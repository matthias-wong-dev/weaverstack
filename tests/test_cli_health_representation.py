"""``weaver health`` at the command line: what it parses, and what it prints.

The CLI owns no semantics. It adapts arguments to :func:`weaver.health` and
renders what comes back, so these assert exactly that boundary. The public
operation is faked, because what health decides is proved where it is
implemented.

The verdict is the exit code and the evidence is the output, which is what makes
``weaver health`` usable as a scheduled check.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest
from support.weaver_test import weaver_test

import weaver
from weaver.health import (
    AMBER,
    GREEN,
    RED,
    TEST_FAILED,
    HealthFinding,
    HealthReport,
    HealthSection,
    LoadActivity,
    LoadWorkflow,
)
from weaver_cli import main

NOW = datetime(2026, 4, 23, 12, 0, tzinfo=timezone.utc)


def _report(**asked) -> HealthReport:
    return HealthReport(
        generated_at=NOW,
        as_of=NOW - timedelta(hours=24),
        load=asked.pop("load", HealthSection(area="load", counts={"succeeded": 18})),
        tests=asked.pop("tests", HealthSection(area="tests", counts={"succeeded": 6})),
        build=asked.pop("build", HealthSection(area="build", subjects=24)),
        targets=asked.pop("targets", ("Lakehouse/Sales_LH",)),
        **asked,
    )


def _failing_tests() -> HealthSection:
    return HealthSection(
        area="tests",
        counts={"succeeded": 6, "failed": 1},
        findings=(
            HealthFinding(
                area="tests",
                code=TEST_FAILED,
                severity=RED,
                status="failed",
                message="the last run failed",
                object_id="Warehouse/Curated/Sales.HarmTotals",
                target="Warehouse/Curated_WH",
                failure_count=4,
            ),
        ),
    )


@pytest.fixture
def captured(monkeypatch, desktop_credential):
    """Whatever the CLI asked the operation for, and a report to give back."""

    seen: dict = {}

    def fake(targets=None, **kwargs):
        seen["targets"] = targets
        seen.update(kwargs)
        return seen.get("report", _report())

    monkeypatch.setattr(weaver, "health", fake)
    return seen


def _run(*args, workspace="Demo"):
    return main(
        [
            "health",
            *args,
            "--workspace",
            workspace,
            "--catalogue",
            "Warehouse/Weaver",
        ]
    )


# --- what it parses -----------------------------------------------------------


@weaver_test()
def test_no_target_means_the_whole_estate(captured):
    _run()

    assert captured["targets"] == []


@weaver_test()
def test_targets_are_passed_through_in_order(captured):
    _run("Lakehouse/Sales_LH", "Warehouse/Reporting_WH")

    assert captured["targets"] == ["Lakehouse/Sales_LH", "Warehouse/Reporting_WH"]


@weaver_test()
def test_as_of_is_passed_through_unparsed(captured):
    _run("--as-of", "2026-04-22T00:00:00Z")

    assert captured["as_of"] == "2026-04-22T00:00:00Z"


@weaver_test()
def test_as_of_defaults_to_none_so_the_operation_resolves_it(captured):
    _run()

    assert captured["as_of"] is None


@weaver_test()
def test_the_physical_read_is_on_unless_it_is_turned_off(captured):
    _run()
    assert captured["inventories"] is True

    _run("--no-inventory")
    assert captured["inventories"] is False


@weaver_test()
def test_the_catalogue_reaches_the_operation_as_a_name(captured):
    _run()

    assert captured["catalogue"] == "Warehouse/Weaver"


@weaver_test()
def test_health_takes_no_environment(captured):
    """Health runs no authored code, so it has no Environment to attach."""

    _run()

    assert "environment" not in captured
    with pytest.raises(SystemExit):
        main(["health", "--environment", "Runtime", "--workspace", "Demo"])


# --- the verdict --------------------------------------------------------------


@weaver_test()
def test_green_exits_zero(captured):
    assert _run() == 0


@weaver_test()
def test_amber_exits_one(captured):
    captured["report"] = _report(
        load=HealthSection(
            area="load",
            findings=(
                HealthFinding(
                    area="load",
                    code="load_pending",
                    severity=AMBER,
                    message="no load has settled since this object was built",
                    object_id="Lakehouse/Sales/Sales.Order",
                ),
            ),
        )
    )

    assert _run() == 1


@weaver_test()
def test_red_exits_one(captured):
    captured["report"] = _report(tests=_failing_tests())

    assert _run() == 1


# --- what it prints -----------------------------------------------------------


@weaver_test()
def test_the_status_words_are_the_contract(captured, capsys):
    captured["report"] = _report(tests=_failing_tests())
    _run()

    printed = capsys.readouterr().out

    assert "Weaver Health  Red" in printed
    assert "Load    Green" in printed
    assert "Tests   Red" in printed
    assert "Build   Green" in printed


@weaver_test()
def test_a_failing_subject_is_named_with_its_message(captured, capsys):
    captured["report"] = _report(tests=_failing_tests())
    _run()

    printed = capsys.readouterr().out

    assert "Warehouse/Curated/Sales.HarmTotals" in printed
    assert "the last run failed" in printed


@weaver_test()
def test_a_consistent_estate_says_so(captured, capsys):
    _run()

    assert "Installed estate consistent (24 objects)" in capsys.readouterr().out


@weaver_test()
def test_the_last_load_activity_is_reported_as_an_age(captured, capsys):
    captured["report"] = _report(
        latest_load=LoadWorkflow(
            workflow_id="workflow-1",
            started_at=NOW - timedelta(hours=6, minutes=20),
            completed_at=NOW - timedelta(hours=6, minutes=14),
        )
    )
    _run()

    assert "Last load activity   6h 14m ago" in capsys.readouterr().out


@weaver_test()
def test_the_slowest_loads_and_the_rows_that_moved_are_shown(captured, capsys):
    captured["report"] = _report(
        load_activity=(
            LoadActivity(
                object_id="Lakehouse/Sales/Sales.HarmSurvey",
                target="Lakehouse/Sales_LH",
                workflow_id="workflow-1",
                duration_ms=31200,
                rows_read=5412,
                rows_inserted=12,
                rows_updated=3,
            ),
        )
    )
    _run()

    printed = capsys.readouterr().out

    assert "Slowest loads" in printed
    assert "31.2s" in printed
    assert "read 5,412  +12 ~3 -0 !0" in printed


@weaver_test()
def test_the_plain_output_carries_no_decoration(captured, capsys):
    """No colour is required for meaning, so redirected output reads the same."""

    captured["report"] = _report(tests=_failing_tests())
    _run()

    printed = capsys.readouterr().out

    assert "\x1b[" not in printed


# --- the JSON contract ---------------------------------------------------------


@weaver_test()
def test_json_stdout_is_json_and_nothing_else(captured, capsys):
    captured["report"] = _report(tests=_failing_tests())
    _run("--json")

    payload = json.loads(capsys.readouterr().out)

    assert payload["format_version"] == 1
    assert payload["status"] == RED
    assert payload["sections"]["tests"]["status"] == RED
    assert payload["sections"]["load"]["status"] == GREEN


@weaver_test()
def test_json_carries_the_findings_a_consumer_acts_on(captured, capsys):
    captured["report"] = _report(tests=_failing_tests())
    _run("--json")

    payload = json.loads(capsys.readouterr().out)
    finding = payload["sections"]["tests"]["findings"][0]

    assert finding["code"] == TEST_FAILED
    assert finding["status"] == "failed"
    assert finding["object_id"] == "Warehouse/Curated/Sales.HarmTotals"
    assert finding["failure_count"] == 4


@weaver_test()
def test_json_timestamps_are_utc_iso_8601(captured, capsys):
    _run("--json")

    payload = json.loads(capsys.readouterr().out)

    assert payload["generated_at"] == NOW.isoformat()
    assert payload["as_of"].endswith("+00:00")
