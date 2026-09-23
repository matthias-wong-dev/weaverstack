"""An acceptance journey's failure reaches pytest's exit status.

Each case runs a small module in its own pytest process, because the claim is
about the outcome pytest reports, and that is only visible from outside.
"""

from __future__ import annotations

import subprocess
import sys
import textwrap
import xml.etree.ElementTree as ElementTree
from dataclasses import dataclass
from pathlib import Path

from support.weaver_test import weaver_test

TESTS = Path(__file__).resolve().parents[1]

PRELUDE = """\
import pytest
from support.acceptance import Acceptance
from weaver.test_report import ValidationRunReport


def boom():
    raise RuntimeError("the build broke")
"""


@dataclass(frozen=True)
class Run:
    exit_code: int
    outcomes: dict[str, str]
    messages: dict[str, str]


def _run(tmp_path: Path, body: str, *args: str) -> Run:
    module = tmp_path / "test_journey.py"
    module.write_text(PRELUDE + textwrap.dedent(body), encoding="utf-8")
    (tmp_path / "pytest.ini").write_text("[pytest]\n", encoding="utf-8")
    report = tmp_path / "report.xml"
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "-c",
            str(tmp_path / "pytest.ini"),
            "--rootdir",
            str(tmp_path),
            f"--junitxml={report}",
            str(module),
            *args,
        ],
        cwd=tmp_path,
        env={**_environment(), "PYTHONPATH": str(TESTS)},
        capture_output=True,
        text=True,
        timeout=120,
    )
    outcomes: dict[str, str] = {}
    messages: dict[str, str] = {}
    for case in ElementTree.parse(report).iter("testcase"):
        name = case.get("name")
        verdicts = [child for child in case if child.tag in _VERDICTS]
        # A teardown error is reported beside the call's own outcome.
        tags = [child.tag for child in verdicts]
        outcomes[name] = (
            "error"
            if "error" in tags
            else _VERDICTS.get(tags[0] if tags else "", "passed")
        )
        messages[name] = " | ".join(
            f"{child.get('message', '')} {child.text or ''}" for child in verdicts
        )
    assert outcomes, completed.stdout + completed.stderr
    return Run(completed.returncode, outcomes, messages)


_VERDICTS = {"failure": "failed", "error": "error", "skipped": "skipped"}


def _environment() -> dict[str, str]:
    import os

    return {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("PYTEST_", "WEAVER_PYTEST"))
    }


@weaver_test()
def test_an_owned_transition_that_raises_fails_its_owner_and_the_run(tmp_path):
    run = _run(
        tmp_path,
        """
        journey = Acceptance(name="journey")

        def test_build():
            journey.step("build", boom)
            journey.require("build")

        def test_load():
            journey.require("build")
        """,
    )

    assert run.exit_code == 1
    assert run.outcomes == {"test_build": "failed", "test_load": "skipped"}
    assert "the build broke" in run.messages["test_build"]
    assert "'build'" in run.messages["test_load"]
    assert "the build broke" not in run.messages["test_load"]


@weaver_test()
def test_an_unsuccessful_report_fails_its_owner_and_stops_its_dependants(tmp_path):
    run = _run(
        tmp_path,
        """
        findings = Acceptance(name="findings")
        invalid = Acceptance(name="invalid")

        def test_findings():
            findings.step("test", lambda: ValidationRunReport(status="failed"))
            findings.require("test")

        def test_invalid():
            invalid.step("test", lambda: ValidationRunReport(status="invalid"))
            invalid.require("test")

        def test_after_findings():
            findings.require("test")

        def test_after_invalid():
            invalid.require("test")
        """,
    )

    assert run.exit_code == 1
    assert run.outcomes == {
        "test_findings": "failed",
        "test_invalid": "failed",
        "test_after_findings": "skipped",
        "test_after_invalid": "skipped",
    }
    assert "failed" in run.messages["test_findings"]
    assert "invalid" in run.messages["test_invalid"]


@weaver_test()
def test_a_successful_journey_passes_whole(tmp_path):
    run = _run(
        tmp_path,
        """
        journey = Acceptance(name="journey")

        def test_build():
            journey.step("build", lambda: "built")
            journey.require("build")
            assert journey["build"].result == "built"

        def test_test():
            journey.require("build")
            journey.step("test", lambda: ValidationRunReport(status="passed"))
            journey.require("test")
        """,
    )

    assert run.exit_code == 0
    assert run.outcomes == {"test_build": "passed", "test_test": "passed"}


EAGER = """
@pytest.fixture(scope="module")
def journey():
    run = Acceptance(name="journey")
    run.step("build", lambda: "built")
    run.step("mirror", boom)
    run.step("load", lambda: "loaded")
    run.step("verify", lambda: "verified")
    yield run
    run.close()

def test_build(journey):
    journey.require("build")

def test_mirror(journey):
    journey.require("mirror")

def test_load(journey):
    journey.require("load")

def test_mirror_again(journey):
    journey.require("mirror")
"""


@weaver_test()
def test_an_eager_fixture_failure_is_reported_once_and_fails_the_run(tmp_path):
    run = _run(tmp_path, EAGER)

    assert run.exit_code == 1
    assert run.outcomes == {
        "test_build": "passed",
        "test_mirror": "failed",
        "test_load": "skipped",
        "test_mirror_again": "skipped",
    }
    assert "the build broke" in run.messages["test_mirror"]
    assert "'mirror'" in run.messages["test_load"]


@weaver_test()
def test_a_selected_dependant_reports_the_upstream_failure_it_reads(tmp_path):
    run = _run(tmp_path, EAGER, "-k", "test_load")

    assert run.exit_code == 1
    assert run.outcomes == {"test_load": "failed"}
    assert "the build broke" in run.messages["test_load"]


@weaver_test()
def test_a_failure_no_test_reads_fails_at_teardown(tmp_path):
    run = _run(tmp_path, EAGER, "-k", "test_build")

    assert run.exit_code == 1
    assert run.outcomes == {"test_build": "error"}
    assert "'mirror'" in run.messages["test_build"]


@weaver_test()
def test_a_prerequisite_that_never_ran_is_a_skip(tmp_path):
    run = _run(
        tmp_path,
        """
        journey = Acceptance(name="journey")

        def test_load():
            journey.require("build")

        def test_optional():
            journey.step("probe", lambda: pytest.skip("no Warehouse here"))

        def test_after_optional():
            journey.require("probe")
        """,
    )

    assert run.exit_code == 0
    assert run.outcomes == {
        "test_load": "skipped",
        "test_optional": "skipped",
        "test_after_optional": "skipped",
    }
    assert "no Warehouse here" in run.messages["test_optional"]


@weaver_test()
def test_an_expected_refusal_leaves_the_journey_running(tmp_path):
    run = _run(
        tmp_path,
        """
        journey = Acceptance(name="journey")

        def refused():
            with pytest.raises(RuntimeError, match="broke"):
                boom()
            return "refused"

        def test_refusal():
            journey.step("refuse", refused)
            journey.require("refuse")

        def test_recovery():
            journey.require("refuse")
            journey.step("recover", lambda: "recovered")
            journey.require("recover")
        """,
    )

    assert run.exit_code == 0
    assert run.outcomes == {"test_refusal": "passed", "test_recovery": "passed"}
