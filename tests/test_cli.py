"""The CLI is an empty but working shell at checkpoint 0."""

from __future__ import annotations

import pytest

from weaver_cli import main


def test_bare_invocation_prints_help(capsys):
    assert main([]) == 0
    assert "usage: weaver" in capsys.readouterr().out


def test_help_exits_cleanly(capsys):
    with pytest.raises(SystemExit) as exit_info:
        main(["--help"])
    assert exit_info.value.code == 0
    assert "usage: weaver" in capsys.readouterr().out


def test_version_reports_the_distribution(capsys):
    with pytest.raises(SystemExit) as exit_info:
        main(["--version"])
    assert exit_info.value.code == 0
    assert "weaverstack" in capsys.readouterr().out


def test_doctor_reports_the_local_spark_requirements(capsys):
    exit_code = main(["doctor"])
    printed = capsys.readouterr().out
    for requirement in ("python", "pyspark", "delta-spark", "java"):
        assert requirement in printed
    assert exit_code in (0, 1)


def test_doctor_can_emit_json(capsys):
    main(["doctor", "--json"])
    import json

    report = json.loads(capsys.readouterr().out)
    assert {check["name"] for check in report["checks"]} == {
        "python", "pyspark", "delta-spark", "java",
    }
    assert isinstance(report["ok"], bool)


def test_doctor_exit_status_follows_the_report(capsys):
    """Non-zero when something is missing, so it can gate a script."""
    import json

    exit_code = main(["doctor", "--json"])
    report = json.loads(capsys.readouterr().out)
    assert exit_code == (0 if report["ok"] else 1)
