"""The local-Spark check itself."""

from __future__ import annotations

import pytest

from weaver.diagnostics import (
    SUPPORTED_JAVA,
    check_local_spark,
    find_java_home,
    java_version,
)


def test_every_requirement_is_reported():
    report = check_local_spark()
    assert {check.name for check in report.checks} == {
        "python", "pyspark", "delta-spark", "java",
    }


def test_the_report_is_serialisable():
    payload = check_local_spark().as_dict()
    assert isinstance(payload["ok"], bool)
    assert payload["checks"]


def test_a_failing_check_carries_a_hint():
    for check in check_local_spark().checks:
        if not check.ok:
            assert check.hint


def test_an_explicit_java_home_is_respected(tmp_path, monkeypatch):
    """A deliberately configured machine is never second-guessed."""
    monkeypatch.setenv("JAVA_HOME", str(tmp_path))
    assert find_java_home() == str(tmp_path)


def test_a_missing_java_home_falls_back_to_discovery(monkeypatch):
    monkeypatch.delenv("JAVA_HOME", raising=False)
    found = find_java_home()
    assert found is None or found


def test_java_version_of_nothing_is_nothing():
    assert java_version(None) is None


def test_spark_supports_more_than_one_jdk():
    """Pinning a single release would exclude a working machine."""
    assert len(SUPPORTED_JAVA) > 1
    assert "11" in SUPPORTED_JAVA and "17" in SUPPORTED_JAVA


@pytest.mark.spark
def test_the_report_agrees_with_a_session_actually_starting(spark):
    assert check_local_spark().ok
    assert spark.range(3).count() == 3
