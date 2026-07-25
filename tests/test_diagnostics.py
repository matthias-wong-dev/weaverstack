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


def _fake_jdk(tmp_path, stderr: str):
    """A JAVA_HOME whose `java -version` writes exactly the given stderr."""

    java = tmp_path / "bin" / "java"
    java.parent.mkdir(parents=True)
    java.write_text(
        "#!/bin/sh\ncat >&2 <<'BANNER'\n" + stderr + "\nBANNER\n", encoding="utf-8"
    )
    java.chmod(0o755)
    return str(tmp_path)


def test_java_version_reads_the_banner_not_the_first_line(tmp_path):
    """A JVM told to pick up options announces them before its own version.

    JAVA_TOOL_OPTIONS is set by proxies and container images, so the banner is
    not reliably line one. Reading the first line regardless reports the
    announcement as the version.
    """

    home = _fake_jdk(
        tmp_path,
        "Picked up JAVA_TOOL_OPTIONS: -Dhttps.proxyHost=127.0.0.1\n"
        'openjdk version "17.0.19" 2026-01-20\n'
        "OpenJDK Runtime Environment (build 17.0.19+7)",
    )
    assert java_version(home) == "17.0.19"


def test_an_unreadable_banner_is_missing_rather_than_wrong(tmp_path):
    """Better to report no JDK than to report nonsense as its version."""

    assert java_version(_fake_jdk(tmp_path, "not a version banner")) is None


def test_spark_supports_more_than_one_jdk():
    """Pinning a single release would exclude a working machine."""
    assert len(SUPPORTED_JAVA) > 1
    assert "11" in SUPPORTED_JAVA and "17" in SUPPORTED_JAVA


@pytest.mark.spark
def test_the_report_agrees_with_a_session_actually_starting(spark):
    assert check_local_spark().ok
    assert spark.range(3).count() == 3
