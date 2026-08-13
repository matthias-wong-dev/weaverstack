"""A CLI-owned local Spark session cannot leak past its context."""

from __future__ import annotations

import os
from types import SimpleNamespace

import pytest

from weaver.spark.session import local_delta_session


class _Builder:
    def __init__(self):
        self.configured = {}

    def appName(self, _value):
        return self

    def master(self, _value):
        return self

    def config(self, name, value):
        self.configured[name] = value
        return self


class _Context:
    def setLogLevel(self, _value):
        pass


class _Conf:
    def __init__(self):
        self.values = {}

    def set(self, key, value):
        self.values[key] = str(value)


class _Session:
    def __init__(self):
        self.sparkContext = _Context()
        self.conf = _Conf()
        self.stopped = False

    def stop(self):
        self.stopped = True


def test_session_stops_and_environment_is_restored_after_failure(monkeypatch):
    session = _Session()
    builder = _Builder()
    monkeypatch.setattr(
        "weaver.spark.session.import_module",
        lambda name: (
            SimpleNamespace(
                configure_spark_with_delta_pip=lambda _builder: SimpleNamespace(
                    getOrCreate=lambda: session
                )
            )
            if name == "delta"
            else SimpleNamespace(SparkSession=SimpleNamespace(builder=builder))
        ),
    )
    monkeypatch.setattr("weaver.spark.session.find_java_home", lambda: "/jdk")
    monkeypatch.setenv("JAVA_HOME", "/previous")
    monkeypatch.delenv("PYSPARK_PYTHON", raising=False)

    with pytest.raises(RuntimeError, match="boom"):
        with local_delta_session() as yielded:
            assert yielded is session
            raise RuntimeError("boom")

    assert session.stopped is True
    assert os.environ["JAVA_HOME"] == "/previous"
    assert "PYSPARK_PYTHON" not in os.environ


def test_workspace_session_uses_a_persistent_workspace_scoped_metastore(
    monkeypatch, tmp_path
):
    session = _Session()
    builder = _Builder()
    monkeypatch.setattr(
        "weaver.spark.session.import_module",
        lambda name: (
            SimpleNamespace(
                configure_spark_with_delta_pip=lambda _builder: SimpleNamespace(
                    getOrCreate=lambda: session
                )
            )
            if name == "delta"
            else SimpleNamespace(SparkSession=SimpleNamespace(builder=builder))
        ),
    )
    monkeypatch.setattr("weaver.spark.session.find_java_home", lambda: "/jdk")

    with local_delta_session(tmp_path):
        pass

    assert builder.configured["spark.sql.catalogImplementation"] == "hive"
    assert str(tmp_path / ".weaver" / "spark") in builder.configured[
        "javax.jdo.option.ConnectionURL"
    ]


def test_an_emulator_session_analyses_identifiers_exactly_for_its_whole_life(
    monkeypatch, tmp_path
):
    """A property of the session, not of whatever happens to run in it.

    The emulator's schema names are folded to lower case and its objects keep
    their declared spelling, and Spark's local catalogue cannot find a
    PascalCase table again once analysis returns to case-insensitive. Held here
    rather than by the first caller to build a catalogue, so the order things
    happen in cannot decide whether a table is findable.
    """

    session = _Session()
    monkeypatch.setattr(
        "weaver.spark.session.import_module",
        lambda name: (
            SimpleNamespace(
                configure_spark_with_delta_pip=lambda _builder: SimpleNamespace(
                    getOrCreate=lambda: session
                )
            )
            if name == "delta"
            else SimpleNamespace(SparkSession=SimpleNamespace(builder=_Builder()))
        ),
    )
    monkeypatch.setattr("weaver.spark.session.find_java_home", lambda: "/jdk")

    with local_delta_session(tmp_path):
        assert session.conf.values["spark.sql.caseSensitive"] == "true"
