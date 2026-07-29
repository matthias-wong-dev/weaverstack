"""A CLI-owned local Spark session cannot leak past its context."""

from __future__ import annotations

import os
from types import SimpleNamespace

import pytest

from weaver.spark.session import local_delta_session


class _Builder:
    def appName(self, _value):
        return self

    def master(self, _value):
        return self

    def config(self, *_args):
        return self


class _Context:
    def setLogLevel(self, _value):
        pass


class _Session:
    def __init__(self):
        self.sparkContext = _Context()
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
