"""What a Session promises about Spark SQL, in both execution positions.

The Installer runs every build action in its own process and reaches Spark
through this capability, so what it guarantees is what the Spark executors are
allowed to rely on:

* ordered statements, run in the order given;
* one submission where they have to cross, so a setup and the query that reads
  it share a session;
* one identifier-case scope over all of them;
* the last statement's rows, as a list of dictionaries.

Both hosts are asserted against the same claims, because an executor cannot see
which one it has.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from weaver.sessions.console import ConsoleScope, ConsoleSession
from weaver.sessions.notebook import NotebookSession
from weaver.workspaces import Workspace

CASE_KEY = "spark.sql.caseSensitive"


class _Conf:
    def __init__(self, values=None):
        self.values = dict(values or {CASE_KEY: "false"})
        self.history: list[tuple[str, str]] = []

    def get(self, key):
        return self.values[key]

    def set(self, key, value):
        self.values[key] = str(value)
        self.history.append((key, str(value)))


class _Row:
    def __init__(self, **data):
        self._data = data

    def asDict(self):
        return dict(self._data)


class _Frame:
    def __init__(self, rows, spark, statement):
        self._rows = rows
        self._spark = spark
        self._statement = statement

    def collect(self):
        self._spark.collected.append(self._statement)
        return self._rows


class _Spark:
    """Records what it ran, what it collected, and the case scope each ran under."""

    def __init__(self, answers=None):
        self.executed: list[tuple[str, str]] = []
        self.collected: list[str] = []
        self.conf = _Conf()
        self._answers = answers or {}

    def sql(self, statement):
        self.executed.append((statement, self.conf.values[CASE_KEY]))
        return _Frame(self._answers.get(statement, []), self, statement)

    @property
    def statements(self) -> list[str]:
        return [statement for statement, _case in self.executed]


class _Livy:
    """One Livy session, counting submissions and answering with a fixed payload."""

    def __init__(self, payload=None):
        self.submitted: list[str] = []
        self._payload = [] if payload is None else payload

    def run(self, code, **kwargs):
        if "weaver.__version__" in code:
            from weaver import __version__

            return SimpleNamespace(returned=True, payload=__version__)
        self.submitted.append(code)
        return SimpleNamespace(returned=True, payload=self._payload)

    def start(self):
        pass

    def close(self, **kwargs):
        pass


@pytest.fixture
def desktop(monkeypatch):
    """A console addressing Fabric: no Spark of its own, everything crosses."""

    monkeypatch.setattr(
        ConsoleScope, "resolver", property(lambda self: SimpleNamespace(workspace=None))
    )

    def make(payload=None):
        livy = _Livy(payload)
        session = ConsoleSession(
            workspace=Workspace(
                workspace="Weaver", catalogue="Warehouse/Weaver", environment="weaver"
            ),
            livy=livy,
        )
        return session, livy

    return make


@pytest.fixture
def notebook():
    """Weaver inside Fabric: Spark is the attached session."""

    def make(spark):
        return NotebookSession(
            workspace=Workspace(
                workspace="Weaver", catalogue="Warehouse/Weaver", environment="weaver"
            ),
            spark=spark,
            resolver=object(),
            store=object(),
        )

    return make


# --- one submission ------------------------------------------------------------


def test_a_batch_that_crosses_crosses_once(desktop):
    """The property the whole batch capability exists for.

    A statement apiece would be a Livy submission apiece, and a setup that
    registers a temporary view would land in a different program from the query
    that reads it.
    """

    session, livy = desktop()
    session.execute_spark_sql_batch(
        ["CREATE TEMPORARY VIEW v AS SELECT 1 AS n", "DESCRIBE QUERY SELECT * FROM v"]
    )

    assert len(livy.submitted) == 1
    submitted = livy.submitted[0]
    assert "CREATE TEMPORARY VIEW v AS SELECT 1 AS n" in submitted
    assert "DESCRIBE QUERY SELECT * FROM v" in submitted


def test_one_statement_is_one_submission_too(desktop):
    session, livy = desktop()
    session.execute_spark_sql("SELECT 1")

    assert len(livy.submitted) == 1


def test_an_empty_batch_crosses_at_all(desktop):
    """Nothing to run is nothing to submit, and an empty answer."""

    session, livy = desktop()

    assert session.execute_spark_sql_batch([]) == []
    assert livy.submitted == []


# --- the rows that come back ---------------------------------------------------


def test_the_last_statement_answers(desktop):
    session, _livy = desktop(payload=[{"col_name": "n", "data_type": "int"}])

    answered = session.execute_spark_sql_batch(["SET x = 1", "DESCRIBE QUERY SELECT 1"])

    assert answered == [{"col_name": "n", "data_type": "int"}]


def test_in_a_session_the_last_statement_answers_and_the_rest_only_run(notebook):
    spark = _Spark(answers={"DESCRIBE QUERY SELECT 1": [_Row(col_name="n")]})
    session = notebook(spark)

    answered = session.execute_spark_sql_batch(
        ["CREATE TEMPORARY VIEW v AS SELECT 1", "DESCRIBE QUERY SELECT 1"]
    )

    assert answered == [{"col_name": "n"}]
    assert spark.statements == [
        "CREATE TEMPORARY VIEW v AS SELECT 1",
        "DESCRIBE QUERY SELECT 1",
    ]
    # Only the answer is materialised; the rest ran for their effect.
    assert spark.collected == ["DESCRIBE QUERY SELECT 1"]


# --- one identifier-case scope over the whole batch ----------------------------


def test_every_statement_in_a_batch_shares_the_exact_case_scope(notebook):
    """A setup analysed under one case and its query under another is a different
    query. Both hosts hold the scope open across the batch, and put it back."""

    spark = _Spark()
    session = notebook(spark)

    session.execute_spark_sql_batch(
        ["CREATE TEMPORARY VIEW V AS SELECT 1 AS N", "DESCRIBE QUERY SELECT N FROM V"],
        exact_case=True,
    )

    assert [case for _statement, case in spark.executed] == ["true", "true"]
    assert spark.conf.values[CASE_KEY] == "false"


def test_a_failing_statement_still_puts_the_case_scope_back(notebook):
    """A session left case-sensitive by a failure would change every statement
    that followed it, including ones from other work in the same session."""

    class _Failing(_Spark):
        def sql(self, statement):
            super().sql(statement)
            raise RuntimeError("analysis failed")

    spark = _Failing()
    session = notebook(spark)

    with pytest.raises(RuntimeError, match="analysis failed"):
        session.execute_spark_sql_batch(["SELECT 1"], exact_case=True)

    assert spark.conf.values[CASE_KEY] == "false"


def test_without_exact_case_the_host_default_stands(notebook):
    spark = _Spark()
    session = notebook(spark)

    session.execute_spark_sql_batch(["SELECT 1"])

    assert spark.executed == [("SELECT 1", "false")]
    assert spark.conf.history == []


def test_a_crossing_carries_the_case_scope_with_the_statements(desktop):
    """The desktop has no Spark to set a conf on, so the scope travels."""

    session, livy = desktop()
    session.execute_spark_sql_batch(["SELECT 1"], exact_case=True)

    submitted = livy.submitted[0]
    assert "_exact = True" in submitted
    assert CASE_KEY in submitted


def test_a_crossing_without_exact_case_touches_no_conf(desktop):
    session, livy = desktop()
    session.execute_spark_sql_batch(["SELECT 1"])

    assert "_exact = False" in livy.submitted[0]


# --- the single-statement form is the batch form -------------------------------


def test_one_statement_is_the_batch_capability(desktop):
    """There is one Spark SQL capability, and the convenience delegates to it."""

    session, _livy = desktop()
    seen = {}

    def record(statements, **kwargs):
        seen["statements"] = list(statements)
        seen.update(kwargs)
        return []

    session.execute_spark_sql_batch = record
    session.execute_spark_sql("SELECT 1", exact_case=True)

    assert seen["statements"] == ["SELECT 1"]
    assert seen["exact_case"] is True
