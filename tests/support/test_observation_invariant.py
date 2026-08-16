"""The evidence payload, tested where it belongs: in pure Python, with no Fabric.

This is the machinery every Fabric assertion now reads through, so a defect in it
would not fail — it would quietly assert something else. It has no Fabric in it
worth speaking of, so nothing here needs a workspace.

The body is checked by *running* it against a fake Spark rather than by matching
its text. What matters is that it collects the right evidence under the right
names, not how it spells the loop.
"""

from __future__ import annotations

import pytest

from .observation import Observation, observation_from, observe_body, observe_in_session


class FakeRow:
    def __init__(self, values: dict) -> None:
        self._values = values

    def asDict(self) -> dict:
        return dict(self._values)


class FakeCatalog:
    def __init__(self, schemas=(), tables=()) -> None:
        self._schemas = set(schemas)
        self._tables = set(tables)

    def databaseExists(self, name: str) -> bool:
        return name in self._schemas

    def tableExists(self, name: str) -> bool:
        return name in self._tables


class FakeSpark:
    """Answers the three questions an observation body asks, and records them."""

    def __init__(self, answers=None, schemas=(), tables=()) -> None:
        self.answers = answers or {}
        self.catalog = FakeCatalog(schemas, tables)
        self.asked: list[str] = []

    def sql(self, statement: str):
        self.asked.append(statement)
        return self

    def collect(self):
        return [FakeRow(row) for row in self.answers.get(self.asked[-1], [])]


def _run(body: str, spark: FakeSpark):
    """Execute an observation body the way both transports do, and return its emit."""

    emitted = []
    exec(
        compile(body, "<observation>", "exec"), {"spark": spark, "emit": emitted.append}
    )
    assert emitted, "the body emitted nothing"
    return emitted[-1]


def test_one_body_gathers_queries_schemas_and_tables_together():
    """The whole point: three kinds of evidence, one submission."""

    spark = FakeSpark(
        answers={"SELECT 1": [{"n": 0}]},
        schemas={"lh__dwg"},
        tables={"lh__dwg.customer"},
    )

    payload = _run(
        observe_body(
            {"rows": "SELECT 1"},
            {"present": "lh__dwg", "absent": "lh__legacy"},
            {"built": "lh__dwg.customer", "orphan": "lh__dwg.oldtable"},
        ),
        spark,
    )
    seen = observation_from(payload)

    assert seen.scalar("rows") == 0
    assert seen.schema("present") is True
    assert seen.schema("absent") is False
    assert seen.table("built") is True
    assert seen.table("orphan") is False


def test_statements_reach_spark_exactly_as_written():
    """Names are the caller's to qualify; the body must not rewrite them."""

    spark = FakeSpark(answers={"SHOW TABLES IN `ws`.`lh`.`DWG`": []})

    _run(observe_body({"tables": "SHOW TABLES IN `ws`.`lh`.`DWG`"}), spark)

    assert spark.asked == ["SHOW TABLES IN `ws`.`lh`.`DWG`"]


def test_a_body_asking_nothing_still_emits_a_payload():
    """An empty observation is empty evidence, not a missing one."""

    seen = observation_from(_run(observe_body({}), FakeSpark()))

    assert seen.rows == {} and seen.schemas == {} and seen.tables == {}


def test_quoting_survives_the_round_trip():
    """Fabric names carry backticks and dots; the body embeds them as literals."""

    statement = "SELECT * FROM `Weaver`.`Sales_LH`.`DWG`.`Customer` WHERE x = 'a'"
    spark = FakeSpark(answers={statement: [{"x": "a"}]})

    seen = observation_from(_run(observe_body({"q": statement}), spark))

    assert seen["q"] == [{"x": "a"}]


def test_values_lowercases_one_column():
    seen = Observation(
        rows={"tables": [{"tableName": "Customer"}, {"tableName": "ORDER"}]}
    )

    assert seen.values("tables", "tableName") == {"customer", "order"}


def test_values_ignores_blank_and_null_entries():
    """DESCRIBE pads its output with blank rows; they are not object names."""

    seen = Observation(
        rows={
            "cols": [
                {"col_name": "CustomerId"},
                {"col_name": ""},
                {"col_name": None},
            ]
        }
    )

    assert seen.values("cols", "col_name") == {"customerid"}


def test_a_scalar_wants_exactly_one_row():
    seen = Observation(rows={"n": [{"n": 1}, {"n": 2}]})

    with pytest.raises(AssertionError, match="returned 2 rows"):
        seen.scalar("n")


def test_asking_for_evidence_that_was_not_observed_says_what_was():
    """The failure mode this shape introduces, made legible.

    Consolidating means an assertion can name evidence the body never collected.
    A bare KeyError would send someone hunting in Fabric for a problem that is
    entirely local, so the message lists what the payload actually holds.
    """

    seen = Observation(rows={"tables": []}, schemas={"dwg": True})

    with pytest.raises(
        AssertionError, match=r"no evidence named 'views'.*\['tables'\]"
    ):
        seen["views"]
    with pytest.raises(AssertionError, match=r"no schema evidence named 'raw'"):
        seen.schema("raw")
    with pytest.raises(AssertionError, match=r"no table evidence named 'customer'"):
        seen.table("customer")


def test_a_session_that_emitted_no_payload_is_an_error_not_an_empty_result():
    """Silence from Fabric must not read as "the estate has nothing in it"."""

    class SilentSession:
        def run(self, body, *, label=None):
            return type("Result", (), {"payload": None})()

    with pytest.raises(AssertionError, match="emitted no payload"):
        observe_in_session(SilentSession(), queries={"q": "SELECT 1"})


def test_a_session_observation_is_one_submission_carrying_every_question():
    """The claim the whole exercise rests on, asserted rather than assumed."""

    class RecordingSession:
        def __init__(self):
            self.bodies = []
            self.labels = []

        def run(self, body, *, label=None):
            self.bodies.append(body)
            self.labels.append(label)
            spark = FakeSpark(answers={"SELECT 1": [{"n": 0}]}, schemas={"s"})
            return type("Result", (), {"payload": _run(body, spark)})()

    session = RecordingSession()
    seen = observe_in_session(
        session,
        queries={"a": "SELECT 1"},
        schemas={"b": "s"},
        tables={"c": "s.t"},
        label="observe install",
    )

    assert len(session.bodies) == 1, "an observation must be one round trip"
    assert session.labels == ["observe install"]
    assert seen.scalar("a") == 0 and seen.schema("b") and not seen.table("c")
