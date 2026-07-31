"""One action executed against one target, with the installer's result semantics.

This is the layer that decides how much Fabric the suite has to buy. An executor
is where Weaver meets the engine, and almost all of what it does is checkable
without one: that the exact statement reaches the session, that a logical name is
resolved against the batch's destination before it does, that a missing
capability fails saying which, and that a failure becomes a *result* rather than
an exception.

What is left for a real workspace afterwards is genuinely narrow — does Fabric
accept this T-SQL, does the object appear in inventory — and answering it no
longer requires parsing a repository, reading a catalogue and installing a bundle
to reach the one statement in question.

`execute_action` and the installer share one execution path, so the semantics
asserted here are the semantics an installation gets.
"""

from __future__ import annotations

import json

import pytest
from factories import (
    FakeSpark,
    FakeSql,
    build_action,
    installation_context,
    resolved_target,
    warehouse_context,
)

from weaver.build_bundle import execute_action
from weaver.build_bundle.executors import default_executors

VIEW_SQL = b"CREATE OR REPLACE VIEW {{object:DWG.ActiveCustomer}} AS SELECT 1\n"


# --- result semantics ---------------------------------------------------------


def test_a_successful_action_reports_succeeded_against_its_target():
    spark = FakeSpark()
    action = build_action(payload="p.sql", payload_sha256="unused")

    result = execute_action(
        action, VIEW_SQL, context=installation_context(spark=spark)
    )

    assert result.status == "succeeded"
    assert result.action_id == action.id
    assert result.resource_node_id == "Lakehouse/Sales/DWG.Customer"
    assert result.target_id == "target-1"
    assert result.executor == "spark_sql"


def test_a_failing_action_is_recorded_rather_than_raised():
    """A failure is data, here exactly as in an installation.

    If this raised, an installer built on the same path could not record one
    action's failure and carry on to report it — and every caller would need its
    own try/except to find out what went wrong.
    """

    class Exploding(FakeSpark):
        def sql(self, statement):
            raise RuntimeError("no such column")

    result = execute_action(
        build_action(payload="p.sql"),
        VIEW_SQL,
        context=installation_context(spark=Exploding()),
    )

    assert result.status == "failed"
    assert result.error_type == "RuntimeError"
    assert "no such column" in result.error_message


def test_an_unknown_executor_is_a_failed_result_naming_it():
    result = execute_action(
        build_action(executor="no_such_executor"),
        b"",
        context=installation_context(spark=FakeSpark()),
    )

    assert result.status == "failed"
    assert "no_such_executor" in result.error_message


def test_an_action_is_timed_even_when_it_fails():
    """The report's durations must cover failures too, or a slow failure hides."""

    result = execute_action(
        build_action(executor="missing"), b"", context=installation_context()
    )

    assert result.started_at is not None
    assert result.finished_at is not None
    assert result.duration_seconds >= 0


def test_a_skipped_execution_reports_skipped_with_its_details():
    """Not every action does work — an endpoint refresh on a host without one."""

    from weaver.build_bundle.executors.base import SkippedExecution

    class Skipping:
        name = "skipping"

        def execute(self, action, payload, context):
            return SkippedExecution(details={"reason": "unsupported host"})

    result = execute_action(
        build_action(executor="skipping"),
        None,
        context=installation_context(),
        executors={"skipping": Skipping()},
    )

    assert result.status == "skipped"
    assert result.details == {"reason": "unsupported host"}


def test_supplied_executors_replace_the_registry_entirely():
    """A test naming its own executors must not silently inherit the real ones."""

    result = execute_action(
        build_action(executor="spark_sql"),
        VIEW_SQL,
        context=installation_context(spark=FakeSpark()),
        executors={},
    )

    assert result.status == "failed"
    assert "spark_sql" in result.error_message


def test_the_default_registry_is_used_when_none_is_named():
    assert "spark_sql" in default_executors()

    result = execute_action(
        build_action(payload="p.sql"),
        VIEW_SQL,
        context=installation_context(spark=FakeSpark()),
    )

    assert result.status == "succeeded"


# --- what actually reaches the engine -----------------------------------------


def test_a_spark_statement_is_resolved_against_the_batchs_destination():
    """The difference between a build that works and one that looks like it does.

    A two-part name resolves through whatever the session is attached to — the
    Weaver Lakehouse — so an unresolved statement would create the object in the
    control plane and then read it back from there, and pass. The token must be
    gone, and gone in favour of *this batch's* destination.
    """

    spark = FakeSpark()

    execute_action(
        build_action(payload="p.sql"),
        VIEW_SQL,
        context=installation_context(spark=spark),
    )

    (statement,) = spark.statements
    assert "{{object:" not in statement
    assert "sales_lh__DWG" in statement


def test_a_spark_action_without_a_session_fails_saying_so():
    result = execute_action(
        build_action(payload="p.sql"), VIEW_SQL, context=installation_context()
    )

    assert result.status == "failed"
    assert "Spark session" in result.error_message


def test_a_spark_action_with_no_destination_refuses_rather_than_guessing():
    """An action with nowhere to go must stop, not land somewhere plausible."""

    result = execute_action(
        build_action(payload="p.sql"),
        VIEW_SQL,
        context=installation_context(
            spark=FakeSpark(), target=resolved_target(destination=None)
        ),
    )

    assert result.status == "failed"
    assert "no Spark destination" in result.error_message


def test_a_spark_action_without_a_payload_fails_saying_so():
    result = execute_action(
        build_action(payload=None),
        None,
        context=installation_context(spark=FakeSpark()),
    )

    assert result.status == "failed"
    assert "no payload" in result.error_message


# --- the Warehouse side -------------------------------------------------------


def test_tsql_sends_the_script_through_unchanged():
    """The executor adds no logic: the generated script is what the engine gets."""

    sql = FakeSql()
    script = b"CREATE TABLE [DWG].[Customer] ([CustomerId] int NOT NULL);\n"

    result = execute_action(
        build_action(executor="tsql", payload="p.sql"),
        script,
        context=warehouse_context(sql=sql),
    )

    assert result.status == "succeeded"
    assert sql.scripts == [script.decode("utf-8")]


def test_a_tsql_action_without_a_sql_executor_fails_saying_so():
    result = execute_action(
        build_action(executor="tsql", payload="p.sql"),
        b"SELECT 1",
        context=installation_context(sql=None),
    )

    assert result.status == "failed"
    assert "SQL executor" in result.error_message


def test_a_failing_warehouse_script_is_recorded_as_a_failed_action():
    sql = FakeSql(error=RuntimeError("Invalid column name 'NoSuchColumn'"))

    result = execute_action(
        build_action(executor="tsql", payload="p.sql"),
        b"CREATE VIEW x AS SELECT NoSuchColumn FROM y",
        context=warehouse_context(sql=sql),
    )

    assert result.status == "failed"
    assert "NoSuchColumn" in result.error_message


def test_a_tsql_batch_submits_each_statement_separately():
    """Not cosmetic: T-SQL rejects two CREATE VIEWs in one batch outright."""

    sql = FakeSql()
    payload = json.dumps(
        ["CREATE OR ALTER VIEW a AS SELECT 1", "CREATE OR ALTER VIEW b AS SELECT 2"]
    ).encode("utf-8")

    result = execute_action(
        build_action(executor="tsql_batch", payload="p.json"),
        payload,
        context=warehouse_context(sql=sql),
    )

    assert result.status == "succeeded"
    assert sql.scripts == [
        "CREATE OR ALTER VIEW a AS SELECT 1",
        "CREATE OR ALTER VIEW b AS SELECT 2",
    ]


def test_a_tsql_batch_payload_that_is_not_an_array_is_rejected():
    result = execute_action(
        build_action(executor="tsql_batch", payload="p.json"),
        b'"CREATE VIEW a AS SELECT 1"',
        context=warehouse_context(),
    )

    assert result.status == "failed"
    assert "array of statements" in result.error_message
