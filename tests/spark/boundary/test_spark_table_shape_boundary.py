"""What ``DESCRIBE QUERY`` says a table's shape is, against real Delta.

The executor used to run its query and read the resulting ``DataFrame``'s schema.
It now asks ``DESCRIBE QUERY`` instead, which is what lets the whole action run
wherever the Installer does: two statements cross, and everything between them —
validation, physical columns, the rendered ``CREATE TABLE`` — is decided here.

That swap is only safe if the describe answers exactly what the schema did, so
these run the real executor against a real engine and check the built table. The
cases are shared with ``tests/fabric/test_spark_table_lakehouse_boundary.py``,
which runs the same ones from a desktop against a real Lakehouse.
"""

from __future__ import annotations

import pytest
from support import spark_table_cases as cases

from weaver.build_bundle import execute_install_action
from weaver.errors import InstallError

pytestmark = pytest.mark.spark

TARGET = "Sales_LH"


@pytest.fixture
def built(lakehouses, spark):
    """The schema and every building case, installed through the real executors."""

    from support.spark_table_cases import BUILDING, EXACT_CASE_VIEW_SQL
    from conftest import context_for

    context = context_for(lakehouses, spark, TARGET)
    results = [
        execute_install_action(
            cases.schema_action(), cases.SCHEMA_PAYLOAD, context=context
        )
    ]
    for case in BUILDING:
        results.append(
            execute_install_action(
                cases.install_action(case), case.payload, context=context
            )
        )
    # The view is the next action in the same build, and reading the exact-case
    # table is the whole of what it proves.
    results.append(
        execute_install_action(cases.view_action(), EXACT_CASE_VIEW_SQL, context=context)
    )

    failures = {
        result.action_id: result.error_message
        for result in results
        if result.status == "failed"
    }
    assert not failures, failures
    return context


def _describe(spark, destination, name):
    return [
        row.asDict()
        for row in spark.sql(
            f"DESCRIBE TABLE {destination.qualify(cases.SCHEMA, name)}"
        ).collect()
    ]


@pytest.mark.parametrize("case", cases.BUILDING, ids=lambda case: case.name)
def test_the_built_table_is_the_shape_its_query_declares(
    case, built, lakehouses, spark
):
    destination = lakehouses.resolver.spark_destination(lakehouses.target)

    cases.assert_case_built(case, _describe(spark, destination, case.name))


def test_the_exact_case_table_is_readable_by_the_next_action(
    built, lakehouses, spark
):
    """A table created as ``CustomerEnriched`` has to be findable as that.

    The view above it was built by the next action in the same installation, so
    a table folded to a different spelling would have failed that action rather
    than this assertion — which is the failure this arrangement exists to keep
    at the point it happens.
    """

    destination = lakehouses.resolver.spark_destination(lakehouses.target)
    view = destination.qualify(cases.SCHEMA, cases.EXACT_CASE_READER)

    assert spark.sql(f"SELECT * FROM {view}").collect() == []


def test_a_query_that_does_not_resolve_fails_naming_the_action(
    built, lakehouses, spark
):
    """The analysis failure moved from running the query to describing it."""

    result = execute_install_action(
        cases.install_action(cases.UNRESOLVED),
        cases.UNRESOLVED.payload,
        context=built,
    )

    assert result.status == "failed"
    assert result.error_type == InstallError.__name__
    assert cases.UNRESOLVED.name in result.error_message
    # Spark's own diagnosis survives, which is the part that says what was wrong.
    assert "NoSuchColumn" in result.error_message
