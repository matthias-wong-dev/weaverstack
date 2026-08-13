"""The ``spark_table`` executor, run from a desktop against a real Lakehouse.

The whole action runs here. Two statements cross — the setup with its
``DESCRIBE QUERY``, then the rendered ``CREATE TABLE`` — and everything between
them is decided in this process: column validation, the physical columns, the
DDL. Nothing imports Weaver on the far side, which is why this is ``remote``.

``DESCRIBE QUERY`` replaced reading a ``DataFrame``'s schema, and the swap is
only safe if Fabric answers it the same way. The cases are shared with
``tests/spark/boundary/test_spark_table_shape_boundary.py`` so the emulator and a
real Lakehouse are held to one set of expectations:

* every structural type a document can declare survives to the built table;
* a query reading a temporary view its own setup registered resolves, which it
  can only do if they travelled in one submission;
* a table declared ``CustomerEnriched`` is created under that spelling and is
  read by the next action in the same installation;
* a query naming a column that is not there fails as an ``InstallError`` naming
  the action and carrying Spark's message;
* the built table holds the query's columns and Weaver's audit columns, and
  nothing else.

The estate is built once and observed once, at the end.
"""

from __future__ import annotations

import pytest
from support import spark_table_cases as cases
from support.observation import observe_in_session

from weaver.errors import InstallError

pytestmark = [pytest.mark.fabric, pytest.mark.remote]


@pytest.fixture(scope="module")
def spark_table_estate(
    fabric_workspace, fabric_client, fabric_target_lakehouse, weaver_session,
    livy_session,
):
    """Every case installed from here, and one observation of what they left."""

    from dataclasses import dataclass
    from typing import Any

    from factories import bound_target
    from weaver.build_bundle import execute_install_action
    from weaver.build_bundle.executors.base import InstallationContext, ResolvedTarget
    from weaver.build_bundle.installer import Installer
    from weaver.fabric import FabricResolver, OneLakeDfsClient
    from weaver.targets import ItemRef

    resolver = FabricResolver(fabric_workspace, client=fabric_client)
    item = ItemRef(fabric_target_lakehouse.name)
    destination = resolver.spark_destination(item)

    # Isolation by emptying, as everything against a fixed item is: a schema left
    # by an earlier run would make every strict create collide.
    livy_session.run(
        f"spark.sql('DROP SCHEMA IF EXISTS {destination.qualified_schema(cases.SCHEMA)} CASCADE')\n"
        "emit({'cleared': True})\n"
    )

    installer = Installer(weaver_session, workspace=fabric_workspace)
    target = ResolvedTarget(
        bound=bound_target(id="target-1", item_id=fabric_target_lakehouse.name),
        lakehouse=item,
        location=resolver.lakehouse_spark_location(item),
        destination=destination,
    )
    context = InstallationContext(
        # From the Installer, as every production context gets them. The executor
        # stays here and only its statements cross.
        spark_sql=installer.spark_sql(),
        spark_sql_batch=installer.spark_sql_batch(),
        resolver=resolver,
        store=OneLakeDfsClient(),
        target=target,
        targets={target.bound.id: target},
    )

    def run(action, payload):
        return execute_install_action(action, payload, context=context)

    results = {"schema": run(cases.schema_action(), cases.SCHEMA_PAYLOAD)}
    for case in cases.BUILDING:
        results[case.name] = run(cases.install_action(case), case.payload)
    results[cases.EXACT_CASE_READER] = run(
        cases.view_action(), cases.EXACT_CASE_VIEW_SQL
    )
    unresolved = run(
        cases.install_action(cases.UNRESOLVED), cases.UNRESOLVED.payload
    )

    built = {
        action_id: result.error_message
        for action_id, result in results.items()
        if result.status == "failed"
    }
    assert not built, built

    @dataclass(frozen=True)
    class Estate:
        results: dict
        unresolved: Any
        observation: Any

    return Estate(
        results=results,
        unresolved=unresolved,
        # One payload, one moment. Everything asserted below is read from it.
        observation=observe_in_session(
            livy_session,
            queries=cases.describe_queries(destination),
            label="observe spark_table",
        ),
    )


@pytest.mark.parametrize("case", cases.BUILDING, ids=lambda case: case.name)
def test_the_built_table_is_the_shape_its_query_declares(case, spark_table_estate):
    cases.assert_case_built(case, spark_table_estate.observation[case.name])


def test_the_exact_case_table_is_readable_by_the_next_action(spark_table_estate):
    """Fabric folds a table identifier to lower case unless analysis is exact.

    The view was built by the next action in the same installation, so a folded
    table would have failed that action rather than this assertion — which is
    where the failure belongs.
    """

    assert spark_table_estate.results[cases.EXACT_CASE_READER].status == "succeeded"
    assert spark_table_estate.observation[cases.EXACT_CASE_READER] == []


def test_a_query_that_does_not_resolve_fails_naming_the_action(spark_table_estate):
    """The analysis failure moved from running the query to describing it."""

    result = spark_table_estate.unresolved

    assert result.status == "failed"
    assert result.error_type == InstallError.__name__
    assert cases.UNRESOLVED.name in result.error_message
    # Spark's own diagnosis survives the crossing, which is the part that says
    # what was actually wrong.
    assert "NoSuchColumn" in result.error_message
