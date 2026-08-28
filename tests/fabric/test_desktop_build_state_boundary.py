"""A desktop reads the state a build plans against without the published wheel.

The state read uses Spark SQL and storage: the catalogue is a ``SELECT`` per
table over TDS, a Lakehouse
inventory is directories over OneLake plus ``SHOW VIEWS``, and a Warehouse
inventory is T-SQL. Nothing on the far side imports Weaver.

What proves it is what is submitted. Every statement the read sends is captured
and inspected: an ``import weaver`` anywhere in them would mean the published
package was still load-bearing, whatever the session happened to have installed.

Load's Python primitives are a different matter and still need the wheel, a
deployed module is imported where Spark is. What is asserted here is the state a
build reads before it plans.

The same fact stated as a requirement: a build names no Fabric Environment. An
Environment is what carries the published Weaver, and a build imports it
nowhere, so the last test here runs one against a workspace that has none.
"""

from __future__ import annotations

import pytest
from factories import item_bindings
from support.weaver_test import weaver_test

from weaver.build_bundle.workflow import read_build_state
from weaver.catalogue.tables import REGISTRY
from weaver.targets import ItemRef


@pytest.fixture(scope="module")
def recorded_session(fabric_workspace, livy_session):
    """An isolated resolver/cache over the suite's one shared Livy session."""

    from weaver.sessions import ConsoleSession

    session = ConsoleSession(workspace=fabric_workspace, livy=livy_session)
    submitted: list[str] = []
    ran = session.execute_spark_sql_batch

    def recording(statements, **kwargs):
        submitted.extend(statements)
        return ran(statements, **kwargs)

    session.execute_spark_sql_batch = recording
    session.submitted = submitted
    with session:
        yield session


@pytest.fixture
def emptied_target(fabric_empty_lakehouse, fabric_target_lakehouse):
    """The target Lakehouse holding no schemas, emptied before the claim begins.

    The read asks Spark for a Lakehouse's views once per schema storage
    discovered, so how much Spark it uses is a fact about the estate. Emptiness
    is this test's premise rather than its luck, and it is established here so
    the Livy the emptying needs is fixture acquisition rather than part of the
    claim.
    """

    fabric_empty_lakehouse(fabric_target_lakehouse.name)
    return fabric_target_lakehouse


@weaver_test(remote=True, resources={"onelake", "rest", "tds"})
def test_build_state_is_read_without_importing_weaver_in_fabric(
    recorded_session, fabric_workspace, emptied_target
):
    """The acceptance condition: state read from a desktop, planning-ready.

    Spark is not declared, because a target with no schemas has no views to ask
    about. That the views come over Spark is
    `test_a_lakehouse_inventory_lists_views_over_spark_sql`, which asks for a
    named schema and so does not depend on what is there.
    """

    bindings = item_bindings(("Lakehouse/Sales", emptied_target.name))
    state = read_build_state(
        bindings,
        required_catalogue_items=(),
        session=recorded_session,
        workspace=fabric_workspace,
    )

    # A catalogue that read cleanly, whatever it holds: an empty workspace and a
    # populated one are both valid answers.
    assert state.catalogue is not None
    inventory = state.target_inventories[bindings.entries[0].item]
    assert inventory.target_name == emptied_target.name
    assert inventory.kind == "lakehouse"


@weaver_test(remote=True)
def test_the_statements_it_submitted_import_nothing(recorded_session):
    """Whatever crossed to Spark is a statement, never a program.

    Nothing is counted here. The catalogue is a Warehouse now, so the read's
    Spark traffic is a Lakehouse's views alone, and a target with no schemas
    has none to list, which is a fact about the estate rather than about the
    read. That the read happened at all is
    `test_build_state_is_read_without_importing_weaver_in_fabric`; that views
    come over Spark is the test below.
    """

    submitted = recorded_session.submitted

    assert not any("import weaver" in statement for statement in submitted), submitted
    assert all(
        statement.split()[0] in {"SELECT", "SHOW", "DESCRIBE"}
        for statement in submitted
    ), submitted


@weaver_test(remote=True, resources={"tds"})
def test_the_catalogue_is_read_over_tds_and_not_over_spark(
    recorded_session, fabric_workspace
):
    """A Warehouse catalogue read appears in TDS telemetry, never Spark."""

    from weaver.catalogue.connection import catalogue_connection

    def tds_queries() -> int:
        measure = recorded_session.telemetry.measures.get("tds.query")
        return measure.calls if measure is not None else 0

    before = tds_queries()
    catalogue_connection(recorded_session, fabric_workspace).columns_of(REGISTRY)

    assert tds_queries() > before
    assert not any(
        "Registry" in statement for statement in recorded_session.submitted
    ), recorded_session.submitted


@weaver_test(remote=True, resources={"livy"})
def test_a_lakehouse_inventory_lists_views_over_spark_sql(
    recorded_session, fabric_workspace, fabric_target_lakehouse
):
    """The one part of an inventory that is not storage.

    A view exists only in the catalogue, so it is the piece that has to be asked
    of Spark, and asking it is a statement rather than a program.
    """

    from weaver.build_bundle.workflow import session_catalogue

    catalogue = session_catalogue(
        recorded_session, fabric_workspace, ItemRef(fabric_target_lakehouse.name)
    )

    # A schema that is not there holds no views, which is an answer rather than
    # a failure, and it is the answer a first build depends on.
    assert catalogue.views("NoSuchSchemaHere") == ()
    assert catalogue.schema_exists("NoSuchSchemaHere") is False


@weaver_test(remote=True, resources={"rest", "tds"})
def test_a_build_runs_against_a_workspace_naming_no_environment(
    fabric_workspace, clean_disposable_warehouse, tmp_path_factory
):
    """The requirement, made real: a whole build with `environment` unset.

    A Warehouse-only estate, so nothing here even starts Spark, the objects are
    T-SQL and the catalogue they are registered in is a Warehouse. What would
    have failed before is the refusal itself, which came before any Fabric call
    and did not depend on what the build turned out to need.
    """

    from dataclasses import replace

    from support.build_envs import WAREHOUSE_ESTATE_FIXTURE
    from support.weaver_test import register_session

    import weaver
    from weaver.sessions import ConsoleSession

    without_environment = replace(fabric_workspace, environment=None)
    assert without_environment.environment is None

    estate = WAREHOUSE_ESTATE_FIXTURE.disposable(tmp_path_factory.mktemp("no-env"))
    warehouse = f"Warehouse/{clean_disposable_warehouse.item.name}"

    with ConsoleSession(workspace=without_environment) as session:
        register_session(session)
        built = weaver.build(
            str(estate.path),
            targets=[f"Warehouse/Reporting={warehouse}"],
            session=session,
        )

    assert built.status == "succeeded", [
        (failure.action_id, failure.message) for failure in built.errors
    ]
