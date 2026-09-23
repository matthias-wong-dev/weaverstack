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
def seeded_target(
    fabric_empty_lakehouse,
    fabric_target_lakehouse,
    fabric_workspace,
    fabric_client,
    livy_session,
):
    """The target Lakehouse holding one real schema and table, seeded before the claim.

    A schema is what makes the read ask Spark for views, so the statements
    inspected below exist because of the estate rather than by luck.
    """

    from weaver.fabric import FabricResolver

    fabric_empty_lakehouse(fabric_target_lakehouse.name)
    destination = FabricResolver(
        fabric_workspace, client=fabric_client
    ).spark_destination(ItemRef(fabric_target_lakehouse.name))
    livy_session.run(
        f"spark.sql('CREATE SCHEMA IF NOT EXISTS {destination.qualified_schema('Sales')}')\n"
        f"spark.sql('CREATE TABLE IF NOT EXISTS {destination.qualify('Sales', 'Customer')} "
        "(Id string) USING delta')\n"
        "emit(True)\n",
    )
    return fabric_target_lakehouse


@weaver_test(remote=True, resources={"livy", "onelake", "rest", "tds"})
def test_build_state_is_read_without_importing_weaver_in_fabric(
    recorded_session, fabric_workspace, seeded_target
):
    """State read from a desktop is planning-ready and sends Spark statements only.

    The catalogue is read over TDS, so no statement sent to Spark names it.
    """

    bindings = item_bindings(
        ("Lakehouse/Sales", seeded_target.name),
        workspace_name=fabric_workspace.workspace,
    )
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
    assert inventory.target_name == seeded_target.name
    assert inventory.kind == "lakehouse"
    assert "Sales" in inventory.schemas, inventory

    submitted = recorded_session.submitted
    assert any(statement.startswith("SHOW VIEWS") for statement in submitted), submitted
    assert not any("import weaver" in statement for statement in submitted), submitted
    assert all(
        statement.split()[0] in {"SELECT", "SHOW", "DESCRIBE"}
        for statement in submitted
    ), submitted
    assert not any(REGISTRY.name in statement for statement in submitted), submitted


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
            items=[f"Warehouse/Reporting={warehouse}"],
            session=session,
        )

    assert built.status == "succeeded", [
        (failure.action_id, failure.message) for failure in built.errors
    ]
