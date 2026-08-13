"""A desktop reads the state a build plans against without the published wheel.

Reading the estate used to submit a program that imported Weaver in the Fabric
session and returned a whole ``BuildState``. It is now Spark SQL and storage: the
catalogue is a ``SELECT`` per table in the Weaver Lakehouse, a Lakehouse
inventory is directories over OneLake plus ``SHOW VIEWS``, and a Warehouse
inventory is T-SQL. Nothing on the far side imports Weaver.

What proves it is what is submitted. Every statement the read sends is captured
and inspected: an ``import weaver`` anywhere in them would mean the published
package was still load-bearing, whatever the session happened to have installed.

Load's Python primitives are a different matter and still need the wheel — a
deployed module is imported where Spark is. What is asserted here is the state a
*build* reads before it plans.
"""

from __future__ import annotations

import pytest
from factories import item_bindings

from weaver.build_bundle.workflow import read_build_state
from weaver.targets import ItemRef

pytestmark = [pytest.mark.fabric, pytest.mark.remote]


@pytest.fixture(scope="module")
def recorded_session(weaver_session):
    """The suite's Session, with every Spark statement it runs captured.

    The harness's session rather than one of its own: a capacity commonly
    permits a single Livy session, and what this asserts is what the *read*
    submits, not what the session was started with.
    """

    submitted: list[str] = []
    ran = weaver_session.execute_spark_sql_batch

    def recording(statements, **kwargs):
        submitted.extend(statements)
        return ran(statements, **kwargs)

    weaver_session.execute_spark_sql_batch = recording
    weaver_session.submitted = submitted
    try:
        yield weaver_session
    finally:
        del weaver_session.execute_spark_sql_batch


def test_build_state_is_read_without_importing_weaver_in_fabric(
    recorded_session, fabric_workspace, fabric_target_lakehouse
):
    """The acceptance condition: state read from a desktop, planning-ready."""

    bindings = item_bindings(("Lakehouse/Sales", fabric_target_lakehouse.name))
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
    assert inventory.target_name == fabric_target_lakehouse.name
    assert inventory.kind == "lakehouse"


def test_the_statements_it_submitted_import_nothing(recorded_session):
    """Guards the way this could pass for the wrong reason.

    A read that had quietly stopped happening would also import nothing, so the
    statements are counted as well as inspected.
    """

    submitted = recorded_session.submitted

    assert submitted, "nothing was submitted, so the read never happened"
    assert not any("import weaver" in statement for statement in submitted), submitted
    # Statements rather than programs, and they reached both Lakehouses the read
    # is about: the control plane for the catalogue, the target for its views.
    assert all(
        statement.split()[0] in {"SELECT", "SHOW", "DESCRIBE"}
        for statement in submitted
    ), submitted


def test_a_lakehouse_inventory_lists_views_over_spark_sql(
    recorded_session, fabric_workspace, fabric_target_lakehouse
):
    """The one part of an inventory that is not storage.

    A view exists only in the catalogue, so it is the piece that has to be asked
    of Spark — and asking it is a statement rather than a program.
    """

    from weaver.build_bundle.workflow import session_catalogue

    catalogue = session_catalogue(
        recorded_session, fabric_workspace, ItemRef(fabric_target_lakehouse.name)
    )

    # A schema that is not there holds no views, which is an answer rather than
    # a failure — and it is the answer a first build depends on.
    assert catalogue.views("NoSuchSchemaHere") == ()
    assert catalogue.schema_exists("NoSuchSchemaHere") is False
