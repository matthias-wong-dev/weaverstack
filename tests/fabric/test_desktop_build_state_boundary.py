"""A desktop reads the state a build plans against without the published wheel.

Reading the estate used to submit a program that imported Weaver in the Fabric
session and returned a whole ``BuildState``. It is now Spark SQL and storage: the
catalogue is a ``SELECT`` per table in the Weaver Lakehouse, a Lakehouse
inventory is directories over OneLake plus ``SHOW VIEWS``, and a Warehouse
inventory is T-SQL. Nothing on the far side imports Weaver.

That is what this proves, and it proves it by removing the wheel from the
question: the Livy session is started with ``require_weaver=False``, so the
Environment's package is never asserted and a body that tried to import it would
have nothing to rely on.

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
def wheel_free_session(fabric_workspace, fabric_client):
    """A Session whose Livy session never asserted a published Weaver.

    Its own session rather than the harness's, because the harness's is started
    with the wheel required — which is the thing this test exists to do without.
    """

    from weaver.fabric import LivySession
    from weaver.session import ConsoleSession

    livy = LivySession.for_workspace(fabric_workspace, require_weaver=False)
    try:
        livy.start()
    except Exception as exc:  # noqa: BLE001 - a capacity problem is not a result
        pytest.skip(f"could not start a Livy session: {exc}")

    submitted: list[str] = []
    ran = livy.run

    def recording(code, **kwargs):
        submitted.append(code)
        return ran(code, **kwargs)

    livy.run = recording
    try:
        with ConsoleSession(
            workspace=fabric_workspace, livy=livy, require_weaver=False
        ) as session:
            session.submitted = submitted
            yield session
    finally:
        livy.close()


def test_build_state_is_read_without_importing_weaver_in_fabric(
    wheel_free_session, fabric_workspace, fabric_target_lakehouse
):
    """The acceptance condition: state, from a desktop, with no wheel in play."""

    bindings = item_bindings(("Lakehouse/Sales", fabric_target_lakehouse.name))
    state = read_build_state(
        bindings,
        required_catalogue_items=(),
        session=wheel_free_session,
        workspace=fabric_workspace,
    )

    # A catalogue that read cleanly, whatever it holds — an empty workspace and
    # a populated one are both valid answers, and neither could be reached at
    # all if the read still needed the published package.
    assert state.catalogue is not None
    inventory = state.target_inventories[bindings.entries[0].item]
    assert inventory.target_name == fabric_target_lakehouse.name
    assert inventory.kind == "lakehouse"


def test_the_statements_it_submitted_import_nothing(wheel_free_session):
    """Guards the way this could pass for the wrong reason.

    A read that had quietly stopped happening would also import nothing, so the
    submissions are counted as well as inspected.
    """

    submitted = wheel_free_session.submitted

    assert submitted, "nothing was submitted, so the read never happened"
    assert not any("import weaver" in source for source in submitted), submitted


def test_a_lakehouse_inventory_lists_views_over_spark_sql(
    wheel_free_session, fabric_workspace, fabric_target_lakehouse
):
    """The one part of an inventory that is not storage.

    A view exists only in the catalogue, so it is the piece that has to be asked
    of Spark — and asking it is a statement rather than a program.
    """

    from weaver.build_bundle.workflow import session_catalogue

    catalogue = session_catalogue(
        wheel_free_session, fabric_workspace, ItemRef(fabric_target_lakehouse.name)
    )

    # A schema that is not there holds no views, which is an answer rather than
    # a failure — and it is the answer a first build depends on.
    assert catalogue.views("NoSuchSchemaHere") == ()
    assert catalogue.schema_exists("NoSuchSchemaHere") is False
