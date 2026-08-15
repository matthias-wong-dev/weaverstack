"""The whole lifecycle, driven from the desktop through one Session.

Every part of the desktop position is proven on its own elsewhere: the state a
build plans against is read over Spark SQL and storage
(`test_desktop_build_state_boundary.py`), a run dispatches each primitive into
the Fabric session (`test_run_dispatch_boundary.py`), a validation does the same
(`test_validation_dispatch_boundary.py`), and one Session serves several
commands (`test_session_reuse_cycle.py`).

What none of them says is that the four operations *compose* — that a build
leaves an estate a load can run, that a load leaves rows a test can reconcile,
and that one Session carries all of it. That is what a user does, and it is what
this asserts, in the order it happens, against a real workspace.

The estate is the cross-item journey's, under logical names of its own. The
catalogue is keyed by logical item, so sharing names with the in-Fabric journey
would make each estate look to the catalogue like a rebuild of the other.

The Session is the suite's. A capacity commonly permits one Livy session, so a
journey that opened its own would queue behind the fixture's; what is asserted
about the Session is that one of them serves the whole sequence, which is true
of a borrowed one.
"""

from __future__ import annotations

import pytest
from support.build_envs import CROSS_ITEM_JOURNEY_FIXTURE, DESKTOP_JOURNEY_NAMES

import weaver

pytestmark = [pytest.mark.fabric, pytest.mark.hosted, pytest.mark.full_integration]


@pytest.fixture(scope="module")
def desktop_estate(tmp_path_factory):
    """The journey's documents, under this journey's own item names."""

    return CROSS_ITEM_JOURNEY_FIXTURE.renamed(
        tmp_path_factory.mktemp("desktop-journey"), DESKTOP_JOURNEY_NAMES
    )


def test_the_desktop_drives_build_load_and_test_in_one_session(
    desktop_estate,
    weaver_session,
    fabric_workspace,
    fabric_target_lakehouse,
    disposable_warehouse,
):
    """Four operations, in order, each asserted where it happened.

    One test rather than four sharing a fixture: the sequence mutates a live
    estate, so a later operation can repair what an earlier one broke and an
    assertion read afterwards would be about a different estate than the one it
    names.
    """

    lakehouse = f"Lakehouse/{fabric_target_lakehouse.name}"
    warehouse = f"Warehouse/{disposable_warehouse.item.name}"

    # From empty, so the build's own certification decides everything after it.
    # The catalogue stays: it is the run's control plane, shared with every other
    # module, and this journey's claims on it go with `unbind_from`.
    weaver.wipe(
        [lakehouse, warehouse],
        unbind_from=fabric_workspace.catalogue,
        session=weaver_session,
    )

    built = weaver.build(
        str(desktop_estate.path),
        bind=[f"{lakehouse}=Stock", f"{warehouse}=Analysis"],
        session=weaver_session,
    )
    assert built.status == "succeeded", [
        (failure.action_id, failure.message) for failure in built.errors
    ]

    loaded = weaver.load([lakehouse, warehouse], session=weaver_session)
    assert loaded.succeeded, loaded.to_mapping()

    tested = weaver.test([lakehouse, warehouse], session=weaver_session)
    totals = tested.totals()
    assert totals["failed"] == 0, tested.to_mapping()
    assert totals["invalid"] == 0, tested.to_mapping()
    # A journey that validated nothing would satisfy the two assertions above.
    assert totals["passed"], tested.to_mapping()
