"""Build, Wipe and Mirror of the 1,000-object estates finish within their ceilings.

Opt in with ``--performance``. Each run empties the performance items, builds
the representative estate, builds it again unchanged, mirrors it and wipes it,
timing each from the caller. A failure prints where Build time went by executor.

The run happens in a module fixture, so the claims below read its timings and
cross no resource themselves.
"""

from __future__ import annotations

import pytest
from support.fabric_performance import LAKEHOUSE, THRESHOLDS, WAREHOUSE, run_estate
from support.weaver_test import register_session, weaver_test

DECLARATIONS = 1_000


@pytest.fixture(scope="module")
def warehouse_estate(fabric_workspace, warehouse_session):
    register_session(warehouse_session)
    return run_estate(
        WAREHOUSE,
        DECLARATIONS,
        session=warehouse_session,
        workspace_name=fabric_workspace.workspace,
        environment=str(fabric_workspace.environment),
    )


@pytest.fixture(scope="module")
def lakehouse_estate(fabric_workspace, weaver_session):
    register_session(weaver_session)
    return run_estate(
        LAKEHOUSE,
        DECLARATIONS,
        session=weaver_session,
        workspace_name=fabric_workspace.workspace,
        environment=str(fabric_workspace.environment),
    )


def _within(run, operation):
    timing = next(t for t in run.timings if t.operation == operation)
    assert timing.succeeded, run.describe()
    assert timing.seconds < THRESHOLDS[run.engine][operation], run.describe()


@pytest.mark.performance
@pytest.mark.parametrize("operation", ["build", "wipe", "mirror"])
@weaver_test(integration=True)
def test_the_warehouse_estate_is_within_its_ceiling(warehouse_estate, operation):
    _within(warehouse_estate, operation)


@pytest.mark.performance
@weaver_test(integration=True)
def test_an_unchanged_warehouse_estate_builds_nothing_quickly(warehouse_estate):
    _within(warehouse_estate, "noop")
    timing = next(t for t in warehouse_estate.timings if t.operation == "noop")
    assert timing.detail["actions"] == 0, warehouse_estate.describe()


@pytest.mark.performance
@pytest.mark.parametrize("operation", ["build", "wipe", "mirror"])
@weaver_test(integration=True)
def test_the_lakehouse_estate_is_within_its_ceiling(lakehouse_estate, operation):
    _within(lakehouse_estate, operation)


@pytest.mark.performance
@weaver_test(integration=True)
def test_an_unchanged_lakehouse_estate_builds_nothing_quickly(lakehouse_estate):
    _within(lakehouse_estate, "noop")
    timing = next(t for t in lakehouse_estate.timings if t.operation == "noop")
    assert timing.detail["actions"] == 0, lakehouse_estate.describe()
