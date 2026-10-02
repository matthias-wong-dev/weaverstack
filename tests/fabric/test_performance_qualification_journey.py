"""Build, Wipe and Mirror of the representative estates finish within their ceilings.

The ceilings were qualified on an F64 capacity with the default concurrency.

Opt in with ``--performance``. Each estate run empties the performance items,
builds the estate, builds it again unchanged, mirrors it and wipes it, timing
each from the caller. A failure prints where Build time went by executor.

The runs happen in a module fixture, one per estate, so the claims below read
its timings and cross no resource themselves. The estates share the
performance items, so they run one after another.
"""

from __future__ import annotations

import pytest
from support.fabric_performance import LAKEHOUSE, WAREHOUSE, record, run_estate
from support.weaver_test import register_session, weaver_test

ESTATES = (
    (WAREHOUSE, 50),
    (LAKEHOUSE, 50),
    (WAREHOUSE, 1_000),
    (LAKEHOUSE, 1_000),
)


@pytest.fixture(
    scope="module", params=ESTATES, ids=lambda estate: f"{estate[0]}-{estate[1]}"
)
def estate(request, fabric_workspace):
    engine, declarations = request.param
    session = request.getfixturevalue(
        "warehouse_session" if engine == WAREHOUSE else "weaver_session"
    )
    register_session(session)
    run = run_estate(
        engine,
        declarations,
        session=session,
        workspace_name=fabric_workspace.workspace,
        environment=str(fabric_workspace.environment),
    )
    record(run)
    return run


def _within(run, operation):
    timing = next(t for t in run.timings if t.operation == operation)
    assert timing.succeeded, run.describe()
    assert timing.seconds < run.ceiling(operation), run.describe()


@pytest.mark.performance
@pytest.mark.parametrize("operation", ["build", "wipe", "mirror"])
@weaver_test(integration=True)
def test_the_estate_is_within_its_ceiling(estate, operation):
    _within(estate, operation)


@pytest.mark.performance
@weaver_test(integration=True)
def test_an_unchanged_estate_builds_nothing_quickly(estate):
    _within(estate, "noop")
    timing = next(t for t in estate.timings if t.operation == "noop")
    assert timing.detail["actions"] == 0, estate.describe()
