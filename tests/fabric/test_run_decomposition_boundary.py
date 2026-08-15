"""A desktop-driven run against a real estate, and the scope it holds open.

This replaces the transport test that used to submit one program calling
``weaver.load`` on the far side. There is no such program any more: the desktop
reads the estate, builds the graph here, and dispatches each node to whatever
can run it. So what is worth proving in Fabric changed with it.

Three claims, and none of them can be made anywhere else:

.. code-block:: text

    the decomposed path runs at all      against a real installed estate
    one scope serves every Python node   open_scope once, dispatch many
    the scope is closed at the end       close_scope once, whatever happened

The middle one is the guarantee the decomposition most had to preserve. A run
that opened a scope per node would still pass every local test — the nodes would
import their modules and load their tables — and would quietly lose the sharing
that lets two objects of one item see each other's ``lib/`` helpers.

It is read off the desktop's own telemetry rather than by asking Fabric what it
holds, because the counts are exactly the claim: one begin, one end, and more
dispatches than either.

``hosted``, because the primitives run as the installed wheel: the scope registry
and the entry points a submission calls are :mod:`weaver.runtime.session_scopes`
and :mod:`weaver.run.entry` inside the Fabric session. The orchestration is here;
what is imported over there is the published package.
"""

from __future__ import annotations

import pytest
from support.build_envs import LAKEHOUSE_JOURNEY_FIXTURE

from weaver.load_report import LoadRunReport

pytestmark = [
    pytest.mark.fabric,
    pytest.mark.hosted,
    pytest.mark.parametrize(
        "weaver_repo_fixture", [LAKEHOUSE_JOURNEY_FIXTURE], indirect=True
    ),
]


@pytest.fixture(scope="module")
def loaded(fabric_lakehouse_estate, weaver_session):
    """One desktop-driven load of the installed estate, and what it spent.

    Module-scoped: one run answers every question below, and running it again
    would buy nothing and cost a Livy round trip per node.
    """

    import weaver

    env = fabric_lakehouse_estate.env
    before = _counts(weaver_session)
    # No workspace argument: the Session carries this env's context, which is
    # the same object it was opened with.
    report = weaver.load(
        [f"Lakehouse/{env.target.name}"],
        session=weaver_session,
    )
    return report, _spent(before, _counts(weaver_session))


def _counts(session) -> dict:
    return {name: measure.calls for name, measure in session.telemetry.measures.items()}


def _spent(before: dict, after: dict) -> dict:
    """What this run alone submitted, since the Session outlives it."""

    return {
        name: calls - before.get(name, 0)
        for name, calls in after.items()
        if calls - before.get(name, 0) > 0
    }


def test_a_desktop_runs_the_estate_it_did_not_have_to_ship(loaded):
    """The decomposed path, end to end, against a real installed estate."""

    report, _ = loaded

    assert isinstance(report, LoadRunReport)
    assert report.succeeded, report.messages
    assert report.nodes


def test_every_python_node_shared_one_runtime_scope(loaded):
    """One scope per logical run, however many nodes it dispatched.

    A run that opened a scope per node would still load every table, and would
    silently stop two objects of one item sharing the ``lib/`` helpers their
    author wrote them against.
    """

    _, spent = loaded

    assert spent.get("livy.open_scope") == 1
    assert spent.get("livy.run_python_primitive", 0) > 1, (
        "this estate should have more than one Python node, or the claim is vacuous"
    )


def test_the_scope_is_closed_when_the_run_ends(loaded):
    """A scope left open is one the next run would inherit — and with it, the
    modules a rebuild has since replaced."""

    _, spent = loaded

    assert spent.get("livy.close_scope") == 1


def test_the_run_read_the_estate_rather_than_shipping_itself(loaded):
    """What the decomposition removed. The estate is read; the run is not shipped.

    The read is Spark SQL now rather than a program, so what says it happened is
    the statements — and what says the run stayed here is that no program named
    a load.
    """

    _, spent = loaded

    assert spent.get("livy.spark_sql", 0) > 1
    assert "livy.load" not in spent
    assert "livy.read_catalogue" not in spent


def test_every_node_reports_where_it_was_dispatched(loaded):
    """Resolution happened here, against the estate that was read across."""

    report, _ = loaded

    assert all(node.dispatch_location for node in report.nodes)
