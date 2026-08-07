"""The program the desktop submits when it crosses into Fabric to run a load.

A load runs where the data is. So a desktop asking for a Fabric workspace cannot
run one itself — it reaches into a session, and that crossing is the CLI's, the
same way it is for ``build`` and ``unbind``. Everything else happens once, inside
``weaver.load``, on the far side.

The crossing has three parts, and they are proved in three places because only
one of them is new:

.. code-block:: text

    open a session      LivySession.for_workspace — shared with build and
                        unbind, and proved by both
    submit the program  the new part, and what this file runs in Fabric
    read the answer     reconstruction and the failure envelope, proved
                        against a recording double in
                        tests/test_cli_load_binding.py

**Why this does not open its own session.** A Fabric capacity commonly permits
one concurrent Spark session, and the harness's ``livy_session`` fixture is
session-scoped — it holds that slot for the whole run. A test that opened a
second would have Fabric kill it, and would fail for a reason with nothing to do
with what it tests. So the program is submitted through the session that already
exists: the same program text, executed the same way, by the same session class.

That constraint is the product's too, and worth knowing: ``weaver load`` from a
desktop needs a free session slot, so it cannot run while a notebook or another
Weaver command holds one.

``hosted``, because the program under test runs ``weaver.load`` on the far side
as the installed wheel.
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


def _cli():
    """The command module itself.

    ``weaver_cli.main`` is a function on the package as well as a submodule, and
    the function is what attribute access finds — so the module is asked for by
    name.
    """

    import sys

    import weaver_cli.main  # noqa: F401 - imported for its effect on sys.modules

    return sys.modules["weaver_cli.main"]


@pytest.fixture(scope="module")
def crossed(fabric_lakehouse_estate, livy_session):
    """One desktop-issued dry run of the installed estate, planned in Fabric.

    The desktop's own program text, run in Fabric, and the payload reconstructed
    by the desktop's own code — so what is asserted below is a real
    :class:`~weaver.load_report.LoadRunReport` rather than a mapping the test
    built for itself.

    Module-scoped: one submission answers every question below, and asking again
    would buy nothing and cost a round trip.
    """

    env = fabric_lakehouse_estate.env
    cli = _cli()

    body = cli._LOAD_BODY.format(
        workspace=env.workspace.workspace,
        environment=env.workspace.environment,
        weaver_lakehouse=env.workspace.weaver_lakehouse,
        targets=[f"Lakehouse/{env.target.name}"],
        fault_tolerant=False,
        dry_run=True,
    )
    payload = livy_session.run(body, label="the desktop's load program").payload

    assert payload is not None, "the submitted program emitted nothing"
    assert not payload.get("failed"), payload
    return LoadRunReport.from_mapping(payload["report"])


def test_the_submitted_program_runs_and_answers(crossed):
    """The part that is genuinely new: a template built by string formatting.

    A typo in it would be invisible to every local test and would ship a
    ``weaver load`` that could not reach Fabric at all.
    """

    assert isinstance(crossed, LoadRunReport)
    assert crossed.dry_run is True
    assert crossed.status == "succeeded", crossed.messages


def test_the_desktop_reconstructs_a_real_report_rather_than_a_mapping(crossed):
    """The renderer is shared with a local run, so anything less than the same
    object would make the CLI the second place that knows what a report means."""

    assert crossed.by_node
    assert all(node.node_id for node in crossed.nodes)
    assert isinstance(crossed.order, tuple)


def test_the_plan_that_crossed_is_the_estate_that_is_installed(crossed):
    """The graph was built in Fabric, from the catalogue, and survived the trip."""

    names = [node.node_id.rsplit("/", 1)[-1] for node in crossed.nodes]

    assert "Raw.CustomerCsv" in names
    assert "DWG.Customer" in names
    # The SQL-authored table takes its place like any other deployed module.
    assert "DWG.NamedCustomer" in names
    assert list(crossed.order) == [node.node_id for node in crossed.nodes]


def test_every_node_crossed_with_its_resolved_dispatch_location(crossed):
    """Resolution happened on the far side, where the estate is."""

    assert all(node.dispatch_location for node in crossed.nodes)
    assert all(node.status == "validated" for node in crossed.nodes)


def test_a_dry_run_wrote_no_evidence_on_either_side(crossed):
    """Validation is not execution, wherever it ran."""

    assert crossed.task_log is None
    assert crossed.task_id is None
