"""A run reaches a deployed primitive in Fabric, and settles what comes back.

Every link in the chain is production code:

.. code-block:: text

    Runner → dispatch → Session → Livy → import → a trivial installed artefact

The only thing not real is what the primitive *does*, and that is the point. A
thin run proves the wiring — that the Runner reaches dispatch, that dispatch
resolves the deployed artefact and reaches it through the Session, and that
whatever comes back is settled into the run's own vocabulary. What a primitive
would have done to data is a Primitive's claim, made elsewhere.

Each artefact exists to produce one controlled outcome, chosen because each is
settled by a different rule:

.. code-block:: text

    Success      a result reporting success
    Rejects      rows written and rows refused — tolerated, or not
    Failure      a reported failure, nothing raised
    Raises       an exception the primitive never normalised
    Malformed    something that is not a result at all

**No build, and almost no estate.** A run needs two things to reach a primitive:
a catalogue saying it is installed, and the artefact being where the catalogue
says. Both are arranged directly, which is why paying for a build to make a
primitive callable is unnecessary when the claim is about dispatch.

``hosted``, because the modules are imported where Spark is, as the installed
wheel: :mod:`weaver.runtime.session_scopes` and :mod:`weaver.run.entry` inside
the Fabric session. The orchestration is here; what is imported over there is
the published package.

``Files`` is object storage and a deployed module sits in a package tree, so a
dispatch that resolved against a directory would prove nothing about either.
"""

from __future__ import annotations

import pytest
from support.thin import OUTCOMES, thin_estate

from weaver.errors import LoadError
from weaver.load_report import FAILED, SUCCEEDED
from weaver.operations.load import run_load

pytestmark = [pytest.mark.fabric, pytest.mark.hosted]

#: The Lakehouse the artefacts are deployed into. Emptied first, because a run
#: that found a previous run's modules would prove nothing about this one.
LAKEHOUSE = "PYTEST_LH_1"


@pytest.fixture(scope="module")
def thin(
    fabric_workspace,
    fabric_client,
    weaver_session,
    fabric_empty_lakehouse,
    tmp_path_factory,
):
    """The trivial artefacts, deployed into a real Lakehouse over OneLake.

    The same builder the pure suite uses, given a real workspace's resolver and
    a OneLake store: what changes is where the modules land and who imports
    them, which is exactly the difference this file exists to cover.
    """

    from weaver.fabric import FabricResolver, OneLakeDfsClient

    fabric_empty_lakehouse(LAKEHOUSE)

    estate = thin_estate(
        tmp_path_factory.mktemp("thin"),
        lakehouse=LAKEHOUSE,
        workspace=fabric_workspace,
        resolver=FabricResolver(fabric_workspace, client=fabric_client),
        store=OneLakeDfsClient(),
        session=weaver_session,
    )
    return estate


def _report(thin, *, fault_tolerant=False):
    return run_load(
        thin.session,
        workspace=thin.workspace,
        requested=[thin.target],
        state=thin.state,
        fault_tolerant=fault_tolerant,
    )


@pytest.fixture(scope="module")
def tolerated(thin):
    """One run over every outcome, tolerant, so one crossing serves them all.

    Fault tolerance is what lets a single run reach every artefact: an
    intolerant run stops at the first failure and the later nodes are never
    dispatched, so the outcomes they were built to settle go unasserted.
    """

    return _report(thin, fault_tolerant=True)


def test_every_deployed_primitive_is_reached(tolerated):
    """Dispatch found and imported all five, whatever each then reported."""

    reached = {node.logical_id.rsplit(".", 1)[1] for node in tolerated.nodes}

    assert reached == set(OUTCOMES), (
        "dispatch did not reach every deployed artefact: a module the catalogue "
        "claims is installed was not imported where Spark is"
    )


def test_a_succeeding_primitive_is_reported_as_succeeded(tolerated):
    node = _node(tolerated, "Success")

    assert node.status == SUCCEEDED
    assert node.result.rows_inserted == 2


def test_a_reported_failure_is_a_failed_node_rather_than_an_exception(tolerated):
    """The primitive returned a failure. Nothing raised, and nothing was lost."""

    node = _node(tolerated, "Failure")

    assert node.status == FAILED
    assert "the source system said no" in _said(node)


def test_an_exception_the_primitive_never_normalised_is_still_one_failed_node(
    tolerated,
):
    """A ``RuntimeError`` crossing Livy settles as this node's failure.

    The claim OneLake and Livy are needed for: the exception is raised inside
    the Fabric session, and what reaches the report here is a message rather
    than a traceback the desktop can re-raise.
    """

    node = _node(tolerated, "Raises")

    assert node.status == FAILED
    assert "unreachable" in _said(node)


def test_a_result_that_cannot_report_an_outcome_fails_that_node_only(tolerated):
    """A primitive that answered with a dict is a fault, not a success."""

    node = _node(tolerated, "Malformed")

    assert node.status == FAILED
    assert any(one.status == SUCCEEDED for one in tolerated.nodes), (
        "one malformed answer failed the whole run rather than its own node"
    )


def test_tolerated_rejections_are_reported_without_failing_the_node(tolerated):
    """Rows refused and rows written, both counted, and the node still stands."""

    node = _node(tolerated, "Rejects")

    assert node.result.rows_rejected == 1
    assert node.result.rows_inserted == 2


def test_an_intolerant_run_raises_and_names_the_node_that_stopped_it(thin):
    """The other half: intolerance raises rather than returning a report.

    Its own crossing, deliberately: what is being asserted is the boundary
    between a run that continues and one that does not, so it cannot share a
    report with the tolerant case. The failure names the node and carries what
    the primitive said, because a run that stopped without saying where is a
    run nobody can act on.
    """

    with pytest.raises(LoadError) as raised:
        _report(thin, fault_tolerant=False)

    said = str(raised.value)
    assert "Thin." in said, f"the failure named no node: {said}"
    assert "reported failure" in said or "rejected" in said


def _said(node) -> str:
    """Everything one node reported, as one string to look in."""

    return (
        " ".join(message.message for message in node.messages)
        + " "
        + str(getattr(node.result, "error_message", "") or "")
    )


def _node(report, outcome: str):
    for node in report.nodes:
        if node.logical_id.endswith(f".{outcome}"):
            return node
    raise AssertionError(
        f"{outcome} is not in {[node.logical_id for node in report.nodes]}"
    )
