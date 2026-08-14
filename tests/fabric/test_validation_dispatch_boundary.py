"""A Test reaches its artefact the way a load does, and its outcome is settled.

The same boundary as ``test_run_dispatch_boundary``, for the other half of the
run: one import, one runtime context, a different engine call. What makes this
its own file is that a Test's artefact returns a Spark frame and Weaver's
comparison reads it — so the claim cannot be made without a real session, and a
double returning a frame-shaped object would be this suite modelling Spark.

Three outcomes, chosen because each is settled by a different rule:

.. code-block:: text

    Agrees       both sides match, so nothing is missing and nothing is extra
    Disagrees    a discrepancy on both sides, reported with its counts
    Unreadable   could not be evaluated at all, which is not "found nothing"

The last is the one that matters most. The answer a validation must never give
is "found nothing" when what happened is that it could not look, and *invalid*
rather than *failed* is how the report says so.

``hosted``, because the comparison is :mod:`weaver.runtime.test_compare` running
inside the Fabric session as the installed wheel.

Its Warehouse twin is ``test_warehouse_validation_primitive``. Two engines, one
set of validation semantics; if the two files disagree, the semantics have
become two.
"""

from __future__ import annotations

import pytest
from support.thin import JUDGEMENTS, thin_estate

from weaver.test import run_test

pytestmark = [pytest.mark.fabric, pytest.mark.hosted]

#: The Lakehouse the artefacts are deployed into. Emptied first, because a run
#: that found a previous run's modules would prove nothing about this one.
LAKEHOUSE = "PYTEST_LH_1"


@pytest.fixture(scope="module")
def judged(
    fabric_workspace,
    fabric_client,
    weaver_session,
    fabric_empty_lakehouse,
    tmp_path_factory,
):
    """Tests with controlled outcomes, deployed into a real Lakehouse."""

    from weaver.fabric import FabricResolver, OneLakeDfsClient

    fabric_empty_lakehouse(LAKEHOUSE)

    return thin_estate(
        tmp_path_factory.mktemp("judged"),
        outcomes=(),
        judgements=JUDGEMENTS,
        lakehouse=LAKEHOUSE,
        workspace=fabric_workspace,
        resolver=FabricResolver(fabric_workspace, client=fabric_client),
        store=OneLakeDfsClient(),
        session=weaver_session,
    )


@pytest.fixture(scope="module")
def report(judged):
    """One run over every judgement, so one crossing serves them all."""

    return run_test(
        judged.session,
        workspace=judged.workspace,
        requested=[judged.target],
        state=judged.state,
    )


def test_every_declared_validation_is_reached(report):
    """Dispatch found and imported all three, whatever each then reported."""

    reached = {node.logical_id.rsplit(".", 1)[1] for node in report.nodes}

    assert reached == set(JUDGEMENTS), (
        "dispatch did not reach every deployed validation: a Test the catalogue "
        "claims is installed was not imported where Spark is"
    )


def test_a_validation_that_agrees_passes(report):
    """Both sides empty, so there is nothing missing and nothing unexpected."""

    node = _node(report, "Agrees")

    assert node.status == "passed"
    assert node.result.missing_count == 0
    assert node.result.unexpected_count == 0


def test_a_disagreement_is_a_failure_carrying_what_it_found(report):
    """Counted on both sides, because which side differs is what a reader acts on."""

    node = _node(report, "Disagrees")

    assert node.status == "failed"
    assert node.result.missing_count == 1
    assert node.result.unexpected_count == 2


def test_a_validation_that_could_not_run_is_invalid_rather_than_failed(report):
    """The distinction the whole report rests on.

    A Test that could not be evaluated has found nothing *and proved nothing*.
    Reporting it as failed would be wrong in one direction and reporting it as
    passed wrong in the other, so it is neither.
    """

    node = _node(report, "Unreadable")

    assert node.status == "invalid"
    assert node.status != "failed"


def test_one_unreadable_validation_does_not_invalidate_the_others(report):
    """Each node is settled on its own evidence."""

    assert _node(report, "Agrees").status == "passed"
    assert _node(report, "Disagrees").status == "failed"


def _node(report, outcome: str):
    for node in report.nodes:
        if node.logical_id.endswith(f".{outcome}"):
            return node
    raise AssertionError(
        f"{outcome} is not in {[node.logical_id for node in report.nodes]}"
    )
