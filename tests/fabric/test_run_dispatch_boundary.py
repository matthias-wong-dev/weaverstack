"""Real dispatch, real Session, real import — trivial artefacts.

The rung between the mock run cycle and a built estate. The mock cycle proves
the Runner's own reasoning with dispatch replaced; an estate run proves what
loads do to data. Neither proves the join: that a node the Runner planned
actually resolves to a deployed module, is reached through the Session that owns
the host, and comes back as something the run can settle.

That join is where the drift lives — a renamed constructor argument, a result
type nobody serialises, a runtime root computed one folder too high. Every one
of those passes a mock and costs a Spark run to discover. Here they cost 0.1
seconds, because the artefacts do nothing and there is no estate to build.

Moved out of the core tier with the emulator. A thin run imports a *deployed*
module and dispatches to it, and a deployed module lives in the Lakehouse's
Files area — which on a desktop is OneLake, not a filesystem. The old file
worked because the emulator's storage was a directory; nothing about that was
Fabric's behaviour.

``hosted``: the modules are imported where Spark is, as the installed wheel.

Currently skipped. The bodies below are the claims, unchanged; what they still
need is the Fabric harness that deploys these artefacts into a real Lakehouse,
which is the acceptance work later in this plan. Recorded as a known gap rather
than deleted, because dispatch and result settlement are worth proving and are
proven nowhere else.

"""

from __future__ import annotations

import pytest

pytestmark = [
    pytest.mark.fabric,
    pytest.mark.hosted,
    pytest.mark.skip(
        reason=(
            "moved from the deleted local Spark tier; needs the Fabric harness "
            "to deploy its artefacts — see Milestone 1 in the Fabric-only "
            "runtime PR"
        )
    ),
]
from support.thin import OUTCOMES, thin_estate

from weaver.errors import LoadError
from weaver.load import run_load
from weaver.test import run_test


@pytest.fixture
def thin(tmp_path):
    """One thin estate per test — it costs a few files, not a build."""

    estate = thin_estate(tmp_path)
    yield estate
    estate.session.close()


def loaded(estate, *, fault_tolerant: bool = False):
    return run_load(
        estate.session,
        workspace=estate.workspace,
        requested=[estate.target],
        state=estate.state,
        fault_tolerant=fault_tolerant,
    )


def explanation(node) -> str:
    """Everything the report offers a reader about why a node ended as it did."""

    carried = "" if node.result is None else (node.result.error_message or "")
    return " ".join([carried, *(message.message for message in node.messages)])


# --- the join itself ----------------------------------------------------------


def test_a_planned_node_reaches_the_artefact_the_catalogue_points_at(thin):
    """The whole point: no build happened, and the primitive still ran."""

    report = loaded(thin, fault_tolerant=True)

    success = thin.result(report, "Success")
    assert success.status == "succeeded"
    assert success.result.rows_read == 2, "the artefact's own numbers came back"
    assert success.result.rows_inserted == 2


def test_every_outcome_is_dispatched_rather_than_planned_and_dropped(thin):
    report = loaded(thin, fault_tolerant=True)

    assert {result.node_id for result in report.nodes} == set(thin.nodes.values())
    assert all(result.status != "pending" for result in report.nodes)


# --- what comes back, and how a run settles it --------------------------------


def test_a_reported_failure_is_a_failure_without_anything_being_raised(thin):
    report = loaded(thin, fault_tolerant=True)

    failure = thin.result(report, "Failure")
    assert failure.status == "failed"
    assert "the source system said no" in explanation(failure)


def test_rejects_are_a_success_with_the_refusal_still_reported(thin):
    report = loaded(thin, fault_tolerant=True)

    rejects = thin.result(report, "Rejects")
    assert rejects.status == "succeeded_with_rejects"
    assert rejects.result.rows_rejected == 1
    assert rejects.result.rows_inserted == 2


def test_an_intolerant_load_fails_where_a_tolerant_one_accepted_rejects(tmp_path):
    """``fault_tolerant`` reaches the artefact, rather than being read on the way.

    A run that decided about rejects for itself would report tolerance the
    primitive never applied — and the rows would be missing from the target
    while the report said they were written. So the same artefact is asked
    twice, and the two answers have to differ.
    """

    estate = thin_estate(tmp_path, outcomes=("Rejects",))
    try:
        tolerant = estate.result(loaded(estate, fault_tolerant=True), "Rejects")
        with pytest.raises(LoadError) as raised:
            loaded(estate, fault_tolerant=False)
    finally:
        estate.session.close()

    assert tolerant.status == "succeeded_with_rejects"
    assert tolerant.result.rows_inserted == 2, "the valid rows were still written"
    strict = raised.value.report.by_node[estate.node("Rejects")]
    assert strict.status == "failed"
    assert "tolerates none" in explanation(strict)


def test_an_exception_the_primitive_never_normalised_is_still_one_failed_node(thin):
    report = loaded(thin, fault_tolerant=True)

    raised = thin.result(report, "Raises")
    assert raised.status == "failed"
    assert "unreachable" in explanation(raised)


def test_a_result_that_cannot_report_an_outcome_fails_that_node_only(thin):
    """A primitive that returns something else is a defect, not a crash.

    The run has to name the node that broke its contract and carry on, because
    the alternative — one malformed answer ending the whole run — makes a
    fifty-node load undiagnosable.
    """

    report = loaded(thin, fault_tolerant=True)

    malformed = thin.result(report, "Malformed")
    assert malformed.status == "failed"
    assert explanation(malformed).strip(), "the report says what went wrong"


def test_one_broken_node_does_not_stop_the_others_from_running(thin):
    report = loaded(thin, fault_tolerant=True)

    statuses = {name: thin.result(report, name).status for name in OUTCOMES}
    assert statuses["Success"] == "succeeded"
    assert sorted(statuses) == sorted(OUTCOMES), "every outcome was reached"


# --- what the run says about itself -------------------------------------------


def test_the_run_reports_failure_when_any_node_failed(thin):
    report = loaded(thin, fault_tolerant=True)

    assert not report.succeeded
    failed = [node.node_id for node in report.nodes if node.status == "failed"]
    assert sorted(failed) == sorted(
        thin.node(name) for name in ("Failure", "Malformed", "Raises")
    )


def test_an_all_succeeding_run_says_so(tmp_path):
    estate = thin_estate(tmp_path, outcomes=("Success",))
    try:
        report = loaded(estate)
        assert report.succeeded
        assert all(node.status == "succeeded" for node in report.nodes)
    finally:
        estate.session.close()


def test_every_node_serialises_whatever_kind_of_result_it_carried(thin):
    """Including the malformed one — a run must be able to write itself down."""

    report = loaded(thin, fault_tolerant=True)

    for result in report.nodes:
        assert result.to_mapping()["node_id"] == result.node_id


# The validation half of this boundary moved to
# ``tests/fabric/test_validation_dispatch.py``: a Test's artefact returns a
# Spark frame and the comparison reads it, so the claim needs a real session.
