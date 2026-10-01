"""Exercise the mixed-archive assertion with the public Build result."""

from __future__ import annotations

import inspect
from types import SimpleNamespace

import pytest
from support.acceptance import Acceptance
from support.bundles import given_build_plan as BuildPlan
from support.bundles import given_execution
from support.weaver_test import weaver_test

from weaver.build_bundle import (
    BoundTarget,
    BuildBatch,
    BuildSelection,
    BuildSequence,
    Impact,
    InstallAction,
)
from weaver.operations.build import BuildResult


@pytest.mark.parametrize(
    "coverage",
    [
        "complete",
        "fallback",
        "missing",
        "reordered",
        "duplicate",
        "wrong-plan",
        "failed-report",
    ],
)
@weaver_test()
def test_journey_archive_assertion_reads_the_installed_plan(monkeypatch, coverage):
    from fabric import test_acceptance_journey as journey

    lakehouse = BoundTarget("lh", "lakehouse", "Sales")
    warehouse = BoundTarget("wh", "warehouse", "Weaver")
    actions = tuple(
        InstallAction(name, "build_folder", None, "folder", None, None)
        for name in ("first", "warehouse", "second")
    )
    batches = tuple(
        BuildBatch(f"batch-{index}", target.id, (action,))
        for index, (target, action) in enumerate(
            zip((lakehouse, warehouse, lakehouse), actions, strict=True)
        )
    )
    sequences = (BuildSequence(10, "mixed", batches),)
    targets = (lakehouse, warehouse)
    plan = BuildPlan(
        targets=targets,
        sequences=sequences,
        selection=BuildSelection(Impact((), (), ()), (), (), ()),
        execution=given_execution(targets, sequences),
    )
    expected = [a.id for _, _, a in plan.actions()]
    result = BuildResult(
        "source", ("Lakehouse/Sales",), "bundle", True, None, "succeeded"
    )
    assert not hasattr(result, "plan")

    class Session:
        def __init__(self):
            self.archive_mutations = []

        def execute_mutation_remote(self, received, payloads=None, **options):
            assert received is plan

    session = Session()
    acceptance = Acceptance("archive coverage")
    acceptance.session = session
    acceptance.workspace = SimpleNamespace(catalogue="Warehouse/Weaver")
    acceptance.targets = ()
    acceptance.repository = "source"
    acceptance.build_items = ()
    monkeypatch.setattr(
        journey.weaver,
        "wipe",
        lambda *args, **kwargs: SimpleNamespace(
            items=(SimpleNamespace(target=acceptance.workspace.catalogue),)
        ),
    )
    monkeypatch.setattr(journey, "_catalogue_rows", lambda *args: [])
    monkeypatch.setattr(journey, "_seed_the_neighbour", lambda *args: None)

    def build(*args, **kwargs):
        if coverage != "fallback":
            session.execute_mutation_remote(plan)
        identities = {
            "complete": expected,
            "fallback": [],
            "missing": expected[:-1],
            "reordered": list(reversed(expected)),
            "duplicate": expected + [expected[-1]],
            "wrong-plan": ["other"],
            "failed-report": expected,
        }[coverage]
        if identities:
            session.archive_mutations.append(
                {
                    "status": "uncertain"
                    if coverage == "failed-report"
                    else "completed",
                    "action_ids": identities,
                }
            )
        return result

    class ObservationReached(Exception):
        pass

    def observe(*args, **kwargs):
        raise ObservationReached

    monkeypatch.setattr(journey.weaver, "build", build)
    monkeypatch.setattr(journey, "_item", lambda *args: "Sales")
    monkeypatch.setattr(journey, "_observe", observe)
    claim = journey.test_a_realistic_estate_builds_from_nothing
    arguments = {"acceptance": acceptance}
    if "monkeypatch" in inspect.signature(claim).parameters:
        arguments["monkeypatch"] = monkeypatch
    with pytest.raises(
        ObservationReached if coverage == "complete" else AssertionError
    ):
        claim(**arguments)
