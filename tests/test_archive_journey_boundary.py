"""Exercise the mixed-archive assertion with the public Build result."""

from __future__ import annotations

import inspect
from types import SimpleNamespace

import pytest
from support.acceptance import Acceptance
from support.bundles import given_build_plan as BuildPlan
from support.bundles import given_execution
from support.sessions import PlanExecution as Installer
from support.weaver_test import weaver_test

from weaver.build_bundle import (
    BoundTarget,
    BuildBatch,
    BuildSelection,
    BuildSequence,
    Impact,
    InstallAction,
)
from weaver.build_bundle.bundle import SUPPORTED_FORMAT_VERSION, BuildBundle
from weaver.locations import Location
from weaver.operations.build import BuildResult


@pytest.mark.parametrize(
    "coverage",
    [
        "complete",
        "fallback",
        "missing",
        "reordered",
        "duplicate",
        "warehouse",
        "no-request",
    ],
)
@weaver_test()
def test_journey_archive_assertion_reads_the_installed_plan(monkeypatch, coverage):
    from fabric import test_acceptance_journey as journey

    lakehouse = BoundTarget("lh", "lakehouse", "Sales")
    warehouse = BoundTarget("wh", "warehouse", "Weaver")
    actions = tuple(
        InstallAction(name, "materialise", None, "spark_sql", None, None)
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
        SUPPORTED_FORMAT_VERSION,
        "bundle",
        "Sales",
        "signature",
        targets,
        sequences,
        BuildSelection(Impact((), (), ()), (), (), ()),
        given_execution(targets, sequences),
    )
    bundle = BuildBundle(Location("bundle"), plan)
    result = BuildResult(
        "source", ("Lakehouse/Sales",), "bundle", True, None, "succeeded"
    )
    assert not hasattr(result, "plan")
    session = SimpleNamespace(archive_installations=[])
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
    monkeypatch.setattr(Installer, "install", lambda *args, **kwargs: None)

    def build(*args, **kwargs):
        Installer(session).install(bundle)
        identities = {
            "complete": ["first", "second"],
            "fallback": [],
            "missing": ["first"],
            "reordered": ["second", "first"],
            "duplicate": ["first", "second", "second"],
            "warehouse": ["first", "warehouse", "second"],
            "no-request": ["first", "second"],
        }[coverage]
        if identities:
            session.archive_installations.append(
                {
                    "request": None if coverage == "no-request" else {},
                    "report": {
                        "sequences": [
                            {"actions": [{"action_id": name} for name in identities]}
                        ]
                    },
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
