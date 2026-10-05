"""What an operator sees while a Build, Install, Wipe or Mirror executes.

A plan's stages are its sequences, one kind of work on its targets. A stage
executed on this host is marked when its first action starts and when its last
one ends. A plan that ran in Fabric returns its ledger, and each stage it ran is
reported once, with the duration Fabric measured.
"""

from __future__ import annotations

import io
import re
from dataclasses import replace

from support.weaver_test import weaver_test

from weaver.sessions import ConsoleSession
from weaver.sessions.mutation_progress import StageProgress


def plan_and_driver():
    """Two stages: ``fail`` fails, and ``dependent`` in the later stage needs it."""

    from test_mutation_plan_representation import _plan

    from weaver.build_bundle.dependencies import object_key
    from weaver.build_bundle.models import BuildBatch, InstallAction
    from weaver.build_bundle.stages import BUILD, PlannedStage, enumerate_stages
    from weaver.build_bundle.targets import BoundTarget
    from weaver.mutation.executor import Completed, Failed, MutationDriver

    def action(name):
        return InstallAction(
            name, "build_folder", "Files/Incoming", "folder", None, None
        )

    stages = (
        PlannedStage(
            BUILD,
            "build dependency layer",
            (BuildBatch("first", "sales", (action("fail"), action("sibling"))),),
            provides={"fail": (object_key("Failed"),)},
        ),
        PlannedStage(
            BUILD,
            "create item-owned schemas",
            (BuildBatch("later", "sales", (action("dependent"), action("other"))),),
            index=1,
            requires={"dependent": (object_key("Failed"),)},
        ),
    )
    sequences, _payloads, _changes, required = enumerate_stages(
        stages,
        targets=(BoundTarget("sales", "lakehouse", "Sales"),),
        completion_target_id="sales",
    )
    from weaver.mutation import BoundTarget as Bound

    plan = replace(
        _plan(()),
        bundle_id="",
        sequences=sequences,
        required_completion=required,
        targets=(Bound("sales", "lakehouse", "sales-id", item_name="Sales"),),
    )
    plan = sealed(plan)

    def run(request):
        return Failed("refused") if request.action.id == "fail" else Completed()

    return plan, {"folder": MutationDriver(run)}


def sealed(plan):
    from weaver.mutation.bundle import compute_bundle_id

    plan = replace(plan, bundle_id="")
    return replace(plan, bundle_id=compute_bundle_id(plan))


def execute(plan, drivers, observer=None):
    from weaver.mutation import MutationExecutor

    return MutationExecutor(
        drivers, limits={"onelake:Sales": 1}, observer=observer
    ).execute(plan, {})


FIRST = "Lakehouse/Sales · Building objects · 2 actions"
LATER = "Lakehouse/Sales · Creating schemas · 2 actions"


def stage_lines(out) -> list[str]:
    return [
        line.rstrip()
        for line in out.getvalue().splitlines()
        if re.match(r"^[→✓✗] ", line)
    ]


@weaver_test()
def test_each_stage_run_here_starts_and_ends_once():
    plan, drivers = plan_and_driver()
    out = io.StringIO()

    with ConsoleSession(progress=out) as session:
        report = execute(plan, drivers, StageProgress(plan, session).observe)

    lines = stage_lines(out)
    assert [line[0] for line in lines if FIRST in line] == ["→", "✗"]
    assert [line[0] for line in lines if LATER in line] == ["→", "✗"]
    assert any(FIRST in line and "(1 failed)" in line for line in lines)
    assert any(LATER in line and "(1 blocked)" in line for line in lines)
    assert report.by_id["dependent"].status == "blocked"


@weaver_test()
def test_a_stage_run_in_fabric_is_reported_once_from_its_ledger():
    plan, drivers = plan_and_driver()
    report = execute(plan, drivers)
    out = io.StringIO()

    with ConsoleSession(progress=out) as session:
        StageProgress(plan, session).replay(report)

    lines = stage_lines(out)
    assert len(lines) == 2
    assert all(line.startswith("✗") for line in lines)
    assert FIRST in lines[0] and LATER in lines[1]
    assert all(re.search(r"\d+\.\ds$", line) for line in lines)


@weaver_test()
def test_a_desktop_session_replays_the_plan_it_carried_into_fabric(monkeypatch):
    from weaver.workspaces import Workspace

    plan, drivers = plan_and_driver()
    plan = sealed(
        replace(
            plan,
            bundle_id="",
            execution=replace(plan.execution, spark_home_target_id="sales"),
        )
    )
    report = execute(plan, drivers)
    monkeypatch.setattr(
        ConsoleSession, "execute_mutation_remote", lambda s, p, b, **kw: report
    )
    out = io.StringIO()

    with ConsoleSession(progress=out, workspace=Workspace(workspace="Demo")) as session:
        returned = session.execute_mutation(plan, {})

    assert returned is report
    assert len(stage_lines(out)) == 2


@weaver_test()
def test_presentation_failing_changes_no_outcome():
    plan, drivers = plan_and_driver()

    def broken(_event):
        raise RuntimeError("the terminal went away")

    observed = execute(plan, drivers, broken)
    plain = execute(plan, drivers)

    assert [r.status for r in observed.results] == [r.status for r in plain.results]
