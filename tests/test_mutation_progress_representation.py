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
def test_a_stage_the_progress_missed_is_reported_from_the_final_ledger():
    plan, drivers = plan_and_driver()
    report = execute(plan, drivers)
    out = io.StringIO()

    with ConsoleSession(progress=out) as session:
        StageProgress(plan, session).finish(report)

    lines = stage_lines(out)
    assert [line[0] for line in lines if FIRST in line] == ["→", "✗"]
    assert [line[0] for line in lines if LATER in line] == ["→", "✗"]
    assert all(re.search(r"\d+\.\ds$", line) for line in lines if line[0] == "✗")


def fabric_plan():
    plan, drivers = plan_and_driver()
    plan = sealed(
        replace(
            plan,
            bundle_id="",
            execution=replace(plan.execution, spark_home_target_id="sales"),
        )
    )
    return plan, drivers


@weaver_test()
def test_a_desktop_session_follows_a_plan_running_in_fabric(monkeypatch):
    """What Fabric's progress showed is not shown again from its final ledger."""

    from weaver.sessions.mutation_progress import progress_record
    from weaver.workspaces import Workspace

    plan, drivers = fabric_plan()
    report = execute(plan, drivers)
    first = {"fail", "sibling"}

    def remote(session, plan, payloads, *, observer, **options):
        for event in report.ledger:
            record = progress_record(event)
            if record is not None and record["action_id"] in first:
                observer(record)
        return report

    monkeypatch.setattr(ConsoleSession, "execute_mutation_remote", remote)
    out = io.StringIO()

    with ConsoleSession(progress=out, workspace=Workspace(workspace="Demo")) as session:
        returned = session.execute_mutation(plan, {})

    assert returned is report
    lines = stage_lines(out)
    assert [line[0] for line in lines if FIRST in line] == ["→", "✗"]
    assert [line[0] for line in lines if LATER in line] == ["→", "✗"]


@weaver_test()
def test_fabric_progress_reaches_the_desktop_while_the_plan_runs(monkeypatch):
    """The writer Fabric runs and the reader the desktop runs, through one file."""

    import sys
    import threading
    import types

    from weaver.sessions import archive_runtime, install_archive
    from weaver.sessions.mutation_progress import progress_record

    monkeypatch.setattr(archive_runtime, "PROGRESS_INTERVAL", 0.01)
    files = {}
    written = threading.Event()

    def put(location, content, overwrite):
        files[location] = content.encode()
        written.set()

    monkeypatch.setitem(
        sys.modules,
        "notebookutils",
        types.SimpleNamespace(fs=types.SimpleNamespace(put=put)),
    )

    class Store:
        def read(self, location):
            return files[location]

    plan, drivers = plan_and_driver()
    seen = []
    with ConsoleSession(progress=False) as session:
        stop_reading = install_archive._follow(
            session, Store(), "progress", seen.append
        )
        observe, stop_writing = archive_runtime._progress_writer("progress")
        report = execute(plan, drivers, observe)
        assert written.wait(5)
        stop_writing()
        expected = len([e for e in report.ledger if progress_record(e)])
        for _attempt in range(500):
            if len(seen) == expected:
                break
            threading.Event().wait(0.01)
        stop_reading()

    terminal = {r["action_id"]: r["status"] for r in seen if r["kind"] == "terminal"}
    assert terminal["fail"] == "failed"
    assert terminal["dependent"] == "blocked"
    assert report.by_id["fail"].status == "failed"


@weaver_test()
def test_presentation_failing_changes_no_outcome():
    plan, drivers = plan_and_driver()

    def broken(_event):
        raise RuntimeError("the terminal went away")

    observed = execute(plan, drivers, broken)
    plain = execute(plan, drivers)

    assert [r.status for r in observed.results] == [r.status for r in plain.results]


@weaver_test()
def test_a_stage_that_names_its_target_says_it_once():
    from weaver.mutation import BoundTarget, MutationBatch, MutationSequence
    from weaver.sessions.mutation_progress import stage_label

    targets = {"t": BoundTarget("t", "warehouse", "t-id", item_name="Sales_Dev")}

    def label(description):
        sequence = MutationSequence(1, description, (MutationBatch("b", "t", ()),))
        return stage_label(sequence, targets, 2)

    assert label("reconstruct Warehouse/Sales_Dev") == (
        "Reconstruct Warehouse/Sales_Dev · 2 actions"
    )
    assert label("build dependency layer") == (
        "Warehouse/Sales_Dev · Building objects · 2 actions"
    )
