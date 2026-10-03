"""Load the benchmark Tables through ``weaver load`` and time each load.

Opt in with ``--performance``. Each engine runs, in order, a tiny initial load
(A), an empty incremental load (B), an unchanged incremental load (C), a large
initial load (D), and a small change to a very large target with the change
that undoes it (E). Build and every reset are setup, outside the timings. Each
scenario's result is verified against the rows its segments generate, so a
timing is only reported for a load that did exactly what it should.

Estate throughput is a separate claim: one ``weaver.load`` reloads independent
branches (A feeds B, C feeds D, E alone) of each engine, and of both together.

``WEAVER_LOAD_BENCH_LARGE_ROWS``, ``WEAVER_LOAD_BENCH_HUGE_ROWS`` and
``WEAVER_LOAD_BENCH_FLOW_ROWS`` scale D, E and the branches.
``WEAVER_PERFORMANCE_RESULTS`` appends every timing as a JSON line.
"""

from __future__ import annotations

import subprocess
import tempfile
from pathlib import Path

import pytest
from support.load_performance import (
    FLOW_ROWS,
    FLOW_ROWS_ENV,
    HUGE_ROWS,
    HUGE_ROWS_ENV,
    LAKEHOUSE,
    LARGE_ROWS,
    LARGE_ROWS_ENV,
    WAREHOUSE,
    LoadBench,
    ScenarioRun,
    record,
    run_flow,
    run_huge,
    run_large,
    run_small,
    scale,
    write_load_estate,
)
from support.weaver_test import register_session, weaver_test

ENGINES = (WAREHOUSE, LAKEHOUSE)
SCENARIOS = ("A", "B", "C", "D", "E", "E inverse")


@pytest.fixture(scope="module")
def bench(fabric_workspace, livy_session):
    from weaver.sessions import ConsoleSession

    with ConsoleSession(workspace=fabric_workspace, livy=livy_session) as session:
        loads = LoadBench(
            session=register_session(session),
            livy=livy_session,
            workspace_name=fabric_workspace.workspace,
            environment=str(fabric_workspace.environment),
        )
        with tempfile.TemporaryDirectory(prefix="weaver-load-bench-") as scratch:
            loads.prepare(write_load_estate(Path(scratch)))
        yield loads


@pytest.fixture(scope="module")
def context(bench):
    revision = subprocess.run(
        ["git", "rev-parse", "--short", "HEAD"],
        capture_output=True,
        text=True,
        cwd=Path(__file__).parent,
    ).stdout.strip()
    spark = bench.spark(
        "_sc = spark.sparkContext\n"
        "emit({'spark': spark.version,"
        " 'parallelism': _sc.defaultParallelism,"
        " 'executors': _sc._jsc.sc().getExecutorMemoryStatus().size() - 1,"
        " 'shuffle_partitions': spark.conf.get('spark.sql.shuffle.partitions'),"
        " 'runtime': spark.conf.get('spark.fabric.runtime.version', None) or"
        " spark.conf.get('spark.synapse.runtime.version', None)})\n"
    )
    scales = {
        "large_rows": scale(LARGE_ROWS_ENV, LARGE_ROWS),
        "huge_rows": scale(HUGE_ROWS_ENV, HUGE_ROWS),
        "flow_rows": scale(FLOW_ROWS_ENV, FLOW_ROWS),
    }
    return {"revision": revision, **scales, **spark}


@pytest.fixture(scope="module", params=ENGINES)
def scenarios(request, bench, context):
    engine = request.param
    run = ScenarioRun(engine)
    for part in (run_small, run_large, run_huge):
        part(bench, engine, run)
    record(run, context)
    print(f"\n{context}\n{run.describe()}")
    return run


@pytest.mark.performance
@pytest.mark.parametrize("scenario", SCENARIOS)
@weaver_test(integration=True)
def test_each_load_does_exactly_what_its_rows_require(scenarios, scenario):
    assert scenario in scenarios.timings, scenarios.describe()
    assert not scenarios.findings.get(scenario), (
        f"{scenarios.findings[scenario]}\n{scenarios.describe()}"
    )


FLOWS = {"lakehouse": (LAKEHOUSE,), "warehouse": (WAREHOUSE,), "mixed": ENGINES}


@pytest.fixture(scope="module", params=list(FLOWS))
def flow(request, bench, context):
    run = ScenarioRun(request.param)
    run_flow(bench, FLOWS[request.param], run, f"F {request.param}")
    record(run, context)
    print(f"\n{context}\n{run.describe()}")
    return run


@pytest.mark.performance
@weaver_test(integration=True)
def test_independent_branches_load_in_dependency_order_and_exactly(flow):
    (scenario,) = flow.timings
    assert not flow.findings.get(scenario), (
        f"{flow.findings[scenario]}\n{flow.describe()}"
    )
