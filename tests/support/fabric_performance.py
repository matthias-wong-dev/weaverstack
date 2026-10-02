"""Time Build, Wipe and Mirror of a representative estate in real Fabric.

The estate is one of the repository's representative generators, bound to the
performance items in ``tests/fabric/performance_estate.py``. Each operation is
timed from the caller's side, and a Build's report is summarised by executor so
a slow run says where its time went.
"""

from __future__ import annotations

import tempfile
import time
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

LAKEHOUSE = "lakehouse"
WAREHOUSE = "warehouse"

#: Wall-clock ceilings in seconds, by engine and operation.
THRESHOLDS = {
    WAREHOUSE: {"build": 5 * 60, "noop": 30, "wipe": 60, "mirror": 2 * 60},
    LAKEHOUSE: {"build": 10 * 60, "noop": 30, "wipe": 2 * 60, "mirror": 5 * 60},
}

#: The representative estates place motif 0 in item 000 and the rest in 001.
ITEMS = ("Representative000", "Representative001")


@dataclass
class Timing:
    operation: str
    seconds: float
    succeeded: bool
    detail: dict = field(default_factory=dict)


@dataclass
class EstateRun:
    engine: str
    declarations: int
    timings: list[Timing] = field(default_factory=list)

    def seconds(self, operation: str) -> float:
        return next(t.seconds for t in self.timings if t.operation == operation)

    def describe(self) -> str:
        lines = [f"{self.engine} estate, {self.declarations} declarations"]
        for timing in self.timings:
            ceiling = THRESHOLDS[self.engine].get(timing.operation)
            bound = f" (limit {ceiling}s)" if ceiling else ""
            state = "ok" if timing.succeeded else "FAILED"
            lines.append(
                f"  {timing.operation:<8} {timing.seconds:8.1f}s{bound}  {state}"
            )
            for key, value in timing.detail.items():
                lines.append(f"      {key}: {value}")
        return "\n".join(lines)


def performance_names(engine: str) -> dict[str, str]:
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "fabric"))
    import performance_estate as estate

    kind = "warehouse" if engine == WAREHOUSE else "lakehouse"
    return {
        "catalogue": estate.name("perf_weaver"),
        "fork": estate.name("perf_weaver_fork"),
        "targets": tuple(estate.name(f"perf_{kind}_{n}") for n in range(2)),
        "mirrors": tuple(estate.name(f"perf_{kind}_mirror_{n}") for n in range(2)),
    }


def write_estate(engine: str, declarations: int, root: Path) -> Path:
    source = root / "source"
    if engine == WAREHOUSE:
        from support.representative_warehouse_estate import (
            RepresentativeWarehouseSpec,
            make_representative_oracle,
            write_representative_estate,
        )

        write_representative_estate(
            source,
            make_representative_oracle(
                RepresentativeWarehouseSpec.from_declarations(declarations)
            ),
        )
    else:
        from support.representative_lakehouse_estate import (
            RepresentativeLakehouseSpec,
            make_representative_lakehouse_plan,
            write_representative_lakehouse_estate,
        )

        write_representative_lakehouse_estate(
            source,
            make_representative_lakehouse_plan(
                RepresentativeLakehouseSpec.from_declarations(declarations)
            ),
        )
    return source


#: The waits B moved out of their actions, reported whatever their share.
WAITS = (
    "shortcut",
    "shortcut_readiness",
    "sql_endpoint_refresh",
    "start_sql_endpoint_refresh",
    "await_sql_endpoint_refresh",
)


def _build_detail(result) -> dict:
    """Action time by executor, and its total against the Build's wall time.

    An action's time includes any wait it spent pending, so a total well above
    the wall time is work that overlapped.
    """

    report = result.installation_report
    if report is None:
        return {"actions": 0}
    by_executor: dict[str, list[float]] = defaultdict(list)
    for action in report.action_results():
        by_executor[action.executor].append(action.duration_seconds or 0.0)
    total = sum(sum(v) for v in by_executor.values())
    detail = {
        "actions": sum(len(v) for v in by_executor.values()),
        "action time": f"{total:.1f}s",
    }
    ranked = sorted(by_executor, key=lambda name: -sum(by_executor[name]))
    shown = list(dict.fromkeys([*ranked[:5], *(w for w in WAITS if w in by_executor)]))
    for executor in shown:
        durations = by_executor[executor]
        detail[executor] = (
            f"{len(durations)} actions, {sum(durations):.1f}s, "
            f"max {max(durations):.1f}s"
        )
    timed = [
        action for action in report.action_results() if action.started_at is not None
    ]
    if timed:
        origin = min(action.started_at for action in timed)

        def span(action) -> str:
            begin = (action.started_at - origin).total_seconds()
            end = (action.finished_at - origin).total_seconds()
            return f"{begin:6.1f}s to {end:6.1f}s"

        for action in timed:
            if action.executor in WAITS:
                detail[f"  {action.action_id}"] = span(action)
        last = sorted(timed, key=lambda action: action.finished_at)[-5:]
        detail["last to finish"] = "; ".join(
            f"{action.action_id} ({span(action)})" for action in last
        )
    if result.errors:
        detail["errors"] = "; ".join(e.describe() for e in result.errors)[:600]
    return detail


def _report_detail(report, plan) -> dict:
    """Action time by executor and the last actions to finish, from a report."""

    executors = {action.id: action.executor for _s, _b, action in plan.actions()}
    by_executor: dict[str, list[float]] = defaultdict(list)
    for result in report.results:
        if executors[result.action_id] != "completion_gate":
            by_executor[executors[result.action_id]].append(
                result.active_seconds + result.wait_seconds
            )
    detail = {
        "actions": sum(len(v) for v in by_executor.values()),
        "action time": f"{sum(sum(v) for v in by_executor.values()):.1f}s",
    }
    for executor in sorted(by_executor, key=lambda name: -sum(by_executor[name]))[:6]:
        durations = by_executor[executor]
        detail[executor] = (
            f"{len(durations)} actions, {sum(durations):.1f}s, "
            f"max {max(durations):.1f}s"
        )
    if report.ledger:
        origin = min(event.at for event in report.ledger)
        began: dict[str, float] = {}
        ended: dict[str, float] = {}
        for event in report.ledger:
            if event.kind == "dispatched":
                began.setdefault(event.action_id, event.at - origin)
            elif event.kind == "terminal":
                ended[event.action_id] = event.at - origin
        last = sorted(ended, key=ended.get)[-6:]
        detail["last to finish"] = "; ".join(
            f"{key} ({began.get(key, ended[key]):.1f}s to {ended[key]:.1f}s)"
            for key in last
        )
    return detail


class _Capturing:
    """Keep the plan and report of each mutation a Session executes."""

    def __init__(self, session):
        self.session = session
        self.executed = []
        self._original = session.execute_mutation

    def __enter__(self):
        def execute(plan, payloads=None, **options):
            report = self._original(plan, payloads, **options)
            self.executed.append((plan, report))
            return report

        self.session.execute_mutation = execute
        return self

    def __exit__(self, *_exc):
        self.session.execute_mutation = self._original


def _timed(run: EstateRun, operation: str, call, detail=lambda result: {}):
    started = time.perf_counter()
    try:
        result = call()
    except Exception as exc:  # noqa: BLE001 - a failed run is reported, not raised
        run.timings.append(
            Timing(
                operation,
                time.perf_counter() - started,
                False,
                {"error": f"{type(exc).__name__}: {exc}"[:600]},
            )
        )
        return None
    elapsed = time.perf_counter() - started
    succeeded = getattr(result, "succeeded", True)
    run.timings.append(Timing(operation, elapsed, succeeded, detail(result)))
    return result


def run_estate(
    engine: str,
    declarations: int,
    *,
    session,
    workspace_name: str,
    environment: str | None = None,
    operations=("build", "noop", "mirror", "wipe"),
) -> EstateRun:
    """Empty the performance items, then time each operation in order.

    ``environment`` is the Fabric Environment a Lakehouse plan's Spark session
    attaches, which supplies Weaver's dependencies.
    """
    import weaver

    names = performance_names(engine)
    kind = "Warehouse" if engine == WAREHOUSE else "Lakehouse"
    catalogue = f"Warehouse/{names['catalogue']}"
    items = [
        f"{kind}/{item}={kind}/{target}"
        for item, target in zip(ITEMS, names["targets"])
    ]
    run = EstateRun(engine, declarations)
    with tempfile.TemporaryDirectory(prefix="weaver-performance-") as scratch:
        source = write_estate(engine, declarations, Path(scratch))
        _empty(names, kind, session=session, workspace_name=workspace_name)
        for operation in operations:
            if operation in ("build", "noop"):
                _timed(
                    run,
                    operation,
                    lambda: weaver.build(
                        str(source),
                        items=items,
                        session=session,
                        workspace=workspace_name,
                        catalogue=catalogue,
                        environment=environment,
                    ),
                    _build_detail,
                )
            elif operation == "mirror":
                capturing = _Capturing(session)
                with capturing:
                    _timed(
                        run,
                        operation,
                        lambda: weaver.mirror(
                            [
                                f"{kind}/{item}={kind}/{target}"
                                for item, target in zip(ITEMS, names["mirrors"])
                            ],
                            session=session,
                            workspace=workspace_name,
                            catalogue=f"Warehouse/{names['fork']}",
                            mirror=catalogue,
                            environment=environment,
                        ),
                    )
                if capturing.executed:
                    plan, report = capturing.executed[-1]
                    run.timings[-1].detail.update(_report_detail(report, plan))
            elif operation == "wipe":
                _timed(
                    run,
                    operation,
                    lambda: weaver.wipe(
                        session=session, workspace=workspace_name, catalogue=catalogue
                    ),
                    lambda result: {"emptied": len(result.emptied)},
                )
    return run


def _empty(names, kind, *, session, workspace_name) -> None:
    """Empty every performance item, then stand the catalogue up, as initialise does."""

    import weaver
    from support.catalogue import build_catalogue_item
    from weaver.fabric import OneLakeDfsClient
    from weaver.operations.wipe import PHYSICAL_ONLY
    from weaver.targets import ItemRef
    from weaver.workspaces import Workspace

    targets = [f"{kind}/{name}" for name in (*names["targets"], *names["mirrors"])] + [
        f"Warehouse/{names['catalogue']}",
        f"Warehouse/{names['fork']}",
    ]
    weaver.wipe(
        targets,
        session=session,
        workspace=workspace_name,
        catalogue_action=PHYSICAL_ONLY,
    )
    result = build_catalogue_item(
        catalogue=ItemRef(names["catalogue"]),
        workspace=Workspace(
            workspace=workspace_name, catalogue=f"Warehouse/{names['catalogue']}"
        ),
        store=OneLakeDfsClient(),
        session=session,
    )
    if not result.succeeded:
        raise AssertionError(f"the performance catalogue was not built: {result}")
