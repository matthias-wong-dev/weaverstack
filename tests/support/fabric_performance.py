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
    WAREHOUSE: {"build": 6 * 60, "wipe": 60, "mirror": 3 * 60},
    LAKEHOUSE: {"build": 10 * 60, "wipe": 2 * 60, "mirror": 5 * 60},
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


def _build_detail(result) -> dict:
    report = result.installation_report
    if report is None:
        return {"actions": 0}
    by_executor: dict[str, list[float]] = defaultdict(list)
    for action in report.action_results():
        by_executor[action.executor].append(action.duration_seconds or 0.0)
    detail = {"actions": sum(len(v) for v in by_executor.values())}
    for executor, durations in sorted(
        by_executor.items(), key=lambda pair: -sum(pair[1])
    )[:6]:
        detail[executor] = (
            f"{len(durations)} actions, {sum(durations):.1f}s busy, "
            f"max {max(durations):.1f}s"
        )
    if result.errors:
        detail["errors"] = "; ".join(e.describe() for e in result.errors)[:600]
    return detail


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
    operations=("build", "noop", "mirror", "wipe"),
) -> EstateRun:
    """Empty the performance items, then time each operation in order."""

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
                    ),
                    _build_detail,
                )
            elif operation == "mirror":
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
                    ),
                )
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
