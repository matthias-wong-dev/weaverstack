#!/usr/bin/env python3
"""Drive the stress estate generate.py wrote, and time what Weaver does to it.

    python examples/stress/run.py stress prepare    # create the Fabric items
    python examples/stress/run.py stress populate   # source: wipe, build, day 0
    python examples/stress/run.py stress perturb    # source: one more day
    python examples/stress/run.py stress measure    # estate: the timed cycle

``measure`` wipes the estate, builds it, loads it in full, builds and loads it
again with nothing changed, then perturbs the source and loads the estate once
per ``--cycles``. ``weaver test`` follows each load, so every cycle also shows
each load behaviour converged. Every step appends a line to ``results.jsonl``.

Only the estate is measured. Populating and perturbing the source is reported
on its own lines and never counted in the estate's timings.

Everything here is the public API. If this file needed an import from below
:mod:`weaver`, the public surface would be missing something.
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import tempfile
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import yaml

import weaver

SOURCE_ITEMS = ["Lakehouse/Source", "Warehouse/SourceWarehouse"]
ESTATE_ITEMS = ["Lakehouse/Lake", "Warehouse/Core", "Warehouse/Mart"]

#: Node outcomes that count as a failure in the results.
FAILED = frozenset({"failed", "blocked", "invalid"})


class Stress:
    def __init__(self, folder: Path, results: Path):
        self.folder = folder
        self.results = results
        self.plan = json.loads((folder / "plan.json").read_text(encoding="utf-8"))

    def config(self, name: str) -> dict:
        return yaml.safe_load((self.folder / f"{name}.yml").read_text(encoding="utf-8"))

    def session(self, name: str):
        return weaver.session(workspace_config=str(self.folder / f"{name}.yml"))

    def physical(self, name: str) -> list[str]:
        """Every physical item a configuration binds, its catalogue last."""

        config = self.config(name)
        bound = [
            f"{logical.split('/')[0]}/{physical}"
            for logical, physical in config["targets"].items()
        ]
        return [*bound, config["catalogue"]]

    def record(self, step: str, project: str, seconds: float, **facts) -> dict:
        line = {
            "at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "step": step,
            "project": project,
            "seconds": round(seconds, 1),
            "objects": self.plan[project]["objects"],
            "scale": self.plan["options"]["scale"],
            **facts,
        }
        with self.results.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(line) + "\n")
        detail = ", ".join(
            f"{key} {value}"
            for key, value in facts.items()
            if not isinstance(value, (dict, list))
        )
        print(f"{step:<28} {seconds:>9.1f}s  {detail}", flush=True)
        return line

    # --- the steps -------------------------------------------------------------

    def prepare(self) -> None:
        """Create any missing item. ``initialise`` creates one of each kind a call."""

        for name in ("source", "estate"):
            config = self.config(name)
            lakehouses = [
                physical
                for logical, physical in config["targets"].items()
                if logical.startswith("Lakehouse/")
            ]
            warehouses = [
                physical
                for logical, physical in config["targets"].items()
                if logical.startswith("Warehouse/")
            ]
            for position in range(max(len(lakehouses), len(warehouses))):
                with tempfile.TemporaryDirectory() as scratch:
                    started = time.perf_counter()
                    report = weaver.initialise(
                        Path(scratch) / "project",
                        workspace=config["workspace"],
                        catalogue=config["catalogue"],
                        environment=config["environment"],
                        lakehouse=_at(lakehouses, position),
                        warehouse=_at(warehouses, position),
                    )
                    self.record(
                        "prepare",
                        name,
                        time.perf_counter() - started,
                        items={o.name: o.status for o in report.resources},
                    )

    def wipe(self, name: str, session) -> None:
        started = time.perf_counter()
        weaver.wipe(self.physical(name), session=session)
        self.record("wipe", name, time.perf_counter() - started)

    def build(self, name: str, items: list[str], session, step: str = "build") -> None:
        started = time.perf_counter()
        built = weaver.build(str(self.folder / name), items=items, session=session)
        seconds = time.perf_counter() - started
        report = built.installation_report
        actions = getattr(report, "actions", None)
        self.record(
            step,
            name,
            seconds,
            status=built.status,
            actions=len(actions) if actions is not None else None,
            errors=[f"{e.action_id}: {e.message}" for e in built.errors][:20],
        )
        if not built.succeeded:
            raise SystemExit(f"{name} {step} {built.status}")

    def load(self, name: str, items: list[str], session, step: str) -> None:
        started = time.perf_counter()
        report = weaver.load(items, session=session, fault_tolerant=True)
        seconds = time.perf_counter() - started
        self.record(step, name, seconds, **_load_facts(report))

    def test(self, name: str, items: list[str], session, step: str) -> None:
        started = time.perf_counter()
        report = weaver.test(items, session=session)
        totals = report.totals()
        self.record(
            step,
            name,
            time.perf_counter() - started,
            status=report.status,
            passed=totals["passed"],
            failed=totals["failed"],
            invalid=totals["invalid"],
        )

    # --- the commands ------------------------------------------------------------

    def populate(self) -> None:
        with self.session("source") as session:
            self.wipe("source", session)
            self.build("source", SOURCE_ITEMS, session)
            self.load("source", SOURCE_ITEMS, session, "populate")

    def perturb(self, days: int) -> None:
        with self.session("source") as session:
            for _ in range(days):
                self.load("source", SOURCE_ITEMS, session, "perturb")

    def measure(self, cycles: int, keep: bool) -> None:
        with self.session("estate") as estate, self.session("source") as source:
            if not keep:
                self.wipe("estate", estate)
            self.build("estate", ESTATE_ITEMS, estate)
            self.load("estate", ESTATE_ITEMS, estate, "first load")
            self.test("estate", ESTATE_ITEMS, estate, "converged")
            self.build("estate", ESTATE_ITEMS, estate, step="unchanged build")
            self.load("estate", ESTATE_ITEMS, estate, "unchanged load")
            for cycle in range(1, cycles + 1):
                self.load("source", SOURCE_ITEMS, source, "perturb")
                self.load("estate", ESTATE_ITEMS, estate, f"day {cycle} load")
                self.test("estate", ESTATE_ITEMS, estate, f"day {cycle} converged")


def _at(values: list[str], position: int) -> str | None:
    return values[position] if position < len(values) else None


def _seconds(node) -> float | None:
    if not node.started_at or not node.finished_at:
        return None
    started = datetime.fromisoformat(node.started_at)
    finished = datetime.fromisoformat(node.finished_at)
    return (finished - started).total_seconds()


def _load_facts(report) -> dict:
    """What a load did, from its report: outcomes, rows and node times."""

    rows = Counter()
    for node in report.nodes:
        result = node.result
        if result is None:
            continue
        for count in ("rows_read", "rows_inserted", "rows_updated", "rows_deleted"):
            rows[count] += getattr(result, count, 0) or 0
    durations = sorted(
        seconds
        for node in report.nodes
        if node.executed and (seconds := _seconds(node)) is not None
    )

    def centile(share: float) -> float | None:
        if not durations:
            return None
        return round(durations[min(len(durations) - 1, int(share * len(durations)))], 2)

    return {
        "status": report.status,
        "nodes": len(report.nodes),
        "executed": sum(1 for node in report.nodes if node.executed),
        "failed": sum(1 for node in report.nodes if node.status in FAILED),
        **dict(rows),
        "node_seconds_median": round(statistics.median(durations), 2)
        if durations
        else None,
        "node_seconds_p95": centile(0.95),
        "node_seconds_max": centile(1.0),
        "by_kind": dict(Counter(node.primitive_kind for node in report.nodes)),
        "failures": [
            f"{node.node_id}: {message.message}"
            for node in report.nodes
            if node.status in FAILED
            for message in node.messages[:1]
        ][:20],
    }


def arguments(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("folder", type=Path, help="The folder generate.py wrote.")
    parser.add_argument(
        "command", choices=("prepare", "populate", "perturb", "measure")
    )
    parser.add_argument("--days", type=int, default=1, help="perturb: days to add.")
    parser.add_argument(
        "--cycles", type=int, default=2, help="measure: perturb cycles."
    )
    parser.add_argument(
        "--keep", action="store_true", help="measure: build onto what is there."
    )
    parser.add_argument(
        "--results", type=Path, help="JSON lines file. Default: results.jsonl beside."
    )
    return parser.parse_args(argv)


def main(argv=None) -> int:
    options = arguments(argv)
    stress = Stress(options.folder, options.results or options.folder / "results.jsonl")
    if options.command == "prepare":
        stress.prepare()
    elif options.command == "populate":
        stress.populate()
    elif options.command == "perturb":
        stress.perturb(options.days)
    else:
        stress.measure(options.cycles, options.keep)
    return 0


if __name__ == "__main__":
    sys.exit(main())
