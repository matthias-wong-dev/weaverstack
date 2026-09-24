#!/usr/bin/env python3
"""Generate and measure a deterministic wide Weaver estate."""

from __future__ import annotations

import argparse
import json
import tempfile
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter
from typing import Iterable

GENERATOR_VERSION = 1
NODES_PER_BRANCH = 25
ITEM = "Lakehouse/Benchmark"
WAREHOUSE_ITEM = "Warehouse/Benchmark"
SCHEMA = "Scale"


@dataclass(frozen=True)
class WideEstateSpec:
    """The stable inputs to one generated estate."""

    branches: int = 10
    seed: int = 20260924
    engine: str = "lakehouse"

    def __post_init__(self) -> None:
        if self.branches < 1:
            raise ValueError("branches must be positive")
        if self.engine not in {"lakehouse", "warehouse"}:
            raise ValueError("engine must be 'lakehouse' or 'warehouse'")

    @property
    def item(self) -> str:
        return ITEM if self.engine == "lakehouse" else WAREHOUSE_ITEM

    @classmethod
    def from_objects(
        cls,
        objects: int,
        *,
        seed: int = 20260924,
        engine: str = "lakehouse",
    ) -> "WideEstateSpec":
        if objects < 2 * NODES_PER_BRANCH:
            raise ValueError("a benchmark needs at least 50 objects in two branches")
        if objects % NODES_PER_BRANCH:
            raise ValueError(f"objects must be divisible by {NODES_PER_BRANCH}")
        return cls(
            branches=objects // NODES_PER_BRANCH,
            seed=seed,
            engine=engine,
        )


@dataclass(frozen=True)
class EstateNode:
    """One generated declaration and its independently stated parents."""

    identity: str
    object_id: str
    kind: str
    branch: int
    role: str
    dependencies: tuple[str, ...] = ()


@dataclass(frozen=True)
class EstateTopology:
    """Declarative source topology, separate from Weaver's parsed graph."""

    spec: WideEstateSpec
    nodes: tuple[EstateNode, ...]

    @property
    def identities(self) -> tuple[str, ...]:
        return tuple(node.identity for node in self.nodes)

    @property
    def edges(self) -> tuple[tuple[str, str], ...]:
        return tuple(
            (dependency, node.identity)
            for node in self.nodes
            for dependency in node.dependencies
        )

    def statistics(self) -> dict[str, int]:
        incoming = Counter(downstream for _upstream, downstream in self.edges)
        outgoing = Counter(upstream for upstream, _downstream in self.edges)
        depth: dict[str, int] = {}
        for node in self.nodes:
            depth[node.identity] = max(
                (depth[parent] + 1 for parent in node.dependencies), default=0
            )
        return {
            "objects": len(self.nodes),
            "edges": len(self.edges),
            "maximum_depth": max(depth.values(), default=0),
            "independent_branches": self.spec.branches,
            "maximum_fan_in": max(incoming.values(), default=0),
            "maximum_fan_out": max(outgoing.values(), default=0),
        }

    def distributions(self) -> dict[str, dict[str, int]]:
        incoming = Counter(downstream for _upstream, downstream in self.edges)
        outgoing = Counter(upstream for upstream, _downstream in self.edges)
        fan_in = Counter(incoming.get(identity, 0) for identity in self.identities)
        fan_out = Counter(outgoing.get(identity, 0) for identity in self.identities)
        return {
            "fan_in": {str(value): count for value, count in sorted(fan_in.items())},
            "fan_out": {str(value): count for value, count in sorted(fan_out.items())},
        }


def _identity(branch: int, role: str, *, item: str = ITEM) -> str:
    area = "/Tables" if item.startswith("Lakehouse/") else ""
    return f"{item}{area}/{SCHEMA}.B{branch:03d}{role}"


def _node(
    branch: int,
    role: str,
    kind: str,
    *parents: str,
    item: str = ITEM,
) -> EstateNode:
    object_id = f"{SCHEMA}.B{branch:03d}{role}"
    return EstateNode(
        identity=_identity(branch, role, item=item),
        object_id=object_id,
        kind=kind,
        branch=branch,
        role=role,
        dependencies=tuple(_identity(branch, parent, item=item) for parent in parents),
    )


def make_topology(spec: WideEstateSpec) -> EstateTopology:
    """Build repeated 25-node motifs with no cross-branch edges."""

    nodes: list[EstateNode] = []

    def node(branch: int, role: str, kind: str, *parents: str) -> EstateNode:
        return _node(branch, role, kind, *parents, item=spec.item)

    for branch in range(spec.branches):
        nodes.append(node(branch, "Root", "table"))

        parent = "Root"
        for index in range(5):
            role = f"Chain{index:02d}"
            nodes.append(node(branch, role, "view", parent))
            parent = role

        for index in range(10):
            nodes.append(node(branch, f"Fan{index:02d}", "view", "Root"))

        base_groups = ((0, 1, 2), (3, 4, 5), (6, 7), (8, 9))
        offset = (spec.seed + branch) % 10
        groups = tuple(
            tuple((member + offset) % 10 for member in group) for group in base_groups
        )
        for index, group in enumerate(groups):
            nodes.append(
                node(
                    branch,
                    f"Merge{index:02d}",
                    "view",
                    *(f"Fan{member:02d}" for member in group),
                )
            )

        nodes.append(
            node(
                branch,
                "Bridge",
                "view",
                "Chain04",
                *(f"Merge{index:02d}" for index in range(4)),
            )
        )
        for index in range(4):
            nodes.append(node(branch, f"Leaf{index:02d}", "view", "Bridge"))

    return EstateTopology(spec=spec, nodes=tuple(nodes))


def descendants(topology: EstateTopology, identity: str) -> tuple[str, ...]:
    """Walk the declarative edges without consulting Weaver's graph."""

    known = set(topology.identities)
    if identity not in known:
        raise ValueError(f"unknown generated identity: {identity}")
    downstream: dict[str, list[str]] = defaultdict(list)
    for upstream, child in topology.edges:
        downstream[upstream].append(child)
    found: set[str] = set()
    pending = list(downstream[identity])
    while pending:
        current = pending.pop()
        if current in found:
            continue
        found.add(current)
        pending.extend(downstream[current])
    return tuple(candidate for candidate in topology.identities if candidate in found)


def _schema_source() -> str:
    return "Schema ID: Scale\nDescription: Synthetic scale benchmark objects.\n"


def _table_source(node: EstateNode) -> str:
    class_name = node.object_id.replace(".", "__")
    return f'''\
"""
Table ID: {node.object_id}
Description: A synthetic scale benchmark root.
Lineage: Generated benchmark input.
Primary key: EntityId
Schema:
  EntityId: integer
"""
from weaver import Table


class {class_name}(Table):
    def read(self):
        return [], []
'''


def _warehouse_table_source(node: EstateNode) -> str:
    return f"""\
/*
Table ID: {node.object_id}
Description: A synthetic scale benchmark root.
Lineage: Generated benchmark input.
Primary key: EntityId
*/
select cast(1 as bigint) as EntityId
"""


def _view_source(node: EstateNode) -> str:
    dependencies = "\n".join(
        f"  - {dependency.rsplit('/', 1)[-1]}" for dependency in node.dependencies
    )
    return f"""\
/*
View ID: {node.object_id}
Description: A synthetic scale benchmark view.
Lineage: Generated benchmark dependencies.
Dependencies:
{dependencies}
*/
select 1 as EntityId
"""


def write_estate(root: Path, topology: EstateTopology) -> None:
    """Materialise one source estate without deleting or absorbing prior files."""

    root = Path(root)
    if root.exists() and any(root.iterdir()):
        raise ValueError(f"estate directory is not empty: {root}")
    item_root = root / topology.spec.item
    schema_path = item_root / "schemas" / f"{SCHEMA}.yml"
    schema_path.parent.mkdir(parents=True, exist_ok=True)
    schema_path.write_text(_schema_source(), encoding="utf-8")
    objects = item_root / "Tables" if topology.spec.engine == "lakehouse" else item_root
    objects.mkdir(parents=True, exist_ok=True)
    for node in topology.nodes:
        suffix = (
            ".py"
            if node.kind == "table" and topology.spec.engine == "lakehouse"
            else ".sql"
        )
        filename = (
            node.object_id.replace(".", "__", 1)
            if node.kind == "table" and topology.spec.engine == "lakehouse"
            else node.object_id
        )
        source = (
            _table_source(node)
            if node.kind == "table" and topology.spec.engine == "lakehouse"
            else _warehouse_table_source(node)
            if node.kind == "table"
            else _view_source(node)
        )
        (objects / f"{filename}{suffix}").write_text(source, encoding="utf-8")


def parse_generated_estate(root: Path):
    """Parse generated files through Weaver's ordinary repository boundary."""

    from weaver.declaration import parse_item_repository
    from weaver.locations import Location

    return parse_item_repository(Location(str(root)))


def _repository_graph_matches(repository, topology: EstateTopology) -> bool:
    graph = repository.dependency_graph
    if graph is None:
        return False
    generated = set(topology.identities)
    actual_nodes = set(graph.nodes) & generated
    actual_edges = {
        (edge.upstream, edge.downstream)
        for edge in graph.edges
        if edge.upstream in generated or edge.downstream in generated
    }
    return actual_nodes == generated and actual_edges == set(topology.edges)


def _changed_registry(registered, roots: Iterable, *, stale_signature: str):
    from weaver.catalogue.state import RegisteredDocument

    changed = dict(registered)
    for root in roots:
        original = changed[root]
        changed[root] = RegisteredDocument(
            identity=original.identity,
            object_type=original.object_type,
            object_role=original.object_role,
            signature=stale_signature,
            build_datetime=original.build_datetime,
        )
    return changed


def benchmark_estate(root: Path, spec: WideEstateSpec) -> dict:
    """Generate, parse and compare six impact scenarios with the oracle."""

    from weaver.build_bundle.incremental import declared_signatures, determine_impact
    from weaver.catalogue.state import RegisteredDocument
    from weaver.declaration.model import WeaverDocumentId

    total_started = perf_counter()
    topology_started = perf_counter()
    topology = make_topology(spec)
    topology_seconds = perf_counter() - topology_started

    write_started = perf_counter()
    write_estate(root, topology)
    write_seconds = perf_counter() - write_started

    parse_started = perf_counter()
    repository = parse_generated_estate(root)
    parse_seconds = perf_counter() - parse_started
    if not _repository_graph_matches(repository, topology):
        raise AssertionError(
            "parsed Weaver graph differs from the independent topology"
        )

    identities = {
        identity: WeaverDocumentId.parse(identity) for identity in topology.identities
    }
    selected = set(identities.values())
    signature_started = perf_counter()
    signatures = declared_signatures(repository, selected)
    signature_seconds = perf_counter() - signature_started

    by_identity = {node.identity: node for node in topology.nodes}
    registered = {
        identity: RegisteredDocument(
            identity=identity,
            object_type=by_identity[text].kind,
            object_role="data",
            signature=signatures[identity],
            build_datetime=None,
        )
        for text, identity in identities.items()
    }
    physical_types = {identities[text]: node.kind for text, node in by_identity.items()}

    scenarios = (
        ("full_cold_build", (), True),
        ("unchanged_build", (), False),
        ("leaf_change", (_identity(0, "Leaf00", item=spec.item),), False),
        ("mid_graph_change", (_identity(0, "Chain02", item=spec.item),), False),
        ("high_fan_out_change", (_identity(0, "Root", item=spec.item),), False),
        (
            "independent_branch_changes",
            (
                _identity(0, "Leaf00", item=spec.item),
                _identity(1, "Root", item=spec.item),
            ),
            False,
        ),
    )
    prepared = []
    for name, root_names, cold in scenarios:
        root_ids = tuple(identities[value] for value in root_names)
        scenario_registered = (
            {}
            if cold
            else _changed_registry(
                registered, root_ids, stale_signature=f"stale-{name}"
            )
        )
        expected_descendants = {
            candidate
            for root_name in root_names
            for candidate in descendants(topology, root_name)
        }
        expected_selected = (
            set(topology.identities) if cold else set(root_names) | expected_descendants
        )
        prepared.append(
            {
                "name": name,
                "root_names": root_names,
                "registered": scenario_registered,
                "physical_types": {} if cold else physical_types,
                "expected_descendants": expected_descendants,
                "expected_selected": expected_selected,
                "cold": cold,
            }
        )

    def evaluate(scenario):
        impact = determine_impact(
            repository,
            scenario["registered"],
            selected=selected,
            physical_types=scenario["physical_types"],
        )
        actual_descendants = {str(value) for value in impact.impacted_descendants}
        actual_selected = {str(value) for value in (*impact.new, *impact.impacted)}
        matches = (
            actual_selected == scenario["expected_selected"]
            and actual_descendants == scenario["expected_descendants"]
        )
        return actual_descendants, actual_selected, matches

    for scenario in prepared:
        _actual_descendants, _actual_selected, matches = evaluate(scenario)
        if not matches:
            raise AssertionError(
                f"{scenario['name']} differs from the independent oracle before timing"
            )

    results = []
    for scenario in prepared:
        started = perf_counter()
        actual_descendants, actual_selected, matches = evaluate(scenario)
        elapsed = perf_counter() - started
        if not matches:
            raise AssertionError(
                f"{scenario['name']} changed after validation; timing was discarded"
            )
        root_names = scenario["root_names"]
        touched_branches = {
            by_identity[identity].branch for identity in actual_selected
        }
        intended_branches = (
            set(range(spec.branches))
            if scenario["cold"]
            else {by_identity[root].branch for root in root_names}
        )
        results.append(
            {
                "name": scenario["name"],
                "seconds": elapsed,
                "changed_roots": list(root_names),
                "expected_descendant_set": sorted(scenario["expected_descendants"]),
                "actual_descendant_set": sorted(actual_descendants),
                "actual_affected_set": sorted(actual_selected),
                "selected_object_count": len(actual_selected),
                "expected_selected_object_count": len(scenario["expected_selected"]),
                "matches_oracle": True,
                "physical_actions_executed": 0,
                "unrelated_branches_untouched": touched_branches <= intended_branches,
            }
        )

    return {
        "generator": {
            "version": GENERATOR_VERSION,
            "seed": spec.seed,
            "engine": spec.engine,
            "nodes_per_branch": NODES_PER_BRANCH,
        },
        "topology": {**topology.statistics(), **topology.distributions()},
        "topology_oracle_matches_repository": True,
        "timings": {
            "topology_seconds": topology_seconds,
            "source_write_seconds": write_seconds,
            "parse_seconds": parse_seconds,
            "signature_seconds": signature_seconds,
            "total_seconds": perf_counter() - total_started,
        },
        "scenarios": results,
        "all_scenarios_match": all(result["matches_oracle"] for result in results),
    }


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--objects", type=int, default=250)
    parser.add_argument("--seed", type=int, default=20260924)
    parser.add_argument(
        "--engine", choices=("lakehouse", "warehouse"), default="lakehouse"
    )
    parser.add_argument("--estate-dir", type=Path)
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


def _run(arguments: argparse.Namespace) -> dict:
    spec = WideEstateSpec.from_objects(
        arguments.objects,
        seed=arguments.seed,
        engine=arguments.engine,
    )
    if arguments.estate_dir is not None:
        return benchmark_estate(arguments.estate_dir, spec)
    with tempfile.TemporaryDirectory(prefix="weaver-wide-estate-") as directory:
        return benchmark_estate(Path(directory), spec)


def main() -> int:
    arguments = _arguments()
    result = _run(arguments)
    rendered = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if arguments.output is not None:
        arguments.output.parent.mkdir(parents=True, exist_ok=True)
        arguments.output.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0 if result["all_scenarios_match"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
