"""Dependency graph primitives.

The graph knows nothing about what an edge *means*. That is deliberate,
because there is more than one graph over the same objects:

**Load order** follows every dependency. To load ``Reporting.OrderReport`` you
first need the rows in ``Sales.Order``.

**Build order** is nearly flat. Building a Folder is a directory; building a
Delta table is a ``CREATE`` from its declared ``Schema`` — neither needs a
single upstream object to exist. Only a Warehouse object has build
dependencies, because its shape is inferred from its query. So a build is every
Folder and every Delta table in one parallel wave, then the Warehouse objects in
order, with a SQL endpoint refresh where the first of them reads Delta.

Both are the same machinery over different edge sets, so this module takes
nodes and edges and answers ordering questions about them.

Order is deterministic: ties are broken by name, so the same repository always
produces the same plan and two plans can be diffed.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Iterable, Mapping

from ..errors import GraphError


@dataclass(frozen=True)
class Edge:
    """``upstream`` must happen before ``downstream``."""

    upstream: str
    downstream: str

    def __str__(self) -> str:
        return f"{self.upstream} -> {self.downstream}"


class Graph:
    """A directed acyclic graph over named nodes."""

    def __init__(self, nodes: Iterable[str], edges: Iterable[tuple[str, str]] = ()) -> None:
        self._nodes = tuple(sorted(set(nodes)))
        known = set(self._nodes)

        seen: set[tuple[str, str]] = set()
        collected: list[Edge] = []
        for upstream, downstream in edges:
            if upstream not in known:
                raise GraphError(f"edge from unknown node {upstream!r}")
            if downstream not in known:
                raise GraphError(f"edge to unknown node {downstream!r}")
            if upstream == downstream:
                raise GraphError(f"{upstream} depends on itself")
            if (upstream, downstream) in seen:
                continue
            seen.add((upstream, downstream))
            collected.append(Edge(upstream=upstream, downstream=downstream))
        self._edges = tuple(sorted(collected, key=lambda edge: (edge.upstream, edge.downstream)))

        self._downstream: Mapping[str, list[str]] = defaultdict(list)
        self._upstream: Mapping[str, list[str]] = defaultdict(list)
        for edge in self._edges:
            self._downstream[edge.upstream].append(edge.downstream)
            self._upstream[edge.downstream].append(edge.upstream)

        # Fail on construction: an unorderable graph is not a graph worth holding.
        self._order = self._topological_order()

    @property
    def nodes(self) -> tuple[str, ...]:
        return self._nodes

    @property
    def edges(self) -> tuple[Edge, ...]:
        return self._edges

    def __len__(self) -> int:
        return len(self._nodes)

    def __contains__(self, node: str) -> bool:
        return node in set(self._nodes)

    def _require(self, node: str) -> None:
        if node not in set(self._nodes):
            raise GraphError(f"unknown node: {node!r}")

    def upstream_of(self, node: str) -> tuple[str, ...]:
        """What this node depends on directly."""

        self._require(node)
        return tuple(sorted(self._upstream[node]))

    def downstream_of(self, node: str) -> tuple[str, ...]:
        """What depends on this node directly."""

        self._require(node)
        return tuple(sorted(self._downstream[node]))

    def roots(self) -> tuple[str, ...]:
        """Nodes that depend on nothing."""

        return tuple(node for node in self._nodes if not self._upstream[node])

    def leaves(self) -> tuple[str, ...]:
        """Nodes nothing depends on."""

        return tuple(node for node in self._nodes if not self._downstream[node])

    # --- ordering ---------------------------------------------------------

    def order(self) -> tuple[str, ...]:
        """Every node, upstream before downstream, ties broken by name."""

        return self._order

    def layers(self) -> tuple[tuple[str, ...], ...]:
        """Waves that may run in parallel.

        Everything in a layer depends only on earlier layers, so a layer can be
        dispatched together and joined before the next begins.
        """

        depth: dict[str, int] = {}
        for node in self._order:
            parents = self._upstream[node]
            depth[node] = max((depth[parent] + 1 for parent in parents), default=0)

        grouped: dict[int, list[str]] = defaultdict(list)
        for node, level in depth.items():
            grouped[level].append(node)
        return tuple(tuple(sorted(grouped[level])) for level in sorted(grouped))

    def _topological_order(self) -> tuple[str, ...]:
        remaining = {node: len(self._upstream[node]) for node in self._nodes}
        ready = sorted(node for node, count in remaining.items() if count == 0)
        ordered: list[str] = []

        while ready:
            node = ready.pop(0)
            ordered.append(node)
            for child in sorted(self._downstream[node]):
                remaining[child] -= 1
                if remaining[child] == 0:
                    ready.append(child)
            ready.sort()

        if len(ordered) != len(self._nodes):
            cycle = self._find_cycle(set(self._nodes) - set(ordered))
            raise GraphError("dependency cycle: " + " -> ".join(cycle))
        return tuple(ordered)

    def _find_cycle(self, candidates: set[str]) -> list[str]:
        """One concrete cycle, so the message names the objects involved."""

        path: list[str] = []
        on_path: set[str] = set()
        seen: set[str] = set()

        def walk(node: str) -> list[str] | None:
            if node in on_path:
                return path[path.index(node):] + [node]
            if node in seen:
                return None
            seen.add(node)
            on_path.add(node)
            path.append(node)
            for child in sorted(self._downstream[node]):
                if child not in candidates:
                    continue
                found = walk(child)
                if found is not None:
                    return found
            path.pop()
            on_path.discard(node)
            return None

        for start in sorted(candidates):
            found = walk(start)
            if found is not None:
                return found
        return sorted(candidates)

    # --- traversal --------------------------------------------------------

    def descendants(self, node: str) -> tuple[str, ...]:
        """Everything reachable downstream, in dependency order.

        This is what a rebuild must uncertify: an object whose upstream
        definition is being rebuilt cannot stay certified.
        """

        return self._reach(node, self._downstream)

    def ancestors(self, node: str) -> tuple[str, ...]:
        """Everything reachable upstream, in dependency order."""

        return self._reach(node, self._upstream)

    def _reach(self, node: str, adjacency: Mapping[str, list[str]]) -> tuple[str, ...]:
        self._require(node)
        found: set[str] = set()
        pending = list(adjacency[node])
        while pending:
            current = pending.pop()
            if current in found:
                continue
            found.add(current)
            pending.extend(adjacency[current])
        return tuple(candidate for candidate in self._order if candidate in found)

    def subgraph(
        self,
        selection: Iterable[str],
        *,
        with_ancestors: bool = False,
        with_descendants: bool = False,
    ) -> "Graph":
        """A graph over a selection, optionally expanded along dependencies."""

        chosen: set[str] = set()
        for node in selection:
            self._require(node)
            chosen.add(node)
            if with_ancestors:
                chosen.update(self.ancestors(node))
            if with_descendants:
                chosen.update(self.descendants(node))
        return Graph(
            chosen,
            [
                (edge.upstream, edge.downstream)
                for edge in self._edges
                if edge.upstream in chosen and edge.downstream in chosen
            ],
        )
