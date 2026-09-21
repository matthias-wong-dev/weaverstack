"""Deterministic directed-acyclic-graph operations."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Iterable, Mapping

from .errors import GraphError


@dataclass(frozen=True)
class Edge:
    """``upstream`` must happen before ``downstream``."""

    upstream: str
    downstream: str

    def __str__(self) -> str:
        return f"{self.upstream} -> {self.downstream}"


class Graph:
    """A directed acyclic graph over named nodes."""

    def __init__(
        self, nodes: Iterable[str], edges: Iterable[tuple[str, str]] = ()
    ) -> None:
        self._nodes = tuple(sorted(set(nodes)))
        # Impact expansion tests membership once per changed root.
        self._known = frozenset(self._nodes)
        known = self._known

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
        self._edges = tuple(
            sorted(collected, key=lambda edge: (edge.upstream, edge.downstream))
        )

        self._downstream: Mapping[str, list[str]] = defaultdict(list)
        self._upstream: Mapping[str, list[str]] = defaultdict(list)
        for edge in self._edges:
            self._downstream[edge.upstream].append(edge.downstream)
            self._upstream[edge.downstream].append(edge.upstream)

        # Construction rejects cycles, so every held graph is orderable.
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
        return node in self._known

    def _require(self, node: str) -> None:
        if node not in self._known:
            raise GraphError(f"unknown node: {node!r}")

    def upstream_of(self, node: str) -> tuple[str, ...]:
        self._require(node)
        return tuple(sorted(self._upstream[node]))

    def downstream_of(self, node: str) -> tuple[str, ...]:
        self._require(node)
        return tuple(sorted(self._downstream[node]))

    def roots(self) -> tuple[str, ...]:
        return tuple(node for node in self._nodes if not self._upstream[node])

    def leaves(self) -> tuple[str, ...]:
        return tuple(node for node in self._nodes if not self._downstream[node])

    # --- ordering ---------------------------------------------------------

    def order(self, key=None) -> tuple[str, ...]:
        """Every node, upstream before downstream, ties broken by name.

        ``key`` maps a node to what breaks a tie, for a caller whose nodes
        carry an order of their own. A load dispatches by physical target before
        logical name, so its plan reads target by target.
        """

        return self._order if key is None else self._topological_order(key)

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

    def _topological_order(self, key=None) -> tuple[str, ...]:
        """Take ready nodes in a settled order for reproducible plans and logs."""

        rank = (lambda node: node) if key is None else key
        remaining = {node: len(self._upstream[node]) for node in self._nodes}
        ready = sorted(
            (node for node, count in remaining.items() if count == 0), key=rank
        )
        ordered: list[str] = []

        while ready:
            node = ready.pop(0)
            ordered.append(node)
            for child in sorted(self._downstream[node]):
                remaining[child] -= 1
                if remaining[child] == 0:
                    ready.append(child)
            ready.sort(key=rank)

        if len(ordered) != len(self._nodes):
            cycle = self._find_cycle(set(self._nodes) - set(ordered))
            raise GraphError("dependency cycle: " + " -> ".join(cycle))
        return tuple(ordered)

    def _find_cycle(self, candidates: set[str]) -> list[str]:
        """One cycle among the nodes ordering could not place.

        The walk carries its own stack. A residual component is as deep as the
        project that declared it, and a diagnostic that ran out of interpreter
        stack would replace the cycle a user has to fix with a crash inside the
        code that was going to name it.

        Roots and children are taken in sorted order, so the same repository
        always reports the same cycle.
        """

        seen: set[str] = set()
        for start in sorted(candidates):
            if start in seen:
                continue
            # The active path, and where each node on it sits, so a back edge is
            # sliced rather than searched for.
            path: list[str] = []
            depth: dict[str, int] = {}
            # Each frame is a node and the children it has left to try, reversed
            # so taking the next one is a pop from the end.
            stack: list[tuple[str, list[str]]] = []
            self._descend(start, candidates, path, depth, stack, seen)
            while stack:
                node, remaining = stack[-1]
                if not remaining:
                    stack.pop()
                    del depth[path.pop()]
                    continue
                child = remaining.pop()
                if child in depth:
                    return path[depth[child] :] + [child]
                if child in seen:
                    continue
                self._descend(child, candidates, path, depth, stack, seen)
        return sorted(candidates)

    def _descend(self, node, candidates, path, depth, stack, seen) -> None:
        """Put ``node`` on the active path with the children it may still try."""

        seen.add(node)
        depth[node] = len(path)
        path.append(node)
        stack.append(
            (
                node,
                sorted(
                    (child for child in self._downstream[node] if child in candidates),
                    reverse=True,
                ),
            )
        )

    # --- traversal --------------------------------------------------------

    def descendants(self, node: str) -> tuple[str, ...]:
        """Everything reachable downstream, in dependency order."""

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
