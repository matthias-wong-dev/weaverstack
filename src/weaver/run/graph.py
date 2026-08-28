"""The inert runtime topology: nodes, dependency edges and selection order."""

from __future__ import annotations

from dataclasses import dataclass
from functools import cached_property
from typing import Mapping

from ..errors import GraphError
from ..graph import Graph
from .result import RunError


@dataclass(frozen=True)
class RunNode:
    """One unit of installed runtime work, or a barrier between two of them."""

    node_id: str
    physical_target: object
    primitive_kind: str
    logical_id: object | None = None
    physical_object: object | None = None
    #: The installed primitive itself, the procedure or the deployed file.
    #: ``None`` for a refresh, which is a capability rather than an artefact.
    primitive_id: object | None = None
    primitive_object: object | None = None
    #: What this node is for, where one graph carries more than one kind.
    role: str | None = None
    #: The installed thing this node runs, as the estate describes it. Opaque
    #: to the Runner: the Runner decides when a node
    #: runs, and only dispatch needs to know what it is.
    installed: object | None = None
    #: A publication barrier only: the Warehouse table it waits on, the consuming
    #: shortcut paths, and the load node that publishes what it waits for.
    publication_of: object | None = None
    publication_targets: tuple[object, ...] = ()
    produced_by: str | None = None

    @property
    def sort_key(self) -> tuple[str, str, str, str, str]:
        """What orders two nodes that became ready at the same moment.

        Target kind, then target, then logical identity, then primitive kind, so
        the order a graph prints is a property of the estate rather than of the
        dictionary iteration that built it.

        ``node_id`` breaks the last tie. Without it, two nodes alike in all four
        fall back to the order the graph happened to be built in, which is the
        very thing the sort exists to remove, and a run whose order depends on
        construction is one a log cannot reproduce.
        """

        return (
            getattr(self.physical_target, "kind", ""),
            getattr(self.physical_target, "name", str(self.physical_target)),
            str(self.logical_id or ""),
            self.primitive_kind,
            self.node_id,
        )


@dataclass(frozen=True)
class RunGraph:
    """The selected runtime graph: nodes, edges and what was requested."""

    nodes: tuple[RunNode, ...] = ()
    edges: tuple[tuple[str, str], ...] = ()
    requested: tuple = ()
    messages: tuple = ()

    @property
    def by_id(self) -> Mapping[str, RunNode]:
        return {node.node_id: node for node in self.nodes}

    @cached_property
    def topology(self) -> Graph:
        """The generic topology over these nodes, built once and kept.

        Built on first use, so a cycle surfaces where a run would meet it.
        """

        try:
            return Graph((node.node_id for node in self.nodes), self.edges)
        except GraphError as exc:
            raise RunError(f"the run graph contains a cycle: {exc}") from None

    def upstream(self, node_id: str) -> frozenset[str]:
        return frozenset(self.topology.upstream_of(node_id))

    def descendants(self, node_id: str) -> frozenset[str]:
        """Every node that may not run once ``node_id`` has failed."""

        return frozenset(self.topology.descendants(node_id))

    def order(self) -> tuple[RunNode, ...]:
        """The deterministic topological order, or a refusal if there is a cycle.

        Ties break on :attr:`RunNode.sort_key`, so a run reads target by
        target and a log reproduces.
        """

        found = self.by_id
        return tuple(
            found[node_id]
            for node_id in self.topology.order(key=lambda node: found[node].sort_key)
        )


def graph_for(request, state) -> RunGraph:
    """The graph one request implies against one observed estate.

    Selection is where the kinds of run differ, and it is the only place they
    do: what runs is a different question per kind, how a run behaves is not.
    """

    from .runner import LOAD, TEST

    if request.kind == LOAD:
        return _load_graph(request, state)
    if request.kind == TEST:
        return _test_graph(request, state)
    raise RunError(f"no selection rule for a {request.kind!r} run")


def _load_graph(request, state) -> RunGraph:
    from ..load_plan import load_dag

    dag = load_dag(state.catalogue.dag(), items=request.items, names=request.names)
    return RunGraph(
        nodes=tuple(
            RunNode(
                node_id=node.node_id,
                physical_target=node.physical_target,
                primitive_kind=node.primitive_kind,
                logical_id=node.logical_id,
                physical_object=node.physical_object,
                primitive_id=node.primitive_id,
                primitive_object=node.primitive_object,
                publication_of=node.publication_of,
                publication_targets=node.publication_targets,
                produced_by=node.produced_by,
                role="load",
            )
            for node in dag.nodes
        ),
        edges=dag.edges,
        requested=dag.requested,
        messages=dag.messages,
    )


def _test_graph(request, state) -> RunGraph:
    from ..test_execution import primitive_kind
    from ..test_plan import ValidationEstate, validation_order

    estate = ValidationEstate.from_catalogue(state.catalogue)
    if request.name is not None:
        selected = (estate.named(request.name, request.items),)
    else:
        selected = validation_order(estate.for_items(request.items))
    return RunGraph(
        nodes=tuple(
            RunNode(
                node_id=str(validation.logical),
                physical_target=validation.target,
                primitive_kind=primitive_kind(validation),
                logical_id=validation.logical,
                primitive_id=validation.artefact,
                role=validation.kind,
                installed=validation,
            )
            for validation in selected
        ),
        # Validations are independent of one another by construction: each reads
        # the estate and reports, and none produces what another consumes. An
        # ordering exists for reporting, not for readiness.
        edges=(),
        requested=tuple(request.items),
    )


__all__ = ["RunGraph", "RunNode", "graph_for"]
