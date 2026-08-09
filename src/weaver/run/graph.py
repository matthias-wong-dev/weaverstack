"""RunGraph — the runtime topology, inspectable before anything runs.

What the Runner decided should run, in what order, and what each node waits for.
An edge means *the upstream node must complete successfully before the
downstream node may execute*, and nothing else: it is not a data-flow statement
and not a claim about what the downstream node reads.

This is the concept ``LoadPlan`` used to be, with one difference that matters.
A LoadPlan owned the runtime lifecycle as well as the topology, so "what should
run" and "what happened to it" lived in the same object and drifted into each
other. Here the graph is inert — nodes, edges, and the order they imply — and
every piece of state that changes during a run belongs to the Runner.

It is also no longer specific to loading. A node is a unit of installed runtime
work: a table load, a folder load, a Warehouse procedure, a validation, an
endpoint refresh treated as a barrier. What differs between them is the
primitive that runs and the rule that selected them, neither of which the graph
needs to know.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Sequence

from ..errors import LoadError


@dataclass(frozen=True)
class RunNode:
    """One unit of installed runtime work, or a barrier between two of them."""

    node_id: str
    physical_target: object
    primitive_kind: str
    logical_id: object | None = None
    physical_object: object | None = None
    #: The installed primitive itself — the procedure or the deployed file.
    #: ``None`` for a refresh, which is a capability rather than an artefact.
    primitive_id: object | None = None
    primitive_object: object | None = None
    #: What this node is for, where one graph carries more than one kind.
    role: str | None = None

    @property
    def sort_key(self) -> tuple[str, str, str, str, str]:
        """What orders two nodes that became ready at the same moment.

        Target kind, then target, then logical identity, then primitive kind —
        so the order a graph prints is a property of the estate rather than of
        the dictionary iteration that happened to build it.

        ``node_id`` breaks the last tie. Without it, two nodes alike in all four
        fall back to the order the graph happened to be built in, which is the
        very thing the sort exists to remove — and a run whose order depends on
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

    def upstream(self, node_id: str) -> frozenset[str]:
        return frozenset(
            upstream for upstream, downstream in self.edges if downstream == node_id
        )

    def descendants(self, node_id: str) -> frozenset[str]:
        """Every node that may not run once ``node_id`` has failed."""

        reached: set[str] = set()
        frontier = [node_id]
        while frontier:
            current = frontier.pop()
            for upstream, downstream in self.edges:
                if upstream == current and downstream not in reached:
                    reached.add(downstream)
                    frontier.append(downstream)
        return frozenset(reached)

    def order(self) -> tuple[RunNode, ...]:
        """The deterministic topological order, or a refusal if there is a cycle.

        Ready nodes are sorted rather than taken as they come, which is what
        makes a dry run inspectable, a log reproducible and a test stable. The
        cycle check is here rather than in a validator because the sort is the
        one place that can see one.
        """

        remaining = {node.node_id: node for node in self.nodes}
        pending = {
            node_id: set(self.upstream(node_id)) & set(remaining)
            for node_id in remaining
        }
        ordered: list[RunNode] = []
        while pending:
            ready = sorted(
                (
                    remaining[node_id]
                    for node_id, waiting in pending.items()
                    if not waiting
                ),
                key=lambda node: node.sort_key,
            )
            if not ready:
                cycle = ", ".join(sorted(pending))
                raise LoadError(f"the run graph contains a cycle among: {cycle}")
            for node in ready:
                ordered.append(node)
                del pending[node.node_id]
            done = {node.node_id for node in ready}
            for waiting in pending.values():
                waiting -= done
        return tuple(ordered)


def graph_for(request, state) -> RunGraph:
    """The graph one request implies against one observed estate.

    Selection is where the kinds of run differ, and it is the *only* place they
    do: what runs is a different question per kind, how a run behaves is not.
    """

    from .request import LOAD, TEST

    if request.kind == LOAD:
        return _load_graph(request, state)
    if request.kind == TEST:
        return _test_graph(request, state)
    raise LoadError(f"no selection rule for a {request.kind!r} run")


def _load_graph(request, state) -> RunGraph:
    from ..load_plan import InstalledEstate, load_dag

    dag = load_dag(
        InstalledEstate.from_catalogue(state.catalogue), targets=request.targets
    )
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
        selected = (estate.named(request.name, request.targets),)
    else:
        selected = validation_order(estate.for_targets(request.targets))
    return RunGraph(
        nodes=tuple(
            RunNode(
                node_id=str(validation.logical),
                physical_target=validation.target,
                primitive_kind=primitive_kind(validation),
                logical_id=validation.logical,
                primitive_id=validation.artefact,
                role=validation.kind,
            )
            for validation in selected
        ),
        # Validations are independent of one another by construction: each reads
        # the estate and reports, and none produces what another consumes. An
        # ordering exists for reporting, not for readiness.
        edges=(),
        requested=tuple(request.targets),
    )


__all__ = ["RunGraph", "RunNode", "graph_for"]
