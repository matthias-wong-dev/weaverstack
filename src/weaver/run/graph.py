"""The inert runtime topology: nodes, dependency edges and selection order."""

from __future__ import annotations

from dataclasses import dataclass
from functools import cached_property
from typing import TYPE_CHECKING, Mapping

from ..errors import GraphError
from ..graph import Graph
from .result import RunError

if TYPE_CHECKING:
    from ..fabric.resources import Item


@dataclass(frozen=True)
class RunNode:
    """Installed runtime work or a barrier between units of work."""

    node_id: str
    physical_target: object
    primitive_kind: str
    logical_id: object | None = None
    physical_object: object | None = None
    primitive_id: object | None = None
    primitive_object: object | None = None
    role: str | None = None
    #: Opaque to the Runner; only dispatch interprets the installed description.
    installed: object | None = None
    #: A publication barrier only: the Warehouse table it waits on, the consuming
    #: shortcut paths, and the load node that publishes what it waits for.
    publication_of: object | None = None
    publication_targets: tuple[object, ...] = ()
    produced_by: str | None = None
    bound_item: Item | None = None
    #: A refresh barrier only: the ``(schema, table)`` pairs it syncs, or
    #: ``None`` for every table.
    refresh_tables: tuple[tuple[str, str], ...] | None = ()

    @property
    def sort_key(self) -> tuple[str, str, str, str, str]:
        """Order ready nodes reproducibly from estate identity, then ``node_id``."""

        return (
            getattr(self.physical_target, "kind", ""),
            getattr(self.physical_target, "name", str(self.physical_target)),
            str(self.logical_id or ""),
            self.primitive_kind,
            self.node_id,
        )


@dataclass(frozen=True)
class RunGraph:
    """The selected runtime graph: nodes, edges and the items selected for."""

    nodes: tuple[RunNode, ...] = ()
    edges: tuple[tuple[str, str], ...] = ()
    #: The logical items this graph was selected for.
    items: tuple = ()
    messages: tuple = ()

    @property
    def by_id(self) -> Mapping[str, RunNode]:
        return {node.node_id: node for node in self.nodes}

    @cached_property
    def topology(self) -> Graph:
        """Build the topology lazily so cycles surface during run planning."""

        try:
            return Graph((node.node_id for node in self.nodes), self.edges)
        except GraphError as exc:
            raise RunError(
                f"Cannot plan the run because its dependencies cycle: {exc}. "
                "Remove one dependency from the cycle."
            ) from None

    def upstream(self, node_id: str) -> frozenset[str]:
        return frozenset(self.topology.upstream_of(node_id))

    def descendants(self, node_id: str) -> frozenset[str]:
        return frozenset(self.topology.descendants(node_id))

    def order(self) -> tuple[RunNode, ...]:
        """Return a deterministic topological order."""

        found = self.by_id
        return tuple(
            found[node_id]
            for node_id in self.topology.order(key=lambda node: found[node].sort_key)
        )


def graph_for(request, state) -> RunGraph:
    """Build the graph selected by a request against the observed estate."""

    from .runner import LOAD, TEST

    if request.kind == LOAD:
        return _load_graph(request, state)
    if request.kind == TEST:
        return _test_graph(request, state)
    raise RunError(f"Cannot plan a {request.kind!r} run: no selection rule exists")


def _load_graph(request, state) -> RunGraph:
    from ..load_plan import load_dag

    dag = load_dag(
        state.catalogue.dag(),
        items=request.items,
        selection=request.selected,
        names=request.names,
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
                publication_of=node.publication_of,
                publication_targets=node.publication_targets,
                produced_by=node.produced_by,
                bound_item=node.bound_item,
                refresh_tables=node.refresh_tables,
                role="load",
            )
            for node in dag.nodes
        ),
        edges=dag.edges,
        items=dag.items,
        messages=dag.messages,
    )


def _test_graph(request, state) -> RunGraph:
    from ..errors import ValidationError
    from ..installed import refuse_uncertified_models
    from ..test_execution import primitive_kind
    from ..test_plan import ValidationEstate, validation_order

    dag = state.catalogue.dag()
    refuse_uncertified_models(
        dag, request.items, operation="Test", error=ValidationError
    )
    estate = ValidationEstate.of(dag)
    selected = validation_order(estate.matching(request.names, request.items))
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
        items=tuple(request.items),
    )


__all__ = ["RunGraph", "RunNode", "graph_for"]
