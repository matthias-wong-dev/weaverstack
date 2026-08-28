"""Build the physical load graph from the installed managed graph.

The graph contains selected targets, load dependencies, and required endpoint
refresh barriers. What depends on what is :mod:`weaver.installed`'s answer; this
module decides which of those nodes run, where a barrier goes between two of
them, and in what order.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import cached_property
from typing import Mapping, Sequence

from .catalogue.state import Catalogue
from .declaration.model import WeaverDocumentId
from .errors import GraphError, LoadError
from .graph import Graph
from .installed import (
    PYTHON_FOLDER,
    PYTHON_TABLE,
    WAREHOUSE_PROCEDURE,
    InstalledDag,
    InstalledNode,
)
from .load_report import DEPENDENCY_EXTERNAL, LoadMessage, info
from .targets import PhysicalObjectRef, PhysicalTargetRef

#: The two barriers a planner inserts between installed loads. Strings rather
#: than a class hierarchy because they cross into a plan file and a task log,
#: where the word itself is what appears.
ENDPOINT_REFRESH = "endpoint_refresh"
ONELAKE_PUBLICATION = "onelake_publication"

PRIMITIVE_KINDS = (
    WAREHOUSE_PROCEDURE,
    PYTHON_TABLE,
    PYTHON_FOLDER,
    ENDPOINT_REFRESH,
    ONELAKE_PUBLICATION,
)


@dataclass(frozen=True)
class OneLakeReadiness:
    """A Lakehouse shortcut path that must see a Warehouse publication."""

    target: PhysicalTargetRef
    schema: str
    object: str


@dataclass(frozen=True)
class LoadNode:
    """One unit of physical load work, or the barrier between two of them."""

    node_id: str
    logical_id: WeaverDocumentId | None
    physical_target: PhysicalTargetRef
    primitive_kind: str
    physical_object: PhysicalObjectRef | None = None
    #: The installed primitive itself, being the procedure or the deployed file.
    #: ``None`` for a refresh, which is a capability rather than an artefact.
    primitive_id: WeaverDocumentId | None = None
    primitive_object: PhysicalObjectRef | None = None
    #: A publication barrier only. The Warehouse table whose OneLake publication
    #: is waited for, carried apart from ``logical_id`` so the barrier leaves no
    #: catalogue state of its own.
    publication_of: WeaverDocumentId | None = None
    #: A publication barrier only. The Lakehouse shortcut paths that must be able
    #: to read the published Delta files before the consumers can begin.
    publication_targets: tuple[OneLakeReadiness, ...] = ()
    #: A publication barrier only. The load node that publishes what it waits for.
    produced_by: str | None = None

    @property
    def sort_key(self) -> tuple[str, str, str, str]:
        """What orders two nodes that became ready at the same moment.

        Target kind, then target, then logical identity, then primitive kind,
        so a plan's order is a property of the estate rather than of dictionary
        iteration.
        """

        return (
            self.physical_target.kind,
            self.physical_target.name,
            str(self.logical_id or ""),
            self.primitive_kind,
        )


@dataclass(frozen=True)
class LoadDag:
    """The selected physical load graph: nodes, edges and what was requested.

    An edge means the upstream node must complete successfully before the
    downstream node may execute, and nothing else. It is not a data-flow
    statement, and not a claim about what the downstream node reads.
    """

    nodes: tuple[LoadNode, ...]
    edges: tuple[tuple[str, str], ...]
    requested: tuple[PhysicalTargetRef, ...] = ()
    messages: tuple[LoadMessage, ...] = ()

    @classmethod
    def from_catalogue(
        cls,
        catalogue: Catalogue,
        *,
        targets: Sequence[PhysicalTargetRef],
        names: Sequence[str] = (),
    ) -> "LoadDag":
        return load_dag(catalogue.dag(), targets=targets, names=names)

    @property
    def by_id(self) -> Mapping[str, LoadNode]:
        return {node.node_id: node for node in self.nodes}

    @cached_property
    def topology(self) -> Graph:
        """The generic topology over these nodes, built once and kept."""

        try:
            return Graph((node.node_id for node in self.nodes), self.edges)
        except GraphError as exc:
            raise LoadError(f"the load graph contains a cycle: {exc}") from None

    def upstream(self, node_id: str) -> frozenset[str]:
        return frozenset(self.topology.upstream_of(node_id))

    def descendants(self, node_id: str) -> frozenset[str]:
        """Every node that may not run once ``node_id`` has failed."""

        return frozenset(self.topology.descendants(node_id))

    def order(self) -> tuple[LoadNode, ...]:
        """The deterministic topological order, or a refusal if there is a cycle.

        Ties break on :attr:`LoadNode.sort_key`, so a plan reads target by
        target.
        """

        found = self.by_id
        return tuple(
            found[node_id]
            for node_id in self.topology.order(key=lambda node: found[node].sort_key)
        )


def load_dag(
    dag: InstalledDag,
    *,
    targets: Sequence[PhysicalTargetRef],
    names: Sequence[str] = (),
) -> LoadDag:
    """The physical load graph for one set of requested physical targets.

    With no name filter, every installed loadable object hosted in the requested
    targets. Dependencies order them but never enlarge the target scope: two
    targets are crossed only when the caller named both.

    With ``names``, exactly those ``Schema.Object`` loadables within the
    requested targets. An operator override, so dependencies add neither nodes
    nor ordering edges.
    """

    requested = tuple(dict.fromkeys(targets))
    return _Planner(dag).plan(requested, names=tuple(names))


class _Planner:
    """One planning run's working state.

    A class rather than free functions because the traversal, the barrier
    placement and the message stream all read the same installed graph.
    """

    def __init__(self, dag: InstalledDag) -> None:
        self.dag = dag
        self.messages: list[LoadMessage] = []
        self.nodes: dict[str, LoadNode] = {}
        self.edges: set[tuple[str, str]] = set()
        self.refresh_nodes: dict[str, LoadNode] = {}
        #: Which physical targets a refresh barrier must wait for, by refresh id.
        self.refresh_sources: dict[str, PhysicalTargetRef] = {}

    # --- planning -------------------------------------------------------------

    def plan(
        self,
        requested: tuple[PhysicalTargetRef, ...],
        *,
        names: tuple[str, ...] = (),
    ) -> LoadDag:
        self._refuse_ambiguity(requested)
        seeds = self._seeds(requested, names=names)
        if names:
            # An exact-name request is not a partial DAG request.
            # The caller chose the nodes and asked Weaver not to infer more work
            # or readiness constraints from their dependencies.
            for node in seeds:
                self._load_node(node)
        else:
            allowed_targets = frozenset(requested)
            visited: set[str] = set()
            for node in seeds:
                self._select(node, visited, allowed_targets=allowed_targets)
            self._place_refresh_barriers()
        dag = LoadDag(
            nodes=tuple(sorted(self.nodes.values(), key=lambda node: node.sort_key)),
            edges=tuple(sorted(self.edges)),
            requested=requested,
            messages=tuple(self.messages),
        )
        # Ordering is what proves acyclicity, so it is done here rather than left
        # to whoever consumes the graph.
        dag.order()
        return dag

    def _seeds(
        self,
        requested: tuple[PhysicalTargetRef, ...],
        *,
        names: tuple[str, ...],
    ) -> tuple[InstalledNode, ...]:
        """The loadables the caller selected, before any ordering is applied."""

        available = self.dag.loadables(targets=requested)
        if not names:
            return available

        selected: list[InstalledNode] = []
        seen: set[str] = set()
        for written in names:
            name = str(written).strip()
            if not name:
                raise LoadError("a load name must be a non-empty Schema.Object")
            folded = name.casefold()
            if folded in seen:
                continue
            seen.add(folded)
            candidates = [
                node
                for node in available
                if (node.load_name or "").casefold() == folded
            ]
            if not candidates:
                known = ", ".join(sorted({node.load_name for node in available}))
                raise LoadError(
                    f"no loadable object named {name!r} is installed in the "
                    f"requested target(s). Installed: {known or 'none'}"
                )
            if len(candidates) > 1:
                found = ", ".join(node.node_id for node in candidates)
                raise LoadError(
                    f"{name!r} names more than one installed loadable object "
                    f"({found}). Qualify the request with a single target"
                )
            selected.append(candidates[0])
        return tuple(selected)

    def _refuse_ambiguity(self, targets: tuple[PhysicalTargetRef, ...]) -> None:
        """Stop if any target this request touches holds a duplicated address.

        Only requested targets can be touched: dependency traversal is bounded
        by this same set, so ambiguity anywhere else is irrelevant to this run.
        """

        for target in targets:
            found = self.dag.ambiguous.get(target)
            if found:
                raise LoadError(
                    f"{target} holds two logical objects at one physical "
                    f"address, so a load of it is ambiguous: {found[0]}"
                )

    def _select(
        self,
        installed: InstalledNode,
        visited: set[str],
        *,
        allowed_targets: frozenset[PhysicalTargetRef],
    ) -> str:
        """Add one in-scope loadable and its in-scope ordering constraints."""

        node = self._load_node(installed)
        if installed.node_id in visited:
            return node.node_id
        visited.add(installed.node_id)
        self._report_external(installed)
        for producer, crossed in self._upstream_loadable(
            installed, allowed_targets=allowed_targets
        ):
            upstream_id = self._select(
                producer, visited, allowed_targets=allowed_targets
            )
            if crossed is None:
                self.edges.add((upstream_id, node.node_id))
            elif isinstance(crossed, OneLakeReadiness):
                # OneLake publishes a Warehouse commit after the transaction, so
                # the barrier replaces the direct edge, as the refresh does.
                barrier = self._publication_node(self.nodes[upstream_id], crossed)
                self.edges.add((upstream_id, barrier.node_id))
                self.edges.add((barrier.node_id, node.node_id))
            else:
                # A shortcut read as SQL: the producer's endpoint has to catch up
                # before the consumer can see it, so the barrier replaces the
                # direct edge rather than sitting beside it.
                refresh_id = self._refresh_node(crossed).node_id
                self.edges.add((refresh_id, node.node_id))
        return node.node_id

    def _report_external(self, installed: InstalledNode) -> None:
        """Say which of this node's reads name a physical object directly."""

        for reference in self.dag.external_references.get(installed.identity, ()):
            self.messages.append(
                info(
                    DEPENDENCY_EXTERNAL,
                    f"{installed.identity} reads {reference}, which names a "
                    "physical object directly and is not part of the load graph",
                    source="load_plan",
                )
            )

    def _load_node(self, installed: InstalledNode) -> LoadNode:
        node_id = f"load:{installed.target}/{installed.load_name}"
        node = self.nodes.get(node_id)
        if node is None:
            node = LoadNode(
                node_id=node_id,
                logical_id=installed.identity,
                physical_target=installed.target,
                primitive_kind=installed.artefact_kind,
                physical_object=installed.physical,
                primitive_id=installed.artefact,
                primitive_object=installed.artefact_physical(installed.artefact_type),
            )
            self.nodes[node_id] = node
        return node

    def _publication_node(self, producer: LoadNode, crossed) -> LoadNode:
        """The one publication barrier behind this Warehouse load."""

        identity = producer.logical_id
        node_id = f"publish:{producer.physical_target}/{identity.object_id.qualified}"
        node = self.nodes.get(node_id)
        readiness = () if node is None else node.publication_targets
        node = LoadNode(
            node_id=node_id,
            logical_id=None,
            physical_target=producer.physical_target,
            primitive_kind=ONELAKE_PUBLICATION,
            publication_of=identity,
            publication_targets=tuple(dict.fromkeys((*readiness, crossed))),
            produced_by=producer.node_id,
        )
        self.nodes[node_id] = node
        return node

    def _refresh_node(self, target: PhysicalTargetRef) -> LoadNode:
        """The one refresh barrier for this Lakehouse, made once per run."""

        node_id = f"refresh:{target}"
        node = self.refresh_nodes.get(node_id)
        if node is None:
            node = LoadNode(
                node_id=node_id,
                logical_id=None,
                physical_target=target,
                primitive_kind=ENDPOINT_REFRESH,
            )
            self.refresh_nodes[node_id] = node
            self.nodes[node_id] = node
            self.refresh_sources[node_id] = target
        return node

    def _place_refresh_barriers(self) -> None:
        """Every selected load in a refreshed Lakehouse runs before its barrier.

        Broad by necessity: one barrier per affected Lakehouse, behind all of
        its selected loads rather than only those a shortcut names. A narrower
        placement would need to know which tables a consumer's query touches,
        and the catalogue records the shortcut rather than the read.
        """

        for node_id, target in self.refresh_sources.items():
            for node in list(self.nodes.values()):
                if node.primitive_kind in (ENDPOINT_REFRESH, ONELAKE_PUBLICATION):
                    continue
                if node.physical_target == target:
                    self.edges.add((node.node_id, node_id))

    # --- dependency traversal --------------------------------------------------

    def _upstream_loadable(
        self,
        installed: InstalledNode,
        *,
        allowed_targets: frozenset[PhysicalTargetRef],
    ) -> tuple[tuple[InstalledNode, object], ...]:
        """The in-scope loadable ancestors, and where each hop crossed.

        Passing through non-loadable producers is what makes a view a conduit:
        it owns no load work, so it is not a node here, but a consumer still
        depends on whatever fills the tables behind it. The traversal stops at
        the requested-target boundary even so.
        """

        found: dict[str, tuple[InstalledNode, object]] = {}
        seen: set[tuple[str, object]] = set()
        frontier: list[tuple[InstalledNode, object]] = [(installed, None)]
        while frontier:
            current, crossing = frontier.pop()
            for producer, hop in self._direct_producers(current):
                if producer.target not in allowed_targets:
                    continue
                crossed = crossing or hop
                if producer.is_loadable:
                    # A closer crossing wins: the barrier belongs to the hop that
                    # actually left the consumer's engine.
                    prior = found.get(producer.node_id)
                    if prior is None or prior[1] is None:
                        found[producer.node_id] = (producer, crossed)
                    continue
                if (producer.node_id, crossed) in seen:
                    continue
                seen.add((producer.node_id, crossed))
                frontier.append((producer, crossed))
        return tuple(found[node_id] for node_id in sorted(found))

    def _direct_producers(
        self, consumer: InstalledNode
    ) -> tuple[tuple[InstalledNode, object], ...]:
        """What one object reads directly, and the barrier each read crosses.

        The installed graph's own shortcut edges are left alone here. A shortcut
        destination is materialised from its source, and the read that crosses is
        the one the consumer declared, which
        :attr:`weaver.installed.InstalledEdge.through` already names.
        """

        unresolved = self.dag.unresolved_for(consumer)
        if unresolved:
            raise LoadError(unresolved[0])
        producers = []
        for edge in self.dag.reads(consumer.identity):
            producer = self.dag.node(edge.upstream)
            crossing = (
                None
                if edge.through is None
                else self._crossing(producer, consumer, edge.through)
            )
            producers.append((producer, crossing))
        return tuple(producers)

    def _crossing(self, producer: InstalledNode, consumer: InstalledNode, through):
        """The barrier one shortcut read crosses, or ``None`` where it crosses none.

        Lakehouse to Warehouse is read through a SQL analytics endpoint, which
        has to catch up. Warehouse to Lakehouse is read through OneLake, which
        publishes the Delta commit after the transaction. Lakehouse to Lakehouse
        is Delta on both sides, with nothing to synchronise.
        """

        if producer.target.is_lakehouse and not consumer.target.is_lakehouse:
            return producer.target
        if not producer.target.is_lakehouse and consumer.target.is_lakehouse:
            return OneLakeReadiness(
                target=consumer.target,
                schema=through.object_id.schema,
                object=through.object_id.object,
            )
        return None


__all__ = [
    "ENDPOINT_REFRESH",
    "ONELAKE_PUBLICATION",
    "PRIMITIVE_KINDS",
    "LoadDag",
    "LoadNode",
    "OneLakeReadiness",
    "load_dag",
]
