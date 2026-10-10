"""Plan physical work from the installed managed graph.

Dependency direction comes from the installed graph. This module selects nodes,
inserts endpoint and publication barriers, and orders their physical dispatch.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from functools import cached_property
from typing import TYPE_CHECKING, Mapping, Sequence

from .catalogue.state import Catalogue
from .catalogue.tables import ROLE_SHORTCUT
from .declaration.model import (
    OBJECT_SHAPE,
    WeaverDocumentId,
    WeaverItemId,
)
from .errors import GraphError, LoadError
from .graph import Graph
from .installed import (
    PYTHON_FOLDER,
    PYTHON_TABLE,
    SEMANTIC_REFRESH,
    WAREHOUSE_PROCEDURE,
    InstalledDag,
    InstalledNode,
    refuse_uncertified_models,
)
from .load_report import DEPENDENCY_EXTERNAL, LoadMessage, info
from .selection import name_patterns
from .targets import PhysicalObjectRef, PhysicalTargetRef

if TYPE_CHECKING:
    from .fabric.resources import Item

#: Barrier kinds written to plan files and task logs.
ENDPOINT_REFRESH = "endpoint_refresh"
ONELAKE_PUBLICATION = "onelake_publication"

PRIMITIVE_KINDS = (
    WAREHOUSE_PROCEDURE,
    PYTHON_TABLE,
    PYTHON_FOLDER,
    SEMANTIC_REFRESH,
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
    """One unit of physical load work, or a barrier between units."""

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
    bound_item: Item | None = None
    #: A refresh barrier only. The ``(schema, table)`` pairs read through the
    #: endpoint, or ``None`` when a read needs every table synced.
    refresh_tables: tuple[tuple[str, str], ...] | None = ()
    #: A semantic refresh only. Every recorded source is read in Direct Lake,
    #: which uses single sign-on, so no connection is bound or needed.
    direct_lake: bool = False

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

    An edge means the upstream node settles before the downstream node may
    execute, and nothing else. Execution policy decides whether an upstream
    failure stops the downstream node. An edge is not a data-flow statement or
    a claim about what the downstream node reads.
    """

    nodes: tuple[LoadNode, ...]
    edges: tuple[tuple[str, str], ...]
    #: The items this plan was asked for, which bounded its traversal.
    items: tuple[WeaverItemId, ...] = ()
    messages: tuple[LoadMessage, ...] = ()

    @classmethod
    def from_catalogue(
        cls,
        catalogue: Catalogue,
        *,
        items: Sequence[WeaverItemId],
        selection: Sequence[WeaverDocumentId] | None = None,
        names: Sequence[str] = (),
        ancestors: bool = False,
        descendants: bool = False,
    ) -> "LoadDag":
        return load_dag(
            catalogue.dag(),
            items=items,
            selection=selection,
            names=names,
            ancestors=ancestors,
            descendants=descendants,
        )

    @property
    def by_id(self) -> Mapping[str, LoadNode]:
        return {node.node_id: node for node in self.nodes}

    @cached_property
    def topology(self) -> Graph:
        try:
            return Graph((node.node_id for node in self.nodes), self.edges)
        except GraphError as exc:
            raise LoadError(
                f"Installed load dependencies contain a cycle: {exc}. Remove one "
                "dependency from the cycle and rebuild."
            ) from None

    def upstream(self, node_id: str) -> frozenset[str]:
        return frozenset(self.topology.upstream_of(node_id))

    def descendants(self, node_id: str) -> frozenset[str]:
        """Every node ordered after ``node_id`` through dependency edges."""

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
    items: Sequence[WeaverItemId],
    selection: Sequence[WeaverDocumentId] | None = None,
    names: Sequence[str] = (),
    ancestors: bool = False,
    descendants: bool = False,
) -> LoadDag:
    """The physical load graph for one set of items.

    Dependencies order the selection. ``names`` narrows it to the loadables its
    regular expressions match, an operator override that adds neither nodes nor
    ordering edges unless expansion is requested. A Lakehouse name carries its
    area, ``Tables/Schema.Object``
    or ``Files/Schema.Object``, and a bare ``Schema.Object`` is accepted where
    it reaches one object.

    ``selection`` names logical loadable seeds, decided by the caller. The traversal
    continues through the ones it leaves out, so two selected loadables keep the
    order the graph gives them. ``None`` selects every loadable the requested
    items own.

    ``ancestors`` and ``descendants`` recursively expand the original seeds,
    separately, within the requested items. Their union runs in dependency order.

    A semantic model that records no managed source waits for every other node
    in the plan. One that records sources waits for those alone.
    """

    requested = tuple(dict.fromkeys(items))
    return _Planner(dag, selection=selection).plan(
        requested, names=tuple(names), ancestors=ancestors, descendants=descendants
    )


class _Planner:
    def __init__(
        self,
        dag: InstalledDag,
        *,
        selection: Sequence[WeaverDocumentId] | None = None,
    ) -> None:
        self.dag = dag
        self.selection = None if selection is None else frozenset(selection)
        self.messages: list[LoadMessage] = []
        self.nodes: dict[str, LoadNode] = {}
        self.edges: set[tuple[str, str]] = set()
        self.refresh_nodes: dict[str, LoadNode] = {}

    # --- planning -------------------------------------------------------------

    def plan(
        self,
        requested: tuple[WeaverItemId, ...],
        *,
        names: tuple[str, ...] = (),
        ancestors: bool = False,
        descendants: bool = False,
    ) -> LoadDag:
        self._refuse_ambiguity(requested)
        seeds = self._seeds(requested, names=names)
        if ancestors or descendants:
            seeds = self._expand(
                seeds, requested, ancestors=ancestors, descendants=descendants
            )
            self.selection = frozenset(node.identity for node in seeds)
        if names and not (ancestors or descendants):
            # An exact-name request is not a partial DAG request.
            # The caller chose the nodes and asked Weaver not to infer more work
            # or readiness constraints from their dependencies.
            for node in seeds:
                self._load_node(node)
        else:
            allowed_items = frozenset(requested)
            visited: set[str] = set()
            for node in seeds:
                self._select(node, visited, allowed_items=allowed_items)
            self._order_untraced_models_last()
        dag = LoadDag(
            nodes=tuple(sorted(self.nodes.values(), key=lambda node: node.sort_key)),
            edges=tuple(sorted(self.edges)),
            items=requested,
            messages=tuple(self.messages),
        )
        # Ordering is what proves acyclicity, so it is done here rather than left
        # to whoever consumes the graph.
        dag.order()
        return dag

    def _expand(
        self,
        seeds: tuple[InstalledNode, ...],
        requested: tuple[WeaverItemId, ...],
        *,
        ancestors: bool,
        descendants: bool,
    ) -> tuple[InstalledNode, ...]:
        allowed_items = frozenset(requested)
        scoped = self.dag.graph.subgraph(
            node.node_id for node in self.dag.nodes if node.item in allowed_items
        )
        expanded = scoped.subgraph(
            (node.node_id for node in seeds),
            with_ancestors=ancestors,
            with_descendants=descendants,
        )
        return tuple(
            self.dag.by_id[node_id]
            for node_id in expanded.order()
            if self.dag.by_id[node_id].can_load
        )

    def _order_untraced_models_last(self) -> None:
        """Make each untraced semantic model wait for every other load node.

        Nothing loads from a semantic model, so these edges cannot form a cycle.
        """

        models = {
            node_id: node
            for node_id, node in self.nodes.items()
            if node.primitive_kind == SEMANTIC_REFRESH
        }
        untraced = [
            node_id
            for node_id, node in models.items()
            if self.dag.is_untraced_model(self.dag.node(node.logical_id))
        ]
        others = [node_id for node_id in self.nodes if node_id not in models]
        self.edges.update(
            (upstream, model) for model in untraced for upstream in others
        )

    def _seeds(
        self,
        requested: tuple[WeaverItemId, ...],
        *,
        names: tuple[str, ...],
    ) -> tuple[InstalledNode, ...]:
        available = self.dag.loadables(items=requested)
        refuse_uncertified_models(
            self.dag, requested, operation="Load", error=LoadError
        )
        if not names:
            return self._chosen(available)

        chosen: set[str] = set()
        for text, pattern in name_patterns(names, error=LoadError):
            chosen.update(
                node.node_id for node in self._matching(text, pattern, available)
            )
        return self._chosen(tuple(node for node in available if node.node_id in chosen))

    def _chosen(self, nodes: tuple[InstalledNode, ...]) -> tuple[InstalledNode, ...]:
        if self.selection is None:
            return nodes
        return tuple(node for node in nodes if node.identity in self.selection)

    def _is_chosen(self, node: InstalledNode) -> bool:
        return self.selection is None or node.identity in self.selection

    @staticmethod
    def _matching(
        text: str, pattern, available: tuple[InstalledNode, ...]
    ) -> list[InstalledNode]:
        """The loadables one pattern selects.

        A pattern matches the area-qualified ``load_key``, or the bare
        ``Schema.Object`` where that names one object in an item: a Lakehouse
        Folder and table of one ``Schema.Object`` are told apart only by area.
        """

        precise = [node for node in available if pattern.fullmatch(node.load_key)]
        bare = [
            node
            for node in available
            if node not in precise and pattern.fullmatch(node.load_name or "")
        ]
        areas: dict[tuple, set[str]] = {}
        for node in bare:
            key = (node.item, (node.load_name or "").casefold())
            areas.setdefault(key, set()).add(node.load_key)
        for keys in areas.values():
            if len(keys) > 1:
                raise LoadError(
                    f"{text!r} names more than one installed loadable object: "
                    f"{', '.join(sorted(keys))}. Choose an area-qualified name"
                )
        matched = precise + bare
        if not matched:
            known = ", ".join(sorted({node.load_key for node in available}))
            raise LoadError(
                f"no installed loadable object in the requested items matches "
                f"'{text}'. Installed: {known or 'none'}"
            )
        return matched

    def _refuse_ambiguity(self, items: tuple[WeaverItemId, ...]) -> None:
        """Stop if a target this request dispatches into holds a duplicated address.

        By physical target, because the collision is physical: two logical
        objects at one address make a dispatch there ambiguous whether or not the
        item that made the other claim was selected.
        """

        for target in dict.fromkeys(
            self.dag.installations[item]
            for item in items
            if item in self.dag.installations
        ):
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
        allowed_items: frozenset[WeaverItemId],
    ) -> str:
        node = self._load_node(installed)
        if installed.node_id in visited:
            return node.node_id
        visited.add(installed.node_id)
        self._report_external(installed)
        for producer, crossed, read in self._upstream_loadable(
            installed, allowed_items=allowed_items
        ):
            upstream_id = self._select(producer, visited, allowed_items=allowed_items)
            if crossed is None:
                self.edges.add((upstream_id, node.node_id))
            elif (
                isinstance(crossed, OneLakeReadiness) or crossed == ONELAKE_PUBLICATION
            ):
                # OneLake publishes a Warehouse commit after the transaction, so
                # the barrier replaces the direct edge, as the refresh does.
                barrier = self._publication_node(
                    self.nodes[upstream_id],
                    None if crossed == ONELAKE_PUBLICATION else crossed,
                )
                self.edges.add((upstream_id, barrier.node_id))
                self.edges.add((barrier.node_id, node.node_id))
            else:
                # A shortcut read as SQL: the producer's endpoint has to catch up
                # before the consumer can see it, so the barrier replaces the
                # direct edge rather than sitting beside it. The refresh waits
                # for what is read through it, and other loads in the Lakehouse
                # run beside it.
                refresh_id = self._refresh_node(crossed, read).node_id
                self.edges.add((upstream_id, refresh_id))
                self.edges.add((refresh_id, node.node_id))
        return node.node_id

    def _report_external(self, installed: InstalledNode) -> None:
        for reference in self.dag.external_references.get(installed.identity, ()):
            self.messages.append(
                info(
                    DEPENDENCY_EXTERNAL,
                    f"{installed.identity} reads physical object {reference} directly. "
                    "Weaver will not use this reference to order loads.",
                    source="load_plan",
                )
            )

    def _load_node(self, installed: InstalledNode) -> LoadNode:
        if installed.artefact_kind == SEMANTIC_REFRESH:
            from .errors import ConfigError
            from .fabric.semantic_model import validate_bound_model

            try:
                validate_bound_model(installed.bound_item)
            except ConfigError as exc:
                raise LoadError(
                    f"{exc}. Build {installed.item} before loading it."
                ) from exc
        node_id = (
            f"load:{installed.target}"
            if installed.artefact_kind == SEMANTIC_REFRESH
            else f"load:{installed.target}/{installed.load_key}"
        )
        node = self.nodes.get(node_id)
        if node is None:
            node = LoadNode(
                node_id=node_id,
                logical_id=installed.identity,
                physical_target=installed.target,
                primitive_kind=installed.artefact_kind,
                physical_object=installed.physical,
                primitive_id=installed.artefact,
                primitive_object=(
                    installed.artefact_physical(installed.artefact_type)
                    if installed.artefact is not None
                    else None
                ),
                bound_item=installed.bound_item,
                direct_lake=installed.artefact_kind == SEMANTIC_REFRESH
                and {
                    edge.source_mode
                    for edge in self.dag.reads(installed.identity)
                    if edge.semantic_table is not None
                }
                == {"directLake"},
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
            publication_targets=tuple(
                dict.fromkeys(
                    (*readiness, *((crossed,) if crossed is not None else ()))
                )
            ),
            produced_by=producer.node_id,
        )
        self.nodes[node_id] = node
        return node

    def _refresh_node(self, target: PhysicalTargetRef, read: InstalledNode) -> LoadNode:
        """The one refresh barrier for this Lakehouse, syncing what is read through it."""

        node_id = f"refresh:{target}"
        node = self.refresh_nodes.get(node_id) or LoadNode(
            node_id=node_id,
            logical_id=None,
            physical_target=target,
            primitive_kind=ENDPOINT_REFRESH,
        )
        table = _endpoint_table(read)
        tables = (
            None
            if node.refresh_tables is None or table is None
            else tuple(sorted({*node.refresh_tables, table}))
        )
        node = replace(node, refresh_tables=tables)
        self.refresh_nodes[node_id] = node
        self.nodes[node_id] = node
        return node

    # --- dependency traversal --------------------------------------------------

    def _upstream_loadable(
        self,
        installed: InstalledNode,
        *,
        allowed_items: frozenset[WeaverItemId],
    ) -> tuple[tuple[InstalledNode, object, InstalledNode | None], ...]:
        """The in-scope loadable ancestors, where each hop crossed, and what it read.

        Passing through non-loadable producers is what makes a view a conduit:
        it owns no load work, so it is not a node here, but a consumer still
        depends on whatever fills the tables behind it. A loadable the selection
        leaves out is crossed the same way. The traversal stops at the
        requested-item boundary even so.
        """

        found: dict[str, tuple[InstalledNode, object, InstalledNode | None]] = {}
        seen: set[tuple[str, object]] = set()
        frontier: list[tuple[InstalledNode, object, InstalledNode | None]] = [
            (installed, None, None)
        ]
        while frontier:
            current, crossing, read = frontier.pop()
            for producer, hop in self._direct_producers(current):
                if producer.item not in allowed_items:
                    continue
                crossed = crossing or hop
                # What the consumer's engine reads at the crossing.
                at = read if crossing else (producer if hop else None)
                if producer.can_load and self._is_chosen(producer):
                    # A closer crossing wins: the barrier belongs to the hop that
                    # actually left the consumer's engine.
                    prior = found.get(producer.node_id)
                    if prior is None or prior[1] is None:
                        found[producer.node_id] = (producer, crossed, at)
                    continue
                if (producer.node_id, crossed) in seen:
                    continue
                seen.add((producer.node_id, crossed))
                frontier.append((producer, crossed, at))
        return tuple(found[node_id] for node_id in sorted(found))

    def _direct_producers(
        self, consumer: InstalledNode
    ) -> tuple[tuple[InstalledNode, object], ...]:
        """What one object reads directly, and the barrier each read crosses.

        A shortcut conduit follows its installed source. The read that crosses
        is the one the consumer declared, which
        :attr:`weaver.installed.InstalledEdge.through` already names.
        """

        unresolved = self.dag.unresolved_for(consumer)
        if unresolved:
            raise LoadError(unresolved[0])
        if consumer.role == ROLE_SHORTCUT:
            return tuple(
                (producer, None) for producer in self.dag.parents(consumer.node_id)
            )
        producers = []
        for edge in self.dag.reads(consumer.identity):
            producer = self.dag.node(edge.upstream)
            crossing = (
                None
                if edge.through is None
                else self._crossing(producer, consumer, edge.through)
            )
            if edge.semantic_table is not None:
                if producer.target.is_lakehouse and edge.source_access == "sql":
                    crossing = producer.target
                elif (
                    producer.target.kind == "warehouse"
                    and producer.object_type == "table"
                    and edge.source_access == "sql"
                    and edge.source_mode == "directLake"
                ):
                    crossing = ONELAKE_PUBLICATION
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


def _endpoint_table(read: InstalledNode | None) -> tuple[str, str] | None:
    """The ``(schema, table)`` a SQL endpoint read names, or ``None`` if not a table."""

    identity = getattr(read, "identity", None)
    if (
        read is None
        or read.effective_object_type != "table"
        or getattr(identity, "is_files", True)
        or getattr(identity, "shape", None) != OBJECT_SHAPE
    ):
        return None
    return (identity.object_id.schema, identity.object_id.object)


__all__ = [
    "ENDPOINT_REFRESH",
    "ONELAKE_PUBLICATION",
    "PRIMITIVE_KINDS",
    "LoadDag",
    "LoadNode",
    "OneLakeReadiness",
    "load_dag",
]
