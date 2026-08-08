"""Item-owned dependency resolution and sparse logical projection."""

from __future__ import annotations

from dataclasses import replace
from typing import Iterable, Mapping

from ..errors import BuildError, DiscoveryError, GraphError
from .graph import Graph
from .metadata import ObjectId
from .model import (
    FILES,
    ItemDependency,
    RepositoryAlias,
    WeaverDocumentId,
    WeaverItemId,
    WeaverRepository,
)
from .source import SourceDocument


def _declared_references(
    source: SourceDocument, consumer: WeaverDocumentId
) -> tuple[tuple[str, WeaverDocumentId], ...]:
    """What the document's ``Dependencies:`` header names."""

    return tuple(
        (dependency.qualified, WeaverDocumentId(consumer.item, dependency))
        for dependency in source.document.dependencies
    )


def _inferred_references(
    source: SourceDocument,
    consumer: WeaverDocumentId,
    edges: list[ItemDependency],
) -> tuple[tuple[str, WeaverDocumentId], ...]:
    """What the document's own source says it reads.

    Python imports, or the relations a SQL body names. A fully qualified SQL
    reference names something outside the item namespace, so it is appended to
    ``edges`` as a physical edge here rather than returned for resolution.
    """

    if source.language == "python":
        return tuple(_python_references(source))

    references: list[tuple[str, WeaverDocumentId]] = []
    for reference in source.discovered_references:
        if reference.call:
            continue
        if reference.is_qualified:
            edges.append(
                ItemDependency(
                    consumer=consumer,
                    reference=str(reference),
                    resolution_kind="physical",
                    is_within_item=False,
                )
            )
        elif reference.object_id is not None:
            references.append(
                (str(reference), WeaverDocumentId(consumer.item, reference.object_id))
            )
    return tuple(references)


def _first_per_destination(
    references: tuple[tuple[str, WeaverDocumentId], ...]
) -> tuple[tuple[str, WeaverDocumentId], ...]:
    """One reference per destination, keeping the first spelling seen.

    A validation that both imports ``Sales__Order`` and declares ``Sales.Order``
    named one edge twice, in two spellings. It is one edge, so it is one row —
    and the spelling kept is the inferred one, because that is how the same
    dependency is recorded when nobody declared it as well.
    """

    seen: dict[WeaverDocumentId, tuple[str, WeaverDocumentId]] = {}
    for written, destination in references:
        seen.setdefault(destination, (written, destination))
    return tuple(seen.values())


def resolve_item_dependencies(repository: WeaverRepository) -> WeaverRepository:
    """Return ``repository`` with exact item-owned edges and a global DAG."""

    native = repository.source_documents
    aliases = {alias.destination: alias.source for alias in repository.aliases}
    folded_native = {str(identity).casefold(): identity for identity in native}
    folded_alias = {str(identity).casefold(): identity for identity in aliases}
    edges: list[ItemDependency] = []
    #: Graph edges, kept separately from ``edges`` because the two answer
    #: different questions — see :func:`_document_graph`.
    graph_edges: set[tuple[str, str]] = set()

    for consumer, source in native.items():
        # Which of the two rules this document's kind uses is decided in one
        # place — see :func:`weaver.declaration.repository.effective_dependencies`
        # for why a validation supplements and an object replaces.
        if source.is_validation:
            inferred = _inferred_references(source, consumer, edges)
            references = _first_per_destination(
                inferred + _declared_references(source, consumer)
            )
        elif source.document.declares_dependencies:
            references = _declared_references(source, consumer)
        else:
            references = _inferred_references(source, consumer, edges)

        for written, destination in references:
            producer, kind = _resolve_destination(
                destination,
                native=native,
                aliases=aliases,
                folded_native=folded_native,
                folded_alias=folded_alias,
                consumer=consumer,
                written=written,
            )
            edges.append(
                ItemDependency(
                    consumer=consumer,
                    producer=producer,
                    reference=written,
                    resolution_kind=kind,
                    is_within_item=producer.item == consumer.item,
                )
            )
            # ``destination`` is what the consumer named in its own namespace: the
            # producer itself when that resolved natively, and the item's alias
            # destination when it resolved through one. The edge above records the
            # producer either way; the graph records the hop actually taken.
            graph_edges.add((str(destination), str(consumer)))

    unique = {
        (edge.consumer, edge.reference, edge.producer, edge.resolution_kind): edge
        for edge in edges
    }
    resolved = tuple(
        sorted(
            unique.values(),
            key=lambda edge: (
                str(edge.consumer),
                edge.reference,
                str(edge.producer) if edge.producer else "",
            ),
        )
    )
    graph = _document_graph(native, aliases, graph_edges)
    item_graph = _item_graph(repository, resolved)
    by_name = {str(item.identity): item.identity for item in repository.items}
    return replace(
        repository,
        dependency_edges=resolved,
        dependency_graph=graph,
        item_graph=item_graph,
        item_layers=tuple(
            tuple(by_name[node] for node in layer) for layer in item_graph.layers()
        ),
    )


def _document_graph(
    native: Mapping[WeaverDocumentId, SourceDocument],
    aliases: Mapping[WeaverDocumentId, WeaverDocumentId],
    graph_edges: set[tuple[str, str]],
) -> Graph:
    """The graph incremental selection is planned against.

    **This is deliberately not a projection of** :attr:`dependency_edges`. An
    edge records what the author wrote and where it resolved to, and
    :data:`~weaver.catalogue.tables.DEPENDENCY` publishes exactly that — an
    alias edge names the *source document* as its producer, because that is the
    truth about where the data comes from. The graph answers a different
    question: what must be built, in what order. There the alias destination is
    a thing in its own right — a shortcut or a view that some build has to
    create — so the path is three hops rather than two:

    .. code-block:: text

        source document → alias destination → consumer document

    Representing it that way is what lets impact propagate across items without
    the planner needing a special case: an alias is an ordinary node, rebuilt
    when its source is, and its consumers are ordinary descendants of it.

    Every alias contributes its ``source → destination`` edge whether or not a
    document consumes it. An alias with no consumer still has to be materialised
    after the thing it points at exists.
    """

    edges = set(graph_edges)
    for destination, source in aliases.items():
        edges.add((str(source), str(destination)))
    nodes = [str(identity) for identity in native]
    nodes.extend(str(destination) for destination in aliases)
    return Graph(nodes, sorted(edges))


def _item_graph(repository: WeaverRepository, resolved: tuple[ItemDependency, ...]) -> Graph:
    """The acyclic item-level graph a multi-item build is planned against.

    One item depends on another when it reaches into it: either a document
    resolves to a document that other item owns, or this item declares an alias
    whose source lives there. The alias edge matters on its own — an alias with
    no consumer yet still has to be materialised after its source exists — so it
    is not left to be implied by the dependency edges.

    Within-item edges are absent by construction: the document graph already
    orders those, and an item cannot wait for itself.

    A circular item graph is a **repository** fault. It is rejected here, while
    the whole declaration is in view, rather than at the point some incremental
    selection happens to exercise it — a repository whose items cannot be
    ordered has no correct build, not merely no correct build today.
    """

    edges: set[tuple[str, str]] = set()
    for edge in resolved:
        if edge.producer is None or edge.producer.item == edge.consumer.item:
            continue
        edges.add((str(edge.producer.item), str(edge.consumer.item)))
    for alias in repository.aliases:
        # Repository parsing rejects a same-item alias, so every alias is an
        # edge between two distinct items.
        edges.add((str(alias.source.item), str(alias.destination.item)))

    try:
        return Graph(
            (str(item.identity) for item in repository.items), sorted(edges)
        )
    except GraphError as exc:
        raise GraphError(f"item {exc}") from exc


def _resolve_destination(
    destination: WeaverDocumentId,
    *,
    native: Mapping[WeaverDocumentId, SourceDocument],
    aliases: Mapping[WeaverDocumentId, WeaverDocumentId],
    folded_native: Mapping[str, WeaverDocumentId],
    folded_alias: Mapping[str, WeaverDocumentId],
    consumer: WeaverDocumentId,
    written: str,
) -> tuple[WeaverDocumentId, str]:
    if destination in native:
        return destination, "native"
    if destination in aliases:
        return aliases[destination], "alias"
    case_match = folded_native.get(str(destination).casefold()) or folded_alias.get(
        str(destination).casefold()
    )
    detail = f"; declared spelling is {case_match}" if case_match else ""
    raise DiscoveryError(
        f"{consumer}: dependency {written!r} does not resolve in item namespace{detail}"
    )


def _python_references(source: SourceDocument) -> list[tuple[str, WeaverDocumentId]]:
    assert source.logical_id is not None
    references: list[tuple[str, WeaverDocumentId]] = []
    for imported in source.python_imports:
        candidates = _resolved_python_modules(source.logical_id, imported)
        for written, components in candidates:
            if components and components[0] == "lib":
                continue
            is_files = bool(components and components[0] == FILES)
            object_module = components[-1] if components else ""
            parts = object_module.split("__")
            if len(parts) != 2 or not all(parts):
                continue
            if len(components) != (2 if is_files else 1):
                raise DiscoveryError(
                    f"{source.node_id}: import {written!r} does not resolve to an "
                    "item object or lib module"
                )
            references.append(
                (
                    written,
                    WeaverDocumentId(
                        source.logical_id.item,
                        ObjectId(parts[0], parts[1]),
                        is_files=is_files,
                    ),
                )
            )
    return references


def _resolved_python_modules(logical_id: WeaverDocumentId, imported) -> list[tuple[str, tuple[str, ...]]]:
    module = tuple(imported.module.split(".")) if imported.module else ()
    if imported.level:
        base = (FILES,) if logical_id.is_files else ()
        parents = imported.level - 1
        if parents > len(base):
            raise DiscoveryError(
                f"{logical_id}: import {imported} escapes the owning Weaver item"
            )
        resolved = base[: len(base) - parents] + module
    else:
        resolved = module

    candidates = [(str(imported), resolved)]
    if not module or not any("__" in component for component in module):
        candidates = [
            (f"{imported}.{name}".replace("..", "."), resolved + (name,))
            for name in imported.names
        ]
    return candidates


def project_bound_documents(
    repository: WeaverRepository,
    bound_items: Iterable[WeaverItemId],
) -> tuple[SourceDocument, ...]:
    """Select physical work by exact item only; do not pull in unbound ancestors."""

    selected_items = set(bound_items)
    if not selected_items:
        raise BuildError("at least one Weaver item must be bound")
    known_items = {item.identity for item in repository.items}
    unknown = selected_items - known_items
    if unknown:
        raise BuildError(
            "binding names item(s) absent from the repository: "
            + ", ".join(sorted(map(str, unknown)))
        )
    selected = {
        str(identity): source
        for identity, source in repository.source_documents.items()
        if identity.item in selected_items
    }
    order = (
        repository.dependency_graph.order()
        if repository.dependency_graph is not None
        else tuple(sorted(selected))
    )
    return tuple(selected[node] for node in order if node in selected)
