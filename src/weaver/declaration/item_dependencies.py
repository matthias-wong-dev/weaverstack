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


def resolve_item_dependencies(repository: WeaverRepository) -> WeaverRepository:
    """Return ``repository`` with exact item-owned edges and a global DAG."""

    native = repository.source_documents
    aliases = {alias.destination: alias.source for alias in repository.aliases}
    folded_native = {str(identity).casefold(): identity for identity in native}
    folded_alias = {str(identity).casefold(): identity for identity in aliases}
    edges: list[ItemDependency] = []

    for consumer, source in native.items():
        if source.document.declares_dependencies:
            references = tuple(
                (dependency.qualified, WeaverDocumentId(consumer.item, dependency))
                for dependency in source.document.dependencies
            )
        elif source.language == "python":
            references = _python_references(source)
        else:
            references = []
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
                        (
                            str(reference),
                            WeaverDocumentId(consumer.item, reference.object_id),
                        )
                    )

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
    graph = Graph(
        (str(identity) for identity in native),
        (
            (str(edge.producer), str(edge.consumer))
            for edge in resolved
            if edge.producer is not None
        ),
    )
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
        if alias.source.item == alias.destination.item:
            continue
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
