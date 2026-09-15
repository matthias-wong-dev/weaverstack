"""Resolve dependencies among an item's declarations."""

from __future__ import annotations

from dataclasses import replace
from typing import Iterable, Mapping

from ..errors import BuildError, DiscoveryError, GraphError
from ..graph import Graph
from .metadata import ObjectId
from .model import (
    AREAS,
    FILES,
    TABLES,
    ItemDependency,
    WeaverDocumentId,
    WeaverItemId,
    WeaverRepository,
)
from .shortcuts import LAKEHOUSE_FILE
from .source import SourceDocument

SHORTCUTS_MODULE = LAKEHOUSE_FILE.removesuffix(".py")


def _declared_references(
    source: SourceDocument, consumer: WeaverDocumentId
) -> tuple[tuple[str, WeaverDocumentId], ...]:
    return tuple(
        (dependency.qualified, WeaverDocumentId(consumer.item, dependency))
        for dependency in source.document.dependencies
    )


def _inferred_references(
    source: SourceDocument,
    consumer: WeaverDocumentId,
    edges: list[ItemDependency],
    shortcuts: Mapping[WeaverItemId, Mapping[str, object]] | None = None,
) -> tuple[tuple[str, WeaverDocumentId], ...]:
    """Infer dependencies from Python imports or SQL relations.

    Qualified SQL names are physical dependencies and need no project
    resolution.
    """

    if source.language == "python":
        return tuple(_python_references(source, consumer, edges, shortcuts or {}))

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


def _reject_validation_producer(
    producer: WeaverDocumentId,
    *,
    native: Mapping[WeaverDocumentId, SourceDocument],
    consumer: WeaverDocumentId,
    written: str,
) -> None:
    """Validations read objects but produce no dependency target.

    Installation ordering and non-exhaustive validation dependencies rely on
    this invariant.
    """

    upstream = native.get(producer)
    if upstream is None or not upstream.is_validation:
        return
    raise DiscoveryError(
        f"{consumer}: dependency {written!r} points to "
        f"{upstream.document.kind} {producer}. Tests and assumptions cannot be "
        "dependency targets. Depend on the object the validation checks instead."
    )


def resolve_item_dependencies(repository: WeaverRepository) -> WeaverRepository:
    native = repository.source_documents
    logical_pairs = {
        pair.destination: pair.source for pair in repository.logical_shortcuts
    }
    shortcuts: dict[WeaverItemId, dict[str, object]] = {}
    for declaration in repository.shortcuts:
        shortcuts.setdefault(declaration.owner, {})[declaration.name] = declaration
    folded_native = {str(identity).casefold(): identity for identity in native}
    folded_logical = {str(identity).casefold(): identity for identity in logical_pairs}
    edges: list[ItemDependency] = []
    # Dependency records name the resolved producer. Ordering retains the
    # shortcut destination as a separate hop.
    graph_edges: set[tuple[str, str]] = set()

    for consumer, source in native.items():
        # A declaration replaces discovery, including an explicit empty list.
        if source.document.declares_dependencies:
            references = _declared_references(source, consumer)
        else:
            references = _inferred_references(source, consumer, edges, shortcuts)

        for written, destination in references:
            producer, kind = _resolve_destination(
                destination,
                native=native,
                logical_pairs=logical_pairs,
                folded_native=folded_native,
                folded_logical=folded_logical,
                consumer=consumer,
                written=written,
            )
            _reject_validation_producer(
                producer, native=native, consumer=consumer, written=written
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
    graph = _document_graph(native, logical_pairs, graph_edges)
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
    logical_pairs: Mapping[WeaverDocumentId, WeaverDocumentId],
    graph_edges: set[tuple[str, str]],
) -> Graph:
    """Build the document graph used for ordering and incremental selection.

    A shortcut remains a distinct hop even though the dependency record names
    its resolved source:

    .. code-block:: text

        source document → shortcut destination → consumer document

    Every shortcut contributes its edge even when no document consumes it, so it
    is materialised after its source.
    """

    edges = set(graph_edges)
    for destination, source in logical_pairs.items():
        edges.add((str(source), str(destination)))
    nodes = [str(identity) for identity in native]
    nodes.extend(str(destination) for destination in logical_pairs)
    # A logical shortcut can read a physical shortcut with no source document;
    # include that source so its ordering edge remains active.
    nodes.extend(str(source) for source in logical_pairs.values())
    return Graph(nodes, sorted(edges))


def _item_graph(
    repository: WeaverRepository, resolved: tuple[ItemDependency, ...]
) -> Graph:
    """Build the acyclic item order for a multi-item build.

    Shortcut sources create item dependencies even without a consumer. Internal
    item edges stay in the document graph. A project with an item cycle has no
    valid build order.
    """

    edges: set[tuple[str, str]] = set()
    for edge in resolved:
        if edge.producer is None or edge.producer.item == edge.consumer.item:
            continue
        edges.add((str(edge.producer.item), str(edge.consumer.item)))
    for shortcut in repository.logical_shortcuts:
        # Repository parsing rejects a same-item shortcut, so every shortcut is an
        # edge between two distinct items.
        edges.add((str(shortcut.source.item), str(shortcut.destination.item)))
    try:
        return Graph((str(item.identity) for item in repository.items), sorted(edges))
    except GraphError as exc:
        raise GraphError(
            f"Project items cannot be built in dependency order: {exc}. "
            "Remove one of the dependencies in the cycle."
        ) from exc


def _resolve_destination(
    destination: WeaverDocumentId,
    *,
    native: Mapping[WeaverDocumentId, SourceDocument],
    logical_pairs: Mapping[WeaverDocumentId, WeaverDocumentId],
    folded_native: Mapping[str, WeaverDocumentId],
    folded_logical: Mapping[str, WeaverDocumentId],
    consumer: WeaverDocumentId,
    written: str,
) -> tuple[WeaverDocumentId, str]:
    if destination in native:
        return destination, "native"
    if destination in logical_pairs:
        return logical_pairs[destination], "shortcut"
    # Validations and Lakehouse relations have separate namespaces.
    validation = WeaverDocumentId.validation(destination.item, destination.object_id)
    _reject_validation_producer(
        validation, native=native, consumer=consumer, written=written
    )
    case_match = folded_native.get(str(destination).casefold()) or folded_logical.get(
        str(destination).casefold()
    )
    if case_match:
        raise DiscoveryError(
            f"{consumer}: dependency {written!r} uses the wrong spelling. "
            f"Change it to {str(case_match.object_id)!r}."
        )
    raise DiscoveryError(
        f"{consumer}: dependency {written!r} does not match an object or shortcut "
        f"in {consumer.item}. Add it to that item or correct the dependency."
    )


def _python_references(
    source: SourceDocument,
    consumer: WeaverDocumentId,
    edges: list[ItemDependency],
    shortcuts: Mapping[WeaverItemId, Mapping[str, object]],
) -> list[tuple[str, WeaverDocumentId]]:
    assert source.logical_id is not None
    declared = shortcuts.get(source.logical_id.item, {})
    references: list[tuple[str, WeaverDocumentId]] = []
    for imported in source.python_imports:
        if imported.module == SHORTCUTS_MODULE and not imported.level:
            references.extend(
                _shortcut_reference(name, declared, source, consumer, edges)
                for name in imported.names
            )
            continue
        candidates = _resolved_python_modules(source.logical_id, imported)
        for written, components in candidates:
            if components and components[0] == "lib":
                continue
            object_module = components[-1] if components else ""
            parts = object_module.split("__")
            if len(parts) != 2 or not all(parts):
                continue
            if len(components) == 1:
                raise DiscoveryError(
                    f"{source.relative_path}: import {written!r} does not say whether "
                    f"{object_module!r} is a Table or File. Import "
                    f"{TABLES}.{object_module} or {FILES}.{object_module}."
                )
            if len(components) != 2 or components[0] not in AREAS:
                raise DiscoveryError(
                    f"{source.relative_path}: import {written!r} does not name an item "
                    f"object. Use {TABLES}.<Schema__Object>, "
                    f"{FILES}.<Schema__Object>, or lib.<module>."
                )
            references.append(
                (
                    written,
                    WeaverDocumentId(
                        source.logical_id.item,
                        ObjectId(parts[0], parts[1]),
                        is_files=components[0] == FILES,
                    ),
                )
            )
    return [reference for reference in references if reference is not None]


def _shortcut_reference(
    name: str,
    declared: Mapping[str, object],
    source: SourceDocument,
    consumer: WeaverDocumentId,
    edges: list[ItemDependency],
):
    """Resolve a named shortcut as a logical or physical dependency.

    Physical shortcuts have no project producer to order. A schema shortcut
    remains a dependency on the schema, whose contents can change independently.
    """

    written = f"{SHORTCUTS_MODULE}.{name}"
    if name in (SHORTCUTS_MODULE, "*"):
        raise DiscoveryError(
            f"{source.relative_path}: import each shortcut by name. Use "
            f"'from {SHORTCUTS_MODULE} import <Name>'."
        )
    declaration = declared.get(name)
    if declaration is None:
        known = ", ".join(sorted(declared))
        next_action = (
            f"Import one of: {known}."
            if known
            else f"Declare it in {consumer.item}/{SHORTCUTS_MODULE}.py first."
        )
        raise DiscoveryError(
            f"{source.relative_path}: shortcut {name!r} is not declared for "
            f"{consumer.item}. {next_action}"
        )
    if declaration.is_logical:
        return (written, declaration.destination)
    edges.append(
        ItemDependency(
            consumer=consumer,
            reference=written,
            resolution_kind="physical",
            is_within_item=False,
        )
    )
    return None


def _resolved_python_modules(
    logical_id: WeaverDocumentId, imported
) -> list[tuple[str, tuple[str, ...]]]:
    module = tuple(imported.module.split(".")) if imported.module else ()
    if imported.level:
        # Relative imports begin in the authored area. Validations have no area.
        area = logical_id.area
        base = (area,) if area else ()
        parents = imported.level - 1
        if parents > len(base):
            raise DiscoveryError(
                f"{logical_id}: import {imported} goes above the item root. "
                "Use an import path within the item."
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
    """Select work for the named items without including their dependencies."""

    selected_items = set(bound_items)
    if not selected_items:
        raise BuildError("Select at least one project item to build")
    known_items = {item.identity for item in repository.items}
    unknown = selected_items - known_items
    if unknown:
        raise BuildError(
            "Item(s) not found in the project: " + ", ".join(sorted(map(str, unknown)))
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
