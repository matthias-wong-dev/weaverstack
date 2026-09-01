"""Pure incremental selection from a prepared repository and trusted catalogue."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Mapping

from ..catalogue.state import RegisteredDocument
from ..catalogue.tables import ROLE_SHORTCUT
from ..declaration.model import (
    WeaverDocumentId,
    WeaverItemId,
    WeaverRepository,
    parse_installed_identity,
)
from ..errors import BuildError
from ..etl import RuntimeArtefact


def _ordered(values: Iterable[WeaverDocumentId]) -> tuple[WeaverDocumentId, ...]:
    return tuple(sorted(set(values), key=str))


@dataclass(frozen=True)
class Impact:
    """Signature classification plus existing descendants affected by changes."""

    new: tuple[WeaverDocumentId, ...]
    changed: tuple[WeaverDocumentId, ...]
    impacted_descendants: tuple[WeaverDocumentId, ...]

    @property
    def impacted(self) -> tuple[WeaverDocumentId, ...]:
        return _ordered(set(self.changed) | set(self.impacted_descendants))

    def to_mapping(self) -> dict[str, list[str]]:
        return {
            "new": [str(value) for value in self.new],
            "changed": [str(value) for value in self.changed],
            "impacted_descendants": [str(value) for value in self.impacted_descendants],
        }

    @classmethod
    def from_mapping(cls, mapping) -> "Impact":
        changed = tuple(
            parse_installed_identity(value) for value in mapping.get("changed", ())
        )
        descendants = mapping.get("impacted_descendants")
        if descendants is None:
            descendants = tuple(
                value
                for value in mapping.get("impacted", ())
                if parse_installed_identity(value) not in set(changed)
            )
        return cls(
            new=tuple(
                parse_installed_identity(value) for value in mapping.get("new", ())
            ),
            changed=changed,
            impacted_descendants=tuple(
                parse_installed_identity(value) for value in descendants
            ),
        )


@dataclass(frozen=True)
class BuildSelection:
    """The complete, inspectable decision before bundle actions are rendered."""

    impact: Impact
    prohibited: tuple[WeaverDocumentId, ...]
    selected_for_drop: tuple[WeaverDocumentId, ...]
    selected_for_build: tuple[WeaverDocumentId, ...]

    def to_mapping(self) -> dict:
        return {
            "impact": self.impact.to_mapping(),
            "prohibited": [str(value) for value in self.prohibited],
            "selected_for_drop": [str(value) for value in self.selected_for_drop],
            "selected_for_build": [str(value) for value in self.selected_for_build],
        }

    @classmethod
    def from_mapping(cls, mapping) -> "BuildSelection":
        return cls(
            impact=Impact.from_mapping(mapping.get("impact", {})),
            prohibited=tuple(
                parse_installed_identity(value)
                for value in mapping.get("prohibited", ())
            ),
            selected_for_drop=tuple(
                parse_installed_identity(value)
                for value in mapping.get("selected_for_drop", ())
            ),
            selected_for_build=tuple(
                parse_installed_identity(value)
                for value in mapping.get("selected_for_build", ())
            ),
        )


def _as_instant(value) -> datetime | None:
    """One build datetime as something comparable, whatever the reader returned.

    Spark hands back a ``datetime``; a hand-built catalogue or a JSON round trip
    hands back the string that was written. Anything else reads as no build datetime
    rather than being guessed at.
    """

    if isinstance(value, datetime):
        return value
    if isinstance(value, str) and value:
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None
    return None


def stale_through_shortcuts(
    repository: WeaverRepository,
    registered: Mapping[WeaverDocumentId, RegisteredDocument],
    *,
    bound_items: Iterable[WeaverItemId],
) -> tuple[WeaverDocumentId, ...]:
    """Nodes on a logical shortcut's chain that something above them outran.

    The half of cross-item freshness the graph cannot answer. A descendant walk
    carries impact only from a producer whose declaration changed; a producer
    rebuilt by an earlier build looks unchanged to this one, and the only
    surviving evidence is its Registry row's build datetime.

    The chain is ``source <= pointer <= consumer``. A pointer dated before its
    source is named, and the descendant walk carries the rebuild on to what
    reads it. A consumer dated before a current pointer is named directly, which
    is the estate a build that refreshed the pointer and then stopped leaves.

    ``bound_items`` scopes it to what this build could act on. An absent row is
    a missing installation, which signature classification calls new.
    """

    graph = repository.dependency_graph
    if graph is None:
        return ()
    bound = set(bound_items)
    by_text = {str(identity): identity for identity in registered}
    behind = []
    for shortcut in repository.logical_shortcuts:
        destination = shortcut.destination
        if destination.item not in bound or str(destination) not in graph:
            continue
        source = registered.get(shortcut.source)
        pointer = registered.get(destination)
        if source is None or pointer is None:
            continue
        source_datetime = _as_instant(source.build_datetime)
        if source_datetime is None:
            continue
        pointer_datetime = _as_instant(pointer.build_datetime)
        if pointer_datetime is None or source_datetime > pointer_datetime:
            behind.append(destination)
            continue
        for node in graph.descendants(str(destination)):
            consumer = by_text.get(node)
            if consumer is None or consumer.item not in bound:
                continue
            consumer_datetime = _as_instant(registered[consumer].build_datetime)
            if consumer_datetime is None or pointer_datetime > consumer_datetime:
                behind.append(consumer)
    return _ordered(behind)


def shortcut_destinations(repository: WeaverRepository) -> set[WeaverDocumentId]:
    """Every destination this repository declares a pointer at, logical or not."""

    return {declaration.destination for declaration in repository.shortcuts} | {
        shortcut.destination for shortcut in repository.logical_shortcuts
    }


def declared_signatures(
    repository: WeaverRepository,
    selected: Iterable[WeaverDocumentId],
) -> dict[WeaverDocumentId, str]:
    """What each selected node's Registry signature should be, from the source.

    Three kinds of node are selectable, signed differently. A document is signed
    by its source file. A shortcut destination by the pair it declares. This
    destination, that source (see
    :attr:`~weaver.declaration.model.RepositoryShortcut.signature`). A load artefact
    by what it is rendered from: a deployed module by its own bytes, a generated
    body by its document plus the generator's version (see :mod:`weaver.etl`).

    A document's own signature is its
    :attr:`~weaver.declaration.source.SourceDocument.physical_signature`, which
    carries the version of the shape Weaver gives it as well as the source.
    """

    from ..etl import artefacts_by_identity, runtime_artefacts

    shortcuts = {
        shortcut.destination: shortcut for shortcut in repository.logical_shortcuts
    }
    shortcuts.update(
        {declaration.destination: declaration for declaration in repository.shortcuts}
    )
    installed = artefacts_by_identity(runtime_artefacts(repository))
    signatures: dict[WeaverDocumentId, str] = {}
    for identity in selected:
        declaration = shortcuts.get(identity)
        artefact = installed.get(identity)
        if declaration is not None:
            signatures[identity] = declaration.signature
        elif artefact is not None:
            signatures[identity] = artefact.signature
        else:
            signatures[identity] = repository.source_documents[
                identity
            ].physical_signature
    return signatures


def _artefacts_standing_for_their_origin(
    repository: WeaverRepository, selected: set[WeaverDocumentId]
) -> dict[WeaverDocumentId, "RuntimeArtefact"]:
    """Each selected declaration whose artefact is its whole physical form.

    One general relationship: a declaration compiles to a separately installed
    artefact, nothing is materialised under the declaration's own identity, and so
    the artefact's row is the only record of it. What kinds of declaration those
    are is the artefact producer's to know, see
    :attr:`~weaver.etl.RuntimeArtefact.stands_for_origin`.

    A load artefact is absent: a table and the module that loads it are both
    installed and both signed, and the table carries a shape version of its own.
    """

    from ..etl import runtime_artefacts

    return {
        artefact.origin: artefact
        for artefact in runtime_artefacts(repository)
        if artefact.stands_for_origin
        and artefact.origin is not None
        and artefact.origin in selected
    }


def determine_impact(
    repository: WeaverRepository,
    registered: Mapping[WeaverDocumentId, RegisteredDocument],
    *,
    selected: Iterable[WeaverDocumentId],
    physical_types: Mapping[WeaverDocumentId, str],
    stale_consumers: Iterable[WeaverDocumentId] = (),
) -> Impact:
    """Classify bound nodes and expand changed roots across the whole graph.

    Propagation is not confined to one item: the graph carries logical shortcut
    destinations as nodes, so ``source → shortcut destination → consumer`` is an
    ordinary walk. Items not in the build are deferred by construction. They
    are not in ``selected``, so nothing reaches them.

    A changed identity carries impact only where the graph holds it. More is
    selectable than the graph holds, and the graph is the only thing that can
    say which.

    ``stale_consumers`` are objects the catalogue already proved out of date,
    built before the source they stand on (see
    :func:`stale_through_shortcuts`). Their declarations are what they were, so
    they are impacted, and the same walk carries impact on from them.
    """

    selected_set = set(selected)
    physical_types = dict(physical_types)
    installed = {
        identity: document.signature
        for identity, document in registered.items()
        if identity in selected_set
    }
    declared = declared_signatures(repository, selected_set)
    # A declaration whose artefact is its whole physical form is classified by
    # that artefact's row, because that row is the only record of it. Nothing here
    # names which declarations those are; the artefact says so.
    standing_for = _artefacts_standing_for_their_origin(repository, selected_set)

    new: set[WeaverDocumentId] = set()
    changed: set[WeaverDocumentId] = set()
    for identity in selected_set:
        artefact = standing_for.get(identity)
        if artefact is not None:
            recorded = registered.get(artefact.identity)
            signature = None if recorded is None else recorded.signature
            wanted = artefact.signature
        else:
            signature = installed.get(identity)
            wanted = declared[identity]
        if identity not in physical_types:
            new.add(identity)
        elif signature != wanted:
            changed.add(identity)
    stale = {
        identity
        for identity in stale_consumers
        if identity in physical_types and identity in selected_set
    }

    existing = set(physical_types)
    roots = changed | stale
    impacted = set(roots)
    graph = repository.dependency_graph
    if graph is not None:
        by_text = {str(identity): identity for identity in selected_set}
        for root in roots:
            # Impact propagates through the graph, so a changed identity carries
            # it only where the graph holds that identity. Membership is asked of
            # the graph rather than derived from a list of exceptions: several
            # kinds of identity are selected, signed and registered without being
            # a node, and each ends a walk rather than starting one.
            #
            # A runtime artefact is signed by its own content and nothing
            # declares against it. A schema shortcut presents a namespace whose
            # contents belong to the item it points at. A physical shortcut
            # destination names a Fabric item this repository does not manage,
            # so it has no producer here to order it against.
            if str(root) not in graph:
                continue
            for node in graph.descendants(str(root)):
                descendant = by_text.get(node)
                if descendant is not None and descendant in existing:
                    impacted.add(descendant)

    return Impact(
        new=_ordered(new),
        changed=_ordered(changed),
        impacted_descendants=_ordered(impacted - changed),
    )


def _installed_as_shortcut(registered, identity) -> bool:
    """Whether what is installed at this identity is a pointer.

    Read from the Registry row's role, because a shortcut destination and an
    owned object share one identity and only the role separates them.
    """

    document = registered.get(identity)
    return document is not None and document.object_role == ROLE_SHORTCUT


def select_build(
    repository: WeaverRepository,
    registered: Mapping[WeaverDocumentId, RegisteredDocument],
    *,
    selected: Iterable[WeaverDocumentId],
    inventories: Mapping[WeaverItemId, object],
    stale_consumers: Iterable[WeaverDocumentId] = (),
) -> BuildSelection:
    selected = set(selected)
    stale_consumers = set(stale_consumers)
    physical_types = _physical_types(
        repository, selected=selected, inventories=inventories
    )
    impact = determine_impact(
        repository,
        registered,
        selected=selected,
        stale_consumers=stale_consumers,
        physical_types=physical_types,
    )
    # ``prohibit_rebuild`` protects landed data, so the installed role is what
    # it answers for. A pointer holds none of Weaver's data and replacing one
    # destroys nothing, so an identity installed as a shortcut stays
    # replaceable. That is the shortcut-to-owned transition: the declaration
    # arrives first, and the pointer is still what stands at the identity.
    prohibited = {
        identity
        for identity in impact.impacted
        if identity in repository.source_documents
        and repository.source_documents[identity].document.prohibit_rebuild
        and not _installed_as_shortcut(registered, identity)
    }
    # A pointer impacted through the graph is refreshed over its own address and
    # never dropped to do it: `CreateOrOverwrite` for a Lakehouse shortcut and
    # `create or alter view` for a Warehouse one both stand on the address
    # already there, and Fabric holds a deleted shortcut's name for tens of
    # seconds. A pointer whose declared pair changed is classified, not
    # propagated, and is replaced like any other changed node.
    pointers = shortcut_destinations(repository)
    untouched = set(impact.impacted_descendants) & pointers
    selected_for_drop = set(impact.impacted) - prohibited - untouched
    refreshed = (set(impact.impacted) & pointers) - prohibited
    return BuildSelection(
        impact=impact,
        prohibited=_ordered(prohibited),
        selected_for_drop=_ordered(selected_for_drop),
        selected_for_build=_ordered(set(impact.new) | selected_for_drop | refreshed),
    )


def _physical_types(repository, *, selected, inventories) -> dict:
    """The selected identities physically present in prepared target state."""

    missing = {
        identity.item for identity in selected if identity.item not in inventories
    }
    if missing:
        raise BuildError(
            "impact classification requires prepared target inventory for: "
            + ", ".join(sorted(map(str, missing)))
        )
    standing_for = _artefacts_standing_for_their_origin(repository, selected)
    present = {}
    for identity in selected:
        inventory = inventories.get(identity.item)
        if inventory is None:
            continue
        artefact = standing_for.get(identity)
        physical_identity = artefact.identity if artefact is not None else identity
        object_type = inventory.physical_type(physical_identity)
        if object_type is not None:
            present[identity] = object_type
    return present
