"""Pure incremental selection from a prepared repository and trusted catalogue."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Mapping

from ..catalogue.state import RegisteredDocument
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
    """One build build_datetime as something comparable, whatever the reader returned.

    Spark hands back a ``datetime``; a hand-built catalogue or a JSON round trip
    hands back the string that was written. Anything else reads as no build_datetime
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


def stale_shortcut_destinations(
    repository: WeaverRepository,
    registered: Mapping[WeaverDocumentId, RegisteredDocument],
    *,
    bound_items: Iterable[WeaverItemId],
) -> tuple[WeaverDocumentId, ...]:
    """Shortcuts whose target was rebuilt since the shortcut was last published.

    The half of cross-item freshness the graph cannot answer. A descendant walk
    carries impact only from a producer whose declaration changed; a producer
    rebuilt by an earlier build looks unchanged to this one, and the only
    surviving evidence is in the catalogue.

    So the Registry rows are compared: each carries the build that published it,
    and a producer published later than the shortcut over it means the shortcut's
    consumers were built against something that has moved on. Naming it here
    joins it to the ordinary changed roots.

    ``bound_items`` scopes it to shortcuts this build could act on; a consumer item
    that is not being built keeps its stale shortcut.

    Silent when either row is absent: that is a missing installation, which
    signature classification already calls new.
    """

    bound = set(bound_items)
    stale = []
    for shortcut in repository.logical_shortcuts:
        if shortcut.destination.item not in bound:
            continue
        destination = registered.get(shortcut.destination)
        source = registered.get(shortcut.source)
        if destination is None or source is None:
            continue
        source_datetime = _as_instant(source.build_datetime)
        if source_datetime is None:
            continue
        destination_datetime = _as_instant(destination.build_datetime)
        if destination_datetime is None or source_datetime > destination_datetime:
            stale.append(shortcut.destination)
    return _ordered(stale)


def declared_signatures(
    repository: WeaverRepository,
    selected: Iterable[WeaverDocumentId],
) -> dict[WeaverDocumentId, str]:
    """What each selected node's Registry signature should be, from the source.

    Three kinds of node are selectable, signed differently. A document is signed
    by its source file. A shortcut destination by the pair it declares — this
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
        declaration.destination: declaration for declaration in repository.shortcuts
    }
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


def runtime_artefact_identities(
    repository: WeaverRepository,
) -> frozenset[WeaverDocumentId]:
    """Everything this repository installs to be *run* rather than to hold rows.

    Derived from the repository, because during a build the artefacts have been
    claimed from the declaration and nothing is installed yet. The same question
    from the other side is
    :attr:`~weaver.catalogue.state.RegisteredDocument.object_role`.
    """

    from ..etl import runtime_artefacts

    return frozenset(artefact.identity for artefact in runtime_artefacts(repository))


def _artefacts_standing_for_their_origin(
    repository: WeaverRepository, selected: set[WeaverDocumentId]
) -> dict[WeaverDocumentId, "RuntimeArtefact"]:
    """Each selected declaration whose artefact is its whole physical form.

    One general relationship: a declaration compiles to a separately installed
    artefact, nothing is materialised under the declaration's own identity, and so
    the artefact's row is the only record of it. What kinds of declaration those
    are is the artefact producer's to know — see
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
    stale_shortcuts: Iterable[WeaverDocumentId] = (),
) -> Impact:
    """Classify bound nodes and expand changed roots across the whole graph.

    Propagation is not confined to one item: the graph carries shortcut
    destinations as nodes, so ``source → shortcut destination → consumer`` is an
    ordinary walk. Items not in the build are deferred by construction — they
    are not in ``selected``, so nothing reaches them.

    ``stale_shortcuts`` are destinations the catalogue already proved out of date,
    their source rebuilt by an earlier build (see
    :func:`weaver.build_bundle.workflow.stale_shortcut_destinations`). They join the
    changed roots and their consumers are picked up by the same walk.
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
    # knows which declarations those are; the artefact says so.
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
    changed |= {identity for identity in stale_shortcuts if identity in physical_types}

    existing = set(physical_types)
    impacted = set(changed)
    graph = repository.dependency_graph
    if graph is not None:
        by_text = {str(identity): identity for identity in selected_set}
        runtime = runtime_artefact_identities(repository)
        for root in changed:
            # A runtime artefact is not a node in the authored graph: nothing
            # depends on a deployed module and it depends on nothing, its
            # signature being its own content, so a changed one ends a walk
            # rather than starting one.
            #
            # Membership is asked of the repository rather than read from the
            # identity's shape, which stopped answering when a Test began
            # compiling to a module and a procedure of its own.
            if root in runtime:
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


def select_build(
    repository: WeaverRepository,
    registered: Mapping[WeaverDocumentId, RegisteredDocument],
    *,
    selected: Iterable[WeaverDocumentId],
    inventories: Mapping[WeaverItemId, object],
    stale_shortcuts: Iterable[WeaverDocumentId] = (),
) -> BuildSelection:
    selected = set(selected)
    physical_types = _physical_types(
        repository, selected=selected, inventories=inventories
    )
    impact = determine_impact(
        repository,
        registered,
        selected=selected,
        stale_shortcuts=stale_shortcuts,
        physical_types=physical_types,
    )
    # A shortcut destination has no source document and therefore no
    # ``prohibit_rebuild``: nothing an author writes can forbid replacing a
    # pointer, because replacing one destroys nothing.
    prohibited = {
        identity
        for identity in impact.impacted
        if identity in repository.source_documents
        and repository.source_documents[identity].document.prohibit_rebuild
    }
    selected_for_drop = set(impact.impacted) - prohibited
    return BuildSelection(
        impact=impact,
        prohibited=_ordered(prohibited),
        selected_for_drop=_ordered(selected_for_drop),
        selected_for_build=_ordered(set(impact.new) | selected_for_drop),
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
