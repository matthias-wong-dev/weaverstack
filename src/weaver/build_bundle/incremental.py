"""Pure incremental selection from a prepared repository and trusted catalogue."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping

from ..catalogue.state import RegisteredDocument
from ..declaration.model import WeaverDocumentId, WeaverRepository


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
            "impacted_descendants": [
                str(value) for value in self.impacted_descendants
            ],
        }

    @classmethod
    def from_mapping(cls, mapping) -> "Impact":
        changed = tuple(
            WeaverDocumentId.parse(value) for value in mapping.get("changed", ())
        )
        descendants = mapping.get("impacted_descendants")
        if descendants is None:
            descendants = tuple(
                value
                for value in mapping.get("impacted", ())
                if WeaverDocumentId.parse(value) not in set(changed)
            )
        return cls(
            new=tuple(WeaverDocumentId.parse(value) for value in mapping.get("new", ())),
            changed=changed,
            impacted_descendants=tuple(
                WeaverDocumentId.parse(value) for value in descendants
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
                WeaverDocumentId.parse(value)
                for value in mapping.get("prohibited", ())
            ),
            selected_for_drop=tuple(
                WeaverDocumentId.parse(value)
                for value in mapping.get("selected_for_drop", ())
            ),
            selected_for_build=tuple(
                WeaverDocumentId.parse(value)
                for value in mapping.get("selected_for_build", ())
            ),
        )


def declared_signatures(
    repository: WeaverRepository,
    selected: Iterable[WeaverDocumentId],
) -> dict[WeaverDocumentId, str]:
    """What each selected node's Registry signature should be, from the source.

    Two kinds of node are selectable and they are signed differently. A document
    is signed by its source file. An alias destination is signed by the pair it
    declares — this destination, that source — because that is the whole of what
    an alias *is* (see :attr:`~weaver.declaration.model.RepositoryAlias.signature`).
    """

    aliases = {alias.destination: alias for alias in repository.aliases}
    signatures: dict[WeaverDocumentId, str] = {}
    for identity in selected:
        alias = aliases.get(identity)
        signatures[identity] = (
            alias.signature
            if alias is not None
            else repository.source_documents[identity].effective_signature
        )
    return signatures


def determine_impact(
    repository: WeaverRepository,
    registered: Mapping[WeaverDocumentId, RegisteredDocument],
    *,
    selected: Iterable[WeaverDocumentId],
    stale_aliases: Iterable[WeaverDocumentId] = (),
) -> Impact:
    """Classify bound nodes and expand changed roots across the whole graph.

    Propagation is no longer confined to one item. The graph carries alias
    destinations as nodes, so the path from a producer to another item's consumer
    is an ordinary walk — ``source → alias destination → consumer`` — and a
    coordinated bundle that binds both items propagates across it exactly as it
    does within one. Items *not* in the build are still deferred, but by
    construction rather than by rule: they are not in ``selected``, so nothing
    reaches them.

    ``stale_aliases`` are destinations the catalogue already proved out of date —
    their source was rebuilt by some earlier build that did not include this item
    (see :func:`weaver.build_bundle.workflow.stale_alias_destinations`). They join
    the changed roots, so their consumers are picked up by the same walk.
    """

    selected_set = set(selected)
    installed = {
        identity: document.signature
        for identity, document in registered.items()
        if identity in selected_set
    }
    declared = declared_signatures(repository, selected_set)

    new: set[WeaverDocumentId] = set()
    changed: set[WeaverDocumentId] = set()
    for identity in selected_set:
        signature = installed.get(identity)
        if signature is None:
            new.add(identity)
        elif signature != declared[identity]:
            changed.add(identity)
    changed |= {
        identity for identity in stale_aliases if identity in installed
    }

    existing = set(installed)
    impacted = set(changed)
    graph = repository.dependency_graph
    if graph is not None:
        by_text = {str(identity): identity for identity in selected_set}
        for root in changed:
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
    stale_aliases: Iterable[WeaverDocumentId] = (),
) -> BuildSelection:
    impact = determine_impact(
        repository, registered, selected=selected, stale_aliases=stale_aliases
    )
    # An alias destination has no source document and therefore no
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
