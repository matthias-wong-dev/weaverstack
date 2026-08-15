"""Pure incremental selection from a prepared repository and trusted catalogue."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Mapping

from ..catalogue.state import RegisteredDocument
from ..declaration.model import WeaverDocumentId, WeaverItemId, WeaverRepository


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
            new=tuple(
                WeaverDocumentId.parse(value) for value in mapping.get("new", ())
            ),
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
                WeaverDocumentId.parse(value) for value in mapping.get("prohibited", ())
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


def _as_instant(value) -> datetime | None:
    """One build epoch as something comparable, whatever the reader returned.

    Spark hands back a ``datetime``; a hand-built catalogue or a JSON round trip
    hands back the string that was written. Anything else reads as no epoch
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


def stale_alias_destinations(
    repository: WeaverRepository,
    registered: Mapping[WeaverDocumentId, RegisteredDocument],
    *,
    bound_items: Iterable[WeaverItemId],
) -> tuple[WeaverDocumentId, ...]:
    """Aliases whose source has been rebuilt since the alias was last published.

    The half of cross-item freshness the graph cannot answer. A descendant walk
    carries impact only from a producer whose declaration changed; a producer
    rebuilt by an earlier build looks unchanged to this one, and the only
    surviving evidence is in the catalogue.

    So the Registry rows are compared: each carries the build that published it,
    and a producer published later than the alias over it means the alias's
    consumers were built against something that has moved on. Naming it here
    joins it to the ordinary changed roots.

    ``bound_items`` scopes it to aliases this build could act on; a consumer item
    that is not being built keeps its stale alias.

    Silent when either row is absent: that is a missing installation, which
    signature classification already calls new.
    """

    bound = set(bound_items)
    stale = []
    for alias in repository.aliases:
        if alias.destination.item not in bound:
            continue
        destination = registered.get(alias.destination)
        source = registered.get(alias.source)
        if destination is None or source is None:
            continue
        source_epoch = _as_instant(source.build_epoch)
        if source_epoch is None:
            continue
        destination_epoch = _as_instant(destination.build_epoch)
        if destination_epoch is None or source_epoch > destination_epoch:
            stale.append(alias.destination)
    return _ordered(stale)


def declared_signatures(
    repository: WeaverRepository,
    selected: Iterable[WeaverDocumentId],
) -> dict[WeaverDocumentId, str]:
    """What each selected node's Registry signature should be, from the source.

    Three kinds of node are selectable, signed differently. A document is signed
    by its source file. An alias destination by the pair it declares — this
    destination, that source (see
    :attr:`~weaver.declaration.model.RepositoryAlias.signature`). A load artefact
    by what it is rendered from: a deployed module by its own bytes, a generated
    body by its document plus the generator's version (see :mod:`weaver.etl`).
    """

    from ..etl import artefacts_by_identity, runtime_artefacts

    aliases = {alias.destination: alias for alias in repository.aliases}
    installed = artefacts_by_identity(runtime_artefacts(repository))
    signatures: dict[WeaverDocumentId, str] = {}
    for identity in selected:
        alias = aliases.get(identity)
        artefact = installed.get(identity)
        if alias is not None:
            signatures[identity] = alias.signature
        elif artefact is not None:
            signatures[identity] = artefact.signature
        else:
            signatures[identity] = repository.source_documents[
                identity
            ].effective_signature
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


def determine_impact(
    repository: WeaverRepository,
    registered: Mapping[WeaverDocumentId, RegisteredDocument],
    *,
    selected: Iterable[WeaverDocumentId],
    stale_aliases: Iterable[WeaverDocumentId] = (),
) -> Impact:
    """Classify bound nodes and expand changed roots across the whole graph.

    Propagation is not confined to one item: the graph carries alias
    destinations as nodes, so ``source → alias destination → consumer`` is an
    ordinary walk. Items not in the build are deferred by construction — they
    are not in ``selected``, so nothing reaches them.

    ``stale_aliases`` are destinations the catalogue already proved out of date,
    their source rebuilt by an earlier build (see
    :func:`weaver.build_bundle.workflow.stale_alias_destinations`). They join the
    changed roots and their consumers are picked up by the same walk.
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
    changed |= {identity for identity in stale_aliases if identity in installed}

    existing = set(installed)
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
