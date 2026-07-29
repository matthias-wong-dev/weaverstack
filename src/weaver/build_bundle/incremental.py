"""Pure incremental selection from a prepared repository and trusted catalogue."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from ..catalogue.state import ReconciledCatalogue
from ..catalogue.tables import REGISTRY
from ..declaration.model import WeaverDocumentId, WeaverRepository


def _ordered(values: Iterable[WeaverDocumentId]) -> tuple[WeaverDocumentId, ...]:
    return tuple(sorted(set(values), key=str))


@dataclass(frozen=True)
class Impact:
    """Signature classification plus existing descendants affected by changes."""

    new: tuple[WeaverDocumentId, ...]
    changed: tuple[WeaverDocumentId, ...]
    impacted: tuple[WeaverDocumentId, ...]
    unchanged: tuple[WeaverDocumentId, ...]

    @property
    def impacted_descendants(self) -> tuple[WeaverDocumentId, ...]:
        return _ordered(set(self.impacted) - set(self.changed))

    def to_mapping(self) -> dict[str, list[str]]:
        return {
            "new": [str(value) for value in self.new],
            "changed": [str(value) for value in self.changed],
            "impacted": [str(value) for value in self.impacted],
            "impacted_descendants": [
                str(value) for value in self.impacted_descendants
            ],
            "unchanged": [str(value) for value in self.unchanged],
        }

    @classmethod
    def from_mapping(cls, mapping) -> "Impact":
        return cls(
            new=tuple(WeaverDocumentId.parse(value) for value in mapping.get("new", ())),
            changed=tuple(
                WeaverDocumentId.parse(value) for value in mapping.get("changed", ())
            ),
            impacted=tuple(
                WeaverDocumentId.parse(value) for value in mapping.get("impacted", ())
            ),
            unchanged=tuple(
                WeaverDocumentId.parse(value) for value in mapping.get("unchanged", ())
            ),
        )


@dataclass(frozen=True)
class IncrementalSelection:
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
    def from_mapping(cls, mapping) -> "IncrementalSelection":
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


def determine_impact(
    repository: WeaverRepository,
    reconciled_catalogue: ReconciledCatalogue,
    *,
    selected: Iterable[WeaverDocumentId],
) -> Impact:
    """Classify bound documents and expand changed roots within their own item.

    Cross-item propagation is intentionally deferred.  A coordinated bundle may
    bind several items, but each item's installed signatures and descendant walk
    are independent until cross-item build semantics are implemented.
    """

    selected_set = set(selected)
    installed: dict[WeaverDocumentId, str] = {}
    for item, tables in reconciled_catalogue.rows.items():
        for row in tables.get(REGISTRY.name, ()):
            schema = str(row.get("schema_name") or "")
            is_files = schema.startswith("Files/")
            logical_schema = schema[len("Files/") :] if is_files else schema
            identity = WeaverDocumentId.parse(
                f"{item}/{'Files/' if is_files else ''}"
                f"{logical_schema}.{row.get('object_name')}"
            )
            if identity in selected_set:
                installed[identity] = str(row.get("signature") or "")

    new: set[WeaverDocumentId] = set()
    changed: set[WeaverDocumentId] = set()
    unchanged: set[WeaverDocumentId] = set()
    for identity in selected_set:
        signature = installed.get(identity)
        if signature is None:
            new.add(identity)
        elif signature != repository.source_documents[identity].effective_signature:
            changed.add(identity)
        else:
            unchanged.add(identity)

    existing = set(installed)
    impacted = set(changed)
    graph = repository.dependency_graph
    if graph is not None:
        by_text = {str(identity): identity for identity in selected_set}
        for root in changed:
            for node in graph.descendants(str(root)):
                descendant = by_text.get(node)
                if (
                    descendant is not None
                    and descendant.item == root.item
                    and descendant in existing
                ):
                    impacted.add(descendant)

    return Impact(
        new=_ordered(new),
        changed=_ordered(changed),
        impacted=_ordered(impacted),
        unchanged=_ordered(unchanged),
    )


def select_incremental_build(
    repository: WeaverRepository,
    reconciled_catalogue: ReconciledCatalogue,
    *,
    selected: Iterable[WeaverDocumentId],
) -> IncrementalSelection:
    impact = determine_impact(
        repository, reconciled_catalogue, selected=selected
    )
    prohibited = {
        identity
        for identity in impact.impacted
        if repository.source_documents[identity].document.prohibit_rebuild
    }
    selected_for_drop = set(impact.impacted) - prohibited
    return IncrementalSelection(
        impact=impact,
        prohibited=_ordered(prohibited),
        selected_for_drop=_ordered(selected_for_drop),
        selected_for_build=_ordered(set(impact.new) | selected_for_drop),
    )
