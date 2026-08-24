"""Explicit catalogue ownership rules for Weaver document types.

Each supported document type declares the tables and row predicates that define
its catalogue claims.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from ..declaration.model import WeaverDocumentId
from ..errors import BuildError
from .tables import (
    COLUMN_DICTIONARY,
    DEPENDENCY,
    FOLDER_DICTIONARY,
    FOREIGN_KEY_DICTIONARY,
    KEY_DICTIONARY,
    OBJECT_TYPES,
    REGISTRY,
    TABLE_DICTIONARY,
    CatalogueTable,
)


def catalogue_schema(identity: WeaverDocumentId) -> str:
    """The ``schema_name`` this identity is stored under.

    A Folder carries its ``Files/`` prefix because that prefix is part of what
    distinguishes it from a table of the same name. A load artefact does not get
    one: its schema is already the real thing — the containing path for a file,
    the Warehouse schema for a procedure — and prefixing it would store something
    that is not the target's own name.
    """

    prefix = "Files/" if identity.is_files else ""
    return f"{prefix}{identity.object_id.schema}"


def catalogue_columns(identity) -> tuple[str, str]:
    """The ``schema_name`` and ``object_name`` this identity is stored under.

    A schema shortcut presents a namespace, so it names the schema in both
    columns. One reader for what :func:`weaver.catalogue.projection._identity`
    writes, so the two cannot drift.
    """

    from ..declaration.model import WeaverSchemaId

    if isinstance(identity, WeaverSchemaId):
        return identity.schema, identity.schema
    return catalogue_schema(identity), identity.object_id.object


def bookmark_row(identity: WeaverDocumentId, at=None) -> dict:
    """One ``_.Bookmark`` row for an object, keyed as the Registry keys it.

    One builder for every writer — a build's invalidation, a run's advance, a
    standalone load's — so the four columns are spelled the same way wherever a
    bookmark is written. ``at`` is left out when only the key is wanted.
    """

    row = {
        "item_type": identity.item.item_type,
        "item_name": identity.item.item_name,
        "schema_name": catalogue_schema(identity),
        "object_name": identity.object_id.object,
    }
    if at is not None:
        row["bookmark_datetime"] = at
    return row


@dataclass(frozen=True)
class CatalogueClaimRule:
    """One table a document type may populate and how it owns its rows."""

    table: CatalogueTable
    predicate_columns: tuple[str, str] = ("schema_name", "object_name")

    def values(self, identity) -> tuple[str, str]:
        return catalogue_columns(identity)

    def owns(self, row: Mapping[str, object], identity: WeaverDocumentId) -> bool:
        expected = self.values(identity)
        return all(
            str(row.get(column)) == value
            for column, value in zip(self.predicate_columns, expected)
        )


@dataclass(frozen=True)
class CatalogueClaim:
    """A concrete document claim collected for deletion."""

    identity: WeaverDocumentId
    rule: CatalogueClaimRule


_COMMON_OBJECT_RULES = (
    CatalogueClaimRule(REGISTRY),
    CatalogueClaimRule(COLUMN_DICTIONARY),
    CatalogueClaimRule(KEY_DICTIONARY),
    # A relationship table names both sides, so the owning object is found under
    # the side it declares rather than under a bare schema/object pair.
    CatalogueClaimRule(
        FOREIGN_KEY_DICTIONARY,
        predicate_columns=("foreign_schema_name", "foreign_object_name"),
    ),
    CatalogueClaimRule(
        DEPENDENCY,
        predicate_columns=("referencing_schema_name", "referencing_object_name"),
    ),
)

# Each Registry object type requires an ownership declaration before reconciliation.
CATALOGUE_CLAIMS_BY_OBJECT_TYPE: Mapping[str, tuple[CatalogueClaimRule, ...]] = {
    "folder": (
        _COMMON_OBJECT_RULES[0],
        CatalogueClaimRule(FOLDER_DICTIONARY),
        *_COMMON_OBJECT_RULES[1:],
    ),
    "table": (
        _COMMON_OBJECT_RULES[0],
        CatalogueClaimRule(TABLE_DICTIONARY),
        *_COMMON_OBJECT_RULES[1:],
    ),
    "view": (
        _COMMON_OBJECT_RULES[0],
        CatalogueClaimRule(TABLE_DICTIONARY),
        *_COMMON_OBJECT_RULES[1:],
    ),
    # A load artefact claims the Registry and nothing else. It declares no
    # columns, no keys, no relationships and no dependencies — it is a deployed
    # module or a generated statement, and the only thing the catalogue records
    # about it is that Weaver installed it and at what signature.
    "file": (CatalogueClaimRule(REGISTRY),),
    "stored_procedure": (CatalogueClaimRule(REGISTRY),),
    # A schema shortcut claims its repeated Schema/Schema Registry identity and
    # no dictionary row: it presents a namespace that its source item owns.
    "schema": (CatalogueClaimRule(REGISTRY),),
}


def claim_rules_for_object_type(object_type: str) -> tuple[CatalogueClaimRule, ...]:
    try:
        return CATALOGUE_CLAIMS_BY_OBJECT_TYPE[object_type]
    except KeyError as exc:
        expected = ", ".join(OBJECT_TYPES)
        raise BuildError(
            f"Registry object_type must be one of {expected}, got {object_type!r}"
        ) from exc


def without_claims(catalogue, claims):
    """The catalogue as the build's claim-deletion stage will leave it.

    A build removes the catalogue claims of everything it is about to drop
    *before* it does any physical work, so a row can never stay certified while
    the object behind it is being replaced. The catalogue the planner read still
    contains those rows, though — it was read before any of this was decided.

    That matters now that publication is a difference. An object dropped and
    rebuilt whose projection did not change would compare equal against the
    catalogue as read, produce no merge, and stay deleted: the before-stage
    removed it and nothing put it back. Comparing against the state the deletes
    will actually produce is what makes "unchanged" mean unchanged.

    It is a narrowing, never a widening — rows are only ever removed here — so
    the worst a mistake in it can do is publish a row that did not need
    publishing.
    """

    from types import MappingProxyType

    from .state import Catalogue

    by_item: dict = {}
    for claim in claims:
        by_item.setdefault(claim.identity.item, []).append(claim)
    if not by_item:
        return catalogue

    rows = {}
    for item, tables in catalogue.rows.items():
        item_claims = by_item.get(item)
        if not item_claims:
            rows[item] = tables
            continue
        kept = {}
        for name, table_rows in tables.items():
            owners = [claim for claim in item_claims if claim.rule.table.name == name]
            kept[name] = tuple(
                row
                for row in table_rows
                if not any(claim.rule.owns(row, claim.identity) for claim in owners)
            )
        rows[item] = MappingProxyType(kept)
    return Catalogue(rows=MappingProxyType(rows), materialised=catalogue.materialised)
