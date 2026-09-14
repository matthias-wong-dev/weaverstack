"""Explicit catalogue ownership rules for Weaver document types.

Each supported document type declares the tables and row predicates that define
its catalogue claims.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from ..declaration.model import AREAS, WeaverDocumentId
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
    """Return the stored ``schema_name`` for an identity.

    Lakehouse data objects include their area, separating ``Tables/Schema`` from
    ``Files/Schema``. Warehouses, validations and runtime artefacts store their
    schema unchanged. :func:`stored_area` is the inverse.
    """

    area = identity.area
    prefix = f"{area}/" if area else ""
    return f"{prefix}{identity.object_id.schema}"


def stored_area(stored: str) -> tuple[str | None, str]:
    """Split a stored ``schema_name`` into an optional area and schema."""

    head, separator, tail = stored.partition("/")
    if separator and head in AREAS:
        return head, tail
    return None, stored


def catalogue_columns(identity) -> tuple[str, str]:
    """Return the stored schema and object identity columns.

    Schema shortcuts repeat the schema because Registry keys both columns.
    """

    from ..declaration.model import WeaverSchemaId

    if isinstance(identity, WeaverSchemaId):
        return identity.schema, identity.schema
    return catalogue_schema(identity), identity.object_id.object


def bookmark_row(identity: WeaverDocumentId, at=None) -> dict:
    """Build a ``_.Bookmark`` row using Registry identity.

    ``at`` is omitted when only the key is needed.
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
    identity: WeaverDocumentId
    rule: CatalogueClaimRule


_COMMON_OBJECT_RULES = (
    CatalogueClaimRule(REGISTRY),
    CatalogueClaimRule(COLUMN_DICTIONARY),
    CatalogueClaimRule(KEY_DICTIONARY),
    # Relationship ownership uses the declaring side's identity columns.
    CatalogueClaimRule(
        FOREIGN_KEY_DICTIONARY,
        predicate_columns=("foreign_schema_name", "foreign_object_name"),
    ),
    CatalogueClaimRule(
        DEPENDENCY,
        predicate_columns=("referencing_schema_name", "referencing_object_name"),
    ),
)

# Every Registry object type requires an ownership declaration.
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
    # Runtime artefacts claim only Registry.
    "file": (CatalogueClaimRule(REGISTRY),),
    "stored_procedure": (CatalogueClaimRule(REGISTRY),),
    # Schema shortcuts certify a namespace owned by their source item.
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
    """Apply the build's pre-work claim deletion in memory.

    Claims are removed before physical work, so an object is not certified while
    it is being replaced.

    Publication compares against this narrowed state, not the earlier catalogue
    read. A rebuilt object whose projection is unchanged must still be
    republished after its certification was removed.
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
