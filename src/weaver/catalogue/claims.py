"""Explicit catalogue ownership rules for Weaver document types.

Catalogue rows are not owned merely because their table happens to contain an
object-shaped pair of columns.  Each supported document type names every table
it may populate and the predicate by which rows in that table are its claims.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from ..declaration.metadata import FOLDER, TABLE, VIEW
from ..declaration.model import WeaverDocumentId
from ..errors import BuildError
from .tables import (
    COLUMN_DICTIONARY,
    DEPENDENCY,
    FOLDER_DICTIONARY,
    FOREIGN_KEY_DICTIONARY,
    INDEX_DICTIONARY,
    OBJECT_TYPES,
    REGISTRY,
    TABLE_DICTIONARY,
    CatalogueTable,
)


def catalogue_schema(identity: WeaverDocumentId) -> str:
    prefix = "Files/" if identity.is_files else ""
    return f"{prefix}{identity.object_id.schema}"


@dataclass(frozen=True)
class CatalogueClaimRule:
    """One table a document type may populate and how it owns its rows."""

    table: CatalogueTable
    predicate_columns: tuple[str, str] = ("schema_name", "object_name")

    def values(self, identity: WeaverDocumentId) -> tuple[str, str]:
        return catalogue_schema(identity), identity.object_id.object

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
    CatalogueClaimRule(INDEX_DICTIONARY),
    CatalogueClaimRule(FOREIGN_KEY_DICTIONARY),
    CatalogueClaimRule(DEPENDENCY),
)

# This is deliberately exhaustive. Adding another Registry object_type requires
# an ownership declaration here before it can participate in reconciliation.
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
}

OBJECT_TYPE_FOR_DOCUMENT_KIND = {FOLDER: "folder", TABLE: "table", VIEW: "view"}


def claim_rules_for_object_type(object_type: str) -> tuple[CatalogueClaimRule, ...]:
    try:
        return CATALOGUE_CLAIMS_BY_OBJECT_TYPE[object_type]
    except KeyError as exc:
        expected = ", ".join(OBJECT_TYPES)
        raise BuildError(
            f"Registry object_type must be one of {expected}, got {object_type!r}"
        ) from exc


def claim_rules_for_document_kind(kind: str) -> tuple[CatalogueClaimRule, ...]:
    try:
        object_type = OBJECT_TYPE_FOR_DOCUMENT_KIND[kind]
    except KeyError as exc:
        raise BuildError(f"unsupported Weaver document kind {kind!r}") from exc
    return claim_rules_for_object_type(object_type)
