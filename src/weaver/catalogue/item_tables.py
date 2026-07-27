"""The item-scoped catalogue representation for repository re-architecture."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from .render import identifier, literal
from .tables import (
    ALIAS as LEGACY_ALIAS,
    BOOLEAN,
    COLUMN_DICTIONARY as LEGACY_COLUMN_DICTIONARY,
    DEPENDENCY as LEGACY_DEPENDENCY,
    FOLDER_DICTIONARY as LEGACY_FOLDER_DICTIONARY,
    FOREIGN_KEY_DICTIONARY as LEGACY_FOREIGN_KEY_DICTIONARY,
    INDEX_DICTIONARY as LEGACY_INDEX_DICTIONARY,
    INSTALLATION as LEGACY_INSTALLATION,
    REGISTRY as LEGACY_REGISTRY,
    SCHEMA_DICTIONARY as LEGACY_SCHEMA_DICTIONARY,
    SIGNATURE,
    TABLE_DICTIONARY as LEGACY_TABLE_DICTIONARY,
    CatalogueColumn,
    CatalogueTable,
)

SCOPE_REPOSITORY = "repository"
SCOPE_ITEM_TYPE = "item_type"
SCOPE_ITEM_NAME = "item_name"
ITEM_SCOPE_COLUMNS = (SCOPE_REPOSITORY, SCOPE_ITEM_TYPE, SCOPE_ITEM_NAME)
OBJECT_NAMESPACE = "object_namespace"
TABLES_NAMESPACE = "Tables"
FILES_NAMESPACE = "Files"


def _scope() -> tuple[CatalogueColumn, ...]:
    return (
        CatalogueColumn(SCOPE_REPOSITORY, not_null=True, description="Owning repository."),
        CatalogueColumn(SCOPE_ITEM_TYPE, not_null=True, description="Logical Weaver item type."),
        CatalogueColumn(SCOPE_ITEM_NAME, not_null=True, description="Logical Weaver item name."),
    )


def _from_legacy(base: CatalogueTable, *, object_scoped: bool) -> CatalogueTable:
    extra_columns = (
        (CatalogueColumn(OBJECT_NAMESPACE, not_null=True, description="Tables or Files."),)
        if object_scoped
        else ()
    )
    extra_key = (OBJECT_NAMESPACE,) if object_scoped else ()
    return CatalogueTable(
        name=base.name,
        description=base.description,
        key=ITEM_SCOPE_COLUMNS + extra_key + base.key[2:],
        columns=_scope() + extra_columns + base.columns[2:],
    )


INSTALLATION = _from_legacy(LEGACY_INSTALLATION, object_scoped=False)
SCHEMA_DICTIONARY = _from_legacy(LEGACY_SCHEMA_DICTIONARY, object_scoped=False)
REGISTRY = _from_legacy(LEGACY_REGISTRY, object_scoped=True)
TABLE_DICTIONARY = _from_legacy(LEGACY_TABLE_DICTIONARY, object_scoped=True)
FOLDER_DICTIONARY = _from_legacy(LEGACY_FOLDER_DICTIONARY, object_scoped=True)
COLUMN_DICTIONARY = _from_legacy(LEGACY_COLUMN_DICTIONARY, object_scoped=True)
INDEX_DICTIONARY = _from_legacy(LEGACY_INDEX_DICTIONARY, object_scoped=True)

FOREIGN_KEY_DICTIONARY = CatalogueTable(
    name=LEGACY_FOREIGN_KEY_DICTIONARY.name,
    description="Item-qualified logical foreign-key relationships.",
    key=(
        *ITEM_SCOPE_COLUMNS,
        OBJECT_NAMESPACE,
        "schema_name",
        "object_name",
        "column_set",
        "reference_item_type",
        "reference_item_name",
        "reference_namespace",
        "reference_schema_name",
        "reference_object_name",
        "reference_column_set",
    ),
    columns=(
        *_scope(),
        CatalogueColumn(OBJECT_NAMESPACE, not_null=True),
        CatalogueColumn("schema_name", not_null=True),
        CatalogueColumn("object_name", not_null=True),
        CatalogueColumn("column_set", not_null=True),
        CatalogueColumn("reference_item_type", not_null=True),
        CatalogueColumn("reference_item_name", not_null=True),
        CatalogueColumn("reference_namespace", not_null=True),
        CatalogueColumn("reference_schema_name", not_null=True),
        CatalogueColumn("reference_object_name", not_null=True),
        CatalogueColumn("reference_column_set", not_null=True),
        CatalogueColumn(SIGNATURE, not_null=True),
    ),
)

DEPENDENCY = CatalogueTable(
    name=LEGACY_DEPENDENCY.name,
    description="Consumer-item dependency names exactly as authored.",
    key=(
        *ITEM_SCOPE_COLUMNS,
        OBJECT_NAMESPACE,
        "schema_name",
        "object_name",
        "dependency_name",
    ),
    columns=(
        *_scope(),
        CatalogueColumn(OBJECT_NAMESPACE, not_null=True),
        CatalogueColumn("schema_name", not_null=True),
        CatalogueColumn("object_name", not_null=True),
        CatalogueColumn("dependency_name", not_null=True),
        CatalogueColumn("is_within_item", BOOLEAN, not_null=True),
        CatalogueColumn(SIGNATURE, not_null=True),
    ),
)

ALIAS = CatalogueTable(
    name=LEGACY_ALIAS.name,
    description="Destination-keyed declarations reproduced from alias.yml.",
    key=(
        *ITEM_SCOPE_COLUMNS,
        "destination_namespace",
        "destination_schema_name",
        "destination_object_name",
    ),
    columns=(
        *_scope(),
        CatalogueColumn("destination_namespace", not_null=True),
        CatalogueColumn("destination_schema_name", not_null=True),
        CatalogueColumn("destination_object_name", not_null=True),
        CatalogueColumn("source_item_type", not_null=True),
        CatalogueColumn("source_item_name", not_null=True),
        CatalogueColumn("source_namespace", not_null=True),
        CatalogueColumn("source_schema_name", not_null=True),
        CatalogueColumn("source_object_name", not_null=True),
        CatalogueColumn(SIGNATURE, not_null=True),
    ),
)

DICTIONARY_TABLES = (
    SCHEMA_DICTIONARY,
    FOLDER_DICTIONARY,
    TABLE_DICTIONARY,
    COLUMN_DICTIONARY,
    INDEX_DICTIONARY,
    FOREIGN_KEY_DICTIONARY,
    DEPENDENCY,
    ALIAS,
)
CATALOGUE_TABLES = DICTIONARY_TABLES + (INSTALLATION, REGISTRY)
TABLES_BY_NAME = {table.name: table for table in CATALOGUE_TABLES}


@dataclass(frozen=True)
class ItemInstallationScope:
    repository: str
    item_type: str
    item_name: str

    @property
    def columns(self) -> tuple[str, ...]:
        return ITEM_SCOPE_COLUMNS

    @property
    def values(self) -> Mapping[str, str]:
        return {
            SCOPE_REPOSITORY: self.repository,
            SCOPE_ITEM_TYPE: self.item_type,
            SCOPE_ITEM_NAME: self.item_name,
        }

    @property
    def predicate(self) -> str:
        return self.predicate_for()

    def predicate_for(self, qualifier: str = "") -> str:
        prefix = f"{qualifier}." if qualifier else ""
        return " AND ".join(
            f"{prefix}{identifier(column)} = {literal(value)}"
            for column, value in self.values.items()
        )

    def owns(self, row) -> bool:
        return all(row.get(column) == value for column, value in self.values.items())

    def __str__(self) -> str:
        return f"{self.repository}/{self.item_type}/{self.item_name}"
