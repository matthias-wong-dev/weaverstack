"""Weaver's item-scoped central catalogue.

The workspace declaration has no repository dimension. An installation is
identified by ``(item_type, item_name)`` and an object by that item identity plus
``(schema_name, object_name)``. Files objects use ``Files/<schema>`` as their
catalogue schema, preserving the same four-part identity without a namespace
column.
"""

from __future__ import annotations

from .item_tables import (
    ALIAS,
    CATALOGUE_TABLES,
    COLUMN_DICTIONARY,
    DEPENDENCY,
    DICTIONARY_TABLES,
    FOLDER_DICTIONARY,
    FOREIGN_KEY_DICTIONARY,
    INDEX_DICTIONARY,
    INSTALLATION,
    ITEM_SCOPE_COLUMNS,
    REGISTRY,
    SCHEMA_DICTIONARY,
    SIGNATURE,
    TABLE_DICTIONARY,
    ItemInstallationScope,
)
from .render import (
    Row,
    column_set,
    identifier,
    literal,
    qualified_name,
    render_delete_obsolete,
    render_delete_scope,
    render_merge,
    sorted_rows,
    typed_literal,
)
from .tables import (
    AUDIT_COLUMN_NAMES,
    CATALOGUE_SCHEMA,
    KEY_PRIMARY,
    KEY_UNIQUE,
    OBJECT_ROLES,
    OBJECT_TYPES,
    ROLE_DATA,
    ROLE_LOAD,
    CatalogueColumn,
    CatalogueTable,
    table,
)

# The short public name now denotes the only installation scope in the active
# architecture. The longer name remains available when explicitness helps.
InstallationScope = ItemInstallationScope

__all__ = [
    "ALIAS", "AUDIT_COLUMN_NAMES", "CATALOGUE_SCHEMA", "CATALOGUE_TABLES",
    "COLUMN_DICTIONARY", "CatalogueColumn", "CatalogueTable", "DEPENDENCY",
    "DICTIONARY_TABLES", "FOLDER_DICTIONARY", "FOREIGN_KEY_DICTIONARY",
    "INDEX_DICTIONARY", "INSTALLATION", "ITEM_SCOPE_COLUMNS",
    "InstallationScope", "ItemInstallationScope", "KEY_PRIMARY", "KEY_UNIQUE",
    "OBJECT_ROLES", "OBJECT_TYPES", "REGISTRY", "ROLE_DATA", "ROLE_LOAD", "Row",
    "SCHEMA_DICTIONARY", "SIGNATURE", "TABLE_DICTIONARY", "column_set",
    "identifier", "literal", "qualified_name", "render_delete_obsolete",
    "render_delete_scope", "render_merge", "sorted_rows", "table", "typed_literal",
]
