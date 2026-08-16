"""Weaver's central catalogue, scoped by logical item.

An installation is identified by ``(item_type, item_name)`` and an object by
that item identity plus ``(schema_name, object_name)``. Files objects use
``Files/<schema>`` as their catalogue schema, so the same four-part identity
covers them without a namespace column.
"""

from __future__ import annotations

from .connection import CatalogueConnection, catalogue_connection
from .render import (
    InstallationScope,
    Row,
    column_set,
    render_delete_obsolete,
    render_delete_scope,
    render_merge,
    sorted_rows,
)
from .state import (
    Catalogue,
    Reconciliation,
    RegisteredDocument,
    for_targets,
    read_catalogue_state,
    reconcile_catalogue_state,
    retaining,
)
from .tables import (
    ALIAS,
    AUDIT_COLUMN_NAMES,
    CATALOGUE_SCHEMA,
    CATALOGUE_TABLES,
    COLUMN_DICTIONARY,
    DEPENDENCY,
    DICTIONARY_TABLES,
    FOLDER_DICTIONARY,
    FOREIGN_KEY_DICTIONARY,
    INSTALLATION,
    ITEM_SCOPE_COLUMNS,
    KEY_DICTIONARY,
    KEY_PRIMARY,
    KEY_UNIQUE,
    OBJECT_ROLES,
    OBJECT_TYPES,
    REGISTRY,
    ROLE_DATA,
    ROLE_LOAD,
    SCHEMA_DICTIONARY,
    SIGNATURE,
    TABLE_DICTIONARY,
    CatalogueColumn,
    CatalogueTable,
    table,
)

__all__ = [
    "ALIAS",
    "AUDIT_COLUMN_NAMES",
    "CATALOGUE_SCHEMA",
    "CATALOGUE_TABLES",
    "COLUMN_DICTIONARY",
    "CatalogueColumn",
    "CatalogueTable",
    "DEPENDENCY",
    "DICTIONARY_TABLES",
    "FOLDER_DICTIONARY",
    "FOREIGN_KEY_DICTIONARY",
    "KEY_DICTIONARY",
    "INSTALLATION",
    "ITEM_SCOPE_COLUMNS",
    "InstallationScope",
    "KEY_PRIMARY",
    "KEY_UNIQUE",
    "OBJECT_ROLES",
    "OBJECT_TYPES",
    "REGISTRY",
    "ROLE_DATA",
    "ROLE_LOAD",
    "Row",
    "SCHEMA_DICTIONARY",
    "SIGNATURE",
    "TABLE_DICTIONARY",
    "column_set",
    "CatalogueConnection",
    "catalogue_connection",
    "render_delete_obsolete",
    "render_delete_scope",
    "render_merge",
    "sorted_rows",
    "table",
    "RegisteredDocument",
    "Catalogue",
    "Reconciliation",
    "retaining",
    "for_targets",
    "read_catalogue_state",
    "reconcile_catalogue_state",
]
