"""Deprecated repository/target-scoped catalogue used by the flat planner only.

The active catalogue is exported by :mod:`weaver.catalogue` and is item-scoped.
This module exists solely while the pre-item planner remains available as a
compatibility seam; new code must not import it.
"""

from __future__ import annotations

from .render import (
    InstallationScope,
    Row,
    column_set,
    identifier,
    literal,
    qualified_name,
    render_delete_obsolete,
    render_delete_repository,
    render_delete_scope,
    render_merge,
    sorted_rows,
    typed_literal,
)
from .tables import (
    ALIAS,
    AUDIT_COLUMN_NAMES,
    CATALOGUE_REPOSITORY,
    CATALOGUE_SCHEMA,
    CATALOGUE_TABLES,
    COLUMN_DICTIONARY,
    DEPENDENCY,
    DICTIONARY_TABLES,
    FOLDER_DICTIONARY,
    FOREIGN_KEY_DICTIONARY,
    INDEX_DICTIONARY,
    INSTALLATION,
    KEY_PRIMARY,
    KEY_UNIQUE,
    LAKEHOUSE,
    OBJECT_ROLES,
    OBJECT_TYPES,
    REGISTRY,
    ROLE_DATA,
    ROLE_LOAD,
    SCHEMA_DICTIONARY,
    SIGNATURE,
    TABLE_DICTIONARY,
    TARGET_TYPES,
    WAREHOUSE,
    CatalogueColumn,
    CatalogueTable,
    table,
    target_type_for_ses_target,
)

__all__ = [name for name in globals() if name.isupper() or name in {
    "CatalogueColumn", "CatalogueTable", "InstallationScope", "Row",
    "column_set", "identifier", "literal", "qualified_name",
    "render_delete_obsolete", "render_delete_repository", "render_delete_scope",
    "render_merge", "sorted_rows", "table", "target_type_for_ses_target",
    "typed_literal",
}]
