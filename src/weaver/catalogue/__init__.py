"""Weaver's central catalogue — an installation-aware projection of SES.

The catalogue records, for every object Weaver has successfully built, what SES
declared about it. It is emphatically **not** a second authoring model: nothing
here is discovered from a physical table, and nothing here can be written by
hand. SES remains authoritative for descriptive metadata, keys, lineage,
dependencies and behavioural flags; the catalogue is where that information lands
once an object exists, so later operations can be driven from one place instead of
by re-reading a repository.

Three ideas carry the design.

**Installation scope is identity.** A repository is installed independently into
its Lakehouse and its Warehouse, and the same ``Schema.Object`` legitimately
exists in both. So every row is keyed on ``repository`` and ``target_type``
first, and a build that bound only one target type can only ever touch that one.
An object omitted because its target was unbound is out of scope, not deleted.

**The catalogue is built by Weaver, from ordinary SES.** ``_weaver`` is a
package-owned repository declaring the catalogue tables in schema ``_``. Setup
materialises it and installs it through the normal planner and installer. There
is no second "create the catalogue tables" path, and that recursion is the proof
that catalogue objects are ordinary Weaver objects.

**Registry is written last.** A row in :data:`~weaver.catalogue.tables.REGISTRY`
means Weaver certifies that object as installed. Because the installer stops at
the first failed barrier, the certification cannot outrun the physical work: any
earlier failure prevents it. A physical table with no Registry row exists but is
not trusted.

The public surface is deliberately small — the fixed table definitions, and the
renderers that turn projected rows into scoped, deterministic Spark SQL.
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

__all__ = [
    "ALIAS",
    "AUDIT_COLUMN_NAMES",
    "CATALOGUE_REPOSITORY",
    "CATALOGUE_SCHEMA",
    "CATALOGUE_TABLES",
    "COLUMN_DICTIONARY",
    "CatalogueColumn",
    "CatalogueTable",
    "DEPENDENCY",
    "DICTIONARY_TABLES",
    "FOLDER_DICTIONARY",
    "FOREIGN_KEY_DICTIONARY",
    "INDEX_DICTIONARY",
    "INSTALLATION",
    "InstallationScope",
    "KEY_PRIMARY",
    "KEY_UNIQUE",
    "LAKEHOUSE",
    "OBJECT_ROLES",
    "OBJECT_TYPES",
    "REGISTRY",
    "ROLE_DATA",
    "ROLE_LOAD",
    "Row",
    "SCHEMA_DICTIONARY",
    "SIGNATURE",
    "TABLE_DICTIONARY",
    "TARGET_TYPES",
    "WAREHOUSE",
    "column_set",
    "identifier",
    "literal",
    "qualified_name",
    "render_delete_obsolete",
    "render_delete_repository",
    "render_delete_scope",
    "render_merge",
    "sorted_rows",
    "table",
    "target_type_for_ses_target",
    "typed_literal",
]
