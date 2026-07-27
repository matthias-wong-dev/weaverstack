"""The fixed shape of every catalogue table, declared once.

These definitions are the single authority on what the catalogue *is*: the
tables, their ordered columns, their types, and the key that identifies a row.
Everything else — the built-in SES that materialises them, the tolerant reader,
the projection, the rendered DML — reads them from here. A column added in one
place and forgotten in another is the failure mode this module exists to make
impossible, so nothing downstream is allowed its own list.

**Installation scope is in the key, not beside it.** Every table is keyed on
``repository`` and ``target_type`` before anything else, because the same
repository is installed independently into its Lakehouse and its Warehouse and
the same ``Schema.Object`` legitimately exists in both:

.. code-block:: text

    SalesRepo | lakehouse | Sales | Customer
    SalesRepo | warehouse | Sales | Customer

Those are two rows. A Lakehouse build must not touch the second. Making the scope
part of the identity is what stops a comparison or a delete from accidentally
spanning both — there is no way to name a row without naming its installation.

**The physical target's name is never identity.** A repository has at most one
current installation per target type, so rebinding it to a different Lakehouse
updates :data:`INSTALLATION`'s ``target_name``; it does not insert a second
installation.

**Signatures and audit columns.** Every table carries ``signature`` — the source
hash of whatever the row projects — plus Weaver's own audit columns, which the
ordinary build appends to any Delta table and which are therefore not declared
here as business columns. ``signature`` is what a later branch will compare to
decide what changed and must be dropped and rebuilt; it is the reason the
catalogue is the foundation for idempotent building rather than a report on it.
"""

from __future__ import annotations

from dataclasses import dataclass

from ..declaration.metadata import AUDIT_COLUMNS, SPARK_SQL, audit_column_name

#: The schema Weaver's own control plane lives in, inside the Weaver Lakehouse.
#: One character, reserved, and never touched by an application build's prune.
CATALOGUE_SCHEMA = "_"

#: What an installed object is, in the catalogue's vocabulary. Deliberately
#: coarse: enough for a later operation to know how to address the thing, without
#: inventing an identity scheme that competes with SES's kind and language.
OBJECT_TYPES = ("folder", "table", "view")

#: What an object is *for*. Everything Weaver builds today holds or shapes data;
#: ``load`` arrives with stored procedures, which do work rather than hold rows.
ROLE_DATA = "data"
ROLE_LOAD = "load"
OBJECT_ROLES = (ROLE_DATA, ROLE_LOAD)

#: How a logical key is classified. Both are declared, neither is built.
KEY_PRIMARY = "primary_key"
KEY_UNIQUE = "unique"
INDEX_TYPES = (KEY_PRIMARY, KEY_UNIQUE)

STRING = "string"
BOOLEAN = "boolean"
TIMESTAMP = "timestamp"

#: The signature column, on every table.
SIGNATURE = "signature"

#: Weaver's audit columns as this Delta table actually spells them. They are not
#: business columns — the build appends them to every table it creates — but the
#: catalogue writes them, so it has to know them.
AUDIT_COLUMN_NAMES = tuple(
    audit_column_name(logical, SPARK_SQL) for logical in AUDIT_COLUMNS
)
AUDIT_INSERT_COLUMN, AUDIT_UPDATE_COLUMN, AUDIT_DELETE_COLUMN = AUDIT_COLUMN_NAMES


@dataclass(frozen=True)
class CatalogueColumn:
    """One column of a catalogue table."""

    name: str
    type: str = STRING
    #: Key and scope columns are not null because they are identity. A row that
    #: could not say which installation it belonged to would be unusable.
    not_null: bool = False
    description: str = ""


@dataclass(frozen=True)
class CatalogueTable:
    """One catalogue table's fixed representation.

    ``columns`` are the business columns in their declared order, ``signature``
    last. ``key`` names the columns that identify a row; they always lead, and
    always begin with the installation scope.
    """

    name: str
    description: str
    key: tuple[str, ...]
    columns: tuple[CatalogueColumn, ...]

    def __post_init__(self) -> None:
        names = [column.name for column in self.columns]
        if len(set(names)) != len(names):
            raise ValueError(f"{self.name}: duplicate column")
        if names[-1] != SIGNATURE:
            raise ValueError(f"{self.name}: signature must be the last business column")
        if names[: len(self.key)] != list(self.key):
            raise ValueError(f"{self.name}: key columns must lead, in key order")
        if self.key[:2] != ITEM_SCOPE_COLUMNS:
            raise ValueError(
                f"{self.name}: every key opens with the logical item scope"
            )
        not_nullable = {column.name for column in self.columns if column.not_null}
        missing = [name for name in self.key if name not in not_nullable]
        if missing:
            raise ValueError(f"{self.name}: key columns must be not null: {missing}")
        if SIGNATURE in self.key:
            # It must be a comparison column, because that is what makes a changed
            # source file a changed row. In the key it would leave a table with
            # nothing to compare, and the merge's MATCHED guard would be empty.
            raise ValueError(
                f"{self.name}: signature is a comparison column, never part of the key"
            )

    @property
    def qualified(self) -> str:
        return f"{CATALOGUE_SCHEMA}.{self.name}"

    @property
    def column_names(self) -> tuple[str, ...]:
        """The business columns, in order."""

        return tuple(column.name for column in self.columns)

    @property
    def comparison_columns(self) -> tuple[str, ...]:
        """Non-key columns, whose change makes a matched row an update.

        ``signature`` is one of them, which is the point: a row whose source file
        changed differs here even when every projected value happens to match.
        """

        return tuple(name for name in self.column_names if name not in self.key)

    @property
    def physical_columns(self) -> tuple[str, ...]:
        """Every column the built table has: business, then Weaver's audit trio."""

        return self.column_names + AUDIT_COLUMN_NAMES

    def column(self, name: str) -> CatalogueColumn:
        for column in self.columns:
            if column.name == name:
                return column
        raise KeyError(f"{self.qualified} has no column {name!r}")


# --- the installation scope, shared by every table ---------------------------

SCOPE_ITEM_TYPE = "item_type"
SCOPE_ITEM_NAME = "item_name"
ITEM_SCOPE_COLUMNS = (SCOPE_ITEM_TYPE, SCOPE_ITEM_NAME)


def _scope() -> tuple[CatalogueColumn, ...]:
    return (
        CatalogueColumn(
            SCOPE_ITEM_TYPE,
            not_null=True,
            description="Logical Weaver item type.",
        ),
        CatalogueColumn(
            SCOPE_ITEM_NAME,
            not_null=True,
            description="Logical Weaver item name.",
        ),
    )


def _object() -> tuple[CatalogueColumn, ...]:
    return (
        CatalogueColumn(
            "schema_name", not_null=True, description="The object's schema."
        ),
        CatalogueColumn(
            "object_name", not_null=True, description="The object's name."
        ),
    )


def _signature(what: str) -> CatalogueColumn:
    return CatalogueColumn(
        SIGNATURE,
        not_null=True,
        description=f"Content hash of {what}, so a change can be detected.",
    )


def _described(*, what: str) -> tuple[CatalogueColumn, ...]:
    """A description and the pointer it was copied from, if it was copied.

    A SES ``Description`` is either prose or exactly one ``$Schema.Object``
    reference. When it is a reference the prose is copied from the target and the
    pointer is kept, so a reader can see both what it says and where it came
    from.
    """

    return (
        CatalogueColumn("description", description=f"What this {what} is."),
        CatalogueColumn(
            "description_reference",
            description=(
                "The $Schema.Object the description was copied from, when it was "
                "declared as a reference rather than written here."
            ),
        ),
    )


def _lineage() -> tuple[CatalogueColumn, ...]:
    return (
        CatalogueColumn("lineage", description="Where this object's data comes from."),
        CatalogueColumn(
            "lineage_reference",
            description="The $Schema.Object the lineage was copied from, if any.",
        ),
    )


def _behaviour() -> tuple[CatalogueColumn, ...]:
    return (
        CatalogueColumn(
            "is_incremental",
            BOOLEAN,
            description="Whether load accumulates rows rather than replacing them.",
        ),
        CatalogueColumn(
            "is_static",
            BOOLEAN,
            description="Whether the object is loaded once rather than refreshed.",
        ),
        CatalogueColumn(
            "prohibit_rebuild",
            BOOLEAN,
            description="Whether build may drop and recreate this object.",
        ),
    )


# --- the tables --------------------------------------------------------------

INSTALLATION = CatalogueTable(
    name="Installation",
    description=(
        "One row per repository installation — a repository against one physical "
        "target type. The bound item's name is an attribute, never identity: "
        "rebinding to a different Lakehouse updates this row rather than adding "
        "a second installation."
    ),
    key=(SCOPE_ITEM_TYPE, SCOPE_ITEM_NAME),
    columns=(
        *_scope(),
        CatalogueColumn(
            "target_name",
            not_null=True,
            description="The physical item currently bound to this installation.",
        ),
        CatalogueColumn(
            "weaver_version",
            not_null=True,
            description="The Weaver version that last reconciled this installation.",
        ),
        _signature("the repository as a whole"),
    ),
)

REGISTRY = CatalogueTable(
    name="Registry",
    description=(
        "Objects Weaver currently certifies as installed. A physical table may "
        "exist without a row here, and Weaver then does not treat it as valid. "
        "Written last in a build, so its presence means everything the object "
        "needed succeeded."
    ),
    key=(SCOPE_ITEM_TYPE, SCOPE_ITEM_NAME, "schema_name", "object_name"),
    columns=(
        *_scope(),
        *_object(),
        CatalogueColumn(
            "object_type",
            not_null=True,
            description="What was installed: folder, table or view.",
        ),
        CatalogueColumn(
            "object_role",
            not_null=True,
            description=(
                "What the object is for: data holds or shapes rows; load does "
                "work, which arrives with stored procedures."
            ),
        ),
        _signature("the object's source file"),
    ),
)

SCHEMA_DICTIONARY = CatalogueTable(
    name="SchemaDictionary",
    description="The declared schemas an installation uses, and what they are for.",
    key=(SCOPE_ITEM_TYPE, SCOPE_ITEM_NAME, "schema_name"),
    columns=(
        *_scope(),
        CatalogueColumn("schema_name", not_null=True, description="The schema."),
        *_described(what="schema"),
        _signature("the schema declaration"),
    ),
)

TABLE_DICTIONARY = CatalogueTable(
    name="TableDictionary",
    description=(
        "Tables and views together — they are described the same way and a "
        "reader asks the same questions of both. Everything here is declared in "
        "SES; nothing is read back from the physical object."
    ),
    key=(SCOPE_ITEM_TYPE, SCOPE_ITEM_NAME, "schema_name", "object_name"),
    columns=(
        *_scope(),
        *_object(),
        CatalogueColumn(
            "object_type", not_null=True, description="table or view."
        ),
        *_described(what="object"),
        *_lineage(),
        CatalogueColumn(
            "primary_key",
            description="The primary key's columns, in declared order.",
        ),
        CatalogueColumn(
            "not_null_columns",
            description="Columns declared not null, beyond the primary key.",
        ),
        CatalogueColumn(
            "identity_column",
            description="Weaver's managed surrogate column, when one is declared.",
        ),
        CatalogueColumn(
            "comparison_columns",
            description="Columns whose change drives an upsert.",
        ),
        *_behaviour(),
        _signature("the object's source file"),
    ),
)

FOLDER_DICTIONARY = CatalogueTable(
    name="FolderDictionary",
    description=(
        "Managed folders. A folder keeps its two-part SES identity rather than "
        "being reduced to a path, and its file key is the scope of what Weaver "
        "manages inside it — reconciliation deletes nothing outside that."
    ),
    key=(SCOPE_ITEM_TYPE, SCOPE_ITEM_NAME, "schema_name", "object_name"),
    columns=(
        *_scope(),
        *_object(),
        *_described(what="folder"),
        *_lineage(),
        CatalogueColumn(
            "file_key",
            description="The glob patterns Weaver manages, in declared order.",
        ),
        *_behaviour(),
        _signature("the object's source file"),
    ),
)

COLUMN_DICTIONARY = CatalogueTable(
    name="ColumnDictionary",
    description=(
        "What an author said about a column, plus Weaver's own surrogate. "
        "Purely descriptive: it holds the columns that carry a note, not every "
        "column of every object. Ordinals, types and nullability are physical "
        "and are recorded separately, so nothing here depends on reading a "
        "built table."
    ),
    key=(
        SCOPE_ITEM_TYPE,
        SCOPE_ITEM_NAME,
        "schema_name",
        "object_name",
        "column_name",
    ),
    columns=(
        *_scope(),
        *_object(),
        CatalogueColumn("column_name", not_null=True, description="The column."),
        *_described(what="column"),
        CatalogueColumn(
            "is_identity",
            BOOLEAN,
            description="Whether this is Weaver's managed surrogate column.",
        ),
        _signature("the object's source file"),
    ),
)

INDEX_DICTIONARY = CatalogueTable(
    name="IndexDictionary",
    description=(
        "Declared logical keys — the primary key and any alternate keys. Neither "
        "is built and neither is enforced; they say which column sets identify a "
        "row. A key is identified by its own columns, so it needs no name."
    ),
    key=(
        SCOPE_ITEM_TYPE,
        SCOPE_ITEM_NAME,
        "schema_name",
        "object_name",
        "index_type",
        "column_set",
    ),
    columns=(
        *_scope(),
        *_object(),
        CatalogueColumn(
            "index_type", not_null=True, description="primary_key or unique."
        ),
        CatalogueColumn(
            "column_set",
            not_null=True,
            description="The key's columns, comma-separated in declared order.",
        ),
        _signature("the object's source file"),
    ),
)

FOREIGN_KEY_DICTIONARY = CatalogueTable(
    name="ForeignKeyDictionary",
    description=(
        "Declared relationships to parent objects — an ER model rather than "
        "database constraints. Nothing is enforced. Because a relationship has "
        "no name, the row is the edge: every column is part of the key, so two "
        "objects may be related several times over and an object may reference "
        "itself. The parent is named by its own logical item."
    ),
    key=(
        SCOPE_ITEM_TYPE,
        SCOPE_ITEM_NAME,
        "schema_name",
        "object_name",
        "column_set",
        "reference_item_type",
        "reference_item_name",
        "reference_schema_name",
        "reference_object_name",
        "reference_column_set",
    ),
    columns=(
        *_scope(),
        *_object(),
        CatalogueColumn(
            "column_set",
            not_null=True,
            description="This object's columns, comma-separated in declared order.",
        ),
        CatalogueColumn(
            "reference_item_type", not_null=True, description="The parent's item type."
        ),
        CatalogueColumn(
            "reference_item_name", not_null=True, description="The parent's item name."
        ),
        CatalogueColumn(
            "reference_schema_name", not_null=True, description="The parent's schema."
        ),
        CatalogueColumn(
            "reference_object_name", not_null=True, description="The parent's name."
        ),
        CatalogueColumn(
            "reference_column_set",
            not_null=True,
            description="The parent's columns, paired in order with this object's.",
        ),
        _signature("the object's source file"),
    ),
)

DEPENDENCY = CatalogueTable(
    name="Dependency",
    description=(
        "One row per resolved dependency edge, scoped to the consuming item and "
        "keeping the reference exactly as the author wrote it. Crossing items or "
        "engines is an alias, recorded separately, not a dependency that quietly "
        "changes namespace."
    ),
    key=(
        SCOPE_ITEM_TYPE,
        SCOPE_ITEM_NAME,
        "schema_name",
        "object_name",
        "dependency_name",
    ),
    columns=(
        *_scope(),
        *_object(),
        CatalogueColumn(
            "dependency_name",
            not_null=True,
            description="The dependency exactly as the owning document wrote it.",
        ),
        CatalogueColumn(
            "is_within_item",
            BOOLEAN,
            description="Whether the producer is owned by the same logical item.",
        ),
        _signature("the owning object's source file"),
    ),
)

ALIAS = CatalogueTable(
    name="Alias",
    description=(
        "The name one item presents for a document another item owns, "
        "reproduced from the consuming item's own alias.yml. This is where the "
        "estate's graph crosses items and engines, so it is kept apart from "
        "Dependency — composing Dependency, Alias and Registry is what yields "
        "the whole DAG, and only that composition may cross."
    ),
    key=(
        SCOPE_ITEM_TYPE,
        SCOPE_ITEM_NAME,
        "destination_schema_name",
        "destination_object_name",
    ),
    columns=(
        *_scope(),
        CatalogueColumn(
            "destination_schema_name",
            not_null=True,
            description="The schema the consuming item presents the alias in.",
        ),
        CatalogueColumn(
            "destination_object_name",
            not_null=True,
            description="The name the consuming item presents.",
        ),
        CatalogueColumn(
            "source_item_type", not_null=True, description="The producer's item type."
        ),
        CatalogueColumn(
            "source_item_name", not_null=True, description="The producer's item name."
        ),
        CatalogueColumn(
            "source_schema_name", not_null=True, description="The producer's schema."
        ),
        CatalogueColumn(
            "source_object_name", not_null=True, description="The producer's name."
        ),
        _signature("the alias declaration"),
    ),
)


#: Every dictionary table, in the order a build reconciles them. Order is fixed
#: so a bundle's payloads and a report's actions read the same way every time.
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

#: Every catalogue table, dictionaries first, then Installation, then Registry.
#: The order is the reconciliation order: dictionaries describe, Installation
#: records the binding, and Registry certifies — so Registry is last.
CATALOGUE_TABLES = DICTIONARY_TABLES + (INSTALLATION, REGISTRY)

TABLES_BY_NAME = {table.name: table for table in CATALOGUE_TABLES}


def table(name: str) -> CatalogueTable:
    """One catalogue table by its object name."""

    try:
        return TABLES_BY_NAME[name]
    except KeyError:
        raise KeyError(
            f"{name!r} is not a catalogue table — expected one of "
            + ", ".join(sorted(TABLES_BY_NAME))
        ) from None


