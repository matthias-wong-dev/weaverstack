"""The physical contract of the Weaver catalogue's ``_`` schema.

Every row starts with the logical ``(item_type, item_name)`` scope.
``Installation`` maps that identity to a physical target; Dictionary and
Registry rows add object identity. :data:`PROJECTED_TABLES` are reconciled from
repository state, while :data:`RUNTIME_TABLES` record execution state.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Mapping

from ..declaration.metadata import AUDIT_COLUMNS, SPARK_SQL, audit_column_name

#: Reserved for the catalogue and never pruned by an application build.
CATALOGUE_SCHEMA = "_"

#: Installed-object vocabulary used for runtime addressing. Files, stored
#: procedures and shortcut schemas are managed objects subject to the lifecycle.
OBJECT_TYPES = ("folder", "table", "view", "file", "stored_procedure", "schema")

#: What an object is for, independent of its physical shape.
ROLE_DATA = "data"
ROLE_LOAD = "load"
ROLE_TEST = "test"
ROLE_ASSUMPTION = "assumption"
#: A pointer to an object another item owns; its physical type varies.
ROLE_SHORTCUT = "shortcut"
#: A managed stored procedure invoked outside Weaver's scheduler.
ROLE_PROGRAMMABLE = "programmable"
OBJECT_ROLES = (
    ROLE_DATA,
    ROLE_LOAD,
    ROLE_TEST,
    ROLE_ASSUMPTION,
    ROLE_SHORTCUT,
    ROLE_PROGRAMMABLE,
)

#: Roles installed to run rather than hold rows.
RUNTIME_ROLES = (ROLE_LOAD, ROLE_TEST, ROLE_ASSUMPTION, ROLE_PROGRAMMABLE)

#: The roles a validation carries, by the kind that declares it.
VALIDATION_ROLES = (ROLE_TEST, ROLE_ASSUMPTION)

#: How a logical key is classified. Both are declared, neither is built.
KEY_PRIMARY = "primary_key"
KEY_UNIQUE = "unique"
KEY_TYPES = (KEY_PRIMARY, KEY_UNIQUE)

#: Public spellings written and read at the persistence boundary.
OBJECT_TYPE_VOCABULARY = {
    "folder": "Folder",
    "table": "Table",
    "view": "View",
    "file": "File",
    "stored_procedure": "Stored procedure",
    "schema": "Schema",
}

OBJECT_ROLE_VOCABULARY = {
    ROLE_DATA: "Data",
    ROLE_LOAD: "Load",
    ROLE_TEST: "Test",
    ROLE_ASSUMPTION: "Assumption",
    ROLE_SHORTCUT: "Shortcut",
    ROLE_PROGRAMMABLE: "Programmable",
}

KEY_TYPE_VOCABULARY = {KEY_PRIMARY: "Primary key", KEY_UNIQUE: "Unique"}

SHORTCUT_TYPE_VOCABULARY = {
    "table": "Table",
    "schema": "Schema",
    "folder": "Folder",
    "view": "View",
}

TARGET_TYPE_VOCABULARY = {"logical": "Logical", "physical": "Physical"}

TEST_TYPE_VOCABULARY = {ROLE_TEST: "Test", ROLE_ASSUMPTION: "Assumption"}

STRING = "string"
BOOLEAN = "boolean"
TIMESTAMP = "timestamp"
BIGINT = "bigint"

#: Warehouse types. Strings default to identifier width; wider columns override it.
WAREHOUSE_TYPES = {
    STRING: "varchar(128)",
    BOOLEAN: "bit",
    TIMESTAMP: "datetime2(6)",
    BIGINT: "bigint",
}

#: Wide enough for prose an author wrote, and for a comma-separated column set.
PROSE_TYPE = "varchar(4000)"
LIST_TYPE = "varchar(1000)"

#: Unbounded, for a column set an author may declare over a wide table. Any
#: bound here would make the catalogue, not the platform, the thing that refuses
#: a project, and narrowing a comparison set to fit storage would change what a
#: load treats as a change. A Fabric Warehouse stores 16 MB in a varchar(max).
WIDE_LIST_TYPE = "varchar(max)"

#: The signature column, on every table.
SIGNATURE = "signature"

#: Initialisms preserved when internal column names become public names.
INITIALISMS = {"id": "ID", "sql": "SQL", "url": "URL", "sk": "SK"}


def public_column_name(name: str) -> str:
    """Convert an internal snake-case name to its public Warehouse spelling."""

    words = [INITIALISMS.get(word, word) for word in name.split("_")]
    first = words[0]
    if first not in INITIALISMS.values():
        first = first[:1].upper() + first[1:]
    return " ".join([first, *words[1:]])


#: Registry publication time. Rebuilt rows are deleted before work and inserted
#: after success; unchanged rows retain their value.
BUILD_DATETIME = "build_datetime"

#: Audit columns appended to every built table, under their internal keys.
AUDIT_COLUMN_NAMES = tuple(
    audit_column_name(logical, SPARK_SQL) for logical in AUDIT_COLUMNS
)
AUDIT_INSERT_COLUMN, AUDIT_UPDATE_COLUMN, AUDIT_DELETE_COLUMN = AUDIT_COLUMN_NAMES


@dataclass(frozen=True)
class CatalogueColumn:
    """An internal column and its public Warehouse representation."""

    name: str
    type: str = STRING
    not_null: bool = False
    description: str = ""
    #: Supplied at publication rather than by catalogue projection.
    published: bool = False
    #: The public spelling, when derivation would not produce it.
    public: str | None = None
    #: Internal value to public value, for a column with a frozen vocabulary.
    #: Outside the comparison because a column's name already determines it, and
    #: a mapping would make the column unhashable.
    vocabulary: Mapping[str, str] | None = field(default=None, compare=False)
    #: The Warehouse type, where an identifier's width is not enough.
    sql_type: str | None = None

    @property
    def public_name(self) -> str:
        return self.public or public_column_name(self.name)

    @property
    def warehouse_type(self) -> str:
        return self.sql_type or WAREHOUSE_TYPES[self.type]

    def to_public(self, value: object) -> object:
        if self.vocabulary is None or value is None:
            return value
        try:
            return self.vocabulary[str(value)]
        except KeyError:
            raise ValueError(
                f"{self.name} does not accept {value!r}; expected one of "
                + ", ".join(sorted(self.vocabulary))
            ) from None

    def from_public(self, value: object) -> object:
        if self.vocabulary is None or value is None:
            return value
        for internal, public in self.vocabulary.items():
            if public == value:
                return internal
        # A newer catalogue may hold a value this Weaver has no name for. It is
        # data rather than a failure, so it is returned as written.
        return value


def _column(qualified: str, columns, name: str) -> CatalogueColumn:
    for column in columns:
        if column.name == name:
            return column
    raise KeyError(f"{qualified} has no column {name!r}")


def _public_name(qualified: str, columns, name: str) -> str:
    if name in AUDIT_COLUMN_NAMES:
        return public_column_name(name)
    return _column(qualified, columns, name).public_name


@dataclass(frozen=True)
class CatalogueTable:
    """A projected catalogue table."""

    name: str
    description: str
    key: tuple[str, ...]
    columns: tuple[CatalogueColumn, ...]

    def __post_init__(self) -> None:
        names = [column.name for column in self.columns]
        if len(set(names)) != len(names):
            raise ValueError(f"{self.name}: duplicate column")
        business = [column.name for column in self.columns if not column.published]
        if business[-1] != SIGNATURE:
            raise ValueError(f"{self.name}: signature must be the last business column")
        if business[: len(self.key)] != list(self.key):
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
        return tuple(column.name for column in self.columns if not column.published)

    @property
    def published_column_names(self) -> tuple[str, ...]:
        return tuple(column.name for column in self.columns if column.published)

    @property
    def comparison_columns(self) -> tuple[str, ...]:
        """Non-key projected columns whose change updates a matched row.

        Published columns are excluded because their new value would make every
        build update every row.
        """

        return tuple(name for name in self.column_names if name not in self.key)

    @property
    def physical_columns(self) -> tuple[str, ...]:
        return self.column_names + self.published_column_names + AUDIT_COLUMN_NAMES

    def column(self, name: str) -> CatalogueColumn:
        return _column(self.qualified, self.columns, name)

    def public_name_of(self, name: str) -> str:
        return _public_name(self.qualified, self.columns, name)

    @property
    def public_columns(self) -> tuple[str, ...]:
        return tuple(self.public_name_of(name) for name in self.physical_columns)


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
        CatalogueColumn("object_name", not_null=True, description="The object's name."),
    )


def _signature(what: str) -> CatalogueColumn:
    return CatalogueColumn(
        SIGNATURE,
        not_null=True,
        description=f"Content hash of {what}, so a change can be detected.",
    )


def _described(*, what: str) -> tuple[CatalogueColumn, ...]:
    """A description and its source when copied from ``$Schema.Object``."""

    return (
        CatalogueColumn(
            "description",
            sql_type=PROSE_TYPE,
            description=f"What this {what} is.",
        ),
        CatalogueColumn(
            "description_reference",
            description="The $Schema.Object this description was copied from, if any.",
        ),
    )


def _lineage() -> tuple[CatalogueColumn, ...]:
    return (
        CatalogueColumn(
            "lineage",
            sql_type=PROSE_TYPE,
            description="Where this object's data comes from.",
        ),
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
        "The physical target, Weaver version and source signature of each logical "
        "item installation."
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
        _signature("the Item declaration"),
    ),
)

REGISTRY = CatalogueTable(
    name="Registry",
    description=(
        "Objects certified as installed. Registry is written last, after each "
        "object and its dependencies succeed."
    ),
    key=(SCOPE_ITEM_TYPE, SCOPE_ITEM_NAME, "schema_name", "object_name"),
    columns=(
        *_scope(),
        *_object(),
        CatalogueColumn(
            "object_type",
            not_null=True,
            vocabulary=OBJECT_TYPE_VOCABULARY,
            description="The installed physical type.",
        ),
        CatalogueColumn(
            "object_role",
            not_null=True,
            vocabulary=OBJECT_ROLE_VOCABULARY,
            description="The object's role.",
        ),
        _signature("the object's source file"),
        CatalogueColumn(
            BUILD_DATETIME,
            TIMESTAMP,
            published=True,
            description=(
                "When this row was published, shared by every row one completed "
                "build wrote."
            ),
        ),
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
        "Declared tables and views. This describes authored documents, not their "
        "physical objects."
    ),
    key=(SCOPE_ITEM_TYPE, SCOPE_ITEM_NAME, "schema_name", "object_name"),
    columns=(
        *_scope(),
        *_object(),
        CatalogueColumn("object_type", not_null=True, description="Table or View."),
        *_described(what="object"),
        *_lineage(),
        CatalogueColumn(
            "primary_key",
            sql_type=LIST_TYPE,
            description="The primary key's columns, in declared order.",
        ),
        CatalogueColumn(
            "not_null_columns",
            sql_type=LIST_TYPE,
            description="Columns declared not null, beyond the primary key.",
        ),
        CatalogueColumn(
            "identity_column",
            description="Weaver's managed surrogate column, when one is declared.",
        ),
        CatalogueColumn(
            "comparison_columns",
            sql_type=WIDE_LIST_TYPE,
            description=(
                "Columns an author named to drive an upsert. Null where none "
                "were named, which is every eligible non-key column."
            ),
        ),
        *_behaviour(),
        _signature("the object's source file"),
    ),
)

FOLDER_DICTIONARY = CatalogueTable(
    name="FolderDictionary",
    description=(
        "Managed folders under their Weaver document identity. The file key limits "
        "which files reconciliation may delete."
    ),
    key=(SCOPE_ITEM_TYPE, SCOPE_ITEM_NAME, "schema_name", "object_name"),
    columns=(
        *_scope(),
        *_object(),
        *_described(what="folder"),
        *_lineage(),
        CatalogueColumn(
            "file_key",
            sql_type=LIST_TYPE,
            description="The glob patterns Weaver manages, in declared order.",
        ),
        *_behaviour(),
        _signature("the object's source file"),
    ),
)

COLUMN_DICTIONARY = CatalogueTable(
    name="ColumnDictionary",
    description=(
        "Authored column descriptions and Weaver-managed surrogate columns. It "
        "does not inventory every physical column."
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

KEY_DICTIONARY = CatalogueTable(
    name="KeyDictionary",
    description=(
        "Declared primary and alternate keys. They identify rows logically but "
        "are not built or enforced."
    ),
    key=(
        SCOPE_ITEM_TYPE,
        SCOPE_ITEM_NAME,
        "schema_name",
        "object_name",
        "key_type",
        "column_set",
    ),
    columns=(
        *_scope(),
        *_object(),
        CatalogueColumn(
            "key_type",
            not_null=True,
            vocabulary=KEY_TYPE_VOCABULARY,
            description="Primary key or Unique.",
        ),
        CatalogueColumn(
            "column_set",
            not_null=True,
            sql_type=LIST_TYPE,
            description="The key's columns, comma-separated in declared order.",
        ),
        _signature("the object's source file"),
    ),
)

FOREIGN_KEY_DICTIONARY = CatalogueTable(
    name="ForeignKeyDictionary",
    description=(
        "Declared relationships, not database constraints. Each row is an unnamed "
        "edge; the primary side carries item identity because it may cross items."
    ),
    key=(
        SCOPE_ITEM_TYPE,
        SCOPE_ITEM_NAME,
        "foreign_schema_name",
        "foreign_object_name",
        "foreign_column_set",
        "primary_item_type",
        "primary_item_name",
        "primary_schema_name",
        "primary_object_name",
        "primary_column_set",
    ),
    columns=(
        *_scope(),
        CatalogueColumn(
            "foreign_schema_name",
            not_null=True,
            description="The schema of the object declaring the relationship.",
        ),
        CatalogueColumn(
            "foreign_object_name",
            not_null=True,
            description="The name of the object declaring the relationship.",
        ),
        CatalogueColumn(
            "foreign_column_set",
            not_null=True,
            sql_type=LIST_TYPE,
            description="The foreign columns, comma-separated in declared order.",
        ),
        CatalogueColumn(
            "primary_item_type", not_null=True, description="The primary item's type."
        ),
        CatalogueColumn(
            "primary_item_name", not_null=True, description="The primary item's name."
        ),
        CatalogueColumn(
            "primary_schema_name", not_null=True, description="The primary schema."
        ),
        CatalogueColumn(
            "primary_object_name", not_null=True, description="The primary object."
        ),
        CatalogueColumn(
            "primary_column_set",
            not_null=True,
            sql_type=LIST_TYPE,
            description="The primary columns, paired in order with the foreign ones.",
        ),
        _signature("the object's source file"),
    ),
)

TEST_DICTIONARY = CatalogueTable(
    name="TestDictionary",
    description=(
        "Declared Tests and Assumptions, not the procedures or modules they compile "
        "to. Both share one logical namespace within an item."
    ),
    key=(SCOPE_ITEM_TYPE, SCOPE_ITEM_NAME, "schema_name", "object_name"),
    columns=(
        *_scope(),
        *_object(),
        CatalogueColumn(
            "test_type",
            not_null=True,
            vocabulary=TEST_TYPE_VOCABULARY,
            description=(
                "Test compares an expected relation with an actual one; "
                "Assumption returns the rows that contradict it."
            ),
        ),
        *_described(what="validation"),
        CatalogueColumn(
            "primary_key",
            sql_type=LIST_TYPE,
            description=(
                "A Test's correlation key, comma-separated in declared order. "
                "Null when undeclared and for every Assumption."
            ),
        ),
        _signature("the validation's source file"),
    ),
)

DEPENDENCY = CatalogueTable(
    name="Dependency",
    description=(
        "Resolved dependency edges and their authored references, scoped to the "
        "referencing item. Cross-item and cross-engine edges are Shortcuts."
    ),
    key=(
        SCOPE_ITEM_TYPE,
        SCOPE_ITEM_NAME,
        "referencing_schema_name",
        "referencing_object_name",
        "dependency_reference",
    ),
    columns=(
        *_scope(),
        CatalogueColumn(
            "referencing_schema_name",
            not_null=True,
            description="The schema of the object declaring the dependency.",
        ),
        CatalogueColumn(
            "referencing_object_name",
            not_null=True,
            description="The name of the object declaring the dependency.",
        ),
        CatalogueColumn(
            "dependency_reference",
            not_null=True,
            sql_type=LIST_TYPE,
            description="The dependency exactly as the owning document wrote it.",
        ),
        CatalogueColumn(
            "referenced_item_type",
            description="The referenced item's type, when the edge resolved.",
        ),
        CatalogueColumn(
            "referenced_item_name",
            description="The referenced item's name, when the edge resolved.",
        ),
        CatalogueColumn(
            "referenced_schema_name",
            description="The referenced schema, when the edge resolved.",
        ),
        CatalogueColumn(
            "referenced_object_name",
            description="The referenced object, when the edge resolved.",
        ),
        _signature("the owning object's source file"),
    ),
)

SHORTCUT = CatalogueTable(
    name="Shortcut",
    description=(
        "Declared cross-item, cross-engine and cross-workspace edges. Logical "
        "targets remain logical; Installation records their physical binding."
    ),
    # Keyed by the shortcut's own id, because a schema shortcut presents a
    # namespace and so names no object, and a merge key cannot be null.
    key=(SCOPE_ITEM_TYPE, SCOPE_ITEM_NAME, "shortcut_id"),
    columns=(
        *_scope(),
        CatalogueColumn(
            "shortcut_id",
            not_null=True,
            description=(
                "The shortcut as its author declared it: 'Sales.Customer' for a "
                "table or folder, 'Reference' for a schema."
            ),
        ),
        CatalogueColumn(
            "schema_name",
            not_null=True,
            description="The schema this item presents the shortcut in.",
        ),
        CatalogueColumn(
            "object_name",
            description=(
                "The object this item presents. Null for a schema shortcut, "
                "which presents a namespace rather than an object."
            ),
        ),
        CatalogueColumn(
            "shortcut_type",
            not_null=True,
            description="What the shortcut is.",
            vocabulary=SHORTCUT_TYPE_VOCABULARY,
        ),
        CatalogueColumn(
            "target_type",
            not_null=True,
            description="Whether the target is a logical item or a physical item.",
            vocabulary=TARGET_TYPE_VOCABULARY,
        ),
        CatalogueColumn(
            "target_item_type", not_null=True, description="The target's item type."
        ),
        CatalogueColumn(
            "target_item_name", not_null=True, description="The target's item name."
        ),
        CatalogueColumn(
            "target_schema_name",
            not_null=True,
            description="The schema or path the target sits in.",
        ),
        CatalogueColumn(
            "target_object_name",
            description=(
                "The target object. Null for a schema or path. Together, the four "
                "target columns preserve a logical producer's identity."
            ),
        ),
        CatalogueColumn(
            "target_workspace_name",
            description=(
                "The workspace the target is in. Null for a logical target, "
                "which is bound, and for a physical one in this workspace."
            ),
        ),
        _signature("the shortcut declaration"),
    ),
)


#: Dictionary reconciliation order, kept stable for payloads and reports.
DICTIONARY_TABLES = (
    SCHEMA_DICTIONARY,
    FOLDER_DICTIONARY,
    TABLE_DICTIONARY,
    COLUMN_DICTIONARY,
    KEY_DICTIONARY,
    FOREIGN_KEY_DICTIONARY,
    TEST_DICTIONARY,
    DEPENDENCY,
    SHORTCUT,
)

#: Reconciliation order: descriptions, binding, then certification.
PROJECTED_TABLES = DICTIONARY_TABLES + (INSTALLATION, REGISTRY)


# --- the catalogue tables maintained at runtime -------------------------------

#: Shared runtime outcomes. Failed means an unacceptable evaluated result; Error
#: means the work could not be evaluated; Blocked means an upstream dependency
#: prevented it; Pending means no outcome for the current incarnation.
PENDING = "pending"
SKIPPED = "skipped"
SUCCEEDED = "succeeded"
FAILED = "failed"
ERROR = "error"
BLOCKED = "blocked"
#: A completed load with rejected rows; valid rows may still have landed.
REJECTED = "rejected"

RESULT_VOCABULARY = {
    PENDING: "Pending",
    SKIPPED: "Skipped",
    SUCCEEDED: "Succeeded",
    FAILED: "Failed",
    ERROR: "Error",
    BLOCKED: "Blocked",
}

#: The same vocabulary plus ``Rejected``, for the tables a load writes.
LOAD_RESULT_VOCABULARY = {**RESULT_VOCABULARY, REJECTED: "Rejected"}

#: Bookmark before any clean load. The text and Python value are the same instant.
BOOKMARK_SENTINEL_TEXT = "1900-01-01 00:00:00.000000"
BOOKMARK_SENTINEL = datetime(1900, 1, 1, tzinfo=timezone.utc)


#: Runtime maintenance modes. History is appended; current state is merged per
#: object incarnation; borrowed state is merged until the object becomes local.
HISTORY = "history"
CURRENT_STATE = "current_state"
BORROWED = "borrowed"
MAINTENANCE = (HISTORY, CURRENT_STATE, BORROWED)

#: Which population's rebuild ends a current-state row's life. A loadable object
#: carries a bookmark and a load status; a validation carries a test status.
BY_LOADABLE = "loadable"
BY_VALIDATION = "validation"
#: The objects _.LoadStatus covers: loadable tables and folders, and Views.
BY_DATA_NODE = "data_node"
INVALIDATED_BY = (BY_DATA_NODE, BY_LOADABLE, BY_VALIDATION)


@dataclass(frozen=True)
class RuntimeTable:
    """A catalogue table maintained by execution rather than projection.

    ``maintenance`` explicitly selects append, current-state merge or borrowed
    merge semantics. ``invalidated_by`` names the population whose rebuild ends
    current state; history and borrowed state are not tied to an incarnation.
    """

    name: str
    description: str
    columns: tuple[CatalogueColumn, ...]
    key: tuple[str, ...] = ()
    maintenance: str = HISTORY
    invalidated_by: str | None = None

    @property
    def is_current_state(self) -> bool:
        return self.maintenance == CURRENT_STATE

    @property
    def is_history(self) -> bool:
        return self.maintenance == HISTORY

    @property
    def is_borrowed(self) -> bool:
        return self.maintenance == BORROWED

    def __post_init__(self) -> None:
        names = [column.name for column in self.columns]
        if len(set(names)) != len(names):
            raise ValueError(f"{self.name}: duplicate column")
        if list(self.key) != names[: len(self.key)]:
            raise ValueError(f"{self.name}: key columns must lead, in key order")
        not_nullable = {column.name for column in self.columns if column.not_null}
        missing = [name for name in self.key if name not in not_nullable]
        if missing:
            raise ValueError(f"{self.name}: key columns must be not null: {missing}")
        if self.maintenance not in MAINTENANCE:
            raise ValueError(f"{self.name}: unknown maintenance {self.maintenance!r}")
        if (
            self.invalidated_by is not None
            and self.invalidated_by not in INVALIDATED_BY
        ):
            raise ValueError(
                f"{self.name}: unknown invalidating population {self.invalidated_by!r}"
            )
        if self.is_history and self.invalidated_by is not None:
            raise ValueError(
                f"{self.name}: history is never invalidated, so it names no "
                "population that would invalidate it"
            )
        if self.is_current_state and self.invalidated_by is None:
            raise ValueError(
                f"{self.name}: current state belongs to an incarnation, so it "
                "names the population whose rebuild ends it"
            )
        if self.is_current_state and not self.key:
            raise ValueError(f"{self.name}: current state is keyed by an identity")
        if self.is_borrowed and self.invalidated_by is not None:
            raise ValueError(
                f"{self.name}: a borrowed row outlives an incarnation, so it "
                "names no population whose rebuild would end it"
            )
        if self.is_borrowed and not self.key:
            raise ValueError(f"{self.name}: a borrowed row is keyed by an identity")

    @property
    def qualified(self) -> str:
        return f"{CATALOGUE_SCHEMA}.{self.name}"

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(column.name for column in self.columns)

    @property
    def comparison_columns(self) -> tuple[str, ...]:
        return tuple(name for name in self.column_names if name not in self.key)

    @property
    def published_column_names(self) -> tuple[str, ...]:
        return ()

    @property
    def physical_columns(self) -> tuple[str, ...]:
        return self.column_names + AUDIT_COLUMN_NAMES

    def column(self, name: str) -> CatalogueColumn:
        return _column(self.qualified, self.columns, name)

    def public_name_of(self, name: str) -> str:
        return _public_name(self.qualified, self.columns, name)

    @property
    def public_columns(self) -> tuple[str, ...]:
        return tuple(self.public_name_of(name) for name in self.physical_columns)


LOG = RuntimeTable(
    name="Log",
    key=("log_sk",),
    maintenance=HISTORY,
    description=(
        "One appended row per settled unit of Weaver work. This is operational "
        "history, not installed state."
    ),
    columns=(
        CatalogueColumn(
            "log_sk",
            not_null=True,
            description=(
                "An immutable surrogate generated by the writer. Fabric Warehouse "
                "tables have no identity columns, and sessions may append concurrently."
            ),
        ),
        CatalogueColumn(
            "workflow_id",
            not_null=True,
            description=(
                "Correlates every row one workflow produced. A composed run "
                "shares one value across its operations."
            ),
        ),
        CatalogueColumn(
            "task_type", not_null=True, description="The kind of work that settled."
        ),
        CatalogueColumn("target_type", description="The physical target's type."),
        CatalogueColumn("target_name", description="The physical target's name."),
        CatalogueColumn("schema_name", description="The object's schema."),
        CatalogueColumn("object_name", description="The object's name."),
        CatalogueColumn(
            "result",
            not_null=True,
            vocabulary=LOAD_RESULT_VOCABULARY,
            description=(
                "How the work ended. A load may also be Rejected, meaning it "
                "completed with rejected rows."
            ),
        ),
        CatalogueColumn(
            "started_datetime", TIMESTAMP, description="When the work started."
        ),
        CatalogueColumn(
            "completed_datetime", TIMESTAMP, description="When the work settled."
        ),
        CatalogueColumn(
            "duration_milliseconds",
            BIGINT,
            description="How long the work took, in milliseconds.",
        ),
        CatalogueColumn(
            "message",
            sql_type=PROSE_TYPE,
            description="Human-readable task summary.",
        ),
        CatalogueColumn(
            "details",
            sql_type=PROSE_TYPE,
            description="Structured task-specific detail, as JSON.",
        ),
    ),
)


BOOKMARK = RuntimeTable(
    name="Bookmark",
    description=(
        "The UTC instant immediately before each loadable object's latest clean "
        "load began. Rebuild and reload reset it to the sentinel; Views have no row."
    ),
    # The Registry's identity exactly, and for the reason a shared key exists at
    # all: a bookmark row and a Registry row describe the same installed object.
    key=(SCOPE_ITEM_TYPE, SCOPE_ITEM_NAME, "schema_name", "object_name"),
    maintenance=CURRENT_STATE,
    invalidated_by=BY_LOADABLE,
    columns=(
        *_scope(),
        *_object(),
        CatalogueColumn(
            "bookmark_datetime",
            TIMESTAMP,
            not_null=True,
            description=(
                "The UTC instant immediately before the most recent clean load "
                f"began, or {BOOKMARK_SENTINEL_TEXT} for an object that has not "
                "had one."
            ),
        ),
    ),
)


def _outcome(*, vocabulary) -> tuple[CatalogueColumn, ...]:
    """The shared result and timing columns for settled work."""

    return (
        CatalogueColumn(
            "result",
            not_null=True,
            vocabulary=vocabulary,
            description="How the work ended.",
        ),
        CatalogueColumn(
            "started_datetime", TIMESTAMP, description="When the work started."
        ),
        CatalogueColumn(
            "completed_datetime", TIMESTAMP, description="When the work settled."
        ),
        CatalogueColumn(
            "duration_milliseconds",
            BIGINT,
            description="How long the work took, in milliseconds.",
        ),
    )


LOAD_STATUS = RuntimeTable(
    name="LoadStatus",
    description=(
        "Current load state for each managed table, folder and View. Rebuilt "
        "tables and folders are Pending until loaded; built Views are Succeeded."
    ),
    key=(SCOPE_ITEM_TYPE, SCOPE_ITEM_NAME, "schema_name", "object_name"),
    maintenance=CURRENT_STATE,
    invalidated_by=BY_DATA_NODE,
    columns=(
        *_scope(),
        *_object(),
        CatalogueColumn(
            "workflow_id",
            description=(
                "The workflow whose load produced this state, so the row can be "
                "read alongside the evidence in _.Log."
            ),
        ),
        *_outcome(vocabulary=LOAD_RESULT_VOCABULARY),
    ),
)


LOAD_STATISTIC = RuntimeTable(
    name="LoadStatistic",
    description=(
        "Append-only counts for each load. Rebuilds do not remove this history."
    ),
    key=("load_statistic_sk",),
    maintenance=HISTORY,
    columns=(
        CatalogueColumn(
            "load_statistic_sk",
            not_null=True,
            description=(
                "An immutable surrogate generated by the writer. Fabric Warehouse "
                "tables have no identity columns, and sessions may append concurrently."
            ),
        ),
        CatalogueColumn(
            "workflow_id",
            not_null=True,
            description="Correlates every row one workflow produced.",
        ),
        *_scope(),
        *_object(),
        CatalogueColumn(
            "started_datetime", TIMESTAMP, description="When the load started."
        ),
        CatalogueColumn(
            "completed_datetime", TIMESTAMP, description="When the load settled."
        ),
        CatalogueColumn(
            "duration_milliseconds",
            BIGINT,
            description="How long the load took, in milliseconds.",
        ),
        CatalogueColumn(
            "rows_read",
            BIGINT,
            description="What the source produced.",
        ),
        CatalogueColumn("rows_inserted", BIGINT, description="Rows the target gained."),
        CatalogueColumn("rows_updated", BIGINT, description="Rows the target changed."),
        CatalogueColumn("rows_deleted", BIGINT, description="Rows the target lost."),
        CatalogueColumn(
            "rows_rejected",
            BIGINT,
            description="Incoming rows the load refused, kept in the reject table.",
        ),
        CatalogueColumn(
            "is_reload",
            BOOLEAN,
            description=(
                "Whether the load re-read a window it had already read. False "
                "until reload is available."
            ),
        ),
        CatalogueColumn(
            "is_static_skip",
            BOOLEAN,
            description=(
                "Whether a Static object was skipped because a clean load had "
                "already run for this incarnation."
            ),
        ),
    ),
)


TEST_STATUS = RuntimeTable(
    name="TestStatus",
    description=(
        "The current result of each Test and Assumption. Rebuilding a "
        "validation sets it to Pending, and running it replaces Pending with "
        "the result."
    ),
    key=(SCOPE_ITEM_TYPE, SCOPE_ITEM_NAME, "schema_name", "object_name"),
    maintenance=CURRENT_STATE,
    invalidated_by=BY_VALIDATION,
    columns=(
        *_scope(),
        *_object(),
        CatalogueColumn(
            "test_type",
            vocabulary=TEST_TYPE_VOCABULARY,
            description="Test or Assumption.",
        ),
        CatalogueColumn(
            "workflow_id",
            description=(
                "The workflow whose run produced this state, so the row can be "
                "read alongside the evidence in _.Log."
            ),
        ),
        *_outcome(vocabulary=RESULT_VOCABULARY),
        CatalogueColumn(
            "failure_count",
            BIGINT,
            description=(
                "Discrepancy rows for a Test or contradicting rows for an "
                "Assumption. Set only when evaluated."
            ),
        ),
    ),
)


MIRROR = RuntimeTable(
    name="Mirror",
    description=(
        "Source and local physical type for installed objects whose data comes "
        "from another target. Removed when the object is built locally."
    ),
    # The Registry's identity exactly: both rows describe one installed object.
    key=(SCOPE_ITEM_TYPE, SCOPE_ITEM_NAME, "schema_name", "object_name"),
    maintenance=BORROWED,
    columns=(
        *_scope(),
        *_object(),
        CatalogueColumn(
            "source_workspace_name",
            not_null=True,
            description="The workspace the data is read from.",
        ),
        CatalogueColumn(
            "source_target_name",
            not_null=True,
            description="The physical item the data is read from.",
        ),
        CatalogueColumn(
            "source_schema_name",
            not_null=True,
            description="The schema the data is read from.",
        ),
        CatalogueColumn(
            "source_object_name",
            not_null=True,
            description="The object the data is read from.",
        ),
        CatalogueColumn(
            "physical_type",
            not_null=True,
            vocabulary=OBJECT_TYPE_VOCABULARY,
            description=(
                "What stands at the local address: a Warehouse View or a "
                "Lakehouse Table or Folder shortcut."
            ),
        ),
    ),
)


#: The catalogue tables maintained during execution.
RUNTIME_TABLES = (LOG, BOOKMARK, LOAD_STATUS, LOAD_STATISTIC, TEST_STATUS)

#: Catalogue views in Warehouses and OneLake shortcuts in Lakehouses.
#: Installation lets ``_.Load`` and ``_.Test`` recover an omitted item name.
STANDARD_SURFACE_TABLES = (INSTALLATION,) + RUNTIME_TABLES

#: The runtime tables describing one object's state now. A build ends the
#: incarnation these describe; the rest is history and survives it.
CURRENT_STATE_TABLES = tuple(
    table for table in RUNTIME_TABLES if table.is_current_state
)

#: The runtime tables recording what happened. Never invalidated.
HISTORY_TABLES = tuple(table for table in RUNTIME_TABLES if table.is_history)

#: Runtime tables created on first borrowed-state write, not by catalogue documents.
BORROWED_TABLES = (MIRROR,)

#: Every catalogue table the ``_weaver`` item declares, however it is
#: maintained. ``_.Mirror`` is not one: see :data:`BORROWED_TABLES`.
CATALOGUE_TABLES = PROJECTED_TABLES + RUNTIME_TABLES

#: Projected state, borrowed nodes and bookmarks read by a run. Status tables are
#: written by runs but only read by builds; see ``state.READ_FOR_BUILD``.
READABLE_TABLES = PROJECTED_TABLES + BORROWED_TABLES + (BOOKMARK,)

TABLES_BY_NAME = {table.name: table for table in CATALOGUE_TABLES + BORROWED_TABLES}


#: The ``_`` schema tables no build may drop, folded for comparison. Every
#: catalogue table holds state a rebuild cannot reproduce: projected rows belong
#: to installations a scoped build has no authority over, and the runtime tables
#: hold a run's own record of what it did and how far it got. All of them are
#: declared ``Prohibit rebuild``, so selection never offers one; this is the
#: guard behind that declaration. ``_.Mirror`` is here because nothing declares
#: it, so an item-scoped prune would find it in the ``_`` inventory unclaimed.
_PROTECTED = frozenset(
    f"{CATALOGUE_SCHEMA}.{table.name}".casefold()
    for table in CATALOGUE_TABLES + BORROWED_TABLES
)


def is_protected(schema: str, name: str) -> bool:
    return f"{schema}.{name}".casefold() in _PROTECTED


def table(name: str) -> CatalogueTable | RuntimeTable:
    try:
        return TABLES_BY_NAME[name]
    except KeyError:
        raise KeyError(
            f"{name!r} is not a catalogue table. Expected one of "
            + ", ".join(sorted(TABLES_BY_NAME))
        ) from None
