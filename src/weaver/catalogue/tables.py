"""The fixed shape of every catalogue table, declared once.

These definitions are the single authority on what the catalogue is: the
tables, their ordered columns, their types, and the key that identifies a row.
The built-in Weaver document that materialises them, the reader, the projection
and the rendered DML all read them from here, so no downstream list can drift.

Installation scope is part of the key. Every table is keyed on
``repository`` and ``target_type`` before anything else, because the same
repository is installed independently into its Lakehouse and its Warehouse and
the same ``Schema.Object`` legitimately exists in both:

.. code-block:: text

    SalesRepo | lakehouse | Sales | Customer
    SalesRepo | warehouse | Sales | Customer

Those are two rows, and a Lakehouse build must not touch the second: with the
scope in the identity, no comparison or delete can span both.

The physical target's name is never identity. A repository has at most one
current installation per target type, so rebinding it updates
:data:`INSTALLATION`'s ``target_name`` rather than inserting a second row.

Every table carries ``signature``, the source hash of whatever the row projects,
which incremental planning compares to decide what changed. Weaver's audit
columns are appended by the ordinary build and so are not declared here.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping

from ..declaration.metadata import AUDIT_COLUMNS, SPARK_SQL, audit_column_name

#: The schema Weaver's own control plane lives in, inside the Weaver Lakehouse.
#: One character, reserved, and never touched by an application build's prune.
CATALOGUE_SCHEMA = "_"

#: Installed-object vocabulary used for runtime addressing.
#: ``file`` and ``stored_procedure`` are what a load layer installs — a deployed
#: module or generated statement, and a generated load procedure — and they are
#: ordinary managed objects rather than infrastructure exempt from the lifecycle.
OBJECT_TYPES = ("folder", "table", "view", "file", "stored_procedure")

#: What an object is *for*. A ``data`` object holds or shapes rows; a ``load``
#: object does the work that fills one, and is installed by an item's load layer;
#: a ``test`` or ``assumption`` object is the runnable form of a validation.
#:
#: The role is the answer, and the physical shape is not. A file and a stored
#: procedure used to mean "load artefact" because they were the only two things
#: a load layer installed — and that stopped being true the moment a Test
#: compiled to a module and a procedure of its own. So planning asks what an
#: artefact is *for*, never what shape it has, which is what keeps a Test out of
#: the load DAG.
ROLE_DATA = "data"
ROLE_LOAD = "load"
ROLE_TEST = "test"
ROLE_ASSUMPTION = "assumption"
OBJECT_ROLES = (ROLE_DATA, ROLE_LOAD, ROLE_TEST, ROLE_ASSUMPTION)

#: The roles a runtime artefact carries — everything installed to be *run*
#: rather than to hold rows. Asked where a selection has to be partitioned.
RUNTIME_ROLES = (ROLE_LOAD, ROLE_TEST, ROLE_ASSUMPTION)

#: The roles a validation carries, by the kind that declares it.
VALIDATION_ROLES = (ROLE_TEST, ROLE_ASSUMPTION)

#: How a logical key is classified. Both are declared, neither is built.
KEY_PRIMARY = "primary_key"
KEY_UNIQUE = "unique"
KEY_TYPES = (KEY_PRIMARY, KEY_UNIQUE)

#: The public spelling of every stored vocabulary. Internal Python keeps its
#: snake-case values; the persistence boundary writes and reads these.
OBJECT_TYPE_VOCABULARY = {
    "folder": "Folder",
    "table": "Table",
    "view": "View",
    "file": "File",
    "stored_procedure": "Stored procedure",
}

OBJECT_ROLE_VOCABULARY = {
    ROLE_DATA: "Data",
    ROLE_LOAD: "Load",
    ROLE_TEST: "Test",
    ROLE_ASSUMPTION: "Assumption",
}

KEY_TYPE_VOCABULARY = {KEY_PRIMARY: "Primary key", KEY_UNIQUE: "Unique"}

TEST_TYPE_VOCABULARY = {ROLE_TEST: "Test", ROLE_ASSUMPTION: "Assumption"}

STRING = "string"
BOOLEAN = "boolean"
TIMESTAMP = "timestamp"
BIGINT = "bigint"

#: How each logical type is spelled in the Warehouse the catalogue lives in.
#: A string defaults to an identifier's width; a column holding prose or a list
#: says so with :attr:`CatalogueColumn.sql_type`.
WAREHOUSE_TYPES = {
    STRING: "varchar(128)",
    BOOLEAN: "bit",
    TIMESTAMP: "datetime2(6)",
    BIGINT: "bigint",
}

#: Wide enough for prose an author wrote, and for a comma-separated column set.
PROSE_TYPE = "varchar(4000)"
LIST_TYPE = "varchar(1000)"

#: The signature column, on every table.
SIGNATURE = "signature"

#: Words the public spelling keeps in upper case. Everything else in a column
#: name is an ordinary word: capitalised when it leads, lower case after.
INITIALISMS = {"id": "ID", "sql": "SQL", "url": "URL", "sk": "SK"}


def public_column_name(name: str) -> str:
    """The public Warehouse spelling of an internal column name.

    Internal keys are snake case and the persistence boundary maps them to the
    sentence-case names the ``_`` schema publishes: first word capitalised,
    ordinary words after it in lower case, established initialisms upper.
    """

    words = [INITIALISMS.get(word, word) for word in name.split("_")]
    first = words[0]
    if first not in INITIALISMS.values():
        first = first[:1].upper() + first[1:]
    return " ".join([first, *words[1:]])


#: When the Registry row was last published — one value shared by every row a
#: completed build writes, so two rows can be ordered against each other.
#:
#: Registry publication is Weaver's completion boundary: a row is written last,
#: after everything the object needed succeeded. So "when was this published"
#: *is* "when was this last built", and comparing two rows answers the one
#: question a cross-item build cannot answer from signatures alone — has this
#: alias's source been rebuilt since the alias was made? A signature cannot say
#: that, because reloading a source changes no declaration.
#:
#: It advances only when the row is inserted. Every rebuild goes through an
#: insert — a rebuilt object has its Registry claim removed before any physical
#: work — so an object that was not rebuilt keeps the datetime it had, even if
#: some other column of its row changed.
BUILD_DATETIME = "build_datetime"

#: Weaver's audit columns as this Delta table actually spells them. They are not
#: business columns — the build appends them to every table it creates — but the
#: catalogue writes them, so it has to know them.
AUDIT_COLUMN_NAMES = tuple(
    audit_column_name(logical, SPARK_SQL) for logical in AUDIT_COLUMNS
)
AUDIT_INSERT_COLUMN, AUDIT_UPDATE_COLUMN, AUDIT_DELETE_COLUMN = AUDIT_COLUMN_NAMES


@dataclass(frozen=True)
class CatalogueColumn:
    """One column of a catalogue table.

    ``name`` is the internal snake-case key every Python layer uses. ``public``
    is how the ``_`` schema spells it, and is derived unless the public
    vocabulary differs from the internal concept.
    """

    name: str
    type: str = STRING
    #: Key and scope columns are not null because they are identity. A row that
    #: could not say which installation it belonged to would be unusable.
    not_null: bool = False
    description: str = ""
    #: Supplied by the installer when the row is written, rather than projected
    #: from the declaration — so it is created and described like any other
    #: column, but never appears in a projected row and never takes part in the
    #: comparison that decides whether a row changed. See
    #: :data:`BUILD_DATETIME`, the only one.
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
        """One projected value as the ``_`` schema stores it."""

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
        """One stored value as internal Python spells it."""

        if self.vocabulary is None or value is None:
            return value
        for internal, public in self.vocabulary.items():
            if public == value:
                return internal
        # A newer catalogue may hold a value this Weaver has no name for. It is
        # data rather than a failure, so it is returned as written.
        return value


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
        """The business columns, in order — those a projection supplies."""

        return tuple(column.name for column in self.columns if not column.published)

    @property
    def published_column_names(self) -> tuple[str, ...]:
        """Columns the installer supplies when it writes the row."""

        return tuple(column.name for column in self.columns if column.published)

    @property
    def comparison_columns(self) -> tuple[str, ...]:
        """Non-key columns, whose change makes a matched row an update.

        ``signature`` is one of them: a row whose source file
        changed differs here even when every projected value happens to match.

        A published column is deliberately absent. One that compared would differ
        on every build by construction — its value is new each time — so every
        row would update every build and the no-op that makes an unchanged
        installation cheap would be gone.
        """

        return tuple(name for name in self.column_names if name not in self.key)

    @property
    def physical_columns(self) -> tuple[str, ...]:
        """Every column the built table has: business, published, audit trio."""

        return self.column_names + self.published_column_names + AUDIT_COLUMN_NAMES

    def column(self, name: str) -> CatalogueColumn:
        for column in self.columns:
            if column.name == name:
                return column
        raise KeyError(f"{self.qualified} has no column {name!r}")

    def public_name_of(self, name: str) -> str:
        """The public spelling of one column, audit columns included."""

        if name in AUDIT_COLUMN_NAMES:
            return public_column_name(name)
        return self.column(name).public_name

    @property
    def public_columns(self) -> tuple[str, ...]:
        """Every physical column as the ``_`` schema spells it, in order."""

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
    """A description and the pointer it was copied from, if it was copied.

    A Weaver document ``Description`` is either prose or exactly one ``$Schema.Object``
    reference. When it is a reference the prose is copied from the target and the
    pointer is kept, so a reader can see both what it says and where it came
    from.
    """

    return (
        CatalogueColumn(
            "description",
            sql_type=PROSE_TYPE,
            description=f"What this {what} is.",
        ),
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
            vocabulary=OBJECT_TYPE_VOCABULARY,
            description=(
                "What was installed: Folder, Table, View, File or Stored procedure."
            ),
        ),
        CatalogueColumn(
            "object_role",
            not_null=True,
            vocabulary=OBJECT_ROLE_VOCABULARY,
            description=(
                "What the object is for: Data holds or shapes rows; Load does "
                "the work that fills one."
            ),
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
        "Tables and views together — they are described the same way and a "
        "reader asks the same questions of both. Everything here is declared in "
        "Weaver document; nothing is read back from the physical object."
    ),
    key=(SCOPE_ITEM_TYPE, SCOPE_ITEM_NAME, "schema_name", "object_name"),
    columns=(
        *_scope(),
        *_object(),
        CatalogueColumn("object_type", not_null=True, description="table or view."),
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
            sql_type=LIST_TYPE,
            description="Columns whose change drives an upsert.",
        ),
        *_behaviour(),
        _signature("the object's source file"),
    ),
)

FOLDER_DICTIONARY = CatalogueTable(
    name="FolderDictionary",
    description=(
        "Managed folders. A folder keeps its two-part Weaver document identity rather than "
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

KEY_DICTIONARY = CatalogueTable(
    name="KeyDictionary",
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
        "Declared relationships to primary objects — an ER model rather than "
        "database constraints. Nothing is enforced. Because a relationship has "
        "no name, the row is the edge: every column is part of the key, so two "
        "objects may be related several times over and an object may reference "
        "itself. The owning item scopes the foreign side; the primary side "
        "carries item identity because it may cross items."
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
        "Tests and Assumptions — the estate's declared validation. It describes "
        "the logical authored declaration, not the procedure or module the "
        "validation compiles to: that is a physical artefact and Registry "
        "certifies it. One table for both kinds because a reader asks the same "
        "questions of each, and because Tests and Assumptions share one logical "
        "namespace within an item and so cannot both claim a key."
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
                "A Test's declared key, comma-separated in declared order. It "
                "correlates the two sides of the comparison and does not change "
                "what is counted. Null for a Test that declares none, and always "
                "null for an Assumption, which has one side to correlate."
            ),
        ),
        _signature("the validation's source file"),
    ),
)

DEPENDENCY = CatalogueTable(
    name="Dependency",
    description=(
        "One row per resolved dependency edge, scoped to the referencing item. "
        "The referenced side is the edge Weaver resolved; the authored spelling "
        "is kept alongside it. Crossing items or engines is an alias, recorded "
        "separately, not a dependency that quietly changes namespace."
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
    KEY_DICTIONARY,
    FOREIGN_KEY_DICTIONARY,
    TEST_DICTIONARY,
    DEPENDENCY,
    ALIAS,
)

#: Every catalogue table, dictionaries first, then Installation, then Registry.
#: The order is the reconciliation order: dictionaries describe, Installation
#: records the binding, and Registry certifies — so Registry is last.
CATALOGUE_TABLES = DICTIONARY_TABLES + (INSTALLATION, REGISTRY)

TABLES_BY_NAME = {table.name: table for table in CATALOGUE_TABLES}


# --- operational evidence -----------------------------------------------------

#: How a settled unit of work ended.
RESULT_VOCABULARY = {
    "succeeded": "Succeeded",
    "failed": "Failed",
    "skipped": "Skipped",
    "blocked": "Blocked",
}


@dataclass(frozen=True)
class EvidenceTable:
    """A Weaver-owned ``_`` table that records what happened rather than what is
    installed.

    It carries no signature, is keyed on a surrogate rather than on installation
    scope, and is append-oriented — so it shares the public-name machinery with
    :class:`CatalogueTable` but none of its reconciliation invariants.

    Weaver's audit trio is appended by the ordinary build, as it is to every
    table Weaver creates. Only ``row_insert_datetime`` carries meaning for an
    append-oriented table; the other two are inert here and are not written
    twice for having a name.
    """

    name: str
    description: str
    columns: tuple[CatalogueColumn, ...]

    @property
    def qualified(self) -> str:
        return f"{CATALOGUE_SCHEMA}.{self.name}"

    @property
    def column_names(self) -> tuple[str, ...]:
        """The declared columns — those a caller supplies."""

        return tuple(column.name for column in self.columns)

    @property
    def physical_columns(self) -> tuple[str, ...]:
        """Every column the built table has: declared, then the audit trio."""

        return self.column_names + AUDIT_COLUMN_NAMES

    def column(self, name: str) -> CatalogueColumn:
        for column in self.columns:
            if column.name == name:
                return column
        raise KeyError(f"{self.qualified} has no column {name!r}")

    def public_name_of(self, name: str) -> str:
        if name in AUDIT_COLUMN_NAMES:
            return public_column_name(name)
        return self.column(name).public_name

    @property
    def public_columns(self) -> tuple[str, ...]:
        return tuple(self.public_name_of(name) for name in self.physical_columns)


LOG = EvidenceTable(
    name="Log",
    description=(
        "One row per settled unit of Weaver work. Operational evidence rather "
        "than installed state, so it is append-oriented and is not reconciled "
        "against a declaration."
    ),
    columns=(
        CatalogueColumn(
            "log_sk",
            not_null=True,
            description=(
                "A meaningless immutable surrogate row key. Generated where the "
                "row is, because a Fabric Warehouse has no identity column and "
                "several sessions may append at once."
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
            vocabulary=RESULT_VOCABULARY,
            description="Succeeded, Failed, Skipped or Blocked.",
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
            description="Concise human-readable information.",
        ),
        CatalogueColumn(
            "details",
            sql_type=PROSE_TYPE,
            description="Structured task-specific detail, as JSON.",
        ),
    ),
)


def table(name: str) -> CatalogueTable:
    """One catalogue table by its object name."""

    try:
        return TABLES_BY_NAME[name]
    except KeyError:
        raise KeyError(
            f"{name!r} is not a catalogue table — expected one of "
            + ", ".join(sorted(TABLES_BY_NAME))
        ) from None
