"""Parse and validate the YAML contract at the top of a Weaver source file.

Known keys, declared columns, and incompatible settings are validated before
build. Query-derived SQL shapes defer column validation until build.
"""

from __future__ import annotations

import ast
import re
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any

import yaml

from ..errors import MetadataError

FOLDER = "Folder"
TABLE = "Table"
VIEW = "View"
OBJECT_KINDS = frozenset({FOLDER, TABLE, VIEW})

#: Logical validation declaration kinds.
TEST = "Test"
ASSUMPTION = "Assumption"
VALIDATION_KINDS = frozenset({TEST, ASSUMPTION})


def is_validation_kind(kind: str) -> bool:
    """Whether this kind declares a validation rather than a data object."""

    return kind in VALIDATION_KINDS


PYTHON = "python"
SQL = "sql"
SPARK_SQL = "spark_sql"
LANGUAGES = frozenset({PYTHON, SQL, SPARK_SQL})

#: Languages whose objects materialise as Delta rather than in a Warehouse.
#: They declare their shape up front and use the underscored audit spelling.
DELTA_LANGUAGES = frozenset({PYTHON, SPARK_SQL})

# The three physical destinations. An object ID is unique *within* one of these,
# not across them: Sales.Order may exist as a folder, as a Delta table and as a
# Warehouse table at the same time, because those are three different places.
FOLDER_TARGET = "folder"
DELTA_TARGET = "delta"
SQL_TARGET = "sql"
TARGET_KINDS = (FOLDER_TARGET, DELTA_TARGET, SQL_TARGET)

# The two execution namespaces a two-part reference may bind in. A Lakehouse
# object (Folder or Delta) resolves its references inside the Lakehouse; a
# Warehouse object (SQL) inside the Warehouse. The two are bridged only by an
# explicit alias, never by inference.
LAKEHOUSE_NAMESPACE = "lakehouse"
WAREHOUSE_NAMESPACE = "warehouse"
NAMESPACES = (LAKEHOUSE_NAMESPACE, WAREHOUSE_NAMESPACE)


def namespace_for_target(target_kind: str) -> str:
    """The namespace a native object of this target binds its references in."""

    return WAREHOUSE_NAMESPACE if target_kind == SQL_TARGET else LAKEHOUSE_NAMESPACE


def target_kind_for(language: str, kind: str) -> str:
    """Where an object materialises, from its language and kind.

    Routing is inferred, never configured — which is what removed the old
    paired source-and-target build command.
    """

    if kind == FOLDER:
        return FOLDER_TARGET
    if language in DELTA_LANGUAGES:
        return DELTA_TARGET
    return SQL_TARGET


_ID_KEYS = {
    "Folder ID": FOLDER,
    "Table ID": TABLE,
    "View ID": VIEW,
    "Test ID": TEST,
    "Assumption ID": ASSUMPTION,
}
_PLACEHOLDERS = {"not declared", "n/a", "tbd", "todo"}

# Cross-engine aliases. A Lakehouse object publishes into the Warehouse with a
# Warehouse alias; a Warehouse object publishes into the Lakehouse with a
# Lakehouse alias. Eligibility is by target, not just kind, so both keys are
# accepted here and refused in _parse_aliases when they sit on the wrong object.
WAREHOUSE_ALIAS = "Warehouse alias"
LAKEHOUSE_ALIAS = "Lakehouse alias"
_ALIAS_KEYS = {WAREHOUSE_ALIAS, LAKEHOUSE_ALIAS}

#: Stability thresholds compare changed rows with the target before the load.
#: The row threshold excludes small targets where a single row can dominate the
#: percentage.
DELETE_THRESHOLD = "Delete percentage threshold"
UPDATE_THRESHOLD = "Update percentage threshold"
STABILITY_ROWS = "Stability row threshold"

#: Deliberately not zero. A load that has never been run against a populated
#: table has nothing to compare with, and a first load inserts everything — so
#: the defaults protect an established table without standing in the way of one
#: being established.
DEFAULT_DELETE_THRESHOLD = 5
DEFAULT_UPDATE_THRESHOLD = 20
DEFAULT_STABILITY_ROWS = 1_000_000

# Keys accepted per kind. Anything else is a typo and is refused by name.
#
# The groups are semantic: each says what a set of keys is about, and each kind
# composes the groups that apply to it. There is no set every document gets — a
# Test has a description, notes and dependencies and materialises nothing, so
# `Lineage`, `Static` and the aliases could not mean anything on it.

#: What any Weaver declaration says about itself.
DOCUMENT_KEYS = frozenset({"Description", "Notes", "Revision notes"})

#: What it reads. Every kind may declare dependencies explicitly where they
#: cannot be inferred mechanically.
DEPENDENCY_KEYS = frozenset({"Dependencies"})

#: Where the *data* came from. A validation reads data but produces none, so it
#: has no lineage of its own to declare.
DATA_LINEAGE_KEYS = frozenset({"Lineage"})

#: How a materialised object is built and rebuilt.
BUILD_BEHAVIOUR_KEYS = frozenset({"Static", "Prohibit rebuild"})

#: The cross-engine publication names. Only something materialised has one.
ALIAS_KEYS = frozenset(_ALIAS_KEYS)

#: What every materialised data object shares.
_DATA_OBJECT_KEYS = (
    DOCUMENT_KEYS
    | DEPENDENCY_KEYS
    | DATA_LINEAGE_KEYS
    | BUILD_BEHAVIOUR_KEYS
    | ALIAS_KEYS
)

#: What both validation kinds share. Notably absent: everything describing
#: materialised data behaviour.
_VALIDATION_KEYS = DOCUMENT_KEYS | DEPENDENCY_KEYS

_KIND_KEYS = {
    FOLDER: _DATA_OBJECT_KEYS | {"File key", "Incremental"},
    TABLE: _DATA_OBJECT_KEYS
    | {
        "Schema",
        "Column notes",
        "Primary key",
        "Unique keys",
        "Foreign keys",
        "Not null",
        "Identity",
        "Comparison columns",
        "Incremental",
        DELETE_THRESHOLD,
        UPDATE_THRESHOLD,
        STABILITY_ROWS,
    },
    # A view's keys are logical: it stores no rows, so they describe the shape of
    # the result rather than constraining storage. They are declared so the model
    # is complete and can be checked for quality; nothing physical follows.
    VIEW: _DATA_OBJECT_KEYS
    | {"Column notes", "Primary key", "Unique keys", "Foreign keys"},
    # A Test's primary key correlates diagnostic rows across the two sides of the
    # symmetric difference. It does not change what is compared or counted, which
    # is why it is the one key-shaped thing a validation may declare.
    TEST: _VALIDATION_KEYS | {"Primary key"},
    # An Assumption has no expected/actual pair to correlate, so a primary key
    # would have nothing to pair. It is refused by name rather than ignored.
    ASSUMPTION: _VALIDATION_KEYS,
}

# Retired keys, refused with the migration rather than as "unknown".
_RETIRED_KEYS = {
    "Auto delete": (
        "Auto delete is no longer supported. Use Incremental with the inverse value:\n"
        "Auto delete: false becomes Incremental: true.\n"
        "Auto delete: true becomes Incremental: false."
    ),
    "Load mode": (
        "Load mode is no longer supported. Behaviour follows from Incremental and "
        "Primary key."
    ),
}

# Multiple independent columns are a YAML list; a column *set* — one key or one
# comparison tuple — is comma-separated.
_LIST_KEYS = {"Not null"}
_SET_KEYS = {"Primary key", "Comparison columns"}

_REFERENCE = re.compile(r"^\$([^\[\]$]+?)(?:\[([^\[\]$]+)\])?$")

# A revision entry opens with a date. Which spelling is the developer's choice;
# holding to one spelling within a document is not, because a mixed list cannot
# be read in order at a glance. Day-first and month-first share a shape and are
# not told apart — Weaver checks the shape, not the reading.
_REVISION_DATE_SHAPES = (
    ("YYYY-MM-DD", re.compile(r"^(\d{4})-(\d{1,2})-(\d{1,2})(?=\s|$)"), True),
    ("YYYY/MM/DD", re.compile(r"^(\d{4})/(\d{1,2})/(\d{1,2})(?=\s|$)"), True),
    ("DD/MM/YYYY", re.compile(r"^(\d{1,2})/(\d{1,2})/(\d{4})(?=\s|$)"), False),
    ("DD-MM-YYYY", re.compile(r"^(\d{1,2})-(\d{1,2})-(\d{4})(?=\s|$)"), False),
    ("DD.MM.YYYY", re.compile(r"^(\d{1,2})\.(\d{1,2})\.(\d{4})(?=\s|$)"), False),
)


# --- audit columns ---------------------------------------------------------

#: Logical audit columns, materialised on every table but never authored.
#: Physical spelling follows the representation: a Warehouse keeps the spaced
#: form already used by the SQL backend, Delta uses lower snake case because
#: spaces in Spark column names need quoting everywhere they appear and Delta's
#: own convention is snake case throughout.
AUDIT_INSERT = "Row insert datetime"
AUDIT_UPDATE = "Row update datetime"
AUDIT_DELETE = "Row delete datetime"
AUDIT_COLUMNS = (AUDIT_INSERT, AUDIT_UPDATE, AUDIT_DELETE)

_AUDIT_TYPES = {PYTHON: "timestamp", SPARK_SQL: "timestamp", SQL: "datetime2(6)"}

#: The delete datetime of a row that is still live. All three audit columns are
#: physically not null, so a live row carries a sentinel maximum rather than an
#: absence — which makes "as at" one range predicate instead of a null check.
AUDIT_LIVE_DELETE_DATETIME = "9999-12-31 23:59:59.999999"

#: The identity column is a surrogate the *engine* generates: build declares it
#: ``bigint identity not null`` and the Warehouse assigns a value to every
#: inserted row. It is Weaver's column, so it is not part of the declared
#: business schema or a query's output, and a load never inserts into it.
IDENTITY_TYPE = "bigint"

#: Which representations can carry an identity column: the Warehouse alone. A
#: value Weaver computed would have to be unique across concurrent writers,
#: which is what an engine's identity provides and neither Delta 3.2 nor
#: Fabric's Spark runtime offers. So a Delta table declares no identity at all.
IDENTITY_LANGUAGES = frozenset({SQL})

_IDENTITY_UNSUPPORTED = (
    "Identity is supported for Warehouse tables only. A Delta table has no "
    "engine-generated identity to sit behind the column — Spark and Delta offer "
    "none Weaver can rely on — so remove the Identity header and use the "
    "business key, or declare the object in a Warehouse item."
)


def audit_column_name(logical: str, language: str) -> str:
    """The physical spelling of one logical audit column for a representation.

    Delta gets lower snake case (``row_insert_datetime``); a Warehouse keeps the
    spaced form (``Row insert datetime``) the SQL backend has always used.
    """

    if language in DELTA_LANGUAGES:
        return logical.replace(" ", "_").lower()
    return logical


#: Every spelling of an audit column an author might reach for, folded for
#: comparison. All are reserved, including the retired ``Row_insert_datetime``
#: form, so a declaration can never collide with the columns Weaver adds.
_RESERVED_AUDIT_NAMES = frozenset(
    spelling.lower()
    for logical in AUDIT_COLUMNS
    for spelling in (logical, logical.replace(" ", "_"))
)


def _audit_columns(language: str) -> tuple["Column", ...]:
    return tuple(
        Column(
            name=audit_column_name(logical, language),
            type=_AUDIT_TYPES[language],
            # Weaver populates all three on every loaded row — insert and update
            # datetimes, and a sentinel maximum delete datetime for a live row —
            # so none has a valid null state and all are physically not null.
            not_null=True,
            is_audit=True,
        )
        for logical in AUDIT_COLUMNS
    )


# --- values ----------------------------------------------------------------


@dataclass(frozen=True)
class ObjectId:
    """Levels two and one — ``Schema.Object`` within a repository."""

    schema: str
    object: str

    @property
    def qualified(self) -> str:
        return f"{self.schema}.{self.object}"

    def __str__(self) -> str:
        return self.qualified


@dataclass(frozen=True)
class Reference:
    """An item-relative or item-qualified exact-case metadata reference."""

    schema: str
    object: str
    column: str | None = None
    item_type: str | None = None
    item_name: str | None = None
    is_files: bool = False

    def __post_init__(self) -> None:
        if (self.item_type is None) != (self.item_name is None):
            raise MetadataError(
                "a qualified reference needs both item type and item name"
            )
        if self.item_type is not None and self.item_type not in (
            "Lakehouse",
            "Warehouse",
        ):
            raise MetadataError(
                f"reference item type must be Lakehouse or Warehouse, got {self.item_type!r}"
            )
        if self.is_files and self.item_type == "Warehouse":
            raise MetadataError("Files references may only name a Lakehouse item")

    @property
    def object_id(self) -> ObjectId:
        return ObjectId(schema=self.schema, object=self.object)

    @property
    def target(self) -> str:
        within = (
            f"Files/{self.schema}.{self.object}"
            if self.is_files
            else self.object_id.qualified
        )
        if self.item_type is None:
            return within
        return f"{self.item_type}/{self.item_name}/{within}"

    @property
    def is_item_qualified(self) -> bool:
        return self.item_type is not None

    def __str__(self) -> str:
        target = f"${self.target}"
        return f"{target}[{self.column}]" if self.column else target


@dataclass(frozen=True)
class MetadataText:
    """Either literal prose or exactly one reference — never a mix.

    ``See $Sales.Order`` is refused: mixed content cannot be resolved
    mechanically. Write ``$$`` for a literal dollar sign.
    """

    literal: str | None = None
    reference: Reference | None = None

    @property
    def is_reference(self) -> bool:
        return self.reference is not None

    def __str__(self) -> str:
        return str(self.reference) if self.reference else (self.literal or "")


@dataclass(frozen=True)
class Revision:
    """One dated entry in the object's revision history."""

    date: str
    note: str

    def __str__(self) -> str:
        return f"{self.date} {self.note}"


@dataclass(frozen=True)
class ForeignKey:
    """One declared relationship to a parent object.

    Semantic rather than physical: nothing is enforced by the engine and no
    index follows, so a key has no name, two objects may be related several
    times over, and an object may reference itself. The parent is a two-part
    ``Schema.Object`` — a logical name in the same repository.
    """

    columns: tuple[str, ...]
    reference: ObjectId
    reference_columns: tuple[str, ...]
    logical_reference: Reference | None = None

    def __str__(self) -> str:
        child = ", ".join(self.columns)
        parent = ", ".join(self.reference_columns)
        target = (
            self.logical_reference.target
            if self.logical_reference is not None
            else self.reference.qualified
        )
        return f"{child}: {target}[{parent}]"


@dataclass(frozen=True)
class Column:
    """One column of a table or view."""

    name: str
    type: str | None = None
    note: MetadataText | None = None
    not_null: bool = False
    is_audit: bool = False
    is_identity: bool = False


# --- the document ----------------------------------------------------------


@dataclass(frozen=True)
class WeaverDocument:
    """A fully validated Weaver document object declaration."""

    kind: str
    language: str
    object_id: ObjectId
    description: MetadataText
    #: Where the data came from. Absent on a validation kind, which produces no
    #: data and so has no lineage of its own to declare.
    lineage: MetadataText | None = None
    notes: str | None = None
    dependencies: tuple[ObjectId, ...] = ()
    #: True when the document wrote a ``Dependencies`` key at all, including an
    #: empty list. An explicit none must suppress discovery the same way a
    #: populated list replaces it — otherwise `Dependencies: []` would silently
    #: mean "discover them for me".
    declares_dependencies: bool = False
    revision_notes: tuple[Revision, ...] = ()
    revision_date_format: str | None = None
    schema: tuple[Column, ...] = ()
    primary_key: tuple[str, ...] = ()
    unique_keys: tuple[tuple[str, ...], ...] = ()
    foreign_keys: tuple[ForeignKey, ...] = ()
    declared_not_null: tuple[str, ...] = ()
    identity: str | None = None
    declared_comparison_columns: tuple[str, ...] = ()
    delete_threshold: int = DEFAULT_DELETE_THRESHOLD
    update_threshold: int = DEFAULT_UPDATE_THRESHOLD
    stability_rows: int = DEFAULT_STABILITY_ROWS
    file_keys: tuple[str, ...] = ()
    is_incremental: bool = False
    prohibit_rebuild: bool = False
    static: bool = False
    warehouse_alias: ObjectId | None = None
    lakehouse_alias: ObjectId | None = None
    raw: dict[str, Any] = field(default_factory=dict)

    @property
    def qualified(self) -> str:
        return self.object_id.qualified

    @property
    def is_validation(self) -> bool:
        """Whether this declaration is a Test or an Assumption.

        Asked wherever a collection holds both, so the distinction is answered
        from the declaration rather than by inferring it from a physical shape.
        """

        return is_validation_kind(self.kind)

    @property
    def has_primary_key(self) -> bool:
        return bool(self.primary_key)

    @property
    def has_declared_schema(self) -> bool:
        return bool(self.schema)

    @property
    def defers_column_validation(self) -> bool:
        """True when column references cannot be checked until build.

        A SQL object infers its shape from its query, so its `Primary key`,
        `Not null`, `Identity`, `Comparison columns` and `Column notes` are
        validated against the built table rather than here.
        """

        return self.kind in (TABLE, VIEW) and not self.has_declared_schema

    @property
    def audit_columns(self) -> tuple[Column, ...]:
        """The architectural columns, spelled for this representation."""

        return _audit_columns(self.language) if self.kind == TABLE else ()

    @property
    def identity_column(self) -> Column | None:
        """The engine-generated surrogate column, when Identity names one.

        A not-null ``bigint`` the Warehouse generates: build declares it
        ``identity`` and every insert leaves it out. Weaver's own column, so it
        stands outside the business schema, though the primary key may name it.
        Only a Warehouse table has one — see :data:`IDENTITY_LANGUAGES`.
        """

        if self.identity is None or self.kind != TABLE:
            return None
        return Column(
            name=self.identity, type=IDENTITY_TYPE, not_null=True, is_identity=True
        )

    @property
    def effective_schema(self) -> tuple[Column, ...]:
        """The full physical shape of a declared table: identity, business, audit.

        ``schema`` stays what the author wrote; this is what gets materialised,
        with the identity column leading and the audit columns trailing.
        """

        identity = (self.identity_column,) if self.identity_column else ()
        return identity + self.schema + self.audit_columns

    @property
    def not_null(self) -> tuple[str, ...]:
        """Declared not-null columns plus the primary key, which always is."""

        return self.primary_key + self.declared_not_null

    @property
    def comparison_columns(self) -> tuple[str, ...]:
        """Columns whose change drives an upsert.

        Defaults to every declared non-key column. Naming a narrower set makes
        the comparison cheaper when a watermark column already implies change.
        """

        if self.declared_comparison_columns:
            return self.declared_comparison_columns
        return tuple(
            column.name
            for column in self.schema
            if column.name not in self.primary_key and not column.is_audit
        )


# Transitional public spelling. R8 removes it after callers have migrated;
# keeping the alias here lets identity and discovery move independently.
SesDocument = WeaverDocument


# --- extraction ------------------------------------------------------------


class _UniqueKeyLoader(yaml.SafeLoader):
    """YAML loader that refuses duplicate mapping keys."""


def _no_duplicate_keys(loader, node, deep=False):
    loader.flatten_mapping(node)
    mapping: dict[Any, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in mapping:
            raise MetadataError(f"duplicate metadata key: {key}")
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


_UniqueKeyLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _no_duplicate_keys,
)


def extract_python_metadata(source: str) -> str:
    """The metadata YAML from a Python object file's module docstring."""

    try:
        module = ast.parse(source)
    except SyntaxError as exc:
        raise MetadataError(f"python object file is not parseable: {exc}") from exc
    doc = ast.get_docstring(module, clean=True)
    if doc is None or not doc.strip():
        raise MetadataError(
            "python object file must begin with a docstring metadata block"
        )
    return doc


def extract_sql_metadata_and_body(source: str) -> tuple[str, str]:
    """Split a SQL object file into (metadata text, executable body)."""

    match = re.match(r"\s*/\*(.*?)\*/(.*)\Z", source, flags=re.DOTALL)
    if not match:
        raise MetadataError(
            "Weaver document SQL must begin with a /* ... */ metadata block"
        )
    return match.group(1).strip("\n"), match.group(2).lstrip()


def parse_python_document(source: str) -> SesDocument:
    return parse_document(extract_python_metadata(source), language=PYTHON)


def parse_sql_document(source: str) -> tuple[SesDocument, str]:
    text, body = extract_sql_metadata_and_body(source)
    return parse_document(text, language=SQL), body


# --- parsing ---------------------------------------------------------------


def parse_document(text: str, *, language: str) -> SesDocument:
    """Parse and exhaustively validate one metadata block."""

    if language not in LANGUAGES:
        raise MetadataError(f"language must be one of {', '.join(sorted(LANGUAGES))}")

    try:
        loaded = yaml.load(text, Loader=_UniqueKeyLoader)
    except MetadataError:
        raise
    except yaml.YAMLError as exc:
        raise MetadataError(f"invalid metadata YAML: {exc}") from exc

    if not isinstance(loaded, dict):
        raise MetadataError("metadata must be a YAML mapping")

    for retired, message in _RETIRED_KEYS.items():
        if retired in loaded:
            raise MetadataError(message)

    kind, object_id = _parse_id(loaded)
    if is_validation_kind(kind):
        return _parse_validation(
            loaded, kind=kind, language=language, object_id=object_id
        )
    _reject_unknown_keys(loaded, kind)

    # A Warehouse (T-SQL) table may declare Schema or omit it: with a declaration
    # the declared types are authoritative; without one the table takes its shape
    # from its query, inferred at build (see how-does-build-work §2).

    declares_dependencies = "Dependencies" in loaded
    dependencies = _parse_dependencies(loaded.get("Dependencies"), object_id)
    if language == SPARK_SQL and not declares_dependencies:
        raise MetadataError(
            "a Spark SQL object must declare Dependencies, because a query may "
            "read by path and a path cannot be resolved back to a managed "
            "object. Write `Dependencies: []` if it depends on nothing."
        )

    description = _parse_text(loaded, "Description")
    lineage = _parse_text(loaded, "Lineage")
    notes = _parse_notes(loaded.get("Notes"))
    revisions, revision_format = _parse_revision_notes(loaded.get("Revision notes"))
    static = _parse_bool(loaded.get("Static"), "Static")
    prohibit_rebuild = _parse_flag_with_default(
        loaded, "Prohibit rebuild", default=kind == FOLDER
    )
    file_keys = _parse_file_keys(loaded.get("File key"), kind=kind)

    if kind == VIEW and "Incremental" in loaded:
        raise MetadataError("Incremental is not supported for View objects")
    is_incremental = _parse_flag_with_default(
        loaded, "Incremental", default=kind == FOLDER
    )

    declared_columns = _parse_schema(loaded.get("Schema"))
    if kind == TABLE and language == PYTHON and not declared_columns:
        raise MetadataError(
            "a Python-backed Delta table must declare Schema: it has no query "
            "to infer a shape from, and is created before it is loaded."
        )

    primary_key = _parse_column_set(loaded.get("Primary key"), "Primary key")
    unique_keys = _parse_unique_keys(loaded.get("Unique keys"), primary_key)
    foreign_keys = _parse_foreign_keys(loaded.get("Foreign keys"), object_id)
    declared_not_null = _parse_column_list(loaded.get("Not null"), "Not null")
    delete_threshold = _parse_percentage(
        loaded, DELETE_THRESHOLD, DEFAULT_DELETE_THRESHOLD
    )
    update_threshold = _parse_percentage(
        loaded, UPDATE_THRESHOLD, DEFAULT_UPDATE_THRESHOLD
    )
    stability_rows = _parse_row_count(loaded, STABILITY_ROWS, DEFAULT_STABILITY_ROWS)
    identity = _parse_identity(loaded.get("Identity"))
    if identity is not None and language not in IDENTITY_LANGUAGES:
        raise MetadataError(_IDENTITY_UNSUPPORTED)
    comparison = _parse_column_set(
        loaded.get("Comparison columns"), "Comparison columns"
    )
    column_notes = _parse_column_notes(loaded.get("Column notes"))

    _validate_columns(
        kind=kind,
        declared_columns=declared_columns,
        primary_key=primary_key,
        unique_keys=unique_keys,
        foreign_keys=foreign_keys,
        declared_not_null=declared_not_null,
        identity=identity,
        comparison=comparison,
        notes=column_notes,
    )

    if kind == TABLE:
        if is_incremental and not primary_key:
            raise MetadataError("Incremental: true requires a Primary key")
        if comparison and not primary_key:
            raise MetadataError(
                "Comparison columns require a Primary key — they drive upsert comparison, "
                "which only happens when rows can be matched"
            )
    schema = _apply_column_details(
        declared_columns, column_notes, primary_key, declared_not_null
    )

    warehouse_alias, lakehouse_alias = _parse_aliases(loaded, language, kind, object_id)

    return SesDocument(
        kind=kind,
        language=language,
        object_id=object_id,
        description=description,
        lineage=lineage,
        notes=notes,
        dependencies=dependencies,
        declares_dependencies=declares_dependencies,
        revision_notes=revisions,
        revision_date_format=revision_format,
        schema=schema,
        primary_key=primary_key,
        unique_keys=unique_keys,
        foreign_keys=foreign_keys,
        declared_not_null=declared_not_null,
        identity=identity,
        declared_comparison_columns=comparison,
        delete_threshold=delete_threshold,
        update_threshold=update_threshold,
        stability_rows=stability_rows,
        file_keys=file_keys,
        is_incremental=is_incremental,
        prohibit_rebuild=prohibit_rebuild,
        static=static,
        warehouse_alias=warehouse_alias,
        lakehouse_alias=lakehouse_alias,
        raw=dict(loaded),
    )


def _parse_validation(
    raw: dict[str, Any], *, kind: str, language: str, object_id: ObjectId
) -> WeaverDocument:
    """Parse a Test or Assumption header.

    A separate path rather than a branch through the object parser: a validation
    has no schema, lineage, build behaviour or alias. What the two share —
    description, notes, revisions, dependencies — goes through the same helpers,
    so they cannot drift.
    """

    if kind == ASSUMPTION and "Primary key" in raw:
        raise MetadataError(
            "an Assumption must not declare a Primary key. A key correlates "
            "the two sides of a Test; an Assumption has one side."
        )

    _reject_unknown_keys(raw, kind)

    # No language is required to declare dependencies here, though Spark SQL is
    # on an object. The header means the same either way — declared replaces
    # inferred, and `Dependencies: []` means none — and only the obligation
    # differs: a Spark SQL object may read by path, while a validation installs
    # last and nothing depends on it. See
    # :func:`weaver.declaration.repository.effective_dependencies`.
    declares_dependencies = "Dependencies" in raw
    dependencies = _parse_dependencies(raw.get("Dependencies"), object_id)

    description = _parse_text(raw, "Description")
    notes = _parse_notes(raw.get("Notes"))
    revisions, revision_format = _parse_revision_notes(raw.get("Revision notes"))
    primary_key = _parse_column_set(raw.get("Primary key"), "Primary key")

    return WeaverDocument(
        kind=kind,
        language=language,
        object_id=object_id,
        description=description,
        lineage=None,
        notes=notes,
        dependencies=dependencies,
        declares_dependencies=declares_dependencies,
        revision_notes=revisions,
        revision_date_format=revision_format,
        primary_key=primary_key,
        raw=dict(raw),
    )


def _parse_id(raw: dict[str, Any]) -> tuple[str, ObjectId]:
    present = [key for key in _ID_KEYS if key in raw and raw[key] is not None]
    if len(present) != 1:
        raise MetadataError(
            "metadata must include exactly one of " + ", ".join(_ID_KEYS)
        )
    key = present[0]
    value = raw[key]
    if not isinstance(value, str) or not value.strip():
        raise MetadataError(f"{key} must be a non-empty Schema.Object string")
    parts = [part.strip() for part in value.strip().split(".")]
    if len(parts) != 2 or not all(parts):
        raise MetadataError(
            f"{key} must be a two-part Schema.Object declaration, got {value!r}"
        )
    return _ID_KEYS[key], ObjectId(schema=parts[0], object=parts[1])


def _listed(items: list[str]) -> str:
    """``a``, ``a and b``, ``a, b and c`` — a list a sentence can contain."""

    if len(items) < 3:
        return " and ".join(items)
    return ", ".join(items[:-1]) + " and " + items[-1]


def _reject_unknown_keys(raw: dict[str, Any], kind: str) -> None:
    allowed = _KIND_KEYS[kind] | set(_ID_KEYS)
    unknown = {str(key) for key in raw} - allowed
    if not unknown:
        return

    what = "validation" if is_validation_kind(kind) else "object"
    article = "an" if kind[0].upper() in "AEIOU" else "a"
    elsewhere = {
        key: sorted(
            other for other, keys in _KIND_KEYS.items() if key in keys and other != kind
        )
        for key in unknown
    }
    known = {key: kinds for key, kinds in elsewhere.items() if kinds}
    detail = ""
    if known:
        detail = (
            " ("
            + "; ".join(
                f"{key} belongs to {_listed(kinds)}"
                for key, kinds in sorted(known.items())
            )
            + ")"
        )
    # A validation declaration is the one place a *correctly spelled* key is
    # commonly wrong, because everything describing materialised data behaviour
    # reads as plausible on a Test until you ask what it would do. Say so rather
    # than reporting a typo.
    if is_validation_kind(kind) and known:
        detail += (
            f". {article.capitalize()} {kind} declares no data of its own, so "
            "metadata describing how data is materialised, keyed or rebuilt has "
            "nothing to apply to"
        )
    raise MetadataError(
        f"unknown metadata key(s) for {article} {kind} {what}: "
        + ", ".join(sorted(unknown))
        + detail
    )


def _parse_text(raw: dict[str, Any], key: str) -> MetadataText:
    value = raw.get(key)
    if not isinstance(value, str) or not value.strip():
        raise MetadataError(f"{key} is required and must be non-empty text")
    return _parse_text_value(value, key)


def _parse_text_value(value: str, key: str) -> MetadataText:
    stripped = value.strip()
    if "$" in stripped.replace("$$", ""):
        match = _REFERENCE.match(stripped)
        if not match:
            raise MetadataError(
                f"{key} must be either prose or exactly one $Schema.Object reference, "
                f"not a mix of both: {stripped!r}. Write $$ for a literal dollar sign."
            )
        target, column = match.groups()
        return MetadataText(
            reference=_parse_logical_reference(
                target.strip(), column=column.strip() if column else None, key=key
            )
        )
    literal = stripped.replace("$$", "$")
    if literal.lower() in _PLACEHOLDERS:
        raise MetadataError(f"{key} must not be a placeholder value ({literal!r})")
    return MetadataText(literal=literal)


def _parse_logical_reference(
    target: str, *, column: str | None = None, key: str
) -> Reference:
    """Parse the shared short/canonical logical-reference grammar."""

    parts = target.split("/")
    item_type: str | None = None
    item_name: str | None = None
    is_files = False
    object_text: str
    if len(parts) == 1:
        object_text = parts[0]
    elif len(parts) == 2 and parts[0] == "Files":
        is_files = True
        object_text = parts[1]
    elif len(parts) == 3:
        item_type, item_name, object_text = parts
    elif len(parts) == 4 and parts[2] == "Files":
        item_type, item_name, _, object_text = parts
        is_files = True
    else:
        raise MetadataError(
            f"{key} reference must be Schema.Object, Files/Schema.Object or an "
            f"item-qualified logical identity, got {target!r}"
        )
    if object_text.count(".") != 1:
        raise MetadataError(
            f"{key} reference must end in Schema.Object, got {target!r}"
        )
    schema, object_name = object_text.split(".")
    names = (
        (schema, object_name, item_name)
        if item_name is not None
        else (schema, object_name)
    )
    if any(not name or name != name.strip() for name in names):
        raise MetadataError(f"{key} reference contains an empty or padded logical name")
    return Reference(
        schema=schema,
        object=object_name,
        column=column,
        item_type=item_type,
        item_name=item_name,
        is_files=is_files,
    )


def _parse_dependencies(value: Any, object_id: ObjectId) -> tuple[ObjectId, ...]:
    """Objects this one depends on, declared rather than discovered.

    Additive: what discovery finds is added to these, never replaced. A missing
    dependency is a wrong build order, so the declared set can only widen the
    graph.
    """

    if value is None:
        return ()
    if not isinstance(value, list):
        raise MetadataError(
            "Dependencies must be a YAML list of Schema.Object names:\n"
            "Dependencies:\n  - Sales.Customer"
        )
    if not value:
        # `Dependencies: []` is a positive declaration of none, which is what the
        # requirement below wants from a Spark SQL author: be explicit. A query
        # built entirely from literals genuinely depends on nothing.
        return ()
    seen: list[ObjectId] = []
    for entry in value:
        if not isinstance(entry, str) or not entry.strip():
            raise MetadataError(
                "Dependencies entries must be non-empty Schema.Object names"
            )
        parts = [part.strip() for part in entry.strip().split(".")]
        if len(parts) != 2 or not all(parts):
            raise MetadataError(
                f"a Dependencies entry must be a two-part Schema.Object name, got {entry!r}"
            )
        dependency = ObjectId(schema=parts[0], object=parts[1])
        if dependency == object_id:
            raise MetadataError(f"{object_id.qualified} cannot depend on itself")
        if dependency in seen:
            raise MetadataError(f"Dependencies repeats {dependency.qualified}")
        seen.append(dependency)
    return tuple(seen)


def _parse_aliases(
    raw: dict[str, Any], language: str, kind: str, object_id: ObjectId
) -> tuple[ObjectId | None, ObjectId | None]:
    """The cross-engine aliases this object publishes, checked for eligibility.

    A Lakehouse object may publish a ``Warehouse alias`` and a Warehouse object
    a ``Lakehouse alias``; neither belongs on a Folder or on the opposite
    engine. The alias may name a different ``Schema.Object`` from the native one
    — a Staging table can surface as Sales.Customer — so it is parsed through
    the same two-part model.
    """

    target = target_kind_for(language, kind)
    warehouse_alias = _parse_alias(raw.get(WAREHOUSE_ALIAS), WAREHOUSE_ALIAS)
    lakehouse_alias = _parse_alias(raw.get(LAKEHOUSE_ALIAS), LAKEHOUSE_ALIAS)

    if warehouse_alias is not None and target != DELTA_TARGET:
        raise MetadataError(
            f"{WAREHOUSE_ALIAS} publishes a Lakehouse object into the Warehouse, so it "
            f"belongs on a Delta table or Spark view, not on {object_id.qualified} "
            + (
                "(a Warehouse object uses Lakehouse alias)"
                if target == SQL_TARGET
                else "(a Folder is not published across engines)"
            )
        )
    if lakehouse_alias is not None and target != SQL_TARGET:
        raise MetadataError(
            f"{LAKEHOUSE_ALIAS} publishes a Warehouse object into the Lakehouse, so it "
            f"belongs on a SQL table or view, not on {object_id.qualified} "
            + (
                "(a Lakehouse object uses Warehouse alias)"
                if target == DELTA_TARGET
                else "(a Folder is not published across engines)"
            )
        )
    return warehouse_alias, lakehouse_alias


def _parse_alias(value: Any, key: str) -> ObjectId | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise MetadataError(f"{key} must be a non-empty Schema.Object name")
    parts = [part.strip() for part in value.strip().split(".")]
    if len(parts) != 2 or not all(parts):
        raise MetadataError(
            f"{key} must be a two-part Schema.Object name, got {value!r}"
        )
    return ObjectId(schema=parts[0], object=parts[1])


def _parse_notes(value: Any) -> str | None:
    """Free-range commentary. Deliberately unpoliced.

    No reference parsing and no placeholder check: this is where an author
    writes whatever helps, including a dollar sign.
    """

    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise MetadataError("Notes must be non-empty text when present")
    return value.strip()


def _parse_revision_notes(value: Any) -> tuple[tuple[Revision, ...], str | None]:
    if value is None:
        return (), None
    if not isinstance(value, list) or not value:
        raise MetadataError(
            "Revision notes must be a non-empty YAML list, each entry opening with a date:\n"
            "Revision notes:\n  - 2026-07-23 Added the amount column."
        )

    revisions: list[Revision] = []
    shape: str | None = None
    for entry in value:
        if isinstance(entry, (date, datetime)):
            # YAML resolves a bare `- 2026-07-23` to a date rather than text.
            raise MetadataError(f"Revision notes entry {entry} has a date but no note")
        if not isinstance(entry, str) or not entry.strip():
            raise MetadataError("Revision notes entries must be non-empty text")
        text = entry.strip()
        matched = _match_revision_date(text)
        if matched is None:
            raise MetadataError(
                f"a Revision notes entry must open with a date, got {text!r}. "
                "Any consistent spelling is accepted, such as 2026-07-23 or 23/07/2026."
            )
        entry_shape, date_text = matched
        if shape is None:
            shape = entry_shape
        elif entry_shape != shape:
            raise MetadataError(
                f"Revision notes mix date formats — {shape} was used first, "
                f"then {entry_shape} in {text!r}. Use one spelling throughout an object."
            )
        note = text[len(date_text) :].strip()
        if not note:
            raise MetadataError(f"Revision notes entry {text!r} has a date but no note")
        revisions.append(Revision(date=date_text, note=note))
    return tuple(revisions), shape


def _match_revision_date(text: str) -> tuple[str, str] | None:
    for shape, pattern, year_first in _REVISION_DATE_SHAPES:
        match = pattern.match(text)
        if match is None:
            continue
        first, second, third = (int(part) for part in match.groups())
        if year_first:
            month, day = second, third
            plausible = 1 <= month <= 12 and 1 <= day <= 31
        else:
            # Day-first and month-first are indistinguishable, so accept either
            # reading rather than pretend to know which was meant.
            plausible = (
                1 <= first <= 31 and 1 <= second <= 31 and (first <= 12 or second <= 12)
            )
        if not plausible:
            raise MetadataError(
                f"Revision notes entry does not open with a real date: {text!r}"
            )
        return shape, match.group(0)
    return None


def _parse_bool(value: Any, key: str) -> bool:
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    raise MetadataError(f"{key} must be a boolean (true/false)")


def _parse_percentage(raw: dict[str, Any], key: str, default: int) -> int:
    """A whole percentage between 0 and 100.

    100 is permitted and means "never trip", which is a clearer way to disable
    one threshold than a separate flag would be.
    """

    if key not in raw or raw[key] is None:
        return default
    value = raw[key]
    if isinstance(value, bool) or not isinstance(value, int):
        raise MetadataError(f"{key} must be a whole percentage, got {value!r}")
    if not 0 <= value <= 100:
        raise MetadataError(f"{key} must be between 0 and 100, got {value}")
    return value


def _parse_row_count(raw: dict[str, Any], key: str, default: int) -> int:
    if key not in raw or raw[key] is None:
        return default
    value = raw[key]
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise MetadataError(f"{key} must be a whole number of rows, got {value!r}")
    return value


def _parse_flag_with_default(raw: dict[str, Any], key: str, *, default: bool) -> bool:
    if key not in raw:
        return default
    return _parse_bool(raw[key], key)


def _parse_column_set(value: Any, key: str) -> tuple[str, ...]:
    """A column *set* is comma-separated: one key, one comparison tuple."""

    if value is None:
        return ()
    if isinstance(value, list):
        raise MetadataError(
            f"{key} is a column set and must be comma-separated text, not a YAML list"
        )
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise MetadataError(f"{key} must be comma-separated text")
    columns = tuple(part.strip() for part in str(value).split(","))
    if any(not column for column in columns):
        raise MetadataError(f"{key} must not contain empty column names")
    if len(set(columns)) != len(columns):
        raise MetadataError(f"{key} must not repeat columns")
    return columns


def _parse_unique_keys(
    value: Any, primary_key: tuple[str, ...]
) -> tuple[tuple[str, ...], ...]:
    """Alternate keys — a YAML list, one comma-separated column *set* per entry.

    Two levels, matching ``Primary key``: independent keys are a list, and one
    key's columns are a comma-separated set whose order is preserved. A key has
    no name, because nothing physical is created from it::

        Unique keys:
          - Order number
          - Customer id, Order date
    """

    if value is None:
        return ()
    if not isinstance(value, list) or not value:
        raise MetadataError(
            "Unique keys must be a non-empty YAML list, one comma-separated column "
            "set per key:\nUnique keys:\n  - Order number\n  - Customer id, Order date"
        )

    keys: list[tuple[str, ...]] = []
    for entry in value:
        if isinstance(entry, list):
            raise MetadataError(
                "each Unique keys entry is one key and must be comma-separated text, "
                "not a nested YAML list"
            )
        columns = _parse_column_set(entry, "Unique keys")
        if not columns:
            raise MetadataError("Unique keys entries must name at least one column")
        if columns in keys:
            raise MetadataError("Unique keys repeats the key " + ", ".join(columns))
        if primary_key and columns == primary_key:
            raise MetadataError(
                "a Unique keys entry repeats the Primary key ("
                + ", ".join(columns)
                + ") — the primary key is already unique, so remove it from Unique keys"
            )
        keys.append(columns)
    return tuple(keys)


#: A foreign key's parent: ``Schema.Object[Column, Column]``. Brackets are
#: required — the parent columns are what make the relationship readable, and a
#: bare parent name would leave them to be guessed.
_FOREIGN_KEY_PARENT = re.compile(r"^([^\[\]]+)\[([^\[\]]+)\]$")


def _parse_foreign_keys(value: Any, object_id: ObjectId) -> tuple[ForeignKey, ...]:
    """Declared relationships to parent objects, as an ER model rather than DDL.

    Each entry is a one-entry mapping from this object's column set to the
    parent's::

        Foreign keys:
          - Customer id: Sales.Customer[Customer id]
          - Region, Country: Sales.Territory[Region, Country]
          - Parent order id: Sales.Order[Order id]

    Several entries may name the same parent, and the parent may be this object
    itself: a hierarchy in one table is an ordinary shape.
    """

    if value is None:
        return ()
    if not isinstance(value, list) or not value:
        raise MetadataError(
            "Foreign keys must be a non-empty YAML list, one relationship per entry:\n"
            "Foreign keys:\n  - Customer id: Sales.Customer[Customer id]"
        )

    keys: list[ForeignKey] = []
    for entry in value:
        if not isinstance(entry, dict) or len(entry) != 1:
            raise MetadataError(
                "each Foreign keys entry maps one column set to one parent:\n"
                "  - Customer id: Sales.Customer[Customer id]"
            )
        raw_columns, raw_parent = next(iter(entry.items()))
        columns = _parse_column_set(raw_columns, "Foreign keys")
        if not columns:
            raise MetadataError("a Foreign keys entry must name at least one column")
        if not isinstance(raw_parent, str) or not raw_parent.strip():
            raise MetadataError(
                f"the Foreign keys entry for {', '.join(columns)} must name a parent "
                "as Schema.Object[Column, Column]"
            )
        match = _FOREIGN_KEY_PARENT.match(raw_parent.strip())
        if match is None:
            raise MetadataError(
                f"the Foreign keys parent for {', '.join(columns)} must be "
                f"Schema.Object[Column, Column], got {raw_parent.strip()!r}"
            )
        raw_target, raw_parent_columns = match.groups()
        try:
            logical_reference = _parse_logical_reference(
                raw_target.strip(), key="Foreign keys"
            )
        except MetadataError as exc:
            raise MetadataError(
                f"the Foreign keys parent for {', '.join(columns)} must be "
                "Schema.Object[Column, Column] or an item-qualified logical "
                f"identity, got {raw_parent.strip()!r}"
            ) from exc
        parent_columns = _parse_column_set(raw_parent_columns, "Foreign keys parent")
        if len(parent_columns) != len(columns):
            raise MetadataError(
                f"the Foreign keys entry for {', '.join(columns)} references "
                f"{len(parent_columns)} parent column(s) — a relationship pairs its "
                "columns, so the two sets must be the same size"
            )
        key = ForeignKey(
            columns=columns,
            reference=logical_reference.object_id,
            reference_columns=parent_columns,
            logical_reference=(
                logical_reference
                if logical_reference.is_item_qualified or logical_reference.is_files
                else None
            ),
        )
        if not key.reference.schema or not key.reference.object:
            raise MetadataError(
                f"the Foreign keys parent for {', '.join(columns)} must be a two-part "
                f"Schema.Object name, got {raw_parent.strip()!r}"
            )
        if key in keys:
            raise MetadataError(f"Foreign keys repeats the relationship {key}")
        keys.append(key)
    return tuple(keys)


def _parse_column_list(value: Any, key: str) -> tuple[str, ...]:
    """Independent columns are a YAML list."""

    if value is None:
        return ()
    if not isinstance(value, list):
        raise MetadataError(
            f"{key} is a list of independent columns and must be a YAML list:\n"
            f"{key}:\n  - Column one\n  - Column two"
        )
    columns: list[str] = []
    for entry in value:
        if not isinstance(entry, str) or not entry.strip():
            raise MetadataError(f"{key} entries must be non-empty column names")
        columns.append(entry.strip())
    if len(set(columns)) != len(columns):
        raise MetadataError(f"{key} must not repeat columns")
    return tuple(columns)


def _parse_file_keys(value: Any, *, kind: str) -> tuple[str, ...]:
    """The globs a Folder manages. Everything else in the folder is not ours."""

    if kind != FOLDER:
        return ()
    if value is None:
        raise MetadataError(
            "a Folder must declare File key — it is the scope of what Weaver manages, "
            "and reconciliation deletes nothing outside it"
        )

    values = [value] if isinstance(value, str) else value
    if not isinstance(values, list) or not values:
        raise MetadataError("File key must be a non-empty string or list of strings")

    patterns: list[str] = []
    for pattern in values:
        if not isinstance(pattern, str) or not pattern.strip():
            raise MetadataError("File key patterns must be non-empty strings")
        normalised = pattern.strip().replace("\\", "/")
        if normalised.startswith("/") or ".." in normalised.split("/"):
            raise MetadataError(
                "File key patterns must be relative and must not traverse with '..'"
            )
        patterns.append(normalised)
    return tuple(patterns)


def _parse_identity(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, list):
        raise MetadataError("Identity must be a single column")
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise MetadataError("Identity must be a single column name")
    name = str(value).strip()
    if not name:
        raise MetadataError("Identity must be a non-empty column name")
    if "," in name:
        raise MetadataError("Identity must be a single column, not a list")
    return name


def _parse_schema(value: Any) -> tuple[Column, ...]:
    if value is None:
        return ()
    if not isinstance(value, dict) or not value:
        raise MetadataError("Schema must be a non-empty mapping of column to type")
    columns: list[Column] = []
    for name, column_type in value.items():
        if not isinstance(name, str) or not name.strip():
            raise MetadataError("Schema column names must be non-empty strings")
        if not isinstance(column_type, str) or not column_type.strip():
            raise MetadataError(f"Schema column {name!r} must declare a non-empty type")
        columns.append(Column(name=name.strip(), type=column_type.strip()))
    return tuple(columns)


def _parse_column_notes(value: Any) -> dict[str, MetadataText]:
    if value is None:
        return {}
    if not isinstance(value, dict) or not value:
        raise MetadataError(
            "Column notes must be a non-empty mapping of column to description"
        )
    notes: dict[str, MetadataText] = {}
    for name, note in value.items():
        if not isinstance(name, str) or not name.strip():
            raise MetadataError("Column notes column names must be non-empty strings")
        if not isinstance(note, str) or not note.strip():
            raise MetadataError(f"Column notes for {name!r} must be non-empty text")
        notes[name.strip()] = _parse_text_value(note, f"Column notes[{name.strip()}]")
    return notes


def _validate_columns(
    *,
    kind: str,
    declared_columns: tuple[Column, ...],
    primary_key: tuple[str, ...],
    unique_keys: tuple[tuple[str, ...], ...],
    foreign_keys: tuple[ForeignKey, ...],
    declared_not_null: tuple[str, ...],
    identity: str | None,
    comparison: tuple[str, ...],
    notes: dict[str, MetadataText],
) -> None:
    """Cross-field column guards, where a declared schema makes them possible."""

    redundant = [column for column in declared_not_null if column in primary_key]
    if redundant:
        raise MetadataError(
            "primary key columns are already not null, so remove them from Not null: "
            + ", ".join(redundant)
        )

    overlapping = [column for column in comparison if column in primary_key]
    if overlapping:
        raise MetadataError(
            "Comparison columns must not include primary key columns — a matched row "
            "has equal keys by definition: " + ", ".join(overlapping)
        )

    colliding = [
        column.name
        for column in declared_columns
        if column.name.lower() in _RESERVED_AUDIT_NAMES
    ]
    if colliding:
        raise MetadataError(
            "these column names are reserved for Weaver's audit columns: "
            + ", ".join(colliding)
        )
    # Identity is a Weaver-managed surrogate column, so it must not clash with the
    # audit columns it sits beside.
    if identity is not None and identity.lower() in _RESERVED_AUDIT_NAMES:
        raise MetadataError(
            f"Identity {identity} collides with a Weaver audit column name"
        )
    # The primary key must not be the identity column: the engine assigns it on
    # insert, so a source never produces it and a load matching on it would
    # insert duplicates every run. Caught here because the engine's "Invalid
    # column name" at install says nothing about the declaration.
    if identity is not None and identity in primary_key:
        raise MetadataError(
            f"Primary key names the Identity column {identity!r}. The engine "
            "assigns an identity on insert, so a load cannot match on it. Key "
            "on the business column that identifies a row across loads."
        )

    if not declared_columns:
        # A SQL object takes its shape from its query; checked at build instead.
        return

    # The identity column is Weaver's, not the author's, so it must not be
    # declared in Schema — but the primary key may name it when the surrogate is
    # the key, so it counts as a known column for the reference checks.
    if identity is not None and identity in {
        column.name for column in declared_columns
    }:
        raise MetadataError(
            f"Identity {identity} names a declared column; the identity column is "
            "Weaver-managed and must not appear in Schema"
        )
    known = {column.name for column in declared_columns}
    if identity is not None:
        known = known | {identity}
    unique_columns = tuple(
        column for unique_key in unique_keys for column in unique_key
    )
    foreign_key_columns = tuple(
        column for foreign_key in foreign_keys for column in foreign_key.columns
    )
    for key, columns in (
        ("Primary key", primary_key),
        ("Unique keys", unique_columns),
        ("Foreign keys", foreign_key_columns),
        ("Not null", declared_not_null),
        ("Comparison columns", comparison),
        ("Column notes", tuple(notes)),
    ):
        missing = [column for column in columns if column not in known]
        if missing:
            raise MetadataError(
                f"{key} names column(s) that are not in Schema: " + ", ".join(missing)
            )


def _apply_column_details(
    declared: tuple[Column, ...],
    notes: dict[str, MetadataText],
    primary_key: tuple[str, ...],
    declared_not_null: tuple[str, ...],
) -> tuple[Column, ...]:
    not_null = set(primary_key) | set(declared_not_null)
    return tuple(
        Column(
            name=column.name,
            type=column.type,
            note=notes.get(column.name),
            not_null=column.name in not_null,
        )
        for column in declared
    )
