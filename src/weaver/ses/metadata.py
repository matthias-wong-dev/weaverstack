"""The SES document contract.

This is the basic unit of work in Weaver: a Folder, a Delta table, or a
Warehouse table or view, declared as YAML at the top of its source file — a
Python module docstring, or the opening ``/* … */`` of a SQL file.

The contract is validated to exhaustion up front. Every key is known, every
column reference is checked against the declared schema where one exists, and
every contradiction is refused before anything physical happens. A mistyped
``Primary Key`` must not parse as "no primary key" and silently become a full
replacement at load time.

Where validation cannot happen here it is recorded rather than skipped: a SQL
object infers its shape from its query, so its column references are parsed but
resolved at build. :attr:`SesDocument.defers_column_validation` says so.

Nothing here imports the module it describes, reads a file, or resolves a
reference to another object. Reference resolution needs sibling documents and
belongs with the repository reader.

**Layout convention.** Separate each subsection with a blank line. This is not
enforced — YAML does not care — but the header is the contract a reader meets
first, and a wall of keys is a worse contract than a legible one::

    Table ID: Sales.Order

    Description: One row per confirmed customer order.

    Lineage: $Sales.OrderExport

    Primary key: Order id

    Schema:
      Order id: string
      Order date: date

    Revision notes:
      - 2026-07-23 Added the amount column.

Fixtures and examples follow it so the convention is learned by reading.
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

_ID_KEYS = {"Folder ID": FOLDER, "Table ID": TABLE, "View ID": VIEW}
_PLACEHOLDERS = {"not declared", "n/a", "tbd", "todo"}

# Keys accepted per kind. Anything else is a typo and is refused by name.
_COMMON_KEYS = {
    "Description",
    "Lineage",
    "Notes",
    "Revision notes",
    "Dependencies",
    "Static",
    "Prohibit rebuild",
}
_KIND_KEYS = {
    FOLDER: {"File key", "Incremental"},
    TABLE: {
        "Schema",
        "Column notes",
        "Primary key",
        "Not null",
        "Identity",
        "Comparison columns",
        "Incremental",
    },
    VIEW: {"Column notes"},
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

_REFERENCE = re.compile(r"^\$([^.\[\]$]+)\.([^.\[\]$]+)(?:\[([^\[\]$]+)\])?$")

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
#: form already used by the SQL backend, Delta uses underscores because spaces
#: in Spark column names need quoting everywhere they appear.
AUDIT_INSERT = "Row insert datetime"
AUDIT_UPDATE = "Row update datetime"
AUDIT_DELETE = "Row delete datetime"
AUDIT_COLUMNS = (AUDIT_INSERT, AUDIT_UPDATE, AUDIT_DELETE)

_AUDIT_TYPES = {PYTHON: "timestamp", SPARK_SQL: "timestamp", SQL: "datetime2(6)"}


def audit_column_name(logical: str, language: str) -> str:
    return logical.replace(" ", "_") if language in DELTA_LANGUAGES else logical


def _audit_columns(language: str) -> tuple["Column", ...]:
    return tuple(
        Column(
            name=audit_column_name(logical, language),
            type=_AUDIT_TYPES[language],
            # A live row carries a sentinel delete datetime, so it is not null.
            not_null=logical == AUDIT_DELETE,
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
    """A ``$Schema.Object`` or ``$Schema.Object[Column]`` reference."""

    schema: str
    object: str
    column: str | None = None

    @property
    def object_id(self) -> ObjectId:
        return ObjectId(schema=self.schema, object=self.object)

    def __str__(self) -> str:
        target = f"${self.schema}.{self.object}"
        return f"{target}[{self.column}]" if self.column else target


@dataclass(frozen=True)
class MetadataText:
    """Either literal prose or exactly one reference — never a mix.

    ``See $Sales.Order`` is refused. Mixed content cannot be resolved
    mechanically, and a contract that is only sometimes machine-readable is not
    a contract. Write ``$$`` for a literal dollar sign.
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
class Column:
    """One column of a table or view."""

    name: str
    type: str | None = None
    note: MetadataText | None = None
    not_null: bool = False
    is_audit: bool = False


# --- the document ----------------------------------------------------------


@dataclass(frozen=True)
class SesDocument:
    """A fully validated SES object declaration."""

    kind: str
    language: str
    object_id: ObjectId
    description: MetadataText
    lineage: MetadataText
    notes: str | None = None
    dependencies: tuple[ObjectId, ...] = ()
    revision_notes: tuple[Revision, ...] = ()
    revision_date_format: str | None = None
    schema: tuple[Column, ...] = ()
    primary_key: tuple[str, ...] = ()
    declared_not_null: tuple[str, ...] = ()
    identity: str | None = None
    declared_comparison_columns: tuple[str, ...] = ()
    file_keys: tuple[str, ...] = ()
    is_incremental: bool = False
    prohibit_rebuild: bool = False
    static: bool = False
    raw: dict[str, Any] = field(default_factory=dict)

    @property
    def qualified(self) -> str:
        return self.object_id.qualified

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
    def effective_schema(self) -> tuple[Column, ...]:
        """Declared columns plus the audit columns.

        ``schema`` stays exactly what the author wrote; this is what gets
        materialised. Both are available because either can be the one you
        need.
        """

        return self.schema + self.audit_columns

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
        raise MetadataError("python object file must begin with a docstring metadata block")
    return doc


def extract_sql_metadata_and_body(source: str) -> tuple[str, str]:
    """Split a SQL object file into (metadata text, executable body)."""

    match = re.match(r"\s*/\*(.*?)\*/(.*)\Z", source, flags=re.DOTALL)
    if not match:
        raise MetadataError("SES SQL must begin with a /* ... */ metadata block")
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
    _reject_unknown_keys(loaded, kind)

    if kind == TABLE and language == SQL and "Schema" in loaded:
        raise MetadataError(
            "Schema is not declared for SQL objects — a Warehouse table takes its "
            "shape from its query and is validated at build. Use Column notes to "
            "describe its columns."
        )

    dependencies = _parse_dependencies(loaded.get("Dependencies"), object_id)
    if language == SPARK_SQL and not dependencies:
        raise MetadataError(
            "a Spark SQL object must declare Dependencies. Its query may read by "
            "path, which cannot be resolved back to a managed object, so the graph "
            "is declared rather than discovered."
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
    is_incremental = _parse_flag_with_default(loaded, "Incremental", default=kind == FOLDER)

    declared_columns = _parse_schema(loaded.get("Schema"))
    if kind == TABLE and language in DELTA_LANGUAGES and not declared_columns:
        raise MetadataError(
            "a Delta table must declare Schema — it is created before it is loaded, "
            "and the declared shape is what lets every column guard run up front"
        )

    primary_key = _parse_column_set(loaded.get("Primary key"), "Primary key")
    declared_not_null = _parse_column_list(loaded.get("Not null"), "Not null")
    identity = _parse_identity(loaded.get("Identity"))
    comparison = _parse_column_set(loaded.get("Comparison columns"), "Comparison columns")
    column_notes = _parse_column_notes(loaded.get("Column notes"))

    _validate_columns(
        kind=kind,
        declared_columns=declared_columns,
        primary_key=primary_key,
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
    if static and is_incremental:
        raise MetadataError(
            "Static and Incremental: true contradict — a static object is loaded once, "
            "so there is nothing to accumulate"
        )

    schema = _apply_column_details(declared_columns, column_notes, primary_key, declared_not_null)

    return SesDocument(
        kind=kind,
        language=language,
        object_id=object_id,
        description=description,
        lineage=lineage,
        notes=notes,
        dependencies=dependencies,
        revision_notes=revisions,
        revision_date_format=revision_format,
        schema=schema,
        primary_key=primary_key,
        declared_not_null=declared_not_null,
        identity=identity,
        declared_comparison_columns=comparison,
        file_keys=file_keys,
        is_incremental=is_incremental,
        prohibit_rebuild=prohibit_rebuild,
        static=static,
        raw=dict(loaded),
    )


def _parse_id(raw: dict[str, Any]) -> tuple[str, ObjectId]:
    present = [key for key in _ID_KEYS if key in raw and raw[key] is not None]
    if len(present) != 1:
        raise MetadataError("metadata must include exactly one of Folder ID, Table ID, View ID")
    key = present[0]
    value = raw[key]
    if not isinstance(value, str) or not value.strip():
        raise MetadataError(f"{key} must be a non-empty Schema.Object string")
    parts = [part.strip() for part in value.strip().split(".")]
    if len(parts) != 2 or not all(parts):
        raise MetadataError(f"{key} must be a two-part Schema.Object declaration, got {value!r}")
    return _ID_KEYS[key], ObjectId(schema=parts[0], object=parts[1])


def _reject_unknown_keys(raw: dict[str, Any], kind: str) -> None:
    allowed = _COMMON_KEYS | _KIND_KEYS[kind] | set(_ID_KEYS)
    unknown = {str(key) for key in raw} - allowed
    if unknown:
        wrong_kind = {
            key
            for key in unknown
            for other_kind, keys in _KIND_KEYS.items()
            if key in keys and other_kind != kind
        }
        detail = ""
        if wrong_kind:
            detail = f" ({', '.join(sorted(wrong_kind))} belongs to another object kind)"
        raise MetadataError(
            f"unknown metadata key(s) for a {kind} object: "
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
        schema, obj, column = match.groups()
        return MetadataText(
            reference=Reference(schema=schema.strip(), object=obj.strip(),
                                column=column.strip() if column else None)
        )
    literal = stripped.replace("$$", "$")
    if literal.lower() in _PLACEHOLDERS:
        raise MetadataError(f"{key} must not be a placeholder value ({literal!r})")
    return MetadataText(literal=literal)


def _parse_dependencies(value: Any, object_id: ObjectId) -> tuple[ObjectId, ...]:
    """Objects this one depends on, declared rather than discovered.

    Additive: whatever discovery finds is added to these, never replaced by
    them. A missing dependency is a wrong build order, which is silent data
    corruption, so the declared set can only ever widen the graph.
    """

    if value is None:
        return ()
    if not isinstance(value, list) or not value:
        raise MetadataError(
            "Dependencies must be a non-empty YAML list of Schema.Object names:\n"
            "Dependencies:\n  - Sales.Customer"
        )
    seen: list[ObjectId] = []
    for entry in value:
        if not isinstance(entry, str) or not entry.strip():
            raise MetadataError("Dependencies entries must be non-empty Schema.Object names")
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
            raise MetadataError(
                f"Revision notes entry {entry} has a date but no note"
            )
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
        note = text[len(date_text):].strip()
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
            raise MetadataError(f"Revision notes entry does not open with a real date: {text!r}")
        return shape, match.group(0)
    return None


def _parse_bool(value: Any, key: str) -> bool:
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    raise MetadataError(f"{key} must be a boolean (true/false)")


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
            raise MetadataError(
                f"Schema column {name!r} must declare a non-empty type"
            )
        columns.append(Column(name=name.strip(), type=column_type.strip()))
    return tuple(columns)


def _parse_column_notes(value: Any) -> dict[str, MetadataText]:
    if value is None:
        return {}
    if not isinstance(value, dict) or not value:
        raise MetadataError("Column notes must be a non-empty mapping of column to description")
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

    audit_names = {name for logical in AUDIT_COLUMNS for name in
                   (logical, audit_column_name(logical, PYTHON))}
    colliding = [column.name for column in declared_columns if column.name in audit_names]
    if colliding:
        raise MetadataError(
            "these column names are reserved for Weaver's audit columns: "
            + ", ".join(colliding)
        )

    if not declared_columns:
        # A SQL object takes its shape from its query; checked at build instead.
        return

    known = {column.name for column in declared_columns}
    for key, columns in (
        ("Primary key", primary_key),
        ("Not null", declared_not_null),
        ("Comparison columns", comparison),
        ("Identity", (identity,) if identity else ()),
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
