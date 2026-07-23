"""One source file, read and checked without executing it.

A :class:`SourceDocument` wraps the validated
:class:`~weaver.ses.metadata.SesDocument` with everything else the file
yielded: its language, its content hash, and the parse — a Python AST or the
split SQL statements. Holding the parse here means later checkpoints read the
repository once rather than once per question.

The contract this enforces is *structural*: the object's declared ID, its
filename and (for Python) its class name must all agree, and the file must
present exactly one unit of work.

+------------+---------------------------------+------------------+
| Language   | File                            | ID               |
+============+=================================+==================+
| Python     | ``Sales__Order.py``             | ``Sales.Order``  |
| Spark SQL  | ``Sales.Order.spark.sql``       | ``Sales.Order``  |
| T-SQL      | ``Reporting.Order.sql``         | ``Reporting.Order`` |
+------------+---------------------------------+------------------+

Python uses ``__`` because a module name cannot contain a dot without breaking
imports; SQL files have no such constraint and use the dot directly. The Python
class carries the same full name as its file — ``class Sales__Order(Table)`` —
so the import at a call site is explicit about which object it names.
"""

from __future__ import annotations

import ast
import codecs
import hashlib
import re
from dataclasses import dataclass, field

from ..errors import DiscoveryError
from ..objects import BASE_CLASSES, BASE_CLASS_NAMES
from .dependencies import (
    RelationReference,
    extract_python_references,
    extract_sql_references,
)
from .metadata import (
    FOLDER,
    PYTHON,
    SPARK_SQL,
    SQL,
    TABLE,
    VIEW,
    ObjectId,
    SesDocument,
    parse_document,
    extract_python_metadata,
    extract_sql_metadata_and_body,
)

PYTHON_SUFFIX = ".py"
SPARK_SQL_SUFFIX = ".spark.sql"
SQL_SUFFIX = ".sql"

#: Python cannot have a dot in a module name, so a schema separator is needed.
PYTHON_ID_SEPARATOR = "__"


def content_hash(data: bytes) -> str:
    """A hash that is stable for the same content on any platform.

    Line endings are normalised and a UTF-8 BOM dropped before hashing: a file
    checked out with ``autocrlf`` is not a changed file, and the hash exists to
    answer "has this changed since it was certified".
    """

    if data.startswith(codecs.BOM_UTF8):
        data = data[len(codecs.BOM_UTF8):]
    return hashlib.sha256(data.replace(b"\r\n", b"\n")).hexdigest()


def language_for_filename(filename: str) -> str | None:
    """The language a filename declares, or None if it is not an object file."""

    if filename.endswith(SPARK_SQL_SUFFIX):
        return SPARK_SQL
    if filename.endswith(SQL_SUFFIX):
        return SQL
    if filename.endswith(PYTHON_SUFFIX):
        return PYTHON
    return None


def _stem(filename: str, language: str) -> str:
    suffix = {
        PYTHON: PYTHON_SUFFIX,
        SPARK_SQL: SPARK_SQL_SUFFIX,
        SQL: SQL_SUFFIX,
    }[language]
    return filename[: -len(suffix)]


def object_id_for_filename(filename: str, language: str) -> ObjectId:
    """The ID a filename claims, before the document is consulted."""

    stem = _stem(filename, language)
    if language == PYTHON:
        if "." in stem:
            raise DiscoveryError(
                f"{filename}: a Python object file separates schema and object with "
                f"{PYTHON_ID_SEPARATOR!r}, not '.', because a module name cannot "
                "contain a dot — expected Schema__Object.py"
            )
        parts = stem.split(PYTHON_ID_SEPARATOR)
    else:
        if PYTHON_ID_SEPARATOR in stem:
            raise DiscoveryError(
                f"{filename}: a SQL object file separates schema and object with '.', "
                f"not {PYTHON_ID_SEPARATOR!r} — expected Schema.Object"
                + (SPARK_SQL_SUFFIX if language == SPARK_SQL else SQL_SUFFIX)
            )
        parts = stem.split(".")
    parts = [part.strip() for part in parts]
    if len(parts) != 2 or not all(parts):
        raise DiscoveryError(
            f"{filename}: an object filename must name Schema and Object, got {stem!r}"
        )
    return ObjectId(schema=parts[0], object=parts[1])


@dataclass(frozen=True)
class SqlAnalysis:
    """What could be established about a SQL body without executing it."""

    statement_count: int
    result_set_count: int | None
    #: Why the result-set count could not be established, when it could not.
    undetermined_because: str | None = None
    statements: tuple[str, ...] = ()
    #: Statements that look like they create a permanent object. Recorded for a
    #: later lint, not refused — see _permanent_ddl.
    permanent_ddl: tuple[str, ...] = ()

    @property
    def determined(self) -> bool:
        return self.result_set_count is not None


@dataclass(frozen=True)
class SourceDocument:
    """One object's source file, parsed and structurally checked."""

    relative_path: str
    language: str
    text: str
    source_hash: str
    document: SesDocument
    class_name: str | None = None
    imported_modules: tuple[str, ...] = ()
    sql_body: str | None = None
    sql_analysis: SqlAnalysis | None = None
    #: Names this file refers to, as written. Whether each resolves is a build
    #: concern — it needs the external-dependency configuration.
    discovered_references: tuple[RelationReference, ...] = ()
    python_ast: ast.Module | None = field(default=None, compare=False, repr=False)

    @property
    def object_id(self) -> ObjectId:
        return self.document.object_id

    @property
    def qualified(self) -> str:
        return self.document.qualified

    @property
    def kind(self) -> str:
        return self.document.kind

    @property
    def referenced_object_ids(self) -> tuple[ObjectId, ...]:
        """Two-part references — candidates for objects in this repository."""

        return tuple(
            reference.object_id
            for reference in self.discovered_references
            if reference.object_id is not None
        )

    @property
    def qualified_references(self) -> tuple[RelationReference, ...]:
        """Three- and four-part references — physical targets the author named."""

        return tuple(
            reference for reference in self.discovered_references if reference.is_qualified
        )

    @property
    def declared_dependencies(self) -> tuple[ObjectId, ...]:
        return self.document.dependencies

    @property
    def module_name(self) -> str | None:
        """The importable module name, for Python objects."""

        if self.language != PYTHON:
            return None
        return self.relative_path[: -len(PYTHON_SUFFIX)]


def read_source_document(relative_path: str, data: bytes) -> SourceDocument:
    """Parse and structurally validate one object file."""

    language = language_for_filename(relative_path)
    if language is None:
        raise DiscoveryError(f"{relative_path}: not a Weaver object file")

    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise DiscoveryError(f"{relative_path}: must be UTF-8 text ({exc})") from exc

    source_hash = content_hash(data)
    filename_id = object_id_for_filename(relative_path, language)

    if language == PYTHON:
        return _read_python(relative_path, text, source_hash, filename_id)
    return _read_sql(relative_path, text, source_hash, filename_id, language)


def _check_declared_id(relative_path: str, document: SesDocument, filename_id: ObjectId) -> None:
    if document.object_id != filename_id:
        raise DiscoveryError(
            f"{relative_path}: declares {document.kind} ID "
            f"{document.qualified!r} but the filename names "
            f"{filename_id.qualified!r} — they must agree"
        )


def _read_python(
    relative_path: str, text: str, source_hash: str, filename_id: ObjectId
) -> SourceDocument:
    document = parse_document(extract_python_metadata(text), language=PYTHON)
    _check_declared_id(relative_path, document, filename_id)

    if document.kind == VIEW:
        raise DiscoveryError(
            f"{relative_path}: a View is declared in SQL, not Python — its query is "
            "its definition"
        )

    module = ast.parse(text)
    expected_class = _stem(relative_path, PYTHON)

    # Ordinary helper classes may live alongside the object. What must be
    # unique is the *Weaver* class — the one inheriting Folder, Table or View.
    candidates = [
        node
        for node in module.body
        if isinstance(node, ast.ClassDef)
        and any(_base_name(base) in BASE_CLASS_NAMES for base in node.bases)
    ]
    if not candidates:
        raise DiscoveryError(
            f"{relative_path}: must define a class inheriting "
            f"{BASE_CLASSES[document.kind].__name__} directly, and none does"
        )
    if len(candidates) > 1:
        found = ", ".join(node.name for node in candidates)
        raise DiscoveryError(
            f"{relative_path}: defines more than one Weaver object class ({found}) — "
            "one file declares one object"
        )

    declared = candidates[0]
    if declared.name != expected_class:
        raise DiscoveryError(
            f"{relative_path}: defines class {declared.name!r} but the file names "
            f"{expected_class!r} — the class, the file and the ID all carry the same name"
        )

    _check_base_class(relative_path, declared, document.kind)
    _check_read_method(relative_path, declared)
    imports = _imported_modules(module)

    return SourceDocument(
        relative_path=relative_path,
        language=PYTHON,
        text=text,
        source_hash=source_hash,
        document=document,
        class_name=declared.name,
        imported_modules=imports,
        discovered_references=extract_python_references(imports),
        python_ast=module,
    )


def _check_base_class(relative_path: str, declared: ast.ClassDef, kind: str) -> None:
    expected = BASE_CLASSES[kind].__name__
    bases = [_base_name(base) for base in declared.bases]
    if expected not in bases:
        found = ", ".join(name for name in bases if name) or "nothing"
        raise DiscoveryError(
            f"{relative_path}: declares {kind} ID, so class {declared.name!r} must "
            f"inherit {expected}, but it inherits {found}"
        )


def _base_name(node: ast.expr) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return None


def _check_read_method(relative_path: str, declared: ast.ClassDef) -> None:
    reads = [
        node
        for node in declared.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == "read"
    ]
    if not reads:
        raise DiscoveryError(
            f"{relative_path}: class {declared.name!r} must implement read()"
        )
    if len(reads) > 1:
        raise DiscoveryError(
            f"{relative_path}: class {declared.name!r} defines read() "
            f"{len(reads)} times — the later one silently replaces the earlier"
        )
    if isinstance(reads[0], ast.AsyncFunctionDef):
        raise DiscoveryError(f"{relative_path}: read() must not be async")


def _imported_modules(module: ast.Module) -> tuple[str, ...]:
    """Top-level module names imported absolutely, in source order.

    Relative imports are helper imports by construction and are excluded. What
    is a dependency rather than a plain import is decided by the repository,
    which knows every object's module name.
    """

    names: list[str] = []
    for node in ast.walk(module):
        if isinstance(node, ast.Import):
            for alias in node.names:
                names.append(alias.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom):
            if node.level:  # from . import x
                continue
            if node.module:
                names.append(node.module.split(".")[0])
    seen: list[str] = []
    for name in names:
        if name not in seen:
            seen.append(name)
    return tuple(seen)


def _read_sql(
    relative_path: str,
    text: str,
    source_hash: str,
    filename_id: ObjectId,
    language: str,
) -> SourceDocument:
    metadata_text, body = extract_sql_metadata_and_body(text)
    document = parse_document(metadata_text, language=language)
    _check_declared_id(relative_path, document, filename_id)

    if document.kind == FOLDER:
        raise DiscoveryError(
            f"{relative_path}: a Folder is declared in Python — it stages files rather "
            "than returning rows"
        )

    analysis = analyse_sql(body)

    if document.kind == VIEW and analysis.statement_count > 1:
        raise DiscoveryError(
            f"{relative_path}: a View is one query — Weaver wraps it in the CREATE "
            f"VIEW, and a view definition cannot carry preceding statements. Found "
            f"{analysis.statement_count}."
        )

    if analysis.determined and analysis.result_set_count != 1:
        raise DiscoveryError(
            f"{relative_path}: a SQL object must produce exactly one result set, "
            f"found {analysis.result_set_count}. Intermediate work is fine — only "
            "one statement may return rows."
        )

    return SourceDocument(
        relative_path=relative_path,
        language=language,
        text=text,
        source_hash=source_hash,
        document=document,
        sql_body=body,
        sql_analysis=analysis,
        discovered_references=extract_sql_references(body),
    )


#: Constructs that put the result-set count beyond static reach. Seeing one,
#: the check stands down rather than blocking a file it cannot read.
_DYNAMIC_SQL = ("exec ", "execute ", "sp_executesql")

#: Intermediate scratch — allowed, because it is working, not the object.
#: ``create temp view``, ``create temporary view``, ``create table #tmp``.
_SCRATCH_DDL = re.compile(
    r"^\s*create\s+(or\s+replace\s+)?(temp|temporary|local\s+temporary)\b"
    r"|^\s*create\s+table\s+#",
    re.IGNORECASE,
)
_PERMANENT_DDL = re.compile(
    r"^\s*create\s+(or\s+replace\s+)?(view|table)\b", re.IGNORECASE
)


def _permanent_ddl(statements: tuple[str, ...]) -> tuple[str, ...]:
    """Statements that appear to create a permanent object.

    Normally the author writes the query and Weaver writes the ``CREATE``, so
    one of these usually means the wrapper has been written by hand. It is
    *recorded*, not refused: this is fail-early validation, not critical-path,
    and there may be a legitimate reason to create something durable inside a
    body. Getting it wrong would block valid work in exchange for an error the
    build would have produced anyway.
    """

    return tuple(
        statement
        for statement in statements
        if _PERMANENT_DDL.match(statement) and not _SCRATCH_DDL.match(statement)
    )


def analyse_sql(body: str) -> SqlAnalysis:
    """Count result-producing statements, or report why that is unknowable.

    Deliberately calibrated to abstain rather than guess: a wrong rejection
    blocks a legitimate object, while a missed one merely fails at build the
    way it does today.
    """

    import sqlparse

    statements = [
        statement
        for statement in sqlparse.parse(body)
        if str(statement).strip() and not _is_only_comments(statement)
    ]

    texts = tuple(str(statement).strip() for statement in statements)

    lowered = body.lower()
    for marker in _DYNAMIC_SQL:
        if marker in lowered:
            return SqlAnalysis(
                statement_count=len(statements),
                result_set_count=None,
                undetermined_because=f"the body uses dynamic SQL ({marker.strip()})",
                statements=texts,
                permanent_ddl=_permanent_ddl(texts),
            )

    return SqlAnalysis(
        statement_count=len(statements),
        result_set_count=sum(1 for statement in statements if _returns_rows(statement)),
        statements=texts,
        permanent_ddl=_permanent_ddl(texts),
    )


def _is_only_comments(statement) -> bool:
    import sqlparse

    return all(
        token.ttype in sqlparse.tokens.Comment
        or token.ttype in sqlparse.tokens.Whitespace
        or token.ttype in sqlparse.tokens.Newline
        for token in statement.flatten()
    )


def _returns_rows(statement) -> bool:
    """A statement returns rows when it selects and does not divert the result."""

    if statement.get_type() != "SELECT":
        return False
    # T-SQL `select … into #tmp` materialises instead of returning; Spark SQL
    # has no such form, so the check is harmless there.
    return not _has_into(statement)


def _has_into(statement) -> bool:
    import sqlparse

    depth = 0
    for token in statement.flatten():
        value = token.value.lower()
        if token.ttype in sqlparse.tokens.Punctuation:
            if value == "(":
                depth += 1
            elif value == ")":
                depth -= 1
        elif depth == 0 and token.ttype in sqlparse.tokens.Keyword and value == "into":
            return True
    return False
