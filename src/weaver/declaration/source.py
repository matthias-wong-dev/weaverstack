"""Read and validate authored Weaver source files."""

from __future__ import annotations

import ast
import codecs
import hashlib
import re
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from ..errors import DiscoveryError, MetadataError
from ..objects import BASE_CLASS_NAMES, BASE_CLASSES
from ..signatures import implementation_signature
from ..sql_statements import parse_sql
from .dependencies import (
    PythonImport,
    RelationReference,
    extract_python_references,
    extract_sql_references,
)
from .metadata import (
    ASSUMPTION,
    FOLDER,
    PYTHON,
    SPARK_SQL,
    SQL,
    TABLE,
    VIEW,
    ObjectId,
    SesDocument,
    extract_python_metadata,
    extract_sql_metadata_and_body,
    parse_document,
)
from .model import LAKEHOUSE, WeaverDocumentId

if TYPE_CHECKING:  # names used only in annotations
    from .ddl import GeneratedDdl
    from .load import GeneratedLoad
    from .validation import GeneratedValidation

PYTHON_SUFFIX = ".py"
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
        data = data[len(codecs.BOM_UTF8) :]
    return hashlib.sha256(data.replace(b"\r\n", b"\n")).hexdigest()


def salted_signature(signature: str, version: int) -> str:
    """Hash source content with its physical implementation version."""

    return implementation_signature(signature, version)


def sql_dialect_for_item_type(item_type: str) -> str:
    """Select Spark SQL for a Lakehouse and T-SQL for a Warehouse."""

    return SPARK_SQL if item_type == LAKEHOUSE else SQL


def language_for_filename(filename: str, item_type: str) -> str | None:
    if filename.endswith(PYTHON_SUFFIX):
        return PYTHON
    if filename.endswith(SQL_SUFFIX):
        return sql_dialect_for_item_type(item_type)
    return None


def _stem(filename: str) -> str:
    name = filename.rsplit("/", 1)[-1]
    for suffix in (PYTHON_SUFFIX, SQL_SUFFIX):
        if name.endswith(suffix):
            return name[: -len(suffix)]
    return name


def python_id_parts(stem: str) -> list[str]:
    """Split ``Schema__Object`` where the schema may itself be underscores.

    ``Sales__Order`` is unambiguous, but ``_`` is a real schema, and ``_`` +
    ``__`` + ``Load`` spells ``___Load``, which an ordinary split reads as an
    empty schema. So in a run of leading underscores the last two are the
    separator and the rest the schema: ``___Load`` is ``_.Load``.

    Only a schema made entirely of underscores reaches that branch.
    """

    leading = len(stem) - len(stem.lstrip("_"))
    if leading >= len(PYTHON_ID_SEPARATOR) + 1:
        return [stem[: leading - len(PYTHON_ID_SEPARATOR)], stem[leading:]]
    return stem.split(PYTHON_ID_SEPARATOR)


_python_id_parts = python_id_parts


def object_id_for_filename(filename: str, language: str) -> ObjectId:
    stem = _stem(filename)
    if language == PYTHON:
        if "." in stem:
            raise DiscoveryError(
                f"{filename}: a Python object filename must be Schema__Object.py"
            )
        parts = _python_id_parts(stem)
    else:
        if PYTHON_ID_SEPARATOR in stem:
            raise DiscoveryError(
                f"{filename}: a SQL object filename must be Schema.Object.sql"
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
    #: later lint, not refused. See _permanent_ddl.
    permanent_ddl: tuple[str, ...] = ()

    @property
    def determined(self) -> bool:
        return self.result_set_count is not None


@dataclass(frozen=True)
class SourceDocument:
    relative_path: str
    language: str
    text: str
    source_hash: str
    document: SesDocument
    #: Owning item type supplied by the source reader for isolated parsing.
    item_type: str
    #: The signature build compares with the installed Registry row.  For a
    #: Python document this also covers every in-item ``lib/`` module reachable
    #: through static imports; for every other document it is ``source_hash``.
    build_signature: str | None = None
    class_name: str | None = None
    imported_modules: tuple[str, ...] = ()
    python_imports: tuple[PythonImport, ...] = ()
    sql_body: str | None = None
    sql_analysis: SqlAnalysis | None = None
    #: Names this file refers to, as written. Whether each resolves is a build
    #: concern, because it needs the external-dependency configuration.
    discovered_references: tuple[RelationReference, ...] = ()
    python_ast: ast.Module | None = field(default=None, compare=False, repr=False)
    #: Item-qualified logical identity, assigned by the reader once the owning
    #: item is known. Unset only while a document is read in isolation.
    logical_id: WeaverDocumentId | None = None

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
    def is_validation(self) -> bool:
        return self.document.is_validation

    @property
    def effective_signature(self) -> str:
        return self.build_signature or self.source_hash

    @property
    def implementation_version(self) -> int:
        """Version of the implementation that materialises this declaration."""

        from .ddl import KEYED_TABLE_VERSION

        if self.document.signature_column is not None:
            return KEYED_TABLE_VERSION
        return 1

    @property
    def physical_signature(self) -> str:
        """What the installed structure represents: source and implementation.

        Direct structures use implementation version 1. A keyed table uses
        :data:`~weaver.declaration.ddl.KEYED_TABLE_VERSION` so a change to its
        Weaver-owned shape rebuilds the table when authored source is unchanged.

        Read by the desired catalogue and by incremental selection, which compare
        the two ends of the same value.
        """

        return salted_signature(self.effective_signature, self.implementation_version)

    @property
    def node_id(self) -> str:
        """The item-qualified identity; object IDs alone are not repository-unique."""

        if self.logical_id is not None:
            return str(self.logical_id)
        return f"{self.item_type}:{self.qualified}"

    @property
    def namespace(self) -> str:
        if self.logical_id is not None:
            return self.logical_id.item.item_type
        return self.item_type

    @property
    def referenced_object_ids(self) -> tuple[ObjectId, ...]:
        """Two-part references, being candidates for objects in this repository.

        Function calls are excluded: ``Sales.SplitLines(…)`` is two parts but
        names a function, not a managed object, so it yields no object identity.
        """

        return tuple(
            reference.object_id
            for reference in self.discovered_references
            if reference.object_id is not None
        )

    @property
    def qualified_references(self) -> tuple[RelationReference, ...]:
        return tuple(
            reference
            for reference in self.discovered_references
            if reference.is_qualified
        )

    @property
    def call_references(self) -> tuple[RelationReference, ...]:
        return tuple(
            reference
            for reference in self.discovered_references
            if reference.call and len(reference.parts) == 2
        )

    @property
    def external_references(self) -> tuple[str, ...]:
        """References that leave the repository: physical names and functions.

        A valid repository resolves every ordinary two-part reference, so what
        remains is outside it: a physically-qualified name, or a table-valued
        function. Recorded, never an error.
        """

        return tuple(
            sorted(
                str(reference)
                for reference in self.qualified_references + self.call_references
            )
        )

    @property
    def declared_dependencies(self) -> tuple[ObjectId, ...]:
        return self.document.dependencies

    @property
    def module_name(self) -> str | None:
        if self.language != PYTHON:
            return None
        return self.relative_path[: -len(PYTHON_SUFFIX)]

    def create_ddl(self, *, destination=None) -> "GeneratedDdl":
        from .ddl import generate_ddl

        return generate_ddl(self, destination=destination)

    def create_load(self, *, destination=None, item=None) -> "GeneratedLoad":
        from .load import generate_load

        return generate_load(self, destination=destination, item=item)

    def create_validation(self, *, destination=None) -> "GeneratedValidation":
        from .validation import generate_validation

        return generate_validation(self, destination=destination)


def read_source_document(
    relative_path: str, data: bytes, item_type: str
) -> SourceDocument:
    language = language_for_filename(relative_path, item_type)
    if language is None:
        raise DiscoveryError(f"{relative_path}: not a Weaver object file")

    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise DiscoveryError(f"{relative_path}: must be UTF-8 text ({exc})") from exc

    source_hash = content_hash(data)
    filename_id = object_id_for_filename(relative_path, language)

    if language == PYTHON:
        return _read_python(
            relative_path, text, source_hash, filename_id, item_type=item_type
        )
    return _read_sql(
        relative_path,
        text,
        source_hash,
        filename_id,
        language,
        item_type=item_type,
    )


@contextmanager
def _metadata_of(relative_path: str):
    """Name the source file whose metadata could not be read.

    The parser is given the extracted block, so any coordinates it reports are
    positions in that block and say so. Applied here and nowhere below, so a
    message names its file once.
    """

    try:
        yield
    except MetadataError as exc:
        raise MetadataError(f"{relative_path}:\n  {_metadata_detail(exc)}") from exc


def _metadata_detail(exc: MetadataError) -> str:
    mark = getattr(getattr(exc, "__cause__", None), "problem_mark", None)
    if mark is None:
        return str(exc)
    problem = getattr(exc.__cause__, "problem", None) or str(exc)
    return f"Metadata line {mark.line + 1}, column {mark.column + 1}: {problem}"


@contextmanager
def _analysis_of(relative_path: str):
    """Name the source file whose SQL the parser could not get through.

    Only the parser's own failures. A defect in Weaver is not an invalid
    statement and must not be reported as one.
    """

    from sqlparse.exceptions import SQLParseError

    try:
        yield
    except (SQLParseError, RecursionError) as exc:
        raise DiscoveryError(
            f"{relative_path}:\n  SQL could not be analysed: "
            f"{exc or type(exc).__name__}."
        ) from exc


def _check_declared_id(
    relative_path: str, document: SesDocument, filename_id: ObjectId
) -> None:
    if document.object_id != filename_id:
        raise DiscoveryError(
            f"{relative_path}: declares {document.kind} ID "
            f"{document.qualified!r} but the filename names "
            f"{filename_id.qualified!r}. They must agree"
        )


def _read_python(
    relative_path: str,
    text: str,
    source_hash: str,
    filename_id: ObjectId,
    *,
    item_type: str,
) -> SourceDocument:
    with _metadata_of(relative_path):
        document = parse_document(extract_python_metadata(text), language=PYTHON)
    _check_declared_id(relative_path, document, filename_id)

    if document.kind == VIEW:
        raise DiscoveryError(f"{relative_path}: declare a View in SQL, not Python")

    module = ast.parse(text)
    expected_class = _stem(relative_path)

    # Ordinary helper classes may live alongside the object. What must be
    # unique is the Weaver class, the one inheriting Folder, Table or View.
    candidates = [
        node
        for node in module.body
        if isinstance(node, ast.ClassDef)
        and any(_base_name(base) in BASE_CLASS_NAMES for base in node.bases)
    ]
    if not candidates:
        raise DiscoveryError(
            f"{relative_path}: define one class that directly inherits "
            f"{BASE_CLASSES[document.kind].__name__}"
        )
    if len(candidates) > 1:
        found = ", ".join(node.name for node in candidates)
        raise DiscoveryError(
            f"{relative_path}: defines more than one Weaver object class ({found}). "
            "One file declares one object"
        )

    declared = candidates[0]
    if declared.name != expected_class:
        raise DiscoveryError(
            f"{relative_path}: class {declared.name!r} does not match filename "
            f"{expected_class!r}; rename the class to {expected_class!r}"
        )

    _check_base_class(relative_path, declared, document.kind)
    if document.is_validation:
        _check_validation_methods(relative_path, declared, document.kind)
    else:
        _check_read_method(relative_path, declared)
    imports = _imported_modules(module)
    python_imports = _python_imports(module)

    return SourceDocument(
        relative_path=relative_path,
        language=PYTHON,
        text=text,
        source_hash=source_hash,
        document=document,
        item_type=item_type,
        class_name=declared.name,
        imported_modules=imports,
        python_imports=python_imports,
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
    _require_method(relative_path, declared, "read")


def _check_validation_methods(
    relative_path: str, declared: ast.ClassDef, kind: str
) -> None:
    """A Test defines expected/actual relations; an Assumption defines violating rows."""

    if kind == ASSUMPTION:
        _require_method(relative_path, declared, "read")
        return

    if _methods(declared, "read"):
        raise DiscoveryError(
            f"{relative_path}: Test class {declared.name!r} must not define read(). "
            "Define expected() and actual(), or declare an Assumption."
        )
    for name in ("expected", "actual"):
        _require_method(relative_path, declared, name)


def _methods(declared: ast.ClassDef, name: str) -> list[ast.stmt]:
    return [
        node
        for node in declared.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == name
    ]


def _require_method(relative_path: str, declared: ast.ClassDef, name: str) -> None:
    found = _methods(declared, name)
    if not found:
        raise DiscoveryError(
            f"{relative_path}: class {declared.name!r} must implement {name}()"
        )
    if len(found) > 1:
        raise DiscoveryError(
            f"{relative_path}: class {declared.name!r} defines {name}() "
            f"{len(found)} times; remove the duplicate definitions"
        )
    if isinstance(found[0], ast.AsyncFunctionDef):
        raise DiscoveryError(f"{relative_path}: {name}() must not be async")


def _imported_modules(module: ast.Module) -> tuple[str, ...]:
    """Module names imported absolutely, in source order.

    The top-level package, except beneath a Lakehouse area: ``Tables`` and
    ``Files`` are the two packages an item's own object modules sit in, so what
    is recorded there is the module inside them. Relative imports are helper
    imports and are excluded. Which of the rest is a dependency is decided by
    the repository, which holds every object's module name.
    """

    names: list[str] = []
    for node in ast.walk(module):
        if isinstance(node, ast.Import):
            for alias in node.names:
                names.append(_imported_name(alias.name))
        elif isinstance(node, ast.ImportFrom):
            if node.level:  # from . import x
                continue
            if node.module:
                names.append(_imported_name(node.module))
    seen: list[str] = []
    for name in names:
        if name not in seen:
            seen.append(name)
    return tuple(seen)


def _imported_name(module: str) -> str:
    from .model import AREAS

    head, _, tail = module.partition(".")
    if head in AREAS and tail:
        return tail.split(".")[0]
    return head


def _python_imports(module: ast.Module) -> tuple[PythonImport, ...]:
    imports: list[PythonImport] = []
    for node in ast.walk(module):
        if isinstance(node, ast.ImportFrom):
            imports.append(
                PythonImport(
                    module=node.module,
                    level=node.level,
                    names=tuple(alias.name for alias in node.names),
                )
            )
        elif isinstance(node, ast.Import):
            imports.extend(
                PythonImport(module=alias.name, names=(alias.name,))
                for alias in node.names
            )
    return tuple(imports)


def _read_sql(
    relative_path: str,
    text: str,
    source_hash: str,
    filename_id: ObjectId,
    language: str,
    *,
    item_type: str,
) -> SourceDocument:
    with _metadata_of(relative_path):
        metadata_text, body = extract_sql_metadata_and_body(text)
        document = parse_document(metadata_text, language=language)
    _check_declared_id(relative_path, document, filename_id)

    if document.kind == FOLDER:
        raise DiscoveryError(f"{relative_path}: declare a Folder in Python, not SQL")

    with _analysis_of(relative_path):
        analysis = analyse_sql(body)

    if document.is_validation:
        _check_sql_validation_program(relative_path, document, body, language)
        return SourceDocument(
            relative_path=relative_path,
            language=language,
            text=text,
            source_hash=source_hash,
            document=document,
            item_type=item_type,
            sql_body=body,
            sql_analysis=analysis,
            discovered_references=extract_sql_references(body),
        )

    if document.kind == VIEW and analysis.statement_count > 1:
        raise DiscoveryError(
            f"{relative_path}: a View must contain one query; found "
            f"{analysis.statement_count} statements"
        )

    if document.kind == TABLE and language in (SPARK_SQL, SQL):
        # A SQL table produces its rows and, at most, the keys to delete, so it
        # may return two results rather than one, and which is which is the
        # program parser's answer, not this counter's. Each dialect has its own
        # parser because each has its own idea of where a statement ends.
        _check_sql_table_program(relative_path, document, body, language)
    elif analysis.determined and analysis.result_set_count != 1:
        raise DiscoveryError(
            f"{relative_path}: a SQL object must produce exactly one result set, "
            f"found {analysis.result_set_count}. Intermediate work is fine, only "
            "one statement may return rows."
        )

    return SourceDocument(
        relative_path=relative_path,
        language=language,
        text=text,
        source_hash=source_hash,
        document=document,
        item_type=item_type,
        sql_body=body,
        sql_analysis=analysis,
        discovered_references=extract_sql_references(body),
    )


def _check_sql_validation_program(
    relative_path: str, document: SesDocument, body: str, language: str
) -> None:
    """Validate the query contract with the parser for the source dialect."""

    from .validation_program import validate_validation_contract

    if language == SPARK_SQL:
        from .spark_sql_program import parse_spark_sql_program as parse
    else:
        from .tsql_program import parse_tsql_program as parse

    program = parse(body, what=relative_path, error=DiscoveryError)
    validate_validation_contract(
        program, what=relative_path, kind=document.kind, error=DiscoveryError
    )


def _check_sql_table_program(
    relative_path: str, document: SesDocument, body: str, language: str
) -> None:
    """Validate the load contract when the SQL result sets can be determined."""

    if language == SPARK_SQL:
        from .spark_sql_program import (
            parse_spark_sql_program as parse,
        )
        from .spark_sql_program import (
            validate_query_contract as validate,
        )
    else:
        from .tsql_program import (
            parse_tsql_program as parse,
        )
        from .tsql_program import (
            validate_query_contract as validate,
        )

    program = parse(body, what=relative_path, error=DiscoveryError)
    validate(
        program,
        what=relative_path,
        primary_key=document.primary_key,
        incremental=document.is_incremental,
        error=DiscoveryError,
    )


#: Constructs that put the result-set count beyond static reach. Seeing one,
#: the check stands down rather than blocking a file it cannot read.
_DYNAMIC_SQL = ("exec ", "execute ", "sp_executesql")

#: Intermediate scratch, allowed because it is working and not the object.
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
    """Record apparent permanent DDL for linting without refusing it."""

    return tuple(
        statement
        for statement in statements
        if _PERMANENT_DDL.match(statement) and not _SCRATCH_DDL.match(statement)
    )


def analyse_sql(body: str) -> SqlAnalysis:
    """Count result-producing statements, or report why that is unknowable.

    Calibrated to abstain rather than guess: a wrong rejection blocks a
    legitimate object, while a missed one fails at build as it does today.

    Authored repository SQL is trusted input. ``sqlparse`` 0.6 applies a
    process-wide 10,000-token grouping ceiling intended for untrusted input;
    disable that ceiling before parsing so the size of a valid statement does
    not decide whether Weaver can build it.
    """

    from sqlparse.engine import grouping

    grouping.MAX_GROUPING_TOKENS = None

    statements = [
        statement
        for statement in parse_sql(body)
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
