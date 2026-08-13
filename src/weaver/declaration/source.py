"""Read and validate one authored Weaver source file.

A SourceDocument retains its metadata, source text, language, hash, and parsed
Python or SQL representation.
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
    TEST,
    VIEW,
    ObjectId,
    SesDocument,
    namespace_for_target,
    target_kind_for,
    parse_document,
    extract_python_metadata,
    extract_sql_metadata_and_body,
)
from .model import LAKEHOUSE, WeaverDocumentId

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
        data = data[len(codecs.BOM_UTF8):]
    return hashlib.sha256(data.replace(b"\r\n", b"\n")).hexdigest()


def sql_dialect_for_item_type(item_type: str) -> str:
    """The SQL a ``.sql`` file speaks inside an item of this type.

    This is the whole reason a Weaver document needs no dialect suffix: the
    containing item already decides. A Lakehouse materialises Delta through
    Spark; a Warehouse materialises tables and views through T-SQL.
    """

    return SPARK_SQL if item_type == LAKEHOUSE else SQL


def language_for_filename(filename: str, item_type: str) -> str | None:
    """The language a filename declares, or None if it is not an object file."""

    if filename.endswith(PYTHON_SUFFIX):
        return PYTHON
    if filename.endswith(SQL_SUFFIX):
        return sql_dialect_for_item_type(item_type)
    return None


def _stem(filename: str) -> str:
    """The filename with its object suffix removed."""

    name = filename.rsplit("/", 1)[-1]
    for suffix in (PYTHON_SUFFIX, SQL_SUFFIX):
        if name.endswith(suffix):
            return name[: -len(suffix)]
    return name


def python_id_parts(stem: str) -> list[str]:
    """Split ``Schema__Object`` where the schema may itself be underscores.

    ``Sales__Order`` is unambiguous, but ``_`` is a real schema — it is where
    generated infrastructure lives — and ``_`` + ``__`` + ``Load`` spells
    ``___Load``, which an ordinary split reads as an empty schema. A run of
    leading underscores is therefore read as what it is: the last two are the
    separator and the rest are the schema, so ``___Load`` is ``_.Load``.

    Only a schema made *entirely* of underscores needs this. ``_ETL__Load`` has
    one leading underscore, splits once, and never reaches the branch.
    """

    leading = len(stem) - len(stem.lstrip("_"))
    if leading >= len(PYTHON_ID_SEPARATOR) + 1:
        return [stem[: leading - len(PYTHON_ID_SEPARATOR)], stem[leading:]]
    return stem.split(PYTHON_ID_SEPARATOR)


#: Private alias, so the helper reads as an implementation detail at its one
#: internal call site while staying importable for the authoring surface.
_python_id_parts = python_id_parts


def object_id_for_filename(filename: str, language: str) -> ObjectId:
    """The ID a filename claims, before the document is consulted."""

    stem = _stem(filename)
    if language == PYTHON:
        if "." in stem:
            raise DiscoveryError(
                f"{filename}: a Python object file separates schema and object with "
                f"{PYTHON_ID_SEPARATOR!r}, not '.', because a module name cannot "
                "contain a dot — expected Schema__Object.py"
            )
        parts = _python_id_parts(stem)
    else:
        if PYTHON_ID_SEPARATOR in stem:
            raise DiscoveryError(
                f"{filename}: a SQL object file separates schema and object with '.', "
                f"not {PYTHON_ID_SEPARATOR!r} — expected Schema.Object{SQL_SUFFIX}"
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
    #: concern — it needs the external-dependency configuration.
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
        """Whether this declares a Test or an Assumption rather than an object."""

        return self.document.is_validation

    @property
    def target_kind(self) -> str:
        """Which physical destination this object materialises into.

        A validation materialises nothing, and this still answers, because the
        answer it gives is the *namespace* every later reader wants: a Test in a
        Warehouse binds its references in the Warehouse, one in a Lakehouse in
        the Lakehouse. Nothing routes a validation to DDL on the strength of it —
        that follows from which collection it is in, not from this.
        """

        return target_kind_for(self.language, self.document.kind)

    @property
    def effective_signature(self) -> str:
        """The exact authored implementation this physical object represents."""

        return self.build_signature or self.source_hash

    @property
    def node_id(self) -> str:
        """Identity within the repository: target and ID together.

        The ID alone is not unique — the same Schema.Object may exist as a
        folder, a Delta table and a Warehouse table simultaneously.
        """

        if self.logical_id is not None:
            return str(self.logical_id)
        return f"{self.target_kind}:{self.qualified}"

    @property
    def namespace(self) -> str:
        """The execution namespace this object's references bind in."""

        return namespace_for_target(self.target_kind)

    @property
    def warehouse_alias(self) -> ObjectId | None:
        """This Lakehouse object's Warehouse-facing name, if it publishes one."""

        return self.document.warehouse_alias

    @property
    def lakehouse_alias(self) -> ObjectId | None:
        """This Warehouse object's Lakehouse-facing name, if it publishes one."""

        return self.document.lakehouse_alias

    @property
    def referenced_object_ids(self) -> tuple[ObjectId, ...]:
        """Two-part references — candidates for objects in this repository.

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
        """Three- and four-part references — physical targets the author named."""

        return tuple(
            reference for reference in self.discovered_references if reference.is_qualified
        )

    @property
    def call_references(self) -> tuple[RelationReference, ...]:
        """Two-part function calls — named like relations, resolved as functions."""

        return tuple(
            reference
            for reference in self.discovered_references
            if reference.call and len(reference.parts) == 2
        )

    @property
    def external_references(self) -> tuple[str, ...]:
        """References that leave the repository: physical names and functions.

        A valid repository resolves every ordinary two-part reference, so what
        remains is deliberately outside: a physically-qualified name, or a
        table-valued function. Recorded, never an error.
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
        """The importable module name, for Python objects."""

        if self.language != PYTHON:
            return None
        return self.relative_path[: -len(PYTHON_SUFFIX)]

    def create_ddl(self) -> "GeneratedDdl":
        """The generated, installable create definition for this source.

        Delegates to :mod:`weaver.ses.ddl`. The source owns this because it is
        the only thing that knows its language, kind, ID and validated body;
        a build planner calls it and never re-derives create syntax.
        """

        from .ddl import generate_ddl

        return generate_ddl(self)

    def create_load(self) -> "GeneratedLoad":
        """The generated, installable load definition for this source.

        The sibling of :meth:`create_ddl`, and owned here for the same reason:
        the source alone knows its language, kind, ID and validated body. The
        load artefact layer asks for this and carries what it gets rather than
        rendering anything itself.
        """

        from .load import generate_load

        return generate_load(self)

    def create_validation(self) -> "GeneratedValidation":
        """The generated, installable primitive for this validation declaration.

        The third sibling of :meth:`create_ddl` and :meth:`create_load`, owned
        here for the same reason: the source alone knows its language, kind, ID
        and validated body.
        """

        from .validation import generate_validation

        return generate_validation(self)


def read_source_document(
    relative_path: str, data: bytes, item_type: str
) -> SourceDocument:
    """Parse and structurally validate one object file."""

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
    expected_class = _stem(relative_path)

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
    """The method contract that makes a validation mean what its kind says.

    A Test declares two relations and Weaver compares them; an Assumption
    declares the violating rows directly. So a Test writes ``expected()`` and
    ``actual()`` and must *not* write ``read()`` — an authored ``read()`` would
    leave the class called a Test while meaning something else, and every
    downstream reading of "the Sales tests pass" depends on it not doing that.

    The runtime refuses the same override, so it fails whether the class is
    written in a notebook or committed here. This check exists as well because a
    repository must not need executing to be refused.
    """

    if kind == ASSUMPTION:
        _require_method(relative_path, declared, "read")
        return

    if _methods(declared, "read"):
        raise DiscoveryError(
            f"{relative_path}: class {declared.name!r} declares a Test, so it must "
            "not define read() — the comparison is Weaver's, so that passing means "
            "the same thing for every Test. Define expected() and actual(); to "
            "author the returned rows directly, declare an Assumption instead"
        )
    for name in ("expected", "actual"):
        _require_method(relative_path, declared, name)


def _methods(declared: ast.ClassDef, name: str) -> list[ast.stmt]:
    return [
        node
        for node in declared.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name
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
            f"{len(found)} times — the later one silently replaces the earlier"
        )
    if isinstance(found[0], ast.AsyncFunctionDef):
        raise DiscoveryError(f"{relative_path}: {name}() must not be async")


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


def _python_imports(module: ast.Module) -> tuple[PythonImport, ...]:
    """All imports needed for item-package dependency resolution."""

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

    if document.is_validation:
        _check_sql_validation_program(relative_path, document, body, language)
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

    if document.kind == VIEW and analysis.statement_count > 1:
        raise DiscoveryError(
            f"{relative_path}: a View is one query — Weaver wraps it in the CREATE "
            f"VIEW, and a view definition cannot carry preceding statements. Found "
            f"{analysis.statement_count}."
        )

    if document.kind == TABLE and language in (SPARK_SQL, SQL):
        # A SQL table produces its rows and, at most, the keys to delete, so it
        # may return two results rather than one — and which is which is the
        # program parser's answer, not this counter's. Each dialect has its own
        # parser because each has its own idea of where a statement ends.
        _check_sql_table_program(relative_path, document, body, language)
    elif analysis.determined and analysis.result_set_count != 1:
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


def _check_sql_validation_program(
    relative_path: str, document: SesDocument, body: str, language: str
) -> None:
    """Refuse a SQL validation whose queries cannot be its contract.

    Through the dialect's own parser, exactly as a SQL table's contract is
    checked, because each dialect has its own idea of where a statement ends.
    What the parser produces then meets one counting rule — see
    :mod:`weaver.declaration.validation_program`.
    """

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
    """Refuse an authored SQL table body that cannot mean a load.

    The same checks the generated artefact depends on, made here so a body that
    could never load is refused by a build rather than discovered by one.

    Note what is *not* refused: a body whose result-set count is beyond static
    reach because it uses dynamic SQL. The contract is about the queries Weaver
    can see, and ``EXEC`` is setup like any other statement — so what matters is
    whether the visible ``SELECT``s make sense, not whether the invisible ones
    can be counted.
    """

    if language == SPARK_SQL:
        from .spark_sql_program import (
            parse_spark_sql_program as parse,
            validate_query_contract as validate,
        )
    else:
        from .tsql_program import (
            parse_tsql_program as parse,
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
