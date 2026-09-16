"""Generate a source's create definition for its bound destination."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING

from ..errors import DiscoveryError
from .columns import metadata_column_references
from .dependencies import address_managed_references
from .metadata import SPARK_SQL, SQL, TABLE, VIEW

if TYPE_CHECKING:
    from .source import SourceDocument

#: Increment when the generated bundle shape changes.
BUILD_FORMAT_VERSION = 3

#: Salt for keyed-table physical signatures; increment when their shape changes.
KEYED_TABLE_VERSION = 1

#: Runtime dispatch key shared by local and Fabric Spark sessions.
SPARK_SQL_EXECUTOR = "spark_sql"
SPARK_SQL_EXTENSION = ".spark.sql"

#: Structured Delta table payload resolved and created inside the Session boundary.
SPARK_TABLE_EXECUTOR = "spark_table"
SPARK_TABLE_EXTENSION = ".spark-table.json"

#: Runs a complete T-SQL script against a Warehouse.
TSQL_EXECUTOR = "tsql"
TSQL_EXTENSION = ".sql"


@dataclass(frozen=True)
class GeneratedDdl:
    executor: str
    content: str
    extension: str


def generate_ddl(document: "SourceDocument", *, destination=None) -> GeneratedDdl:
    """Generate an installable create definition.

    ``destination`` is the Spark destination the object is bound to, and every
    managed name in the result is rendered against it. A Warehouse object needs
    none: its script is T-SQL, addressed by the connection it runs on.
    """

    if document.language == SQL:
        return _tsql_ddl(document)
    if destination is None:
        raise DiscoveryError(
            f"{document.relative_path}: the Spark object has no bound destination. "
            "Bind it to a Lakehouse before generating its create definition."
        )
    if document.kind == TABLE:
        return _spark_table_ddl(document, destination)
    if document.kind == VIEW:
        return _view_ddl(document, destination)
    raise NotImplementedError(
        f"{document.relative_path}: a {document.kind} has no create DDL"
    )


def _tsql_ddl(document: "SourceDocument") -> GeneratedDdl:
    """A Warehouse object's build: a self-contained T-SQL script.

    A table materialises and inspects its own query shape server-side and creates
    only its main table; a view is a strict ``CREATE VIEW`` over its body.
    """

    from .tsql_ddl import generate_tsql_table_script, generate_tsql_view_script

    body = document.sql_body or ""
    if document.kind == TABLE:
        content = generate_tsql_table_script(document.document, body)
    elif document.kind == VIEW:
        content = generate_tsql_view_script(document.document, body)
    else:  # pragma: no cover - a SQL Folder is impossible (reader refuses it)
        raise NotImplementedError(
            f"{document.relative_path}: a {document.kind} has no create DDL"
        )
    return GeneratedDdl(
        executor=TSQL_EXECUTOR, content=content, extension=TSQL_EXTENSION
    )


def _object_name(document: "SourceDocument", destination) -> str:
    return destination.qualify(document.object_id.schema, document.object_id.object)


def _spark_table_ddl(document: "SourceDocument", destination) -> GeneratedDdl:
    """Freeze the inputs needed to build a Delta table in its session.

    Spark SQL query shape remains deferred until execution. A Python table carries
    its declared shape and no query. Neither path reopens or executes the source.
    """

    ses = document.document
    declared = ses.has_declared_schema
    if document.language == SPARK_SQL:
        setup, query = _shape_program(document)
    else:
        if not declared:  # pragma: no cover - parsing requires it
            raise NotImplementedError(
                f"{document.relative_path}: a Python-backed Delta table must "
                "declare its schema"
            )
        setup, query = (), None
    payload = {
        "object": _object_name(document, destination),
        "schema_mode": "declared" if declared else "inferred",
        "declared_columns": (
            [_column_entry(column) for column in ses.schema] if declared else None
        ),
        # Only the first result query determines table shape; preceding setup must
        # run first, while a later delete query is irrelevant here.
        "setup": [
            address_managed_references(statement, destination) for statement in setup
        ],
        "source_query": (
            address_managed_references(query, destination)
            if query is not None
            else None
        ),
        "references": [list(pair) for pair in metadata_column_references(ses)],
        "identity_column": (
            _column_entry(ses.identity_column) if ses.identity_column else None
        ),
        "audit_columns": [_column_entry(column) for column in ses.audit_columns],
        # Keyed tables append the row signature after audit columns.
        "internal_columns": [
            _column_entry(column)
            for column in ses.internal_columns
            if column not in ses.audit_columns
        ],
        "column_mapping": True,
    }
    content = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    return GeneratedDdl(
        executor=SPARK_TABLE_EXECUTOR, content=content, extension=SPARK_TABLE_EXTENSION
    )


def _shape_program(document: "SourceDocument") -> tuple[tuple[str, ...], str]:
    """Return setup before the first result query and that query."""

    from .spark_sql_program import parse_spark_sql_program

    program = parse_spark_sql_program(
        document.sql_body or "",
        what=document.relative_path,
        error=DiscoveryError,
    )
    setup: list[str] = []
    for statement in program.statements:
        if statement.produces_result:
            return tuple(setup), statement.sql
        setup.append(statement.sql)
    # Keep generation safe when called with a document that bypassed validation.
    raise DiscoveryError(
        f"{document.relative_path}: the Spark SQL table has no query that "
        "produces rows. Add a staging query."
    )


def _view_ddl(document: "SourceDocument", destination) -> GeneratedDdl:
    """Create a view with every managed reference bound to its Lakehouse.

    The body is otherwise unchanged.
    """

    body = address_managed_references((document.sql_body or "").rstrip(), destination)
    content = f"CREATE VIEW {_object_name(document, destination)} AS\n{body}\n"
    return GeneratedDdl(
        executor=SPARK_SQL_EXECUTOR, content=content, extension=SPARK_SQL_EXTENSION
    )


def _column_entry(column) -> list:
    return [column.name, column.type, column.not_null]
