"""Generate one source's create definition against its bound destination.

Build definitions create structures; load definitions populate tables. Every
managed name is rendered here, so an executor runs the statement as written.
"""

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

#: The bundle format version this generator targets. A change to the generated
#: shape is a change to this number. Version 2 dropped the ``spark_table``
#: payload's identity column: a Delta table no longer has one to carry.
BUILD_FORMAT_VERSION = 2

#: The version of the physical shape Weaver gives a *keyed* table, salted into
#: :attr:`~weaver.declaration.source.SourceDocument.physical_signature`. It is
#: what makes a change to that shape rebuild the tables it changed even though no
#: authored source moved. Version 1 added the row-signature column.
#:
#: Keyed only, so an unkeyed table — which gains nothing — is not rebuilt for a
#: change it does not carry.
KEYED_TABLE_VERSION = 1

#: The executor a concrete Spark statement runs through. It names a runtime
#: dispatch key, not an engine — a Fabric Spark session and a local one both use
#: ``spark_sql``.
SPARK_SQL_EXECUTOR = "spark_sql"
SPARK_SQL_EXTENSION = ".spark.sql"

#: The executor that completes a Spark SQL table's build: it runs the query,
#: reads the resulting ``DataFrame`` schema, validates, and creates the table.
#: Its payload is JSON, not SQL, because the DDL cannot be finished until the
#: query's shape is known in the session.
SPARK_TABLE_EXECUTOR = "spark_table"
SPARK_TABLE_EXTENSION = ".spark-table.json"

#: The executor that runs a T-SQL script against the Warehouse. Its payload is a
#: finished, self-contained script — a table build materialises and inspects its
#: own query shape server-side, so no round-trip is needed.
TSQL_EXECUTOR = "tsql"
TSQL_EXTENSION = ".sql"

#: Delta column mapping keeps declared column names with spaces (``Order id``)
#: legal without quoting them everywhere they later appear.
_COLUMN_MAPPING = "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')"


@dataclass(frozen=True)
class GeneratedDdl:
    """One source's generated, installable create definition."""

    executor: str
    content: str
    extension: str


def generate_ddl(document: "SourceDocument", *, destination=None) -> GeneratedDdl:
    """The installable create definition for one validated source.

    ``destination`` is the Spark destination the object is bound to, and every
    managed name in the result is rendered against it. A Warehouse object needs
    none: its script is T-SQL, addressed by the connection it runs on.

    Folders have no create DDL — a Folder is a directory, created by the
    installer rather than by a statement — so this is never called for one.
    """

    if document.language == SQL:
        return _tsql_ddl(document)
    if destination is None:
        raise DiscoveryError(
            f"{document.relative_path}: a Spark object needs a bound destination "
            "before its create definition can be generated"
        )
    if document.kind == TABLE:
        if document.language == SPARK_SQL:
            return _spark_table_ddl(document, destination)
        return _python_table_ddl(document, destination)
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
    """How a payload names the object it builds."""

    return destination.qualify(document.object_id.schema, document.object_id.object)


def _python_table_ddl(document: "SourceDocument", destination) -> GeneratedDdl:
    """A Delta table from its declared columns, plus the audit columns.

    A Python-backed table has no query to infer from, so the reader requires a
    declared schema. The build is an empty table of that shape with Weaver's
    audit columns appended — the concrete statement is known now, so it is frozen
    directly rather than deferred to an executor.
    """

    columns = document.document.effective_schema
    if not document.document.schema:  # pragma: no cover - the reader requires it
        raise NotImplementedError(
            f"{document.relative_path}: a Python-backed Delta table must declare "
            "its schema; schema inference needs a query"
        )
    content = _create_table_sql(_object_name(document, destination), columns)
    return GeneratedDdl(
        executor=SPARK_SQL_EXECUTOR, content=content, extension=SPARK_SQL_EXTENSION
    )


def _spark_table_ddl(document: "SourceDocument", destination) -> GeneratedDdl:
    """A Spark SQL table's deferred, deterministic build instruction.

    Declared or inferred, its shape is only settled by running the query in the
    session, so the payload is not finished SQL. It is a JSON instruction the
    ``spark_table`` executor completes in one self-contained action: run the
    query, read the ``DataFrame`` schema, validate the columns (the same guards a
    declared schema passes at parse), choose the physical business columns,
    append the audit columns, and create the table. Everything the executor needs
    is frozen here, so it never reopens the Weaver document source (how-does-build-work §2).
    """

    ses = document.document
    declared = ses.has_declared_schema
    setup, query = _shape_program(document)
    payload = {
        "object": _object_name(document, destination),
        "schema_mode": "declared" if declared else "inferred",
        "declared_columns": (
            [_column_entry(column) for column in ses.schema] if declared else None
        ),
        # The statements that must run first — a temporary view the query reads —
        # and the one query whose shape *is* the table's. A body may hold two
        # queries (staging, then the keys to delete), and only the first says
        # what the table looks like.
        "setup": [
            address_managed_references(statement, destination) for statement in setup
        ],
        "source_query": address_managed_references(query, destination),
        "references": [list(pair) for pair in metadata_column_references(ses)],
        "audit_columns": [_column_entry(column) for column in ses.audit_columns],
        # Weaver's other own columns, after the audit ones. Empty unless the
        # table is keyed, which is the only kind that carries a row signature.
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
    """The setup a shape inference must run, and the query whose shape it reads.

    A Spark SQL table's body may set a temporary view up before selecting from
    it, and may carry a second query naming the keys to delete. Neither is the
    table's shape: the first query is, and the setup before it is what has to
    have run for that query to resolve. Handing the whole body to one
    ``spark.sql`` call would fail on the first semicolon.
    """

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
    # Unreachable for a validated document — parsing already refused a body with
    # no query — but a build must not depend on that having happened.
    raise DiscoveryError(
        f"{document.relative_path}: a Spark SQL table must end in a query that "
        "produces its rows, and this body has none"
    )


def _view_ddl(document: "SourceDocument", destination) -> GeneratedDdl:
    """A persistent view over the validated body, its managed names addressed.

    The body is otherwise untouched. What changes is that every reference to
    another managed object now says which Lakehouse it means — without which a
    view built in one destination would read its inputs from whichever Lakehouse
    the session happened to be attached to.
    """

    body = address_managed_references((document.sql_body or "").rstrip(), destination)
    content = f"CREATE VIEW {_object_name(document, destination)} AS\n{body}\n"
    return GeneratedDdl(
        executor=SPARK_SQL_EXECUTOR, content=content, extension=SPARK_SQL_EXTENSION
    )


def _column_entry(column) -> list:
    """A payload column triple ``[name, type, not_null]``.

    Nullability travels with the column so the executor emits the same
    constraint — the audit columns are always not null, and a declared primary
    key or ``Not null`` column carries its constraint through too.
    """

    return [column.name, column.type, column.not_null]


def _create_table_sql(qualified: str, columns) -> str:
    """A strict ``CREATE TABLE`` over concrete columns."""

    column_lines = ",\n".join(
        f"    {_ident(c.name)} {c.type}{' NOT NULL' if c.not_null else ''}"
        for c in columns
    )
    return (
        f"CREATE TABLE {qualified} (\n"
        f"{column_lines}\n"
        ")\n"
        "USING delta\n"
        f"{_COLUMN_MAPPING}\n"
    )


def _ident(name: str) -> str:
    """Back-tick quote a column identifier so spaces and keywords are safe."""

    return "`" + name.replace("`", "``") + "`"
