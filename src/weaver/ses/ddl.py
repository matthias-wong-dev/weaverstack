"""Generated create DDL — the *build* form of an SES source.

Build creates structure; it does not load data. So the create definition for a
source is pure structure: a table becomes an empty table of the right shape, a
view becomes ``CREATE OR REPLACE VIEW`` over its query body. Nothing here runs an
object's ``read()`` or reads a row — populating a table is *load*, a separate
phase, and the repository is read once to freeze a bundle, never again.

The source is the right place for this because it alone knows its language,
object kind, ID and validated body/schema. A build planner calls
:meth:`SourceDocument.create_ddl` and never re-derives create syntax.

Two invariants hold:

- **deterministic** — the same validated source and format version always produce
  the same :class:`GeneratedDdl`.
- **path-free** — a table or view is addressed by its two-part ``Schema.Object``
  name and binds through the catalog the installer sets up; no Lakehouse,
  workspace or filesystem path is baked into a payload.

Schema is **declared or inferred** (build-philosophy §7). A Python-backed Delta
table has no query and must declare its schema; the generated DDL is a concrete
``CREATE OR REPLACE TABLE`` over the declared columns. A Spark SQL table has a
query, so it may declare its schema or leave it to be inferred at build — either
way the shape is only settled by running the query in the target session, so its
payload is not finished SQL but a deterministic instruction the ``spark_table``
executor completes in one self-contained install action (build-philosophy §7.3).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING

from .columns import metadata_column_references
from .metadata import SPARK_SQL, SQL, TABLE, VIEW

if TYPE_CHECKING:
    from .source import SourceDocument

#: The bundle format version this generator targets. A change to the generated
#: shape is a change to this number.
BUILD_FORMAT_VERSION = 1

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


def generate_ddl(document: "SourceDocument") -> GeneratedDdl:
    """The installable create definition for one validated source.

    Folders have no create DDL — a Folder is a directory, created by the
    installer rather than by a statement — so this is never called for one.
    """

    if document.language == SQL:
        return _tsql_ddl(document)
    if document.kind == TABLE:
        if document.language == SPARK_SQL:
            return _spark_table_ddl(document)
        return _python_table_ddl(document)
    if document.kind == VIEW:
        return _view_ddl(document)
    raise NotImplementedError(
        f"{document.relative_path}: a {document.kind} has no create DDL"
    )


def _tsql_ddl(document: "SourceDocument") -> GeneratedDdl:
    """A Warehouse object's build: a self-contained T-SQL script.

    A table materialises and inspects its own query shape server-side and creates
    only its main table; a view is a ``CREATE OR ALTER VIEW`` over its body.
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


def _python_table_ddl(document: "SourceDocument") -> GeneratedDdl:
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
    content = _create_table_sql(document.qualified, columns)
    return GeneratedDdl(
        executor=SPARK_SQL_EXECUTOR, content=content, extension=SPARK_SQL_EXTENSION
    )


def _spark_table_ddl(document: "SourceDocument") -> GeneratedDdl:
    """A Spark SQL table's deferred, deterministic build instruction.

    Declared or inferred, its shape is only settled by running the query in the
    session, so the payload is not finished SQL. It is a JSON instruction the
    ``spark_table`` executor completes in one self-contained action: run the
    query, read the ``DataFrame`` schema, validate the columns (the same guards a
    declared schema passes at parse), choose the physical business columns,
    append the audit columns, and create the table. Everything the executor needs
    is frozen here, so it never reopens the SES source (build-philosophy §7.3).
    """

    ses = document.document
    declared = ses.has_declared_schema
    payload = {
        "object": document.qualified,
        "schema_mode": "declared" if declared else "inferred",
        "declared_columns": (
            [_column_entry(column) for column in ses.schema] if declared else None
        ),
        "source_query": (document.sql_body or "").strip(),
        "references": [list(pair) for pair in metadata_column_references(ses)],
        "identity": ses.identity,
        "audit_columns": [_column_entry(column) for column in ses.audit_columns],
        "column_mapping": True,
    }
    content = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    return GeneratedDdl(
        executor=SPARK_TABLE_EXECUTOR, content=content, extension=SPARK_TABLE_EXTENSION
    )


def _view_ddl(document: "SourceDocument") -> GeneratedDdl:
    """A persistent view over the validated query body, body otherwise untouched."""

    body = (document.sql_body or "").rstrip()
    content = f"CREATE OR REPLACE VIEW {document.qualified} AS\n{body}\n"
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
    """A ``CREATE OR REPLACE TABLE`` over concrete columns.

    ``OR REPLACE`` so a rebuild is idempotent: build owns structure, and a table
    carries no build-phase data to protect (populating it is load).
    """

    column_lines = ",\n".join(
        f"    {_ident(c.name)} {c.type}{' NOT NULL' if c.not_null else ''}"
        for c in columns
    )
    return (
        f"CREATE OR REPLACE TABLE {qualified} (\n"
        f"{column_lines}\n"
        ")\n"
        "USING delta\n"
        f"{_COLUMN_MAPPING}\n"
    )


def _ident(name: str) -> str:
    """Back-tick quote a column identifier so spaces and keywords are safe."""

    return "`" + name.replace("`", "``") + "`"
