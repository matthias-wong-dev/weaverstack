"""Generated create DDL — the *build* form of an SES source.

Build creates structure; it does not load data. So the create definition for a
source is pure DDL: a Delta table becomes ``CREATE TABLE`` over its **declared**
schema, a view becomes ``CREATE OR REPLACE VIEW`` over its query body. Nothing
here runs an object's ``read()`` or reads a row — populating a table is *load*, a
separate phase, and the repository is read once to freeze a bundle, never again.

The source is the right place for this because it alone knows its language,
object kind, ID and validated body/schema. A build planner calls
:meth:`SourceDocument.create_ddl` and never re-derives create syntax.

Two invariants hold:

- **deterministic** — the same validated source and format version always produce
  the same :class:`GeneratedDdl`.
- **path-free** — a table or view is addressed by its two-part ``Schema.Object``
  name and binds through the catalog the installer sets up; no Lakehouse,
  workspace or filesystem path is baked into a payload. The one physical path a
  build needs — a schema's storage location — is supplied by the planner in the
  schema-create action, not here.

Schema is always **declared**, never inferred. Inferring a Delta schema from a
CSV or spreadsheet is too risky to do silently, so a Delta table without a
declared ``Schema`` is already refused by the reader; there is no inference path.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from .metadata import SPARK_SQL, SQL, TABLE, VIEW

if TYPE_CHECKING:
    from .source import SourceDocument

#: The bundle format version this generator targets. A change to the generated
#: shape is a change to this number.
BUILD_FORMAT_VERSION = 1

#: The one executor a create DDL runs through. It names a runtime dispatch key,
#: not an engine — a Fabric Spark session and a local one are both ``spark_sql``.
SPARK_SQL_EXECUTOR = "spark_sql"
SPARK_SQL_EXTENSION = ".spark.sql"

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
    """The installable create DDL for one validated source.

    Folders have no create DDL — a Folder is a directory, created by the
    installer rather than by a statement — so this is never called for one.
    """

    if document.language == SQL:
        raise NotImplementedError(
            "T-SQL executable generation is not supported by build bundle v1"
        )
    if document.kind == TABLE:
        return _table_ddl(document)
    if document.kind == VIEW:
        return _view_ddl(document)
    raise NotImplementedError(
        f"{document.relative_path}: a {document.kind} has no create DDL"
    )


def _table_ddl(document: "SourceDocument") -> GeneratedDdl:
    """A Delta table from its declared columns — Python and Spark SQL alike.

    Both declare their schema (the reader requires it for a Delta table), so both
    build the same way: an empty table of the declared shape. A Spark SQL table's
    query body is a *load* concern and is not consulted here.
    """

    columns = document.document.schema
    if not columns:  # pragma: no cover - the reader requires a declared schema
        raise NotImplementedError(
            f"{document.relative_path}: a Delta table must declare its schema; "
            "schema inference is not supported"
        )
    column_lines = ",\n".join(f"    {_ident(c.name)} {c.type}" for c in columns)
    # OR REPLACE so a rebuild is idempotent: build owns structure, and a table
    # carries no build-phase data to protect (populating it is load).
    content = (
        f"CREATE OR REPLACE TABLE {document.qualified} (\n"
        f"{column_lines}\n"
        ")\n"
        "USING delta\n"
        f"{_COLUMN_MAPPING}\n"
    )
    return GeneratedDdl(
        executor=SPARK_SQL_EXECUTOR, content=content, extension=SPARK_SQL_EXTENSION
    )


def _view_ddl(document: "SourceDocument") -> GeneratedDdl:
    """A persistent view over the validated query body, body otherwise untouched."""

    body = (document.sql_body or "").rstrip()
    content = f"CREATE OR REPLACE VIEW {document.qualified} AS\n{body}\n"
    return GeneratedDdl(
        executor=SPARK_SQL_EXECUTOR, content=content, extension=SPARK_SQL_EXTENSION
    )


def _ident(name: str) -> str:
    """Back-tick quote a column identifier so spaces and keywords are safe."""

    return "`" + name.replace("`", "``") + "`"
