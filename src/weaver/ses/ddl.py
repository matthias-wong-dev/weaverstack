"""Generated executable definitions — the *create* form of an SES source.

A :class:`~weaver.ses.source.SourceDocument` holds a validated body: a Python
class whose ``read()`` proposes rows, or a Spark SQL query. Neither is directly
installable — the author writes the query, and Weaver writes the ``CREATE``.
:func:`generate_ddl` is where that wrapper is written.

The source is the right place for this because it is the only thing that knows
its own language, object kind, ID and validated body. A build planner should
call :meth:`SourceDocument.create_ddl` and never re-derive create syntax.

Despite the name, the *content* may be Spark SQL, T-SQL or Python — ``create_ddl``
means "the generated create/materialisation definition", whatever engine runs it.
Two invariants hold:

- **deterministic.** The same validated source and bundle format version always
  produce the same :class:`GeneratedDdl`. No timestamps, no physical paths — a
  path is bound by the installer, not baked into the payload.
- **path-free.** Spark SQL addresses objects by their two-part ``Schema.Object``
  name, which binds through the catalog the installer registers; a Python payload
  imports the object module and hands the class to the runtime, which supplies the
  bound target. Neither embeds a Lakehouse, workspace or filesystem path.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from .metadata import PYTHON, SPARK_SQL, SQL, TABLE, VIEW

if TYPE_CHECKING:
    from .source import SourceDocument

#: The bundle format version this generator targets. Part of the determinism
#: contract: a change to the generated shape is a change to this number.
BUILD_FORMAT_VERSION = 1

#: Executor names. These name a runtime dispatch key, not an engine — a Fabric
#: Spark session and a local Spark session are both ``spark_sql``.
PYTHON_EXECUTOR = "python"
SPARK_SQL_EXECUTOR = "spark_sql"

PYTHON_EXTENSION = ".py"
SPARK_SQL_EXTENSION = ".spark.sql"


@dataclass(frozen=True)
class GeneratedDdl:
    """One source's generated, installable create definition.

    ``executor`` selects the runtime that will run ``content``; ``extension`` is
    the payload file suffix a bundle writes it under, chosen to match.
    """

    executor: str
    content: str
    extension: str


def generate_ddl(document: "SourceDocument") -> GeneratedDdl:
    """The installable create definition for one validated source."""

    if document.language == SQL:
        raise NotImplementedError(
            "T-SQL executable generation is not supported by build bundle v1"
        )
    if document.language == SPARK_SQL:
        return _spark_sql_ddl(document)
    if document.language == PYTHON:
        return _python_ddl(document)
    raise NotImplementedError(
        f"no build bundle v1 generator for language {document.language!r}"
    )


def _spark_sql_ddl(document: "SourceDocument") -> GeneratedDdl:
    """Wrap a Spark SQL body in its create statement, body otherwise untouched.

    A View becomes ``CREATE OR REPLACE VIEW``; a Table materialises Delta and
    registers under its two-part name so a later Spark reader binds it by name,
    the way a declared table does in a Fabric Lakehouse. Column mapping is on so
    the audit columns' spaced logical names survive.
    """

    body = (document.sql_body or "").rstrip()
    name = document.qualified

    if document.kind == VIEW:
        content = f"CREATE OR REPLACE VIEW {name} AS\n{body}\n"
    elif document.kind == TABLE:
        content = (
            f"CREATE OR REPLACE TABLE {name}\n"
            "USING delta\n"
            "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')\n"
            f"AS\n{body}\n"
        )
    else:  # pragma: no cover - a Folder is never Spark SQL; read() refuses it.
        raise NotImplementedError(
            f"Spark SQL {document.kind} is not a build bundle v1 object"
        )

    return GeneratedDdl(
        executor=SPARK_SQL_EXECUTOR,
        content=content,
        extension=SPARK_SQL_EXTENSION,
    )


def _python_ddl(document: "SourceDocument") -> GeneratedDdl:
    """A small wrapper that imports the object and hands it to the runtime.

    The wrapper is deterministic in the object's module and class names alone;
    everything physical — the Spark session, the resolved target, the store — is
    the installer's ambient context, which ``materialise`` reads. This keeps the
    payload independent of where the bundle is installed, and lets the object's
    own support imports resolve against the certified snapshot on the path.
    """

    module = document.module_name
    class_name = document.class_name
    if module is None or class_name is None:  # pragma: no cover - guaranteed by read()
        raise NotImplementedError(
            f"{document.relative_path}: a Python object needs a module and class to "
            "generate a payload"
        )

    content = (
        f'"""Weaver build payload — {document.node_id}. Generated; do not edit."""\n'
        f"from {module} import {class_name}\n"
        "\n"
        "from weaver.build_bundle.runtime import materialise\n"
        "\n"
        f"materialise({class_name})\n"
    )
    return GeneratedDdl(
        executor=PYTHON_EXECUTOR,
        content=content,
        extension=PYTHON_EXTENSION,
    )
