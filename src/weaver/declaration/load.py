"""Generate load definitions from validated sources."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from .metadata import SPARK_SQL, SQL, TABLE

if TYPE_CHECKING:
    from .source import SourceDocument

#: Increment the relevant salt whenever generated load output changes.
TSQL_LOAD_VERSION = 17
SPARK_LOAD_VERSION = 9

#: Generated Warehouse loads are procedures; Lakehouse loads are deployed files.
PROCEDURE_OBJECT = "stored_procedure"
FILE_OBJECT = "file"

TSQL_LOAD_EXTENSION = ".sql"
SPARK_LOAD_EXTENSION = ".py"


@dataclass(frozen=True)
class GeneratedLoad:
    """An installable load payload and the generator version that signs it.

    The payload is passed to the installer, not executed directly. The template
    version lets generated output change its signature independently of authored
    Python.
    """

    object_type: str
    payload: bytes
    template_version: int
    extension: str


def generate_load(
    document: "SourceDocument", *, destination=None, item=None
) -> GeneratedLoad:
    """Generate the installable load payload for a table.

    Call :func:`has_generated_load` before this function.
    """

    if document.kind != TABLE:
        raise NotImplementedError(
            f"{document.relative_path}: a {document.kind} has no generated load"
        )
    if document.language == SQL:
        return _tsql_load(document, item)
    if document.language == SPARK_SQL:
        return _spark_load(document, destination)
    raise NotImplementedError(
        f"{document.relative_path}: a {document.language} table's load is its "
        "authored module, which is deployed rather than generated"
    )


def load_identity(document: "SourceDocument") -> tuple[str, int]:
    if document.language == SQL:
        return PROCEDURE_OBJECT, TSQL_LOAD_VERSION
    return FILE_OBJECT, SPARK_LOAD_VERSION


def has_generated_load(document: "SourceDocument") -> bool:
    if not document.document.has_load_procedure:
        return False
    return document.kind == TABLE and document.language in (SQL, SPARK_SQL)


def _tsql_load(document: "SourceDocument", item) -> GeneratedLoad:
    from ..etl import load_procedure_name
    from .tsql_load import generate_tsql_load_script

    content = generate_tsql_load_script(
        document.document,
        document.sql_body or "",
        procedure_name=load_procedure_name(document.object_id),
        item=item,
    )
    return GeneratedLoad(
        object_type=PROCEDURE_OBJECT,
        payload=content.encode("utf-8"),
        template_version=TSQL_LOAD_VERSION,
        extension=TSQL_LOAD_EXTENSION,
    )


def _spark_load(document: "SourceDocument", destination) -> GeneratedLoad:
    from .metadata import extract_sql_metadata_and_body
    from .spark_sql_module import addressed, render_spark_sql_module

    # The primitive reads its contract and target columns when it runs. This
    # payload is therefore the finished module.
    header, _body = extract_sql_metadata_and_body(document.text)
    content = render_spark_sql_module(
        document.document,
        header=header,
        body=addressed((document.sql_body or "").strip(), destination),
        source_name=document.relative_path.rpartition("/")[2],
    )
    return GeneratedLoad(
        object_type=FILE_OBJECT,
        payload=content.encode("utf-8"),
        template_version=SPARK_LOAD_VERSION,
        extension=SPARK_LOAD_EXTENSION,
    )


__all__ = [
    "FILE_OBJECT",
    "load_identity",
    "PROCEDURE_OBJECT",
    "SPARK_LOAD_VERSION",
    "TSQL_LOAD_VERSION",
    "GeneratedLoad",
    "generate_load",
    "has_generated_load",
]
