"""Generate load definitions from validated Weaver document sources.

Warehouse tables generate installer scripts; Spark SQL tables generate Python
modules; Python tables and folders use their authored modules.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from .metadata import SPARK_SQL, SQL, TABLE

if TYPE_CHECKING:
    from .source import SourceDocument

#: Signature salts for generated load output. Increment the corresponding value
#: whenever that generator changes.
TSQL_LOAD_VERSION = 8
SPARK_LOAD_VERSION = 9

#: What object a generated load installs, in the catalogue's vocabulary. A
#: Warehouse load is a stored procedure; a Lakehouse load is a file in the
#: deployed runtime tree.
PROCEDURE_OBJECT = "stored_procedure"
FILE_OBJECT = "file"

TSQL_LOAD_EXTENSION = ".sql"
#: A Spark SQL table's load is a deployed Python module, so it is spelled as one.
SPARK_LOAD_EXTENSION = ".py"


@dataclass(frozen=True)
class GeneratedLoad:
    """One source's generated load payload — installable, not yet executable.

    ``payload`` is what the bundle carries and what the installer is handed. It
    is deliberately *not* a finished program: a Warehouse load is a script that
    assembles the procedure server-side, and a Spark SQL load is an instruction
    the executor renders once it can see the built table. Calling it a completed
    executable definition would misdescribe both, and invite a reader to write
    the file down unchanged.

    ``template_version`` is the generator's version, carried out so the artefact
    layer can salt a signature with it without knowing which generator ran. That
    is what makes a change to load generation rebuild exactly the loads it
    changed, and leave deployed Python — signed by its own bytes — alone.
    """

    object_type: str
    payload: bytes
    template_version: int
    extension: str


def generate_load(document: "SourceDocument", *, destination=None) -> GeneratedLoad:
    """The installable load payload for one validated source.

    Only a table has one. A Folder's load is its authored module and a View has
    no load at all, so neither reaches here — :func:`has_generated_load` is the
    question to ask first.
    """

    if document.kind != TABLE:
        raise NotImplementedError(
            f"{document.relative_path}: a {document.kind} has no generated load"
        )
    if document.language == SQL:
        return _tsql_load(document)
    if document.language == SPARK_SQL:
        return _spark_load(document, destination)
    raise NotImplementedError(
        f"{document.relative_path}: a {document.language} table's load is its "
        "authored module, which is deployed rather than generated"
    )


def load_identity(document: "SourceDocument") -> tuple[str, int]:
    """One generated load's object type and template version, without rendering.

    What an artefact *is* does not depend on where it is bound, so a caller
    listing identities and signatures asks this instead of generating a payload
    it would throw away — and, for a Spark load, could not render at all
    without a destination.
    """

    if document.language == SQL:
        return PROCEDURE_OBJECT, TSQL_LOAD_VERSION
    return FILE_OBJECT, SPARK_LOAD_VERSION


def has_generated_load(document: "SourceDocument") -> bool:
    """Whether this source's load is generated rather than deployed verbatim."""

    return document.kind == TABLE and document.language in (SQL, SPARK_SQL)


def _tsql_load(document: "SourceDocument") -> GeneratedLoad:
    from ..etl import load_procedure_name
    from .tsql_load import generate_tsql_load_script

    content = generate_tsql_load_script(
        document.document,
        document.sql_body or "",
        procedure_name=load_procedure_name(document.object_id),
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

    # The finished module, not an instruction. Nothing here needs the built
    # table: the primitive reads its own contract from the docstring and its own
    # columns from the target when it runs, which is the same question answered
    # at the same place a Python-authored table answers it.
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
