"""Generate runnable Test and Assumption definitions.

T-SQL validations become procedures, Spark SQL validations become Python
modules, and Python validations use their authored module directly.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from .load import FILE_OBJECT, PROCEDURE_OBJECT
from .metadata import PYTHON, SPARK_SQL, SQL

if TYPE_CHECKING:
    from .source import SourceDocument

#: The validation generators' versions, separate for the reason the load
#: generators' are: a change to the rendered Warehouse procedure has no bearing
#: on what a Spark module should contain, and bumping one must not invalidate
#: the other's artefacts. Each is a *signature salt*, never part of an identity.
#:
#: **Raise one whenever its generated output changes**, or an edit to a
#: generator produces different bytes with an unchanged signature — and
#: incremental selection, correctly, rebuilds nothing, leaving the estate
#: running the previous generation's primitives.
SPARK_VALIDATION_VERSION = 1
TSQL_VALIDATION_VERSION = 1

SPARK_VALIDATION_EXTENSION = ".py"
TSQL_VALIDATION_EXTENSION = ".sql"


@dataclass(frozen=True)
class GeneratedValidation:
    """One declaration's generated validation payload."""

    object_type: str
    payload: bytes
    template_version: int
    extension: str


def generate_validation(document: "SourceDocument", *, destination=None) -> GeneratedValidation:
    """The installable primitive for one validated validation declaration.

    :func:`has_generated_validation` is the question to ask first — a Python
    validation is deployed verbatim and does not reach here.
    """

    if not document.is_validation:
        raise NotImplementedError(
            f"{document.relative_path}: a {document.kind} is not a validation"
        )
    if document.language == SPARK_SQL:
        return _spark_validation(document, destination)
    if document.language == SQL:
        return _tsql_validation(document)
    raise NotImplementedError(
        f"{document.relative_path}: a {document.language} validation is its "
        "authored module, which is deployed rather than generated"
    )


def validation_identity(document: "SourceDocument") -> tuple[str, int]:
    """One generated validation's object type and template version, unrendered.

    The sibling of :func:`weaver.declaration.load.load_identity`, and there for
    the same reason: identity and signature are destination-free.
    """

    if document.language == SQL:
        return PROCEDURE_OBJECT, TSQL_VALIDATION_VERSION
    return FILE_OBJECT, SPARK_VALIDATION_VERSION


def has_generated_validation(document: "SourceDocument") -> bool:
    """Whether this validation is compiled rather than deployed verbatim."""

    return document.is_validation and document.language != PYTHON


def _spark_validation(document: "SourceDocument", destination) -> GeneratedValidation:
    from .metadata import extract_sql_metadata_and_body
    from .spark_sql_module import addressed, render_spark_sql_module

    header, _body = extract_sql_metadata_and_body(document.text)
    content = render_spark_sql_module(
        document.document,
        header=header,
        body=addressed((document.sql_body or "").strip(), destination),
        source_name=document.relative_path.rpartition("/")[2],
    )
    return GeneratedValidation(
        object_type=FILE_OBJECT,
        payload=content.encode("utf-8"),
        template_version=SPARK_VALIDATION_VERSION,
        extension=SPARK_VALIDATION_EXTENSION,
    )


def _tsql_validation(document: "SourceDocument") -> GeneratedValidation:
    from ..etl import validation_procedure_name
    from .tsql_validation import generate_tsql_validation_script

    content = generate_tsql_validation_script(
        document.document,
        document.sql_body or "",
        procedure_name=validation_procedure_name(
            document.document.kind, document.object_id
        ),
    )
    return GeneratedValidation(
        object_type=PROCEDURE_OBJECT,
        payload=content.encode("utf-8"),
        template_version=TSQL_VALIDATION_VERSION,
        extension=TSQL_VALIDATION_EXTENSION,
    )


__all__ = [
    "SPARK_VALIDATION_VERSION",
    "TSQL_VALIDATION_VERSION",
    "GeneratedValidation",
    "generate_validation",
    "has_generated_validation",
]
