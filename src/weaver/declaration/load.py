"""Generated load definitions — the *load* form of a Weaver document source.

The sibling of :mod:`weaver.declaration.ddl`, and the division between them is
the one Weaver is built on: build creates structure, load puts rows in it. A
source knows how to produce both, because it alone holds its language, kind, ID
and validated body — and neither generator ever reopens a repository.

.. code-block:: text

    SourceDocument
        knows the parsed contract, source body and language
        creates the executable load definition

    LoadArtefact
        carries the completed payload into claiming, planning,
        installation, registration and pruning

:class:`weaver.etl.LoadArtefact` remains the lifecycle object and does not
generate itself. It asks for a payload and carries what it gets, which is why
replacing what this module returns moves exactly the artefacts whose bytes
changed and nothing else.

Which sources own a load, and in what form:

.. code-block:: text

    Warehouse table (T-SQL)     an installer script for [_].[Load S.N]
    Lakehouse table (Spark SQL) a runnable Spark SQL program
    Lakehouse table (Python)    the authored module itself
    Folder (Python)             the authored module itself

The two Python forms are not generated at all, and that is the honest answer
rather than a gap: the author's module *is* the executable artefact, so it is
deployed verbatim and signed by its own bytes. A view owns no load — its
definition is its query, so there is nothing to run.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from .metadata import SPARK_SQL, SQL, TABLE

if TYPE_CHECKING:
    from .source import SourceDocument

#: The T-SQL load generator's version, and the Spark SQL one's. Separate because
#: the two evolve independently: a change to the Spark DML has no bearing on what
#: a Warehouse procedure should contain, and bumping one must not invalidate the
#: other's artefacts. Each is a *signature salt*, never part of an identity.
#:
#: **Raise one whenever its generated output changes.** A signature is the
#: source's plus this number, so an edit to a generator that leaves both alone
#: produces different bytes with an unchanged signature — and incremental
#: selection, correctly, rebuilds nothing. The estate then keeps running the
#: previous generation's artefacts, which is the failure this exists to prevent
#: and which cost a Fabric round trip to notice.
TSQL_LOAD_VERSION = 5
SPARK_LOAD_VERSION = 7

#: What object a generated load installs, in the catalogue's vocabulary. A
#: Warehouse load is a stored procedure; a Lakehouse load is a file in the
#: deployed runtime tree.
PROCEDURE_OBJECT = "stored_procedure"
FILE_OBJECT = "file"

TSQL_LOAD_EXTENSION = ".sql"
SPARK_LOAD_EXTENSION = ".spark.sql"


@dataclass(frozen=True)
class GeneratedLoad:
    """One source's generated, installable load definition.

    ``template_version`` is the generator's version, carried out so the artefact
    layer can salt a signature with it without knowing which generator ran. That
    is what makes a change to load generation rebuild exactly the loads it
    changed, and leave deployed Python — signed by its own bytes — alone.
    """

    object_type: str
    payload: bytes
    template_version: int
    extension: str


def generate_load(document: "SourceDocument") -> GeneratedLoad:
    """The installable load definition for one validated source.

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
        return _spark_load(document)
    raise NotImplementedError(
        f"{document.relative_path}: a {document.language} table's load is its "
        "authored module, which is deployed rather than generated"
    )


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


def _spark_load(document: "SourceDocument") -> GeneratedLoad:
    from .spark_load import generate_spark_load_instruction

    # An instruction, not the program: the columns a load writes are the built
    # table's, and a Spark SQL table may infer its schema at build. The
    # installer reads them and renders the file into place — the same two-phase
    # shape the Warehouse load uses with sys.columns.
    content = generate_spark_load_instruction(
        document.document, document.sql_body or ""
    )
    return GeneratedLoad(
        object_type=FILE_OBJECT,
        payload=content.encode("utf-8"),
        template_version=SPARK_LOAD_VERSION,
        extension=SPARK_LOAD_EXTENSION,
    )


__all__ = [
    "FILE_OBJECT",
    "PROCEDURE_OBJECT",
    "SPARK_LOAD_VERSION",
    "TSQL_LOAD_VERSION",
    "GeneratedLoad",
    "generate_load",
    "has_generated_load",
]
