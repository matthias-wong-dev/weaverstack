"""What a repository's load layer owns, derived from the source alone.

A load artefact is not a side effect of building an object. It is a target in its
own right — claimed during interpretation, registered in the catalogue, signed,
selected incrementally, built by a physical action and pruned when its source
stops declaring it. This module answers the one question the rest of the build
asks about them: *given this repository, which load artefacts exist, where do
they go, and what is each one's signature.*

Three artefacts, from three kinds of source:

.. code-block:: text

    Warehouse/Reporting/Sales__Customer.sql   -> _.[Load Sales.Customer]
    Lakehouse/Sales/lib/dates.py              -> Files/_/Load/lib/dates.py
    Lakehouse/Sales/Sales__Customer.sql       -> Files/_/Load/Sales__Customer.sql

Views produce nothing on either side. A view's definition *is* its query, so
there is no work to schedule and nothing for a load to do.

**Nothing here inspects a target.** The current repository contents determine the
complete set of claims, which is what lets a deleted or renamed source produce an
ordinary prune through catalogue reconciliation rather than needing a scan to
notice its artefact has been orphaned.

**The generated bodies are proxies.** T-SQL and Spark SQL loads carry
deterministic placeholder statements, because this establishes the lifecycle and
not the ETL. Their signatures already include a template version, so replacing a
proxy with real generation later invalidates exactly the artefacts it changes and
nothing else.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Iterable, Mapping

from .declaration.metadata import PYTHON, SPARK_SQL, TABLE, ObjectId
from .declaration.model import (
    FILE_SHAPE,
    PROCEDURE_SHAPE,
    WAREHOUSE,
    WeaverDocumentId,
    WeaverItemId,
    WeaverRepository,
)
from .declaration.source import content_hash

#: The version of the generated Spark SQL load body, and of the generated T-SQL
#: one. They are separate because the two generators evolve independently: a
#: change to the Spark DML has no bearing on what a Warehouse procedure should
#: contain, and bumping one should not invalidate the other's artefacts.
#:
#: Each is a *signature salt*, never part of an identity. Raising one rebuilds
#: every artefact it renders and leaves everything else — including deployed
#: Python, which is signed by its own bytes — untouched. It does not reach
#: ``repository.signature`` either: that describes authored content, and the
#: renderer's version is not something an author wrote.
SPARK_ETL_TEMPLATE_VERSION = 1
TSQL_ETL_TEMPLATE_VERSION = 1

#: Where generated infrastructure lives, in both physical forms. The Warehouse
#: gets a schema named ``_`` holding the load procedures; the Lakehouse gets a
#: managed folder ``Files/_/Load`` holding the deployed runtime tree.
#:
#: Same name, same principle, different physical object — and neither is a
#: reserved word. Both are projected as ordinary managed objects while the item
#: has load artefacts, so the ordinary inventory, keep-set and prune machinery
#: gives them their whole lifecycle, including removal once the last artefact
#: goes.
ETL_SCHEMA = "_"
LOAD_FOLDER = "Load"

#: The deployed runtime tree, relative to a Lakehouse's ``Files`` area. It is
#: also the Python import root the orchestrator will execute with, which is why
#: the authored tree is reproduced beneath it verbatim: ``from lib.dates import
#: parse_date`` keeps working because ``lib`` sits exactly where it was authored.
LOAD_ROOT = f"{ETL_SCHEMA}/{LOAD_FOLDER}"

#: What a generated load procedure is called: the object it loads, spelled out.
#: ``Load Sales.Customer`` is a real name in a real schema, not an encoding, so
#: the catalogue stores it exactly as the Warehouse holds it.
LOAD_PROCEDURE_PREFIX = "Load "

FILE_TYPE = "file"
PROCEDURE_TYPE = "stored_procedure"

PYTHON_SUFFIX = ".py"


@dataclass(frozen=True)
class LoadArtefact:
    """One load target, complete: what it is, where it goes, what it contains.

    Everything a build needs about a load artefact and nothing about how it was
    reached. The identity is the catalogue key; the signature is what incremental
    selection compares; the payload is the frozen bytes the installer writes, so
    an artefact carries its own content rather than sending the installer back to
    a repository it must never reopen.

    ``origin`` is the declaration this artefact was derived from, where there was
    one. A deployed helper module under ``lib/`` has none: it is authored source
    that no Weaver document declares, which is exactly why it needs a claim of
    its own or nothing would ever notice it had been deleted.
    """

    identity: WeaverDocumentId
    object_type: str
    signature: str
    payload: bytes
    origin: WeaverDocumentId | None = None

    @property
    def is_file(self) -> bool:
        return self.object_type == FILE_TYPE

    @property
    def target_path(self) -> str:
        """Where a file artefact lands, relative to the Lakehouse ``Files`` area."""

        if not self.is_file:
            raise ValueError(f"{self.identity} is not a file and has no path")
        return f"{self.identity.object_id.schema}/{self.identity.object_id.object}"


def load_artefacts(repository: WeaverRepository) -> tuple[LoadArtefact, ...]:
    """Every load artefact the whole repository claims, in identity order."""

    artefacts: list[LoadArtefact] = []
    for model in repository.items:
        artefacts.extend(item_load_artefacts(repository, item=model.identity))
    return tuple(sorted(artefacts, key=lambda artefact: str(artefact.identity)))


def item_load_artefacts(
    repository: WeaverRepository, *, item: WeaverItemId
) -> tuple[LoadArtefact, ...]:
    """One item's load artefacts, derived from what it declares.

    The built-in ``Lakehouse/_weaver`` owns none, and is excluded here rather
    than filtered out downstream. It is Weaver's own control plane — the tables
    that record what was installed — not a user ETL package, and letting its
    claims through planning only to suppress them later would leave the question
    "does the catalogue have a load layer?" answered in two places.
    """

    if _is_builtin(item):
        return ()
    if item.item_type == WAREHOUSE:
        return _warehouse_artefacts(repository, item=item)
    return _lakehouse_artefacts(repository, item=item)


def _warehouse_artefacts(
    repository: WeaverRepository, *, item: WeaverItemId
) -> tuple[LoadArtefact, ...]:
    """One generated load procedure per Warehouse table."""

    artefacts = []
    for identity, source in sorted(repository.source_documents.items(), key=_by_text):
        if identity.item != item or source.kind != TABLE:
            continue
        procedure = WeaverDocumentId(
            item,
            ObjectId(
                schema=ETL_SCHEMA,
                object=f"{LOAD_PROCEDURE_PREFIX}{identity.object_id.qualified}",
            ),
            shape=PROCEDURE_SHAPE,
        )
        body = render_load_procedure(procedure, identity)
        artefacts.append(
            LoadArtefact(
                identity=procedure,
                object_type=PROCEDURE_TYPE,
                signature=_salted(source.effective_signature, TSQL_ETL_TEMPLATE_VERSION),
                payload=body.encode("utf-8"),
                origin=identity,
            )
        )
    return tuple(artefacts)


def _lakehouse_artefacts(
    repository: WeaverRepository, *, item: WeaverItemId
) -> tuple[LoadArtefact, ...]:
    """The deployed Python tree, plus one generated file per Spark SQL table."""

    artefacts = []
    for identity, source in sorted(repository.source_documents.items(), key=_by_text):
        if identity.item != item or source.relative_path in repository.generated_files:
            continue
        relative = _within_item(source.relative_path, item)
        if source.language == PYTHON:
            # A Python document authors a structural object *and* is runtime
            # source. Both are true and they are separate targets: the table it
            # declares, and the module a load will import.
            artefacts.append(
                _file_artefact(
                    item,
                    relative,
                    payload=source.text.encode("utf-8"),
                    signature=content_hash(source.text.encode("utf-8")),
                    origin=identity,
                )
            )
        elif source.language == SPARK_SQL and source.kind == TABLE:
            body = render_load_statement(identity)
            artefacts.append(
                _file_artefact(
                    item,
                    relative,
                    payload=body.encode("utf-8"),
                    signature=_salted(
                        source.effective_signature, SPARK_ETL_TEMPLATE_VERSION
                    ),
                    origin=identity,
                )
            )
    for relative, content in sorted(repository.support_file_contents.items()):
        if not relative.endswith(PYTHON_SUFFIX):
            continue
        parts = relative.split("/")
        if len(parts) < 3 or WeaverItemId(parts[0], parts[1]) != item:
            continue
        artefacts.append(
            _file_artefact(
                item,
                _within_item(relative, item),
                payload=content,
                signature=content_hash(content),
            )
        )
    return tuple(artefacts)


def _file_artefact(
    item: WeaverItemId,
    relative: str,
    *,
    payload: bytes,
    signature: str,
    origin: WeaverDocumentId | None = None,
) -> LoadArtefact:
    """One deployed file, at the item-relative path reproduced under the root.

    The authored path is preserved whole, ``Files/`` segment included. That
    segment is not noise: ``Sales__Customer.py`` at the item root and
    ``Files/Sales__Customer.py`` are legitimately different documents, and
    flattening them would deploy two files to one path.
    """

    path = f"{LOAD_ROOT}/{relative}"
    directory, _, name = path.rpartition("/")
    return LoadArtefact(
        identity=WeaverDocumentId(
            item, ObjectId(schema=directory, object=name), shape=FILE_SHAPE
        ),
        object_type=FILE_TYPE,
        signature=signature,
        payload=payload,
        origin=origin,
    )


def render_load_procedure(
    procedure: WeaverDocumentId, source: WeaverDocumentId
) -> str:
    """The T-SQL body of a generated load procedure — a proxy, for now.

    Deterministic, and shaped like what replaces it: one create-or-alter naming
    the real procedure, so the identity, the statement and the catalogue row all
    agree before any of the loading logic exists.
    """

    name = f"{_tsql_ident(procedure.object_id.schema)}.{_tsql_ident(procedure.object_id.object)}"
    return (
        f"create or alter procedure {name}\n"
        "as\n"
        "begin\n"
        "    set nocount on;\n"
        f"    /* Loads {source.object_id.qualified}. Generated by Weaver. */\n"
        "    return;\n"
        "end;\n"
    )


def render_load_statement(source: WeaverDocumentId) -> str:
    """The Spark SQL body of a generated load file — a proxy, for now."""

    return (
        f"-- Loads {source.object_id.qualified}. Generated by Weaver.\n"
        f"select 1 as weaver_load_placeholder\n"
        f" where 1 = 0\n"
    )


def _salted(signature: str, version: int) -> str:
    """A generated artefact's signature: what it is rendered from, and by what.

    Both halves have to be in it. The document alone would leave every generated
    body stale after the generator changed, with nothing to say so; the version
    alone would rebuild the estate whenever anything at all was edited.
    """

    digest = hashlib.sha256()
    digest.update(signature.encode("ascii"))
    digest.update(b"\0")
    digest.update(str(version).encode("ascii"))
    return digest.hexdigest()


def _within_item(relative: str, item: WeaverItemId) -> str:
    """``Lakehouse/Sales/lib/dates.py`` -> ``lib/dates.py``."""

    prefix = f"{item}/"
    if not relative.startswith(prefix):
        raise ValueError(f"{relative} does not belong to item {item}")
    return relative[len(prefix) :]


def _by_text(entry):
    return str(entry[0])


def _is_builtin(item: WeaverItemId) -> bool:
    from .catalogue.builtin import ITEM_ROOT

    return str(item) == ITEM_ROOT


def _tsql_ident(name: str) -> str:
    return "[" + name.replace("]", "]]") + "]"


#: The generated folder document that owns the deployed tree, and the schema
#: declaration it needs. ``_`` + ``__`` + ``Load`` spells ``___Load``, which the
#: parser reads as ``_.Load`` — see
#: :func:`weaver.declaration.source.python_id_parts`.
FOLDER_DOCUMENT = f"Files/{ETL_SCHEMA}{'__'}{LOAD_FOLDER}.py"
SCHEMA_DOCUMENT = f"schemas/{ETL_SCHEMA}.yml"

_FOLDER_CLASS = f"{ETL_SCHEMA}{'__'}{LOAD_FOLDER}"


def render_folder_document() -> str:
    """The declaration for ``Files/_/Load``, generated per item that needs one.

    An ordinary Folder document, deliberately. The deployed tree could have been
    a reserved path the prune was taught to skip, but then its removal would need
    a rule of its own; declared as a folder it is claimed, registered, inventoried
    and pruned by the machinery that already exists, and when the last load
    artefact goes the folder stops being projected and the whole subtree is
    removed by ordinary folder prune.

    ``Static: true`` because nothing loads *into* it — a build writes the runtime
    tree, and the files inside it are separately claimed objects of their own.
    ``Incremental: false`` for the same reason: a Folder accumulates by default,
    and nothing here accumulates, since each deployed file is replaced whole by
    the claim that owns it.
    """

    return f'''\
"""
Folder ID: {ETL_SCHEMA}.{LOAD_FOLDER}

Description: The runtime tree Weaver deploys this item's load code into.

Lineage: Generated by Weaver from the item's own authored source.

File key: "**/*"

Incremental: false

Static: true
"""
from weaver import Folder


class {_FOLDER_CLASS}(Folder):
    def read(self):
        return self.staging_folder(), []
'''


def render_schema_document() -> str:
    """The ``_`` schema declaration the generated folder document needs."""

    return (
        f"Schema ID: {ETL_SCHEMA}\n"
        "\n"
        "Description: Generated Weaver infrastructure — the runtime tree a "
        "Lakehouse item's load code is deployed into, and the schema a "
        "Warehouse item's generated load procedures live in.\n"
    )


def generated_item_files(
    item: WeaverItemId, *, documents: Iterable, support_paths: Iterable[str]
) -> dict[str, bytes]:
    """The generated declarations one item needs, or nothing if it needs none.

    Keyed by repository-relative path, so they compose with the built-in
    catalogue item's generated files and are read by the same static readers as
    authored content — no second parsing path, and no source mutated on disk.

    Takes the item's documents rather than a repository because it runs *during*
    interpretation: the folder document has to exist before the artefacts landing
    inside it can be claimed, since it is what owns the directory they land in.
    """

    if _is_builtin(item):
        return {}
    schema = {f"{item}/{SCHEMA_DOCUMENT}": render_schema_document().encode("utf-8")}
    if item.item_type == WAREHOUSE:
        # A Warehouse needs the schema its generated load procedures live in and
        # nothing else — there is no Files area, so no runtime tree and no folder
        # to own one.
        documents = tuple(documents)
        return schema if any(source.kind == TABLE for source in documents) else {}
    if not has_deployable_source(item, documents=documents, support_paths=support_paths):
        return {}
    return {
        **schema,
        f"{item}/{FOLDER_DOCUMENT}": render_folder_document().encode("utf-8"),
    }


def has_deployable_source(
    item: WeaverItemId, *, documents: Iterable, support_paths: Iterable[str]
) -> bool:
    """Whether this Lakehouse item has anything for a load layer to deploy."""

    for source in documents:
        if source.language == PYTHON:
            return True
        if source.language == SPARK_SQL and source.kind == TABLE:
            return True
    prefix = f"{item}/"
    return any(
        relative.startswith(prefix) and relative.endswith(PYTHON_SUFFIX)
        for relative in support_paths
    )


def load_schemas(artefacts: Iterable[LoadArtefact]) -> tuple[str, ...]:
    """The Warehouse schemas these artefacts need, which is ``_`` or nothing.

    Derived rather than assumed, so an item with no procedures asks for no schema
    and the ordinary schema prune can then remove one left behind.
    """

    return tuple(
        sorted(
            {
                artefact.identity.object_id.schema
                for artefact in artefacts
                if artefact.object_type == PROCEDURE_TYPE
            }
        )
    )


def load_artefacts_by_identity(
    artefacts: Iterable[LoadArtefact],
) -> Mapping[WeaverDocumentId, LoadArtefact]:
    return {artefact.identity: artefact for artefact in artefacts}


__all__ = [
    "ETL_SCHEMA",
    "FILE_TYPE",
    "LOAD_FOLDER",
    "LOAD_PROCEDURE_PREFIX",
    "LOAD_ROOT",
    "LoadArtefact",
    "PROCEDURE_TYPE",
    "SPARK_ETL_TEMPLATE_VERSION",
    "TSQL_ETL_TEMPLATE_VERSION",
    "item_load_artefacts",
    "load_artefacts",
    "load_artefacts_by_identity",
    "load_schemas",
    "render_load_procedure",
    "render_load_statement",
]
