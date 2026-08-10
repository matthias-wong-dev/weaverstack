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

**Nothing here generates a payload.** A T-SQL or Spark SQL load's bytes come
from :meth:`weaver.declaration.source.SourceDocument.create_load`, and deployed
Python is its own authored source. This module asks for a payload and carries
what it gets, so the question of *what a load does* stays with the generator
that knows the language, and the question of *which loads exist and how they are
signed* stays here. Each generated payload arrives with its generator's version,
which salts the signature — so a change to load generation invalidates exactly
the artefacts it changed, and leaves deployed Python untouched.
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

#: What a runtime artefact is *for*. Repeated from
#: :mod:`weaver.catalogue.tables` rather than imported, because the catalogue
#: package imports this one and the cycle would be real; the two are asserted
#: identical by ``tests/test_core_boundary.py``.
#:
#: The role is what a build carries and what the Registry keeps, and it is the
#: answer nothing may infer from a physical shape. A load module and a Test
#: module are both files; a load procedure and a Test procedure are both
#: procedures.
ROLE_LOAD = "load"
ROLE_TEST = "test"
ROLE_ASSUMPTION = "assumption"

#: Which role a validation kind's artefact carries.
VALIDATION_ROLE = {"Test": ROLE_TEST, "Assumption": ROLE_ASSUMPTION}

PYTHON_SUFFIX = ".py"


@dataclass(frozen=True)
class RuntimeArtefact:
    """One installed runnable target: what it is, where it goes, what it holds.

    Everything a build needs about an artefact and nothing about how it was
    reached. The identity is the catalogue key; the signature is what incremental
    selection compares; the payload is the frozen bytes the installer is *given*
    — deployed source for a Python object, and for a generated load an installer
    script or an instruction the executor completes against the built target. An
    artefact carries its own content either way, so the installer is never sent
    back to a repository it must never reopen.

    ``role`` is what it is *for*, and it is carried rather than inferred. A load
    module and a Test module are both files; a load procedure and a Test
    procedure are both procedures. One lifecycle serves all of them — claimed,
    signed, selected, installed, registered, pruned — and the role is what keeps
    a Test out of the load DAG at the other end.

    ``origin`` is the declaration this artefact was derived from, where there was
    one. A deployed helper module under ``lib/`` has none: it is authored source
    that no Weaver document declares, which is exactly why it needs a claim of
    its own or nothing would ever notice it had been deleted.

    ``source_path`` is the authored file this came from, relative to the
    repository root — the path the developer has open in their editor, not the
    deployed one. It is *carried*, never reconstructed: by the time an install
    fails, ``_/Load/Sales__Customer.py`` is all that is left, and the mapping
    back to ``Lakehouse/Sales/Sales__Customer.py`` is only knowable here.
    """

    identity: WeaverDocumentId
    object_type: str
    signature: str
    payload: bytes
    role: str = ROLE_LOAD
    origin: WeaverDocumentId | None = None
    source_path: str | None = None

    @property
    def is_validation(self) -> bool:
        return self.role in (ROLE_TEST, ROLE_ASSUMPTION)

    @property
    def is_file(self) -> bool:
        return self.object_type == FILE_TYPE

    @property
    def target_path(self) -> str:
        """Where a file artefact lands, relative to the Lakehouse ``Files`` area."""

        if not self.is_file:
            raise ValueError(f"{self.identity} is not a file and has no path")
        return f"{self.identity.object_id.schema}/{self.identity.object_id.object}"


def runtime_artefacts(repository: WeaverRepository) -> tuple[RuntimeArtefact, ...]:
    """Everything this repository installs to be *run*, in identity order.

    Loads and validations together, because from here on they have one
    lifecycle: the same claiming, signing, incremental selection, installation,
    registration and pruning. The producers below stay focused — what a load is
    and what a validation is are different questions — and only the answers are
    pooled.
    """

    artefacts: list[RuntimeArtefact] = []
    for model in repository.items:
        artefacts.extend(item_runtime_artefacts(repository, item=model.identity))
    return tuple(sorted(artefacts, key=lambda artefact: str(artefact.identity)))


def item_runtime_artefacts(
    repository: WeaverRepository, *, item: WeaverItemId
) -> tuple[RuntimeArtefact, ...]:
    """One item's runnable artefacts, loads and validations alike."""

    return item_load_artefacts(repository, item=item) + item_validation_artefacts(
        repository, item=item
    )


def load_artefacts(repository: WeaverRepository) -> tuple[RuntimeArtefact, ...]:
    """Every load artefact the whole repository claims, in identity order."""

    artefacts: list[RuntimeArtefact] = []
    for model in repository.items:
        artefacts.extend(item_load_artefacts(repository, item=model.identity))
    return tuple(sorted(artefacts, key=lambda artefact: str(artefact.identity)))


def validation_artefacts(repository: WeaverRepository) -> tuple[RuntimeArtefact, ...]:
    """Every validation artefact the whole repository claims, in identity order."""

    artefacts: list[RuntimeArtefact] = []
    for model in repository.items:
        artefacts.extend(item_validation_artefacts(repository, item=model.identity))
    return tuple(sorted(artefacts, key=lambda artefact: str(artefact.identity)))


def item_validation_artefacts(
    repository: WeaverRepository, *, item: WeaverItemId
) -> tuple[RuntimeArtefact, ...]:
    """One item's validation artefacts, derived from what it declares.

    A Warehouse validation is a procedure in the generated ``_`` schema; a
    Lakehouse one is a module under the *existing* deployed runtime tree, in a
    ``tests/`` or ``assumptions/`` subdirectory of it. Under the same root
    rather than beside it, deliberately: that root is the item's Python import
    root, so ``from Sales__Order import Sales__Order`` resolves from a
    validation exactly as it does from a load, with no second import root and no
    duplicated object modules.
    """

    if _is_builtin(item):
        return ()
    model = next(
        (each for each in repository.items if each.identity == item), None
    )
    if model is None:
        return ()

    artefacts = []
    for identity in sorted(model.validations, key=str):
        source = repository.source_documents[identity]
        kind = source.document.kind
        role = VALIDATION_ROLE[kind]
        if source.language == PYTHON:
            # Authored source *is* the primitive, exactly as a Python table's
            # module is — so it is deployed rather than generated, and signed by
            # its own bytes.
            payload = source.text.encode("utf-8")
            artefacts.append(
                RuntimeArtefact(
                    identity=validation_artefact_id(item, kind, identity.object_id),
                    object_type=FILE_TYPE,
                    signature=content_hash(payload),
                    payload=payload,
                    role=role,
                    origin=identity,
                    source_path=source.relative_path,
                )
            )
            continue

        generated = source.create_validation()
        artefacts.append(
            RuntimeArtefact(
                identity=validation_artefact_id(item, kind, identity.object_id),
                object_type=generated.object_type,
                signature=_salted(
                    source.effective_signature, generated.template_version
                ),
                payload=generated.payload,
                role=role,
                origin=identity,
                source_path=source.relative_path,
            )
        )
    return tuple(artefacts)


def item_load_artefacts(
    repository: WeaverRepository, *, item: WeaverItemId
) -> tuple[RuntimeArtefact, ...]:
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
) -> tuple[RuntimeArtefact, ...]:
    """One generated load procedure per Warehouse table."""

    artefacts = []
    for identity, source in sorted(repository.source_documents.items(), key=_by_text):
        if identity.item != item or source.kind != TABLE:
            continue
        generated = source.create_load()
        artefacts.append(
            RuntimeArtefact(
                identity=load_procedure_id(item, identity.object_id),
                object_type=generated.object_type,
                signature=_salted(
                    source.effective_signature, generated.template_version
                ),
                payload=generated.payload,
                origin=identity,
                source_path=source.relative_path,
            )
        )
    return tuple(artefacts)


def _lakehouse_artefacts(
    repository: WeaverRepository, *, item: WeaverItemId
) -> tuple[RuntimeArtefact, ...]:
    """The deployed Python tree, plus one generated file per Spark SQL table."""

    artefacts = []
    for identity, source in sorted(repository.source_documents.items(), key=_by_text):
        if identity.item != item or source.relative_path in repository.generated_files:
            continue
        # A validation is deployed too, and by its own producer — which decides
        # where it lands and what role it carries. Claiming it here as well
        # would deploy one module twice, the second time calling a Test a load.
        if source.is_validation:
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
                    source_path=source.relative_path,
                )
            )
        elif source.language == SPARK_SQL and source.kind == TABLE:
            generated = source.create_load()
            artefacts.append(
                _file_artefact(
                    item,
                    # Not the authored path. A Spark SQL table is compiled into a
                    # deployed module, so it lands where a module lands and under
                    # the name a module is imported by — which is what lets
                    # orchestration stop caring which language it was authored in.
                    _deployed_module_relative(relative, identity.object_id),
                    payload=generated.payload,
                    signature=_salted(
                        source.effective_signature, generated.template_version
                    ),
                    origin=identity,
                    source_path=source.relative_path,
                )
            )
    for relative, content in sorted(repository.support_file_contents.items()):
        parts = relative.split("/")
        # Everything beneath ``lib/``, whatever it is. The tree is reproduced
        # verbatim, so a helper module's data file travels with the module that
        # reads it — an ``alias.yml`` beside it is a declaration rather than
        # runtime source and stays behind.
        if len(parts) < 4 or parts[2] != "lib":
            continue
        if WeaverItemId(parts[0], parts[1]) != item:
            continue
        artefacts.append(
            _file_artefact(
                item,
                _within_item(relative, item),
                payload=content,
                signature=content_hash(content),
                source_path=relative,
            )
        )
    return tuple(artefacts)


def _file_artefact(
    item: WeaverItemId,
    relative: str,
    *,
    payload: bytes,
    signature: str,
    role: str = ROLE_LOAD,
    origin: WeaverDocumentId | None = None,
    source_path: str | None = None,
) -> RuntimeArtefact:
    """One deployed file, at the item-relative path reproduced under the root.

    The authored path is preserved whole, ``Files/`` segment included. That
    segment is not noise: ``Sales__Customer.py`` at the item root and
    ``Files/Sales__Customer.py`` are legitimately different documents, and
    flattening them would deploy two files to one path.
    """

    path = f"{LOAD_ROOT}/{relative}"
    directory, _, name = path.rpartition("/")
    return RuntimeArtefact(
        identity=WeaverDocumentId(
            item, ObjectId(schema=directory, object=name), shape=FILE_SHAPE
        ),
        object_type=FILE_TYPE,
        signature=signature,
        payload=payload,
        role=role,
        origin=origin,
        source_path=source_path,
    )


def load_procedure_id(item: WeaverItemId, source: ObjectId) -> WeaverDocumentId:
    """The identity of the procedure that loads one Warehouse table.

    ``Load Sales.Customer`` is a real name in a real schema, not an encoding, so
    the catalogue stores it exactly as the Warehouse holds it.
    """

    return WeaverDocumentId(
        item,
        ObjectId(
            schema=ETL_SCHEMA,
            object=f"{LOAD_PROCEDURE_PREFIX}{source.qualified}",
        ),
        shape=PROCEDURE_SHAPE,
    )


def load_procedure_name(source: ObjectId) -> str:
    """How the generated procedure spells its own name in T-SQL.

    Derived from the same parts as :func:`load_procedure_id`, so the identity
    the catalogue registers and the name the script creates cannot drift.
    """

    schema = _tsql_ident(ETL_SCHEMA)
    procedure = _tsql_ident(f"{LOAD_PROCEDURE_PREFIX}{source.qualified}")
    return f"{schema}.{procedure}"


#: What a generated validation procedure is called. Read as a sentence — the
#: kind, then the logical validation it runs — and stored exactly as the
#: Warehouse holds it, for the same reason a load procedure's name is.
#:
#: The *logical* validation stays ``Sales.IncrementalCount``; this is only its
#: installed executable form, and the two are deliberately different names for
#: different things. ``_.TestDictionary`` describes the first; ``_.Registry``
#: certifies the second.
VALIDATION_PROCEDURE_PREFIX = {"Test": "Test ", "Assumption": "Assumption "}

#: Where a compiled validation module lands in the deployed runtime tree. Under
#: the existing root rather than beside it, so ``from Sales__Order import
#: Sales__Order`` resolves from a validation exactly as it does from a load —
#: one deployed tree per item, and the imports keep working.
VALIDATION_FOLDER = {"Test": "tests", "Assumption": "assumptions"}


def validation_procedure_id(
    item: WeaverItemId, kind: str, source: ObjectId
) -> WeaverDocumentId:
    """The identity of the procedure that runs one Warehouse validation."""

    return WeaverDocumentId(
        item,
        ObjectId(
            schema=ETL_SCHEMA,
            object=f"{VALIDATION_PROCEDURE_PREFIX[kind]}{source.qualified}",
        ),
        shape=PROCEDURE_SHAPE,
    )


def validation_procedure_name(kind: str, source: ObjectId) -> str:
    """How the generated validation procedure spells its own name in T-SQL.

    Derived from the same parts as :func:`validation_procedure_id`, so the
    identity the catalogue registers and the name the script creates cannot
    drift.
    """

    schema = _tsql_ident(ETL_SCHEMA)
    procedure = _tsql_ident(
        f"{VALIDATION_PROCEDURE_PREFIX[kind]}{source.qualified}"
    )
    return f"{schema}.{procedure}"


def validation_artefact_id(
    item: WeaverItemId, kind: str, source: ObjectId
) -> WeaverDocumentId:
    """The runtime artefact one logical validation compiles to.

    **The function that connects `_.TestDictionary` to `_.Registry.`** A
    validation has no Registry row of its own — nothing is materialised under
    its logical ID — so orchestration finds its installed primitive by computing
    the identity rather than by looking the logical ID up. That only works while
    exactly one function computes it, which is why the build claims its
    artefacts through this too.

    Which physical form follows from the owning item, and nothing else: a
    Warehouse installs a procedure, a Lakehouse a module in its runtime tree.
    """

    if item.item_type == WAREHOUSE:
        return validation_procedure_id(item, kind, source)
    path = f"{LOAD_ROOT}/{validation_module_path(kind, source)}"
    directory, _, name = path.rpartition("/")
    return WeaverDocumentId(
        item, ObjectId(schema=directory, object=name), shape=FILE_SHAPE
    )


def validation_module_path(kind: str, source: ObjectId) -> str:
    """Where a compiled Lakehouse validation module lands, under the runtime root.

    ``_/Load/tests/Sales__OrdersReconcile.py``. The subdirectory keeps validation
    legible in a deployed tree without moving it out of the import root.
    """

    from .declaration.spark_sql_module import deployed_module_name

    return f"{VALIDATION_FOLDER[kind]}/{deployed_module_name(source)}"


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


def _deployed_module_relative(relative: str, object_id: ObjectId) -> str:
    """``Sales.OrderSummary.sql`` -> ``Sales__OrderSummary.py``, where it was.

    The directory is preserved and only the filename is recompiled, so a
    document's position in the item is still what decides its position in the
    deployed tree — the same rule the authored Python files follow.
    """

    from .declaration.spark_sql_module import deployed_module_name

    directory, _, _name = relative.rpartition("/")
    module = deployed_module_name(object_id)
    return f"{directory}/{module}" if directory else module


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
        # A Warehouse needs the schema its generated procedures live in and
        # nothing else — there is no Files area, so no runtime tree and no folder
        # to own one. A validation puts a procedure there too, so an item that
        # only validates still needs the schema.
        documents = tuple(documents)
        return (
            schema
            if any(source.kind == TABLE or source.is_validation for source in documents)
            else {}
        )
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
        # A validation is deployed into the same tree whatever it was authored
        # in, so an item that only validates still owns a runtime tree to put it
        # in — otherwise the folder its module lands in would not exist.
        if source.is_validation:
            return True
    prefix = f"{item}/lib/"
    return any(relative.startswith(prefix) for relative in support_paths)


def load_schemas(artefacts: Iterable[RuntimeArtefact]) -> tuple[str, ...]:
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


def artefacts_by_identity(
    artefacts: Iterable[RuntimeArtefact],
) -> Mapping[WeaverDocumentId, RuntimeArtefact]:
    return {artefact.identity: artefact for artefact in artefacts}


__all__ = [
    "ETL_SCHEMA",
    "FILE_TYPE",
    "LOAD_FOLDER",
    "LOAD_PROCEDURE_PREFIX",
    "LOAD_ROOT",
    "RuntimeArtefact",
    "PROCEDURE_TYPE",
    "item_load_artefacts",
    "load_artefacts",
    "artefacts_by_identity",
    "item_runtime_artefacts",
    "item_validation_artefacts",
    "runtime_artefacts",
    "validation_artefacts",
    "validation_module_path",
    "validation_artefact_id",
    "validation_procedure_id",
    "validation_procedure_name",
    "load_schemas",
    "load_procedure_id",
    "load_procedure_name",
]
