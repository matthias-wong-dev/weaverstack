"""Derive a repository's runnable artefacts from its source.

A runtime artefact is claimed, signed, installed, registered and pruned as its
own target.

Three artefacts, from three kinds of source:

.. code-block:: text

    Warehouse/Reporting/Sales__Customer.sql    -> _.[Load Sales.Customer]
    Lakehouse/Sales/lib/dates.py               -> Files/_/Load/lib/dates.py
    Lakehouse/Sales/Tables/Sales.Customer.sql  -> Files/_/Load/Tables/Sales__Customer.py

Views produce no runnable artefact. Generated payload signatures include the
generator version; authored Python is signed from its own bytes.

This module does not inspect destinations. Repository contents are the complete
claims; catalogue reconciliation prunes sources that were removed or renamed.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Iterable, Mapping

from .declaration.metadata import FOLDER, PYTHON, SPARK_SQL, TABLE, ObjectId
from .declaration.model import (
    FILE_SHAPE,
    PROCEDURE_SHAPE,
    WAREHOUSE,
    WeaverDocumentId,
    WeaverItemId,
    WeaverRepository,
)
from .declaration.source import content_hash, salted_signature
from .errors import BuildError

if TYPE_CHECKING:
    from .declaration.programmable import Programmable
    from .declaration.source import SourceDocument

#: Runtime infrastructure is managed under Warehouse schema ``_`` or Lakehouse
#: folder ``Files/_/Load`` and is pruned with the item's last runtime artefact.
ETL_SCHEMA = "_"
LOAD_FOLDER = "Load"

#: The deployed tree is also its Python import root, so authored paths are
#: preserved.
LOAD_ROOT = f"{ETL_SCHEMA}/{LOAD_FOLDER}"

SHORTCUTS_MODULE = "shortcuts.py"

#: ``Load Sales.Customer`` is the procedure's real Warehouse name, not an
#: encoding.
LOAD_PROCEDURE_PREFIX = "Load "

FILE_TYPE = "file"
PROCEDURE_TYPE = "stored_procedure"

#: Repeated here to avoid a catalogue import cycle. Roles are never inferred
#: from physical shape because loads and validations share file and procedure
#: shapes.
ROLE_LOAD = "load"
ROLE_TEST = "test"
ROLE_ASSUMPTION = "assumption"
VALIDATION_ROLE = {"Test": ROLE_TEST, "Assumption": ROLE_ASSUMPTION}
VALIDATION_ROLES = (ROLE_TEST, ROLE_ASSUMPTION)

PYTHON_SUFFIX = ".py"


@dataclass(frozen=True)
class RuntimeArtefact:
    """A runnable target and its installed content.

    ``identity`` is its catalogue key, ``signature`` drives incremental
    selection, and ``payload`` is the frozen content given to the installer.

    ``role`` is explicit because loads and validations share physical shapes.

    ``origin`` is the declaration that produced it; helper modules have none.

    ``source_path`` retains the authored path for installation errors.
    """

    identity: WeaverDocumentId
    object_type: str
    source_signature: str
    #: Generated modules have no payload until their Lakehouse destination is
    #: known.
    payload: bytes | None
    role: str = ROLE_LOAD
    implementation_version: int = 1
    origin: WeaverDocumentId | None = None
    source_path: str | None = None

    @property
    def signature(self) -> str:
        return salted_signature(self.source_signature, self.implementation_version)

    @property
    def installed_bytes(self) -> bytes:
        if self.payload is None:
            raise BuildError(
                f"{self.identity} has no installable content because its item has no "
                "target. Bind the item to a target and generate the bundle again."
            )
        return self.payload

    @property
    def is_validation(self) -> bool:
        return self.role in VALIDATION_ROLES

    @property
    def is_file(self) -> bool:
        return self.object_type == FILE_TYPE

    @property
    def target_path(self) -> str:
        if not self.is_file:
            raise ValueError(
                f"{self.identity} does not install as a file. target_path applies "
                "only to file artefacts."
            )
        return f"{self.identity.object_id.schema}/{self.identity.object_id.object}"


def runtime_artefacts(repository: WeaverRepository) -> tuple[RuntimeArtefact, ...]:
    """Return all load and validation artefacts in identity order."""

    artefacts: list[RuntimeArtefact] = []
    for model in repository.items:
        artefacts.extend(item_runtime_artefacts(repository, item=model.identity))
    return tuple(sorted(artefacts, key=lambda artefact: str(artefact.identity)))


def item_bookmarkable_objects(
    repository: WeaverRepository, *, item: WeaverItemId
) -> tuple[WeaverDocumentId, ...]:
    """Return the objects in one item that Weaver loads and bookmarks.

    Deriving this from load artefacts keeps bookmark state aligned with runnable
    loads. Views, externally populated tables, helpers and validations are excluded.
    """

    bookmarkable = {FOLDER, TABLE}
    found = set()
    for artefact in item_load_artefacts(repository, item=item):
        origin = artefact.origin
        if origin is None or artefact.role != ROLE_LOAD:
            continue
        source = repository.source_documents.get(origin)
        if source is not None and source.kind in bookmarkable:
            found.add(origin)
    return tuple(sorted(found, key=str))


def item_view_objects(
    repository: WeaverRepository, *, item: WeaverItemId
) -> tuple[WeaverDocumentId, ...]:
    """Return Views whose build status can make downstream objects stale."""

    from .declaration.metadata import VIEW

    found = {
        identity
        for identity, source in repository.source_documents.items()
        if identity.item == item and source.kind == VIEW
    }
    return tuple(sorted(found, key=str))


def item_data_nodes(
    repository: WeaverRepository, *, item: WeaverItemId
) -> tuple[WeaverDocumentId, ...]:
    """Return loadable objects and Views that carry ``_.LoadStatus``."""

    return tuple(
        sorted(
            {
                *item_bookmarkable_objects(repository, item=item),
                *item_view_objects(repository, item=item),
            },
            key=str,
        )
    )


def item_validated_objects(
    repository: WeaverRepository, *, item: WeaverItemId
) -> tuple[WeaverDocumentId, ...]:
    """Return the validations in one item that carry test status.

    Deriving these from validation artefacts keeps status aligned with runnable
    validations.

    The identity is the validation's own ``Schema.Object``, not its compiled
    artefact's: a test status describes the Test, and the module or procedure it
    compiles to is how the Test is run.
    """

    if item.item_type == WAREHOUSE:
        origins = (
            programmable.origin
            for programmable in repository.programmables.values()
            if programmable.identity.item == item
            and programmable.role in VALIDATION_ROLES
        )
    else:
        origins = (
            artefact.origin
            for artefact in item_validation_artefacts(repository, item=item)
        )
    return tuple(sorted({each for each in origins if each is not None}, key=str))


def item_runtime_artefacts(
    repository: WeaverRepository, *, item: WeaverItemId, destination=None
) -> tuple[RuntimeArtefact, ...]:
    """One item's runnable artefacts, loads and validations alike.

    ``destination`` is required only to render generated module payloads;
    identities and signatures do not depend on it.
    """

    return item_load_artefacts(
        repository, item=item, destination=destination
    ) + item_validation_artefacts(repository, item=item, destination=destination)


def load_artefacts(repository: WeaverRepository) -> tuple[RuntimeArtefact, ...]:
    artefacts: list[RuntimeArtefact] = []
    for model in repository.items:
        artefacts.extend(item_load_artefacts(repository, item=model.identity))
    return tuple(sorted(artefacts, key=lambda artefact: str(artefact.identity)))


def validation_artefacts(repository: WeaverRepository) -> tuple[RuntimeArtefact, ...]:
    artefacts: list[RuntimeArtefact] = []
    for model in repository.items:
        artefacts.extend(item_validation_artefacts(repository, item=model.identity))
    return tuple(sorted(artefacts, key=lambda artefact: str(artefact.identity)))


def item_validation_artefacts(
    repository: WeaverRepository, *, item: WeaverItemId, destination=None
) -> tuple[RuntimeArtefact, ...]:
    """Return one item's declared validation artefacts.

    Lakehouse validations are modules under the runtime import root. Warehouse
    validations come from the repository's generated Programmables.
    """

    if _is_builtin(item) or item.item_type == WAREHOUSE:
        return ()
    model = next((each for each in repository.items if each.identity == item), None)
    if model is None:
        return ()

    artefacts = []
    for identity in sorted(model.validations, key=str):
        source = repository.source_documents[identity]
        kind = source.document.kind
        role = VALIDATION_ROLE[kind]
        if source.language == PYTHON:
            # Authored source is the primitive, as a Python table's module is, so
            # it is deployed rather than generated and signed by its own bytes.
            payload = source.text.encode("utf-8")
            artefacts.append(
                RuntimeArtefact(
                    identity=validation_artefact_id(item, kind, identity.object_id),
                    object_type=FILE_TYPE,
                    source_signature=content_hash(payload),
                    payload=payload,
                    role=role,
                    origin=identity,
                    source_path=source.relative_path,
                )
            )
            continue

        from .declaration.validation import validation_identity

        object_type, template_version = validation_identity(source)
        # Only a Spark body names a destination. A Warehouse validation compiles
        # to T-SQL, which the connection addresses, so it renders either way.
        generated = (
            source.create_validation(destination=destination)
            if destination is not None or source.language != SPARK_SQL
            else None
        )
        artefacts.append(
            RuntimeArtefact(
                identity=validation_artefact_id(item, kind, identity.object_id),
                object_type=object_type,
                source_signature=source.effective_signature,
                payload=None if generated is None else generated.payload,
                role=role,
                implementation_version=template_version,
                origin=identity,
                source_path=source.relative_path,
            )
        )
    return tuple(artefacts)


def item_load_artefacts(
    repository: WeaverRepository, *, item: WeaverItemId, destination=None
) -> tuple[RuntimeArtefact, ...]:
    """Return one item's declared load artefacts.

    The built-in catalogue Warehouse has no load layer.
    """

    if _is_builtin(item):
        return ()
    if item.item_type == WAREHOUSE:
        return _warehouse_artefacts(repository, item=item)
    return _lakehouse_artefacts(repository, item=item, destination=destination)


def item_generated_programmables(
    *, item: WeaverItemId, documents: Iterable["SourceDocument"]
) -> tuple["Programmable", ...]:
    """Return generated load and validation procedures for one Warehouse item.

    They join the repository through the same composition path as authored
    declarations.
    """

    if item.item_type != WAREHOUSE or _is_builtin(item):
        return ()

    from .declaration.load import has_generated_load
    from .declaration.programmable import generated_programmable
    from .declaration.validation import validation_identity

    found: list[Programmable] = []
    for source in documents:
        identity = source.logical_id
        if identity is None:
            continue
        if not source.is_validation:
            if source.kind != TABLE or not has_generated_load(source):
                continue
            generated = source.create_load(item=item)
            found.append(
                generated_programmable(
                    load_procedure_id(item, identity.object_id),
                    text=generated.payload.decode("utf-8"),
                    source_signature=source.effective_signature,
                    implementation_version=generated.template_version,
                    role=ROLE_LOAD,
                    origin=identity,
                )
            )
            continue

        object_type, template_version = validation_identity(source)
        assert object_type == PROCEDURE_TYPE
        kind = source.document.kind
        generated = source.create_validation(destination=None)
        found.append(
            generated_programmable(
                validation_procedure_id(item, kind, identity.object_id),
                text=generated.payload.decode("utf-8"),
                source_signature=source.effective_signature,
                implementation_version=template_version,
                role=VALIDATION_ROLE[kind],
                origin=identity,
            )
        )

    return tuple(sorted(found, key=lambda each: str(each.identity)))


def _warehouse_artefacts(
    repository: WeaverRepository, *, item: WeaverItemId
) -> tuple[RuntimeArtefact, ...]:
    found = []
    for programmable in repository.programmables.values():
        if programmable.identity.item != item:
            continue
        found.append(
            RuntimeArtefact(
                identity=programmable.identity,
                object_type=PROCEDURE_TYPE,
                source_signature=programmable.source_signature,
                payload=programmable.payload,
                role=programmable.role,
                implementation_version=programmable.implementation_version,
                origin=programmable.origin,
                source_path=programmable.relative_path,
            )
        )
    return tuple(sorted(found, key=lambda artefact: str(artefact.identity)))


def _lakehouse_artefacts(
    repository: WeaverRepository, *, item: WeaverItemId, destination=None
) -> tuple[RuntimeArtefact, ...]:
    artefacts = []
    for identity, source in sorted(repository.source_documents.items(), key=_by_text):
        if identity.item != item or source.relative_path in repository.generated_files:
            continue
        # The validation producer owns its path and role; do not claim it as a load.
        if source.is_validation:
            continue
        relative = _within_item(source.relative_path, item)
        if source.language == PYTHON:
            # A Python document declares one structural target and one runtime target.
            artefacts.append(
                _file_artefact(
                    item,
                    relative,
                    payload=source.text.encode("utf-8"),
                    source_signature=content_hash(source.text.encode("utf-8")),
                    origin=identity,
                    source_path=source.relative_path,
                )
            )
        elif source.language == SPARK_SQL and source.kind == TABLE:
            from .declaration.load import has_generated_load, load_identity

            if not has_generated_load(source):
                continue
            _object_type, template_version = load_identity(source)
            generated = (
                source.create_load(destination=destination, item=item)
                if destination is not None
                else None
            )
            artefacts.append(
                _file_artefact(
                    item,
                    # All compiled tables use the same importable module path.
                    _deployed_module_relative(relative, identity.object_id),
                    payload=None if generated is None else generated.payload,
                    source_signature=source.effective_signature,
                    implementation_version=template_version,
                    origin=identity,
                    source_path=source.relative_path,
                )
            )
    declared = tuple(
        declaration
        # Weaver-owned references are infrastructure, not runtime imports.
        for declaration in repository.shortcuts
        if declaration.owner == item and declaration.destination_identity is None
    )
    if declared:
        # Runtime code imports readers, not authored shortcut declarations.
        from .shortcuts import render_runtime_module

        payload = render_runtime_module(declared).encode("utf-8")
        artefacts.append(
            _file_artefact(
                item,
                SHORTCUTS_MODULE,
                payload=payload,
                source_signature=content_hash(payload),
                source_path=declared[0].relative_path,
            )
        )
    for relative, content in sorted(repository.support_file_contents.items()):
        parts = relative.split("/")
        # Preserve all of lib/, including data files; declaration files stay behind.
        if len(parts) < 4 or parts[2] != "lib":
            continue
        if WeaverItemId(parts[0], parts[1]) != item:
            continue
        artefacts.append(
            _file_artefact(
                item,
                _within_item(relative, item),
                payload=content,
                source_signature=content_hash(content),
                source_path=relative,
            )
        )
    return tuple(artefacts)


def _file_artefact(
    item: WeaverItemId,
    relative: str,
    *,
    payload: bytes,
    source_signature: str,
    role: str = ROLE_LOAD,
    implementation_version: int = 1,
    origin: WeaverDocumentId | None = None,
    source_path: str | None = None,
) -> RuntimeArtefact:
    """Create a deployed file while preserving its path beneath the item.

    The authored path is preserved whole, area included:
    ``Tables/Sales__Customer.py`` and ``Files/Sales__Customer.py`` are different
    documents, and flattening them would deploy two files to one path.
    """

    path = f"{LOAD_ROOT}/{relative}"
    directory, _, name = path.rpartition("/")
    return RuntimeArtefact(
        identity=WeaverDocumentId(
            item, ObjectId(schema=directory, object=name), shape=FILE_SHAPE
        ),
        object_type=FILE_TYPE,
        source_signature=source_signature,
        payload=payload,
        role=role,
        implementation_version=implementation_version,
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
    """Render the same procedure name used by :func:`load_procedure_id`."""

    schema = _tsql_ident(ETL_SCHEMA)
    procedure = _tsql_ident(f"{LOAD_PROCEDURE_PREFIX}{source.qualified}")
    return f"{schema}.{procedure}"


#: Prefixes name installed executables; logical validations keep their own IDs.
VALIDATION_PROCEDURE_PREFIX = {"Test": "Test ", "Assumption": "Assumption "}

#: Validation modules stay under the runtime import root.
VALIDATION_FOLDER = {"Test": "tests", "Assumption": "assumptions"}


def validation_procedure_id(
    item: WeaverItemId, kind: str, source: ObjectId
) -> WeaverDocumentId:
    return WeaverDocumentId(
        item,
        ObjectId(
            schema=ETL_SCHEMA,
            object=f"{VALIDATION_PROCEDURE_PREFIX[kind]}{source.qualified}",
        ),
        shape=PROCEDURE_SHAPE,
    )


def validation_procedure_name(kind: str, source: ObjectId) -> str:
    """Render the same procedure name used by :func:`validation_procedure_id`."""

    schema = _tsql_ident(ETL_SCHEMA)
    procedure = _tsql_ident(f"{VALIDATION_PROCEDURE_PREFIX[kind]}{source.qualified}")
    return f"{schema}.{procedure}"


def validation_artefact_id(
    item: WeaverItemId, kind: str, source: ObjectId
) -> WeaverDocumentId:
    """Return the runtime artefact identity for a logical validation.

    Build and orchestration must use this same mapping between logical
    validations and their installed primitives.

    The physical form follows from the owning item: a Warehouse installs a
    procedure, a Lakehouse a module in its runtime tree.
    """

    if item.item_type == WAREHOUSE:
        return validation_procedure_id(item, kind, source)
    path = f"{LOAD_ROOT}/{validation_module_path(kind, source)}"
    directory, _, name = path.rpartition("/")
    return WeaverDocumentId(
        item, ObjectId(schema=directory, object=name), shape=FILE_SHAPE
    )


def validation_module_path(kind: str, source: ObjectId) -> str:
    """Return a compiled validation's path beneath the runtime root."""

    from .declaration.spark_sql_module import deployed_module_name

    return f"{VALIDATION_FOLDER[kind]}/{deployed_module_name(source)}"


def _deployed_module_relative(relative: str, object_id: ObjectId) -> str:
    """``Sales.OrderSummary.sql`` -> ``Sales__OrderSummary.py``, where it was.

    The directory is preserved; only the filename changes.
    """

    from .declaration.spark_sql_module import deployed_module_name

    directory, _, _name = relative.rpartition("/")
    module = deployed_module_name(object_id)
    return f"{directory}/{module}" if directory else module


def _within_item(relative: str, item: WeaverItemId) -> str:
    prefix = f"{item}/"
    if not relative.startswith(prefix):
        raise ValueError(
            f"Path {relative!r} is outside item {item}. Pass a path beneath {item}/"
        )
    return relative[len(prefix) :]


def _by_text(entry):
    return str(entry[0])


def _is_builtin(item: WeaverItemId) -> bool:
    from .catalogue.builtin import BUILTIN_ITEM

    return item == BUILTIN_ITEM


def _tsql_ident(name: str) -> str:
    return "[" + name.replace("]", "]]") + "]"


#: Fragment declaring the managed runtime tree for a Lakehouse.
FOLDER_DOCUMENT = f"Files/{ETL_SCHEMA}{'__'}{LOAD_FOLDER}.py"


def has_deployable_source(
    item: WeaverItemId,
    *,
    documents: Iterable["SourceDocument"],
    support_paths: Iterable[str],
) -> bool:
    for source in documents:
        if source.language == PYTHON:
            return True
        if source.language == SPARK_SQL and source.kind == TABLE:
            return True
        if source.is_validation:
            return True
    prefix = f"{item}/lib/"
    return any(relative.startswith(prefix) for relative in support_paths)


def load_schemas(artefacts: Iterable[RuntimeArtefact]) -> tuple[str, ...]:
    """Return required Warehouse schemas, either ``_`` or none.

    Deriving this from procedures lets schema pruning remove an unused ``_``.
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
    "FOLDER_DOCUMENT",
    "LOAD_FOLDER",
    "LOAD_PROCEDURE_PREFIX",
    "LOAD_ROOT",
    "RuntimeArtefact",
    "PROCEDURE_TYPE",
    "has_deployable_source",
    "item_generated_programmables",
    "item_bookmarkable_objects",
    "item_load_artefacts",
    "load_artefacts",
    "artefacts_by_identity",
    "item_runtime_artefacts",
    "item_validated_objects",
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
