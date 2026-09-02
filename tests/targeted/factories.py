"""Constructors for the smallest input each seam actually needs.

The old suite's habit was to reach for a complete estate, a parsed repository, a
projected catalogue, a generated bundle, whatever the subject was. That made
every test slow to read and, worse, made failures ambiguous: a broken signature
comparison and a broken catalogue projection failed the same test the same way.

So each constructor here builds exactly one thing, and tests take the narrowest
one that can answer their question. A test that needs one Registry row builds one
Registry row. Reaching for a richer constructor than the subject requires is the
smell this module exists to remove.

They are plain functions rather than fixtures because most tests want several,
parametrised differently, and a fixture cannot be called twice with different
arguments without ceremony.
"""

from __future__ import annotations

from pathlib import Path
from typing import Mapping

from support.workspaces import WORKSPACE

from weaver.build_bundle import (
    BuildBatch,
    BuildPlan,
    InstallAction,
    ItemBinding,
    ItemBindings,
    LakehouseBinding,
    WarehouseBinding,
    compute_bundle_id,
    write_bundle,
)
from weaver.build_bundle.bundle import SUPPORTED_FORMAT_VERSION
from weaver.build_bundle.prune import TargetInventory
from weaver.build_bundle.targets import BoundTarget
from weaver.catalogue.builtin import BUILTIN_ITEM
from weaver.catalogue.claims import catalogue_schema
from weaver.catalogue.state import Catalogue, RegisteredDocument
from weaver.catalogue.tables import (
    CATALOGUE_SCHEMA,
    REGISTRY,
    STANDARD_SURFACE_TABLES,
)
from weaver.declaration import parse_item_repository
from weaver.declaration.metadata import ObjectId
from weaver.declaration.model import (
    TABLES,
    WAREHOUSE,
    WeaverDocumentId,
    WeaverItemId,
)
from weaver.etl import (
    FILE_TYPE,
    PROCEDURE_TYPE,
    item_runtime_artefacts,
    load_schemas,
)
from weaver.locations import Location
from weaver.store import FilesystemStore
from weaver.targets import ItemRef

#: Neutral names, per the environment-neutrality rule: no product, workspace or
#: tenant may be inferable from a fixture.
ITEM = "Lakehouse/Sales"
WAREHOUSE_ITEM = "Warehouse/Reporting"


# --- identities ---------------------------------------------------------------


def document_id(text: str) -> WeaverDocumentId:
    """``Lakehouse/Sales/Tables/DWG.Customer``, or the item-relative spelling.

    A bare ``Schema.Object`` names a relation in :data:`ITEM`, so it becomes a
    ``Tables`` identity. Spell a validation out, or use :func:`validation_id`.
    """

    if "/" not in text:
        text = f"{ITEM}/{TABLES}/{text}"
    return WeaverDocumentId.parse(text)


def validation_id(text: str, *, item: str = ITEM) -> WeaverDocumentId:
    """One Test or Assumption identity, which names no Lakehouse area."""

    return WeaverDocumentId.validation(
        WeaverItemId.parse(item), ObjectId(*text.split(".", 1))
    )


def item_id(text: str = ITEM) -> WeaverItemId:
    return WeaverItemId.parse(text)


# --- catalogue --------------------------------------------------------------


def registered_document(
    identity: str | WeaverDocumentId,
    *,
    object_type: str = "table",
    object_role: str = "data",
    signature: str = "signature-1",
    build_datetime=None,
) -> RegisteredDocument:
    """One validated Registry row, as incremental selection consumes it.

    This is all `determine_impact` and `select_build` ever look at, so a test
    about signature comparison needs nothing else, no catalogue, no projection,
    no Spark. Constructing one directly is the difference between a test that
    fails because a signature comparison is wrong and one that fails because
    anything at all in a catalogue projection is wrong.
    """

    if isinstance(identity, str):
        identity = document_id(identity)
    return RegisteredDocument(
        identity=identity,
        object_type=object_type,
        object_role=object_role,
        signature=signature,
        build_datetime=build_datetime,
    )


def registry_row(
    identity: str | WeaverDocumentId,
    *,
    object_type: str = "table",
    object_role: str = "data",
    signature: str = "signature-1",
    build_datetime=None,
) -> dict:
    """One Registry row in its stored form, keyed as the catalogue writes it."""

    from weaver.catalogue.claims import catalogue_columns

    if isinstance(identity, str):
        identity = document_id(identity)
    # Keyed through the production rule, so a fixture cannot describe a row the
    # catalogue would never write.
    schema_name, object_name = catalogue_columns(identity)
    return {
        "item_type": identity.item.item_type,
        "item_name": identity.item.item_name,
        "schema_name": schema_name,
        "object_name": object_name,
        "object_type": object_type,
        "object_role": object_role,
        "signature": signature,
        "build_datetime": build_datetime,
    }


def installation_row(item: str | WeaverItemId, target_name: str) -> dict:
    """One Installation row binding a logical item to a physical target."""

    if isinstance(item, str):
        item = item_id(item)
    return {
        "item_type": item.item_type,
        "item_name": item.item_name,
        "target_name": target_name,
        "weaver_version": "0.0.0+test",
        "signature": "installation-signature",
    }


def dependency_row(
    consumer: str | WeaverDocumentId,
    reference: str,
    *,
    referenced: str | WeaverDocumentId | None = None,
) -> dict:
    """One Dependency row, holding the reference exactly as an author wrote it.

    ``referenced`` is the edge Weaver resolved, recorded beside the spelling. It
    defaults to the consumer's own item, which is where a two-part
    ``Schema.Object`` resolves. A reference of any other shape left the estate,
    so the referenced columns stay null, as they do for an edge that did not
    resolve.
    """

    if isinstance(consumer, str):
        consumer = document_id(consumer)
    if referenced is None:
        parts = reference.split(".")
        if len(parts) == 2:
            referenced = WeaverDocumentId(consumer.item, ObjectId(*parts))
    elif isinstance(referenced, str):
        referenced = document_id(referenced)
    resolved = (
        {
            "referenced_item_type": None,
            "referenced_item_name": None,
            "referenced_schema_name": None,
            "referenced_object_name": None,
        }
        if referenced is None
        else {
            "referenced_item_type": referenced.item.item_type,
            "referenced_item_name": referenced.item.item_name,
            "referenced_schema_name": catalogue_schema(referenced),
            "referenced_object_name": referenced.object_id.object,
        }
    )
    return {
        "item_type": consumer.item.item_type,
        "item_name": consumer.item.item_name,
        "referencing_schema_name": catalogue_schema(consumer),
        "referencing_object_name": consumer.object_id.object,
        "dependency_reference": reference,
        **resolved,
        "signature": "dependency",
    }


def shortcut_row(
    destination: str | WeaverDocumentId,
    source: str | WeaverDocumentId,
    *,
    target_type: str = "logical",
    shortcut_type: str = "table",
) -> dict:
    """One Shortcut row, as a consuming item's declaration publishes it."""

    if isinstance(destination, str):
        destination = document_id(destination)
    if isinstance(source, str):
        source = document_id(source)
    return {
        "item_type": destination.item.item_type,
        "item_name": destination.item.item_name,
        "shortcut_id": destination.object_id.qualified,
        "schema_name": catalogue_schema(destination),
        "object_name": destination.object_id.object,
        "shortcut_type": shortcut_type,
        "target_type": target_type,
        "target_item_type": source.item.item_type,
        "target_item_name": source.item.item_name,
        "target_schema_name": catalogue_schema(source),
        "target_object_name": source.object_id.object,
        "target_workspace_name": None,
        "signature": "shortcut",
    }


def validation_row(
    logical: str | WeaverDocumentId,
    *,
    test_type: str = "test",
    primary_key: str | None = None,
    description: str = "A declared validation.",
) -> dict:
    """One TestDictionary row: the logical declaration, not its artefact."""

    if isinstance(logical, str):
        logical = document_id(logical)
    return {
        "item_type": logical.item.item_type,
        "item_name": logical.item.item_name,
        "schema_name": logical.object_id.schema,
        "object_name": logical.object_id.object,
        "test_type": test_type,
        "description": description,
        "description_reference": None,
        "primary_key": primary_key,
        "signature": "validation",
    }


def load_status_row(
    identity: str | WeaverDocumentId,
    *,
    result: str = "succeeded",
    workflow_id: str = "workflow-1",
    started_at=None,
    completed_at=None,
) -> dict:
    """One ``_.LoadStatus`` row: how an object's most recent load ended."""

    if isinstance(identity, str):
        identity = document_id(identity)
    return {
        "item_type": identity.item.item_type,
        "item_name": identity.item.item_name,
        "schema_name": catalogue_schema(identity),
        "object_name": identity.object_id.object,
        "workflow_id": workflow_id,
        "result": result,
        "started_datetime": started_at,
        "completed_datetime": completed_at,
        "duration_milliseconds": None,
    }


def validation_status_row(
    identity: str | WeaverDocumentId,
    *,
    result: str = "succeeded",
    test_type: str = "test",
    workflow_id: str = "workflow-1",
    started_at=None,
    completed_at=None,
    failure_count=None,
) -> dict:
    """One ``_.TestStatus`` row: how a validation's most recent run ended."""

    if isinstance(identity, str):
        identity = document_id(identity)
    return {
        "item_type": identity.item.item_type,
        "item_name": identity.item.item_name,
        "schema_name": catalogue_schema(identity),
        "object_name": identity.object_id.object,
        "test_type": test_type,
        "workflow_id": workflow_id,
        "result": result,
        "started_datetime": started_at,
        "completed_datetime": completed_at,
        "duration_milliseconds": None,
        "failure_count": failure_count,
    }


def bookmark_row_for(identity: str | WeaverDocumentId, at) -> dict:
    """One ``_.Bookmark`` row: how far an object has been loaded."""

    from weaver.catalogue.claims import bookmark_row

    if isinstance(identity, str):
        identity = document_id(identity)
    return bookmark_row(identity, at)


class FixtureCatalogue(Catalogue):
    """The production `Catalogue`, with the ways a test needs to populate one.

    A subclass rather than a separate type. Every build decision
    runs against the real class; what changes is only *where the state came
    from*. Beginning from a hand-written Registry row is the same move as
    installing a frozen bundle instead of building one from a repository, a
    later starting point on the real thing, not a stand-in for it.

    These constructors are here and not on `Catalogue` because a Registry row
    means work succeeded: it is written last in a build, and the planner has a
    whole `uncertified` mechanism to withhold rows for work that was not done. A
    production method that manufactured rows from declarations would be a way to
    forge that guarantee, and eventually something would call it on a build path.
    """

    @classmethod
    def from_registry_rows(cls, *rows, item: str | WeaverItemId = ITEM) -> "Catalogue":
        """A catalogue holding exactly the Registry rows given.

        Physically incomplete on purpose, no other catalogue table is present.
        Right for reconciliation and selection, which read the Registry alone;
        wrong for testing the catalogue read, which is a Spark-boundary claim
        and has to see a complete catalogue.
        """

        if isinstance(item, str):
            item = item_id(item)
        return cls(
            rows={item: {REGISTRY.name: tuple(rows)}},
            materialised=frozenset({REGISTRY.name}),
        )

    @classmethod
    def holding(cls, item: str | WeaverItemId = ITEM, **tables) -> "Catalogue":
        """A catalogue holding the named tables' rows: Registry and beyond.

        `from_registry_rows` is the common case; this is for the claims that are
        about more than the Registry, such as the order in which a build
        removes a certification and the description behind it. A claim is only
        collected where a row actually exists, so a catalogue with no dictionary
        rows cannot demonstrate dictionary ordering.
        """

        if isinstance(item, str):
            item = item_id(item)
        return cls(
            rows={item: {name: tuple(rows) for name, rows in tables.items()}},
            materialised=frozenset(tables),
        )

    @classmethod
    def certifying(cls, *registered: RegisteredDocument) -> "Catalogue":
        """A catalogue certifying exactly these documents, with no row data.

        For tests whose subject is downstream of the Registry, selection,
        shortcut staleness, planning, where the rows themselves are never read.
        """

        return cls(
            rows={},
            registered={document.identity: document for document in registered},
        )

    @classmethod
    def from_repository(
        cls, repository, *, item: str | WeaverItemId = ITEM
    ) -> "Catalogue":
        """The catalogue a successful build of this repository would have left.

        The "already installed, nothing changed" state, which is the premise of
        every incremental claim and previously took a real build to reach. Each
        declared object is certified at its currently declared signature, so
        selection sees an estate that is exactly correct.

        Load artefacts are certified alongside the documents, and have to be: a
        catalogue that certified only the documents would describe an estate
        whose runtime tree was never installed, and every incremental claim built
        on it would be reasoning about a half-built one.
        """

        from weaver.build_bundle.incremental import declared_signatures

        if isinstance(item, str):
            item = item_id(item)
        # A validation declares no physical object under its logical ID, so it
        # gets no Registry row here either. What is certified is the artefact it
        # compiles to, which has an identity of its own.
        identities = {
            identity
            for identity, source in repository.source_documents.items()
            if identity.item == item and not source.is_validation
        }
        types = {
            identity: _object_type_of(repository, identity) for identity in identities
        }
        roles = {identity: "data" for identity in identities}
        # Loads and validations alike: a catalogue certifying only the loads
        # would describe an estate whose Tests were never installed, and every
        # validation run against it would report them missing.
        for artefact in item_runtime_artefacts(repository, item=item):
            identities.add(artefact.identity)
            types[artefact.identity] = artefact.object_type
            roles[artefact.identity] = artefact.role
        signatures = declared_signatures(repository, identities)
        return cls.from_registry_rows(
            *(
                registry_row(
                    identity,
                    object_type=types[identity],
                    object_role=roles[identity],
                    signature=signatures[identity],
                )
                for identity in sorted(identities, key=str)
            ),
            item=item,
        )


def _object_type_of(repository, identity) -> str:
    """What the Registry would have recorded this document as."""

    kind = repository.source_documents[identity].kind
    return {"Table": "table", "View": "view", "Folder": "folder"}[str(kind)]


def installed_catalogue(
    repository, bindings: ItemBindings, *, session=None
) -> Catalogue:
    """The whole catalogue a successful build of this estate would have left.

    Where `FixtureCatalogue.from_repository` gives one item's Registry, this
    gives the estate: every item's dictionaries, its dependencies, its shortcuts,
    the shortcut destinations the build certified, and the Installation rows that
    say which physical target each logical item is bound to.

    That last part is what load orchestration cannot do without. A build is
    handed its bindings; an orchestrator is handed physical target names and has
    to discover the bindings from the catalogue, so a fixture that omitted them
    would let a reverse-binding claim pass without a binding to reverse.

    Composed from production constructors, `Catalogue.from_repository` and
    `for_targets`, rather than hand-written rows, so the fixture cannot drift
    from what a build actually publishes.
    """

    from weaver.catalogue.state import for_targets
    from weaver.catalogue.tables import INSTALLATION

    desired = Catalogue.from_repository(repository)
    identities = set()
    target_kinds = {}
    for model in repository.items:
        item = model.identity
        identities.update(
            identity
            for identity in repository.source_documents
            if identity.item == item
        )
        identities.update(
            artefact.identity
            for artefact in item_runtime_artefacts(repository, item=item)
        )
        # A shortcut destination is certified by the build that made it, and it
        # has to be here: it is the name the consuming item reads through, so an
        # estate without it has a dependency pointing at nothing. Every kind,
        # because a physical shortcut is installed here exactly as a logical one.
        identities.update(
            declaration.destination
            for declaration in repository.shortcuts
            if declaration.owner == item
        )
        identities.update(
            shortcut.destination
            for shortcut in repository.logical_shortcuts
            if shortcut.destination.item == item
        )
        target_kinds[item] = (
            "warehouse" if item.item_type == "Warehouse" else "lakehouse"
        )
    bound = for_targets(desired, repository, identities, target_kinds)

    physical = {
        binding.item: (
            binding.target.lakehouse.name
            if isinstance(binding.target, LakehouseBinding)
            else binding.target.warehouse.name
        )
        for binding in bindings.entries
    }
    rows = {}
    for item, tables in bound.rows.items():
        target_name = physical.get(item)
        if target_name is None:
            # An unbound item is not installed, so it contributes no rows at all
            # , exactly as a build that never targeted it would leave things.
            continue
        rows[item] = {
            **{name: tuple(table_rows) for name, table_rows in tables.items()},
            INSTALLATION.name: (
                {
                    "item_type": item.item_type,
                    "item_name": item.item_name,
                    "target_name": target_name,
                    "weaver_version": "0.0.0+test",
                    "signature": "installation-signature",
                },
            ),
        }
    # A catalogue a run writes to, because a run records what it did into it.
    # Through the Session where there is one, so a claim about the statements a
    # run submits still sees them; a recorder otherwise.
    from support.catalogues import Recording

    from weaver.catalogue.writer import writer_for

    return Catalogue(
        rows=rows,
        writer=Recording() if session is None else writer_for(session),
    )


# --- physical state ---------------------------------------------------------


def target_inventory(
    *,
    target_id: str = "target-1",
    kind: str = "lakehouse",
    target_name: str = "Sales_LH",
    schemas: tuple[str, ...] = (),
    folder_schemas: tuple[str, ...] = (),
    folders: tuple[str, ...] = (),
    tables: tuple[str, ...] = (),
    views: tuple[str, ...] = (),
    files: tuple[str, ...] = (),
    procedures: tuple[str, ...] = (),
) -> TargetInventory:
    """Exactly the physical state asked for, and nothing else.

    Every default is empty, so a test states the estate it means rather than
    inheriting one. "Empty inventory" is then a claim a test makes explicitly,
    which matters: an unexpectedly populated inventory is what makes a prune
    test pass for the wrong reason.
    """

    return TargetInventory(
        target_id=target_id,
        kind=kind,
        target_name=target_name,
        schemas=schemas,
        folder_schemas=folder_schemas,
        folders=folders,
        tables=tables,
        views=views,
        files=files,
        procedures=procedures,
    )


class FixtureInventory(TargetInventory):
    """The production `TargetInventory`, with the ways a test needs to fill one.

    Safe to populate freely in a way a catalogue is not, and the asymmetry is
    worth knowing: a wrong inventory degrades a decision, prune removes
    nothing, a schema is not created, whereas a wrong catalogue *forges a
    guarantee* that an object was installed. Both belong in tests, but only one
    of them would be dangerous on the production class.
    """

    @classmethod
    def from_repository(
        cls,
        repository,
        *,
        item: str | WeaverItemId = ITEM,
        target_id: str = "target-1",
        kind: str = "lakehouse",
        target_name: str = "Sales_LH",
    ) -> TargetInventory:
        """The physical state a successful build of this item would have left.

        The "already built, nothing to do" inventory. Reaching it previously
        meant standing up a Lakehouse and building into it, purely so a prune
        test could assert that a declared object is spared.

        Built from the documents rather than from `managed_sets`.
        The two hold the same objects, but `managed_sets` folds case for
        comparison while a real inventory reports the names the target actually
        has. Since the point of this class is to stand in for a real read, it
        keeps declared case, otherwise it would be easier to satisfy than the
        thing it imitates.
        """

        if isinstance(item, str):
            item = item_id(item)
        warehouse = item.item_type == WAREHOUSE
        documents = [
            document
            for identity, document in repository.source_documents.items()
            if identity.item == item
        ]

        def qualified(*, of_kind: str, files: bool) -> tuple[str, ...]:
            return tuple(
                sorted(
                    document.qualified
                    for document in documents
                    if (document.kind == "Folder") is files
                    and (files or str(document.kind) == of_kind)
                )
            )

        def schemas_of(names: tuple[str, ...]) -> tuple[str, ...]:
            return tuple(sorted({name.split(".", 1)[0] for name in names}))

        tables = qualified(of_kind="Table", files=False)
        views = qualified(of_kind="View", files=False)
        folders = qualified(of_kind="Folder", files=True)
        artefacts = item_runtime_artefacts(repository, item=item)
        if warehouse and str(item) != str(BUILTIN_ITEM):
            # A built Warehouse holds the standard Weaver catalogue surface
            # under its own names, so a generated procedure can read its own
            # bookmark and record what it did.
            views = tuple(
                sorted(
                    views
                    + tuple(
                        f"{CATALOGUE_SCHEMA}.{table.name}"
                        for table in STANDARD_SURFACE_TABLES
                    )
                )
            )
        return cls(
            target_id=target_id,
            kind=kind,
            target_name=target_name,
            schemas=tuple(
                sorted(
                    set(schemas_of(tables + views))
                    | set(load_schemas(artefacts) if warehouse else ())
                )
            ),
            folder_schemas=schemas_of(folders),
            folders=folders,
            tables=tables,
            views=views,
            files=tuple(
                sorted(
                    artefact.target_path
                    for artefact in artefacts
                    if artefact.object_type == FILE_TYPE
                )
            ),
            procedures=tuple(
                sorted(
                    artefact.identity.object_id.qualified
                    for artefact in artefacts
                    if artefact.object_type == PROCEDURE_TYPE
                )
            ),
            runtime_references=(
                ()
                if warehouse or str(item) == str(BUILTIN_ITEM)
                else tuple(sorted(table.name for table in STANDARD_SURFACE_TABLES))
            ),
        )


def bound_target(
    *,
    id: str = "target-1",
    kind: str = "lakehouse",
    item_id: str = "Sales_LH",
    item_name: str | None = None,
    workspace_name: str = WORKSPACE,
    logical_item_name: str | None = "Sales",
    logical_item_type: str | None = "Lakehouse",
    **extra,
) -> BoundTarget:
    # Both halves of the physical identity, as a real one carries: the id
    # resolves the item, and the display names are what four-part Spark naming
    # is spelled with.
    return BoundTarget(
        id=id,
        kind=kind,
        item_id=item_id,
        item_name=item_name if item_name is not None else item_id,
        workspace_name=workspace_name,
        logical_item_name=logical_item_name,
        logical_item_type=logical_item_type,
        **extra,
    )


def catalogue_target(
    *, id: str = "control-warehouse-Weaver", item_id: str = "Weaver", **extra
) -> BoundTarget:
    """The Warehouse the Weaver catalogue lives in, as the planner receives it."""

    return bound_target(
        id=id,
        kind="warehouse",
        item_id=item_id,
        logical_item_name="_weaver",
        logical_item_type="Warehouse",
        **extra,
    )


def catalogue_inventory(
    *, holding: bool = True, target_id: str = "control-warehouse-Weaver"
):
    """The catalogue Warehouse's own inventory, as the planner receives it.

    ``holding`` says whether it physically holds the runtime tables. That is what
    decides the two things the build that creates the catalogue cannot do: a
    Lakehouse shortcut has nothing to point at, and runtime-state reconciliation
    has no table to reconcile.
    """

    from weaver.catalogue.tables import (
        CATALOGUE_SCHEMA,
        PROJECTED_TABLES,
        STANDARD_SURFACE_TABLES,
    )

    held = tuple(
        f"{CATALOGUE_SCHEMA}.{table.name}"
        for table in (
            *PROJECTED_TABLES,
            *(STANDARD_SURFACE_TABLES if holding else ()),
        )
    )
    return target_inventory(
        target_id=target_id,
        kind="warehouse",
        target_name="Weaver",
        schemas=(CATALOGUE_SCHEMA,),
        tables=held,
    )


#: The logical item the Weaver catalogue is built as, which is what the planner
#: keys its inventory by.
CATALOGUE_ITEM = WeaverItemId("Warehouse", "_weaver")


# --- repositories -------------------------------------------------------------


def _write(root: Path, relative: str, text: str) -> None:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def schema_document(name: str) -> str:
    return f"Schema ID: {name}\nDescription: {name} objects.\n"


def lakehouse_table(object_id: str, *, columns: Mapping[str, str] | None = None) -> str:
    columns = columns or {"CustomerId": "string"}
    body = "\n".join(f"  {name}: {kind}" for name, kind in columns.items())
    class_name = object_id.replace(".", "__")
    return f'''\
"""
Table ID: {object_id}
Description: A declared table.
Lineage: A source system.
Primary key: {next(iter(columns))}
Schema:
{body}
"""
from weaver import Table

class {class_name}(Table):
    def read(self):
        return [], []
'''


def lakehouse_test(object_id: str, *, primary_key: str = "CustomerId") -> str:
    """A declared Python Test: two sides, both empty, so it declares and no more."""

    class_name = object_id.replace(".", "__")
    return f'''\
"""
Test ID: {object_id}
Description: A declared test.
Primary key: {primary_key}
"""
from weaver import Test

class {class_name}(Test):
    def expected(self):
        return None

    def actual(self):
        return None
'''


def warehouse_table(
    object_id: str,
    *,
    select: str = "select cast(1 as int) as CustomerId",
    primary_key: str = "CustomerId",
    identity: str | None = None,
    has_load_procedure: bool = True,
) -> str:
    """A Warehouse table, whose query defines its schema.

    Unlike a Lakehouse table, the columns are not listed, the result set is the
    declaration, and Weaver infers types from it. A caller wanting particular
    physical types casts them in the select, which is also how the real fixtures
    read.

    ``identity`` names an engine-generated surrogate key: a column the query does
    not produce, which build declares ``identity`` and the Warehouse
    assigns, which is why it needs an engine to confirm.
    """

    identity_line = f"Identity: {identity}\n\n" if identity else ""
    # A table something other than Weaver populates says so, and then has no
    # load artefact and no bookmark.
    load_line = "" if has_load_procedure else "Has load procedure: false\n\n"
    return f"""\
/*
Table ID: {object_id}

Description: A declared table.

Lineage: A source system.

Primary key: {primary_key}

{load_line}{identity_line}*/
{select}
"""


def warehouse_view(object_id: str, *, select: str, depends_on: str) -> str:
    return f"""\
/*
View ID: {object_id}

Description: A declared view.

Lineage: ${depends_on}
*/
{select}
"""


def spark_view(object_id: str, *, depends_on: str) -> str:
    return f"""\
/*
View ID: {object_id}

Description: A declared view.

Lineage: ${depends_on}

Dependencies:
  - {depends_on}
*/
select 1 as CustomerId from {depends_on}
"""


def folder_document(object_id: str) -> str:
    class_name = object_id.replace(".", "__")
    return f'''\
"""
Folder ID: {object_id}
Description: A declared folder.
Lineage: A source system.
File key: "*.csv"
"""
from weaver import Folder

class {class_name}(Folder):
    def read(self):
        return self.staging_folder(), []
'''


def logical_shortcuts(consumer: str, **references: str) -> tuple[str, str]:
    """Where a consumer declares its logical shortcuts, and what it says.

    Returns the repository-relative path and the file's text, so a caller states
    the shortcuts and not which surface the item type declares them on.
    """

    if consumer.startswith("Warehouse/"):
        body = "\n".join(
            f"  {consumer}/{local}: {source}"
            for local, source in sorted(references.items())
        )
        return f"{consumer}/shortcuts.yml", f"logical:\n{body}\n"

    declarations = "\n\n".join(
        f"{local.replace('.', '__')} = Shortcut(\n"
        f'    shortcut_type="table",\n'
        f'    target_type="logical",\n'
        f'    target="{source}",\n'
        f")"
        for local, source in sorted(references.items())
    )
    return (
        f"{consumer}/shortcuts.py",
        "from weaver import Shortcut\n\n" + declarations + "\n",
    )


def physical_folder_shortcut(
    owner: str, *, name: str, target: str, workspace: str
) -> tuple:
    """Where a Lakehouse declares a physical folder shortcut, and what it says.

    ``name`` is the ``Schema.Object`` the destination is known by in ``owner``.
    The target names a path beneath another item's ``Files``, which Weaver does
    not manage, so this destination is not a node in the dependency graph.
    """

    return (
        f"{owner}/shortcuts.py",
        "from weaver import Shortcut\n\n"
        f"{name.replace('.', '__')} = Shortcut(\n"
        '    shortcut_type="folder",\n'
        '    target_type="physical",\n'
        f'    target="{target}",\n'
        f'    workspace="{workspace}",\n'
        ")\n",
    )


def physical_schema_shortcut(owner: str, *, target: str, workspace: str) -> tuple:
    """Where a Lakehouse declares a physical schema shortcut, and what it says.

    A schema shortcut must be physical: what appears inside belongs to the item
    it points at, so Weaver binds objects rather than namespaces.
    """

    schema = target.rsplit("/", 1)[-1]
    return (
        f"{owner}/shortcuts.py",
        "from weaver import Shortcut\n\n"
        f"{schema} = Shortcut(\n"
        '    shortcut_type="schema",\n'
        '    target_type="physical",\n'
        f'    target="{target}",\n'
        f'    workspace="{workspace}",\n'
        ")\n",
    )


def shortcut_repository(
    root: Path,
    *,
    producer: str = "Lakehouse/Raw",
    consumer: str = "Lakehouse/Curated",
    schema: str = "DWG",
    consumer_view: bool = True,
):
    """Two items, the second shortcutting a table in the first.

    The smallest repository that can express a cross-item shortcut, which is the
    one shape a single-item fixture cannot reach: a shortcut that did not cross
    would not be one. The consumer also builds a view over the shortcut name, so
    the ordering claim, the consumer's whole group waits for the producer's,
    has something to order.
    """

    _write(root, f"{producer}/schemas/{schema}.yml", schema_document(schema))
    _write(
        root,
        f"{producer}/Tables/{schema}__Customer.py",
        lakehouse_table(f"{schema}.Customer"),
    )
    _write(root, f"{consumer}/schemas/{schema}.yml", schema_document(schema))
    _write(
        root,
        *logical_shortcuts(
            consumer,
            **{f"{schema}.PortableCustomer": f"{producer}/Tables/{schema}.Customer"},
        ),
    )
    if consumer_view:
        # A Warehouse consumer reads its shortcut through T-SQL over the producer's
        # SQL endpoint, so the view it builds is spelled differently, and for a
        # probe that only needs the shortcut itself, no view is wanted at all.
        if consumer.startswith("Warehouse/"):
            _write(
                root,
                f"{consumer}/{schema}.CustomerName.sql",
                warehouse_view(
                    f"{schema}.CustomerName",
                    select=f"select CustomerId from [{schema}].[PortableCustomer]",
                    depends_on=f"{schema}.PortableCustomer",
                ),
            )
        else:
            _write(
                root,
                f"{consumer}/Tables/{schema}.CustomerName.sql",
                spark_view(
                    f"{schema}.CustomerName", depends_on=f"{schema}.PortableCustomer"
                ),
            )
    return parse_item_repository(Location(str(root)))


SPARK_TABLE = """\
/*
Table ID: {object_id}

Description: A Spark SQL table.

Lineage: Declared for a test.

Dependencies: []

Schema:
  CustomerId: string
*/
select cast(null as string) as CustomerId
 where 1 = 0;
"""


def full_estate(root: Path):
    """One repository holding every artefact Weaver can install.

    A table, a view, a folder, a deployed module, a generated Spark SQL load
    file, and a generated stored procedure, across both item types, because the
    two physical sides are not symmetric and a Lakehouse-only estate stops being
    representative exactly where that asymmetry starts.

    Shared because three separate claims need completeness rather than a narrow
    subject: that the catalogue registers every artefact kind, that a build's
    declared effect covers every action it emits, and that a build converges. A
    narrower fixture cannot answer any of them.
    """

    for relative, text in {
        f"{ITEM}/schemas/DWG.yml": schema_document("DWG"),
        f"{ITEM}/schemas/Raw.yml": schema_document("Raw"),
        f"{ITEM}/Tables/DWG__Customer.py": lakehouse_table("DWG.Customer"),
        f"{ITEM}/Tables/DWG.Summary.sql": SPARK_TABLE.format(object_id="DWG.Summary"),
        f"{ITEM}/Tables/DWG.ActiveCustomer.sql": spark_view(
            "DWG.ActiveCustomer", depends_on="DWG.Customer"
        ),
        f"{ITEM}/Files/Raw__CustomerCsv.py": folder_document("Raw.CustomerCsv"),
        f"{ITEM}/lib/dates.py": "def parse(value):\n    return value\n",
        f"{WAREHOUSE_ITEM}/schemas/Sales.yml": schema_document("Sales"),
        f"{WAREHOUSE_ITEM}/Sales.Customer.sql": warehouse_table("Sales.Customer"),
        f"{WAREHOUSE_ITEM}/Sales.Live.sql": warehouse_view(
            "Sales.Live", select="select 1 as CustomerId", depends_on="Sales.Customer"
        ),
    }.items():
        _write(root, relative, text)
    return parse_item_repository(Location(str(root)))


def estate_bindings():
    """Both items of :func:`full_estate`, bound to neutral physical targets."""

    return item_bindings((ITEM, "Sales_LH"), (WAREHOUSE_ITEM, "Reporting_WH"))


def estate_inventories(repository, *, empty: bool = False):
    """Each bound target's inventory, either as built or as nothing at all."""

    bound = {b.item: b.to_bound_target() for b in estate_bindings().entries}
    made = {}
    for item, kind in (
        (ITEM, "lakehouse"),
        (WAREHOUSE_ITEM, "warehouse"),
    ):
        identity = item_id(item)
        arguments = dict(
            target_id=bound[identity].id, kind=kind, target_name=bound[identity].name
        )
        made[identity] = (
            target_inventory(**arguments)
            if empty
            else FixtureInventory.from_repository(repository, item=item, **arguments)
        )
    return made


#: The two items of :func:`load_estate`, and the physical targets they bind to.
LOAD_PRODUCER = "Lakehouse/Raw"
LOAD_CONSUMER = "Warehouse/Reporting"
#: The consumer's bound reference to the producer's table, as its own surface
#: spells it.
BOUND_CONSUMER_PATH, BOUND_CONSUMER_TEXT = logical_shortcuts(
    LOAD_CONSUMER, **{"Sales.Order": f"{LOAD_PRODUCER}/Tables/Sales.Order"}
)
LOAD_PRODUCER_TARGET = "Raw_LH"
LOAD_CONSUMER_TARGET = "Reporting_WH"


def load_estate(root: Path):
    """The canonical physical load scenario, and the smallest estate holding it.

    .. code-block:: text

        Raw Delta table  →  endpoint refresh  →  Warehouse shortcut consumer
                                              →  downstream Warehouse table

    Every element earns its place. ``Sales.Order`` is the upstream Delta load;
    ``Sales.Daily`` proves a second dispatch kind and an ordinary within-item
    edge; ``Sales.Export`` proves the folder kind. On the Warehouse side the
    shortcut is what makes the dependency cross, ``Sales.Summary`` is what
    consumes it, and ``Sales.Live`` is a view, no load work of its own, but a
    conduit a downstream table still depends through.

    Deliberately mixed across both physical sides, because a Lakehouse-only
    estate cannot express the one crossing that needs a barrier: a Delta table
    read as SQL through an endpoint that has not caught up.
    """

    for relative, text in {
        f"{LOAD_PRODUCER}/schemas/Sales.yml": schema_document("Sales"),
        f"{LOAD_PRODUCER}/Tables/Sales__Order.py": lakehouse_table(
            "Sales.Order", columns={"OrderId": "string", "Amount": "decimal(18,2)"}
        ),
        f"{LOAD_PRODUCER}/Tables/Sales.Daily.sql": SPARK_TABLE_WITH_DEPENDENCY.format(
            object_id="Sales.Daily", depends_on="Sales.Order"
        ),
        f"{LOAD_PRODUCER}/Files/Sales__Export.py": folder_document("Sales.Export"),
        f"{LOAD_CONSUMER}/schemas/Sales.yml": schema_document("Sales"),
        BOUND_CONSUMER_PATH: BOUND_CONSUMER_TEXT,
        f"{LOAD_CONSUMER}/Sales.Summary.sql": warehouse_table(
            "Sales.Summary",
            select="select OrderId, Amount from [Sales].[Order]",
            primary_key="OrderId",
        ),
        f"{LOAD_CONSUMER}/Sales.Live.sql": warehouse_view(
            "Sales.Live",
            select="select OrderId from [Sales].[Summary]",
            depends_on="Sales.Summary",
        ),
    }.items():
        _write(root, relative, text)
    return parse_item_repository(Location(str(root)))


def load_estate_bindings() -> ItemBindings:
    """Both items of :func:`load_estate`, bound to neutral physical targets."""

    return item_bindings(
        (LOAD_PRODUCER, LOAD_PRODUCER_TARGET), (LOAD_CONSUMER, LOAD_CONSUMER_TARGET)
    )


SPARK_TABLE_WITH_DEPENDENCY = """\
/*
Table ID: {object_id}

Description: A Spark SQL table over another table in its own item.

Lineage: ${depends_on}

Dependencies:
  - {depends_on}

Schema:
  OrderId: string
*/
select OrderId from {depends_on};
"""


def single_document_repository(
    root: Path,
    *,
    item: str = ITEM,
    schemas: tuple[str, ...] = ("DWG",),
    documents: Mapping[str, str],
):
    """The smallest legal repository holding the documents given.

    One item, its schema declarations, and the files named. Nothing else. The
    point is that a parse, a DDL or an action-rendering claim is made against a
    repository whose entire content is visible in the test that wrote it.

    ``documents`` maps a relative filename to its content, so a caller composes
    from the document builders above and stays explicit about what exists. Every
    schema a document uses must be declared, so ``schemas`` widens when a fixture
    reaches beyond ``DWG``, a Files folder under ``Raw``, say.

    One thing to know before asserting over ``repository.source_documents``: a
    parsed repository always carries the builtin ``Warehouse/_weaver``
    catalogue documents as well as the ones written here. Look documents up by
    identity rather than iterating, or the answer will be about the catalogue.
    """

    for schema in schemas:
        _write(root, f"{item}/schemas/{schema}.yml", schema_document(schema))
    for relative, content in documents.items():
        _write(root, f"{item}/{relative}", content)
    return parse_item_repository(Location(str(root)))


# --- actions and bundles ------------------------------------------------------


def build_action(
    *,
    id: str = "object-Lakehouse--Sales--Tables--DWG.Customer",
    kind: str = "build_table",
    resource_node_id: str = "Lakehouse/Sales/Tables/DWG.Customer",
    executor: str = "spark_sql",
    payload: str | None = None,
    payload_sha256: str | None = None,
) -> InstallAction:
    return InstallAction(
        id=id,
        kind=kind,
        resource_node_id=resource_node_id,
        executor=executor,
        payload=payload,
        payload_sha256=payload_sha256,
    )


def single_action_bundle(
    location: Location,
    *,
    store: FilesystemStore,
    action: InstallAction,
    payload: bytes | None = None,
    target: BoundTarget | None = None,
    description: str = "one action",
):
    """The smallest valid bundle: one target, one sequence, one batch, one action.

    Installer claims, sequencing, failure semantics, reporting, payload loading,
    are about the installer, not about whatever repository happened to produce
    a bundle. Generating a real one to test them makes the planner a dependency
    of every installer failure.
    """

    target = target or bound_target()
    payloads = {}
    if payload is not None and action.payload is not None:
        payloads[action.payload] = payload
    plan = BuildPlan(
        format_version=SUPPORTED_FORMAT_VERSION,
        bundle_id="",
        repository_name="weaver_items",
        repository_signature="repository-signature",
        targets=(target,),
        sequences=(
            _sequence(description=description, target_id=target.id, action=action),
        ),
    )
    plan = _with_identity(plan)
    return write_bundle(location, plan=plan, payloads=payloads, store=store)


def _sequence(*, description: str, target_id: str, action: InstallAction):
    from weaver.build_bundle.models import BuildSequence

    return BuildSequence(
        number=1,
        description=description,
        batches=(BuildBatch(id="batch-1", target_id=target_id, actions=(action,)),),
    )


def _with_identity(plan: BuildPlan) -> BuildPlan:
    from dataclasses import replace

    return replace(plan, bundle_id=compute_bundle_id(plan))


# --- execution ----------------------------------------------------------------
#
# Fakes, not mocks: each records what it was asked to do and answers plainly, so
# a test asserts on the *statement that reached the engine* rather than on a call
# signature. That is the claim worth making about an executor. It adds no logic
# of its own, and it is the one a mock's `assert_called_with` would obscure.


class FakeSpark:
    """A Spark session that records statements instead of running them."""

    def __init__(self, tables=(), schemas=()) -> None:
        self.statements: list[str] = []
        self.conf = _FakeConf()
        self.catalog = _FakeCatalog(schemas, tables)

    def sql(self, statement: str):
        self.statements.append(statement)
        return self

    def collect(self):
        return []


class _FakeConf:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}

    def get(self, key, default=None):
        return self.values.get(key, default)

    def set(self, key, value) -> None:
        self.values[key] = value


class _FakeCatalog:
    def __init__(self, schemas=(), tables=()) -> None:
        self._schemas = set(schemas)
        self._tables = set(tables)

    def databaseExists(self, name: str) -> bool:
        return name in self._schemas

    def tableExists(self, name: str) -> bool:
        return name in self._tables


class FakeSql:
    """A SQL capability that records the scripts it was given."""

    def __init__(self, error: Exception | None = None) -> None:
        self.scripts: list[str] = []
        self._error = error

    def execute_script(self, script: str) -> None:
        self.scripts.append(script)
        if self._error is not None:
            raise self._error


def lakehouse_catalogue(spark, resolver, item: str):
    """Catalogue operations against one Lakehouse, as a caller with Spark builds them."""

    from weaver.spark import SparkCatalogue

    return SparkCatalogue(spark, resolver.spark_destination(ItemRef(item)))


def spark_destination(item: str = "Sales_LH", *, workspace: str = WORKSPACE):
    """How Fabric Spark addresses one Lakehouse."""

    from weaver.spark import FabricSparkTarget

    return FabricSparkTarget(workspace=workspace, lakehouse=item)


#: Distinguishes "caller said nothing, give the usual one" from "caller means
#: none". A Warehouse target resolves to no Spark destination, and that
#: is a state executors must handle, so it has to be expressible.
DEFAULT = object()


def resolved_target(
    *,
    bound: BoundTarget | None = None,
    destination=DEFAULT,
    location=None,
    lakehouse: str = "Sales_LH",
):
    from weaver.build_bundle.executors.base import ResolvedTarget

    bound = bound or bound_target()
    return ResolvedTarget(
        bound=bound,
        lakehouse=ItemRef(lakehouse),
        location=location,
        destination=spark_destination() if destination is DEFAULT else destination,
    )


def spark_sql_capability(spark):
    """A Session's Spark SQL capability, backed by a session that is right here.

    The same two things a host running in a Spark session does: hold one
    identifier-case scope over the statements, and answer with the last one's
    rows. A test supplying a ``spark`` therefore gets a context that behaves as
    the in-session position does, and can still assert on what reached it.
    """

    from weaver.build_bundle.executors.spark_case import exact_identifier_case
    from weaver.sessions.base import run_spark_statements

    def many(statements, *, exact_case: bool = False):
        ordered = list(statements)
        if not ordered:
            return []
        with exact_identifier_case(spark, enabled=exact_case):
            return run_spark_statements(spark, ordered)

    def one(statement: str, *, exact_case: bool = False):
        return many([statement], exact_case=exact_case)

    return one, many


def installation_context(
    *,
    spark=None,
    sql=None,
    target=None,
    store=None,
    resolver=None,
    targets=None,
    build_datetime: str | None = None,
    snapshot: Location | None = None,
):
    """The one target and the runtime services an executor is handed.

    Everything defaults to absent rather than to something plausible, because an
    executor's behaviour when a capability is missing is a real claim, a
    ``tsql`` action with no SQL executor must fail saying so, not fail obscurely
    somewhere deeper.

    A supplied ``spark`` also supplies the Spark SQL capability over it, because
    that is what a host with a session of its own hands an executor.
    """

    from weaver.build_bundle.executors.base import InstallationContext

    one, many = (None, None) if spark is None else spark_sql_capability(spark)
    return InstallationContext(
        resolver=resolver,
        store=store,
        target=target if target is not None else resolved_target(),
        sql=sql,
        spark_sql=one,
        spark_sql_batch=many,
        targets=targets or {},
        build_datetime=build_datetime,
    )


def warehouse_context(*, sql=None, **extra):
    """A Warehouse batch's context: reached over TDS, so no Spark address at all."""

    return installation_context(
        sql=sql if sql is not None else FakeSql(),
        target=resolved_target(
            bound=bound_target(kind="warehouse", item_id="Reporting_WH"),
            destination=None,
        ),
        **extra,
    )


def item_bindings(*pairs: tuple[str, str]) -> ItemBindings:
    """``("Lakehouse/Sales", "Sales_LH")`` pairs, typed by the logical item."""

    bindings = []
    for logical, physical in pairs:
        item = WeaverItemId.parse(logical)
        # Four-part Spark naming is spelled with the workspace's display name,
        # so a binding that carried none could not name what it builds.
        binding = (
            LakehouseBinding(ItemRef(physical), workspace_name=WORKSPACE)
            if item.item_type == "Lakehouse"
            else WarehouseBinding(ItemRef(physical), workspace_name=WORKSPACE)
        )
        bindings.append(ItemBinding(item, binding))
    return ItemBindings(tuple(bindings))
