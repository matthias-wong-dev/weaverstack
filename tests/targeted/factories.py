"""Constructors for the smallest input each seam actually needs.

The old suite's habit was to reach for a complete estate — a parsed repository, a
projected catalogue, a generated bundle — whatever the subject was. That made
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

from weaver import ItemRef, LocalStore, Location
from weaver.build_bundle import (
    BuildAction,
    BuildBatch,
    BuildPlan,
    ItemBinding,
    ItemBindings,
    LakehouseBinding,
    WarehouseBinding,
    compute_bundle_id,
    write_bundle,
)
from weaver.build_bundle.prune import TargetInventory
from weaver.declaration.metadata import DELTA_TARGET, FOLDER_TARGET, SQL_TARGET
from weaver.build_bundle.stages import PlannedStage
from weaver.build_bundle.targets import BoundTarget
from weaver.catalogue.state import Catalogue, RegisteredDocument
from weaver.catalogue.tables import REGISTRY
from weaver.declaration import parse_item_repository
from weaver.declaration.model import WeaverDocumentId, WeaverItemId
from weaver.etl import (
    FILE_TYPE,
    PROCEDURE_TYPE,
    item_load_artefacts,
    load_schemas,
)

#: Neutral names, per the environment-neutrality rule: no product, workspace or
#: tenant may be inferable from a fixture.
ITEM = "Lakehouse/Sales"
WAREHOUSE_ITEM = "Warehouse/Reporting"


# --- identities ---------------------------------------------------------------


def document_id(text: str) -> WeaverDocumentId:
    """``Lakehouse/Sales/DWG.Customer``, or the item-relative spelling."""

    if text.count("/") == 0:
        text = f"{ITEM}/{text}"
    return WeaverDocumentId.parse(text)


def item_id(text: str = ITEM) -> WeaverItemId:
    return WeaverItemId.parse(text)


# --- catalogue --------------------------------------------------------------


def registered_document(
    identity: str | WeaverDocumentId,
    *,
    object_type: str = "table",
    signature: str = "signature-1",
    build_epoch=None,
) -> RegisteredDocument:
    """One validated Registry row, as incremental selection consumes it.

    This is all `determine_impact` and `select_build` ever look at, so a test
    about signature comparison needs nothing else — no catalogue, no projection,
    no Spark. Constructing one directly is the difference between a test that
    fails because a signature comparison is wrong and one that fails because
    anything at all in a catalogue projection is wrong.
    """

    if isinstance(identity, str):
        identity = document_id(identity)
    return RegisteredDocument(
        identity=identity,
        object_type=object_type,
        signature=signature,
        build_epoch=build_epoch,
    )


def registry_row(
    identity: str | WeaverDocumentId,
    *,
    object_type: str = "table",
    object_role: str = "data",
    signature: str = "signature-1",
    build_epoch=None,
) -> dict:
    """One Registry row in its stored form, keyed as the catalogue writes it."""

    if isinstance(identity, str):
        identity = document_id(identity)
    schema = identity.object_id.schema
    return {
        "item_type": identity.item.item_type,
        "item_name": identity.item.item_name,
        # Only a Folder carries the prefix. A load artefact's schema is already
        # the real one — a path beneath Files, or a Warehouse schema.
        "schema_name": f"Files/{schema}" if identity.is_files else schema,
        "object_name": identity.object_id.object,
        "object_type": object_type,
        "object_role": object_role,
        "signature": signature,
        "build_epoch": build_epoch,
    }


class FixtureCatalogue(Catalogue):
    """The production `Catalogue`, with the ways a test wants to populate one.

    A subclass rather than a separate type, deliberately. Every build decision
    runs against the real class; what changes is only *where the state came
    from*. Beginning from a hand-written Registry row is the same move as
    installing a frozen bundle instead of building one from a repository — a
    later starting point on the real thing, not a stand-in for it.

    These constructors are here and not on `Catalogue` because a Registry row
    means work *succeeded*: it is written last in a build, and the planner has a
    whole `uncertified` mechanism to withhold rows for work that was not done. A
    production method that manufactured rows from declarations would be a way to
    forge that guarantee, and eventually something would call it on a build path.
    """

    @classmethod
    def from_registry_rows(cls, *rows, item: str | WeaverItemId = ITEM) -> "Catalogue":
        """A catalogue holding exactly the Registry rows given.

        Physically incomplete on purpose — no other catalogue table is present.
        Right for reconciliation and selection, which read the Registry alone;
        wrong for testing the catalogue *read*, which is a Spark-boundary claim
        and has to see a complete catalogue.
        """

        if isinstance(item, str):
            item = item_id(item)
        return cls(
            rows={item: {REGISTRY.name: tuple(rows)}},
            present_tables=frozenset({REGISTRY.name}),
        )

    @classmethod
    def holding(cls, item: str | WeaverItemId = ITEM, **tables) -> "Catalogue":
        """A catalogue holding the named tables' rows — Registry and beyond.

        `from_registry_rows` is the common case; this is for the claims that are
        *about* more than the Registry, such as the order in which a build
        removes a certification and the description behind it. A claim is only
        collected where a row actually exists, so a catalogue with no dictionary
        rows cannot demonstrate dictionary ordering.
        """

        if isinstance(item, str):
            item = item_id(item)
        return cls(
            rows={item: {name: tuple(rows) for name, rows in tables.items()}},
            present_tables=frozenset(tables),
        )

    @classmethod
    def certifying(cls, *registered: RegisteredDocument) -> "Catalogue":
        """A catalogue certifying exactly these documents, with no row data.

        For tests whose subject is downstream of the Registry — selection,
        alias staleness, planning — where the rows themselves are never read.
        """

        return cls(
            rows={},
            registered={document.identity: document for document in registered},
        )

    @classmethod
    def from_repository(cls, repository, *, item: str | WeaverItemId = ITEM) -> "Catalogue":
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
        identities = {
            identity
            for identity in repository.source_documents
            if identity.item == item
        }
        types = {
            identity: _object_type_of(repository, identity) for identity in identities
        }
        for artefact in item_load_artefacts(repository, item=item):
            identities.add(artefact.identity)
            types[artefact.identity] = artefact.object_type
        signatures = declared_signatures(repository, identities)
        return cls.from_registry_rows(
            *(
                registry_row(
                    identity,
                    object_type=types[identity],
                    object_role="load" if identity.is_load_artefact else "data",
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
    which matters: an unexpectedly *populated* inventory is what makes a prune
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
    """The production `TargetInventory`, with the ways a test wants to fill one.

    Safe to populate freely in a way a catalogue is not, and the asymmetry is
    worth knowing: a wrong inventory *degrades a decision* — prune removes
    nothing, a schema is not created — whereas a wrong catalogue *forges a
    guarantee* that an object was installed. Both belong in tests, but only one
    of them would be dangerous on the production class.
    """

    @classmethod
    def from_repository(
        cls,
        repository,
        *,
        item: str | WeaverItemId = ITEM,
        target_kind: str = DELTA_TARGET,
        target_id: str = "target-1",
        kind: str = "lakehouse",
        target_name: str = "Sales_LH",
    ) -> TargetInventory:
        """The physical state a successful build of this item would have left.

        The "already built, nothing to do" inventory. Reaching it previously
        meant standing up a Lakehouse and building into it, purely so a prune
        test could assert that a declared object is spared.

        Built from the documents rather than from `managed_sets`, deliberately.
        The two hold the same objects, but `managed_sets` folds case for
        comparison while a real inventory reports the names the target actually
        has. Since the point of this class is to stand in for a real read, it
        keeps declared case — otherwise it would be easier to satisfy than the
        thing it imitates.
        """

        if isinstance(item, str):
            item = item_id(item)
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
                    if (document.target_kind == FOLDER_TARGET) is files
                    and (
                        files
                        or (
                            document.target_kind == target_kind
                            and str(document.kind) == of_kind
                        )
                    )
                )
            )

        def schemas_of(names: tuple[str, ...]) -> tuple[str, ...]:
            return tuple(sorted({name.split(".", 1)[0] for name in names}))

        tables = qualified(of_kind="Table", files=False)
        views = qualified(of_kind="View", files=False)
        folders = qualified(of_kind="Folder", files=True)
        artefacts = item_load_artefacts(repository, item=item)
        return cls(
            target_id=target_id,
            kind=kind,
            target_name=target_name,
            schemas=schemas_of(tables + views)
            + tuple(load_schemas(artefacts) if target_kind == SQL_TARGET else ()),
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
        )


def bound_target(
    *,
    id: str = "target-1",
    kind: str = "lakehouse",
    item_id: str = "Sales_LH",
    logical_item_name: str | None = "Sales",
    logical_item_type: str | None = "Lakehouse",
    **extra,
) -> BoundTarget:
    return BoundTarget(
        id=id,
        kind=kind,
        item_id=item_id,
        logical_item_name=logical_item_name,
        logical_item_type=logical_item_type,
        **extra,
    )


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


def warehouse_table(
    object_id: str,
    *,
    select: str = "select cast(1 as int) as CustomerId",
    primary_key: str = "CustomerId",
    identity: str | None = None,
) -> str:
    """A Warehouse table, whose *query* defines its schema.

    Unlike a Lakehouse table, the columns are not listed — the result set is the
    declaration, and Weaver infers types from it. A caller wanting particular
    physical types casts them in the select, which is also how the real fixtures
    read.

    ``identity`` names a Weaver-managed surrogate key: a column the query does
    *not* produce and Weaver adds, which is why it needs an engine to confirm.
    """

    identity_line = f"Identity: {identity}\n\n" if identity else ""
    return f'''\
/*
Table ID: {object_id}

Description: A declared table.

Lineage: A source system.

Primary key: {primary_key}

{identity_line}*/
{select}
'''


def warehouse_view(object_id: str, *, select: str, depends_on: str) -> str:
    return f'''\
/*
View ID: {object_id}

Description: A declared view.

Lineage: ${depends_on}
*/
{select}
'''


def spark_view(object_id: str, *, depends_on: str) -> str:
    return f'''\
/*
View ID: {object_id}

Description: A declared view.

Lineage: ${depends_on}

Dependencies:
  - {depends_on}
*/
select 1 as CustomerId from {depends_on}
'''


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


def alias_declaration(**aliases: str) -> str:
    """An item's ``alias.yml``: local name to the document it points at."""

    lines = "\n".join(
        f"  {local}: {source}" for local, source in sorted(aliases.items())
    )
    return f"aliases:\n{lines}\n"


def alias_repository(
    root: Path,
    *,
    producer: str = "Lakehouse/Raw",
    consumer: str = "Lakehouse/Curated",
    schema: str = "DWG",
    consumer_view: bool = True,
):
    """Two items, the second aliasing a table in the first.

    The smallest repository that can express a cross-item alias, which is the
    one shape a single-item fixture cannot reach: an alias that did not cross
    would not be one. The consumer also builds a view over the aliased name, so
    the ordering claim — the consumer's whole group waits for the producer's —
    has something to order.
    """

    _write(root, f"{producer}/schemas/{schema}.yml", schema_document(schema))
    _write(
        root,
        f"{producer}/{schema}__Customer.py",
        lakehouse_table(f"{schema}.Customer"),
    )
    _write(root, f"{consumer}/schemas/{schema}.yml", schema_document(schema))
    _write(
        root,
        f"{consumer}/alias.yml",
        alias_declaration(**{f"{schema}.PortableCustomer": f"{producer}/{schema}.Customer"}),
    )
    if consumer_view:
        # A Warehouse consumer reads its alias through T-SQL over the producer's
        # SQL endpoint, so the view it builds is spelled differently — and for a
        # probe that only wants the alias itself, no view is wanted at all.
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
                f"{consumer}/{schema}.CustomerName.sql",
                spark_view(
                    f"{schema}.CustomerName", depends_on=f"{schema}.PortableCustomer"
                ),
            )
    return parse_item_repository(Location(str(root)))


def single_document_repository(
    root: Path,
    *,
    item: str = ITEM,
    schemas: tuple[str, ...] = ("DWG",),
    documents: Mapping[str, str],
):
    """The smallest legal repository holding the documents given.

    One item, its schema declarations, and the files named — nothing else. The
    point is that a parse, a DDL or an action-rendering claim is made against a
    repository whose entire content is visible in the test that wrote it.

    ``documents`` maps a relative filename to its content, so a caller composes
    from the document builders above and stays explicit about what exists. Every
    schema a document uses must be declared, so ``schemas`` widens when a fixture
    reaches beyond ``DWG`` — a Files folder under ``Raw``, say.

    One thing to know before asserting over ``repository.source_documents``: a
    parsed repository *always* carries the builtin ``Lakehouse/_weaver``
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
    id: str = "object-Lakehouse--Sales--DWG.Customer",
    kind: str = "build_table",
    resource_node_id: str = "Lakehouse/Sales/DWG.Customer",
    executor: str = "spark_sql",
    payload: str | None = None,
    payload_sha256: str | None = None,
) -> BuildAction:
    return BuildAction(
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
    store: LocalStore,
    action: BuildAction,
    payload: bytes | None = None,
    target: BoundTarget | None = None,
    description: str = "one action",
):
    """The smallest valid bundle: one target, one sequence, one batch, one action.

    Installer claims — sequencing, failure semantics, reporting, payload loading —
    are about the *installer*, not about whatever repository happened to produce
    a bundle. Generating a real one to test them makes the planner a dependency
    of every installer failure.
    """

    target = target or bound_target()
    payloads = {}
    if payload is not None and action.payload is not None:
        payloads[action.payload] = payload
    plan = BuildPlan(
        format_version=1,
        bundle_id="",
        repository_name="weaver_items",
        repository_signature="repository-signature",
        targets=(target,),
        sequences=(
            _sequence(description=description, target_id=target.id, action=action),
        ),
    )
    plan = _with_identity(plan)
    return write_bundle(
        location, plan=plan, payloads=payloads, snapshot={}, store=store
    )


def _sequence(*, description: str, target_id: str, action: BuildAction):
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
# signature. That is the claim worth making about an executor — it adds no logic
# of its own — and it is the one a mock's `assert_called_with` would obscure.


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


def spark_destination(
    item: str = "Sales_LH", *, schema_prefix: str = "sales_lh__", **extra
):
    from weaver.spark import SparkDestination

    return SparkDestination(item=item, schema_prefix=schema_prefix, **extra)


#: Distinguishes "caller said nothing, give the usual one" from "caller means
#: none". A Warehouse target genuinely resolves to no Spark destination, and that
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


def installation_context(
    *,
    spark=None,
    sql=None,
    target=None,
    store=None,
    resolver=None,
    targets=None,
    epoch: str | None = None,
    snapshot: Location | None = None,
):
    """The one target and the runtime services an executor is handed.

    Everything defaults to absent rather than to something plausible, because an
    executor's behaviour when a capability is *missing* is a real claim — a
    ``tsql`` action with no SQL executor must fail saying so, not fail obscurely
    somewhere deeper.
    """

    from weaver.build_bundle.executors.base import InstallationContext

    return InstallationContext(
        spark=spark,
        resolver=resolver,
        store=store,
        snapshot=snapshot or Location("/snapshot"),
        target=target if target is not None else resolved_target(),
        sql=sql,
        targets=targets or {},
        epoch=epoch,
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
        binding = (
            LakehouseBinding(ItemRef(physical))
            if item.item_type == "Lakehouse"
            else WarehouseBinding(ItemRef(physical))
        )
        bindings.append(ItemBinding(item, binding))
    return ItemBindings(tuple(bindings))
