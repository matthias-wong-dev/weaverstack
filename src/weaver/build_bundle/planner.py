"""Projecting a repository graph onto the supplied physical bindings.

The repository graph is complete and logical — every Folder, Delta and Warehouse
object with its full dependency closure. A build, though, is against whatever
targets the caller actually bound. Projection derives the *maximal coherent*
subgraph deployable with those bindings: keep everything whose target is bound,
drop everything whose target is not, and then drop anything left stranded above a
dropped producer, so no retained node is ever planned with a missing upstream.

.. code-block:: text

    Folder A -> Delta B -> Warehouse C     (Lakehouse only)  -> keep A, B; omit C
    Warehouse A -> Delta B                 (Lakehouse only)  -> omit A and B

Every omission is recorded with a reason, so a missing Warehouse binding is
visible rather than a mysterious absence. This module owns projection only; plan
generation builds on it at the next checkpoint.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Mapping

from ..errors import BuildError
from ..hosts import BUILD_BUNDLES_AREA, REPOS_AREA, Host
from ..locations import Location
from ..resolution import resolver_for
from ..spark import SparkCatalogue, object_token, schema_token
from ..catalogue.tables import CATALOGUE_SCHEMA
from ..ses.graph import Graph
from ..ses.metadata import TABLE, VIEW, DELTA_TARGET, FOLDER_TARGET, SQL_TARGET
from ..ses.repository import SesRepository, read_repository
from ..ses.source import SourceDocument
from ..store import Store
from ..targets import ItemRef, RepositoryRef
from .bundle import SUPPORTED_FORMAT_VERSION, BuildBundle, compute_bundle_id, write_bundle
from .models import (
    BUILD_FOLDER,
    BUILD_TABLE,
    BUILD_VIEW,
    CREATE_SCHEMA,
    OMIT_DEPENDS_ON_OMITTED,
    OMIT_TARGET_UNBOUND,
    PRUNE_FOLDER,
    PRUNE_SCHEMA,
    PRUNE_TABLE,
    PRUNE_VIEW,
    PUBLISH_REGISTRY,
    RECONCILE_CATALOGUE,
    RECORD_INSTALLATION,
    BuildAction,
    BuildBatch,
    BuildPlan,
    BuildSequence,
    OmittedNode,
)
from .payloads import (
    CATALOGUE_SEQUENCE,
    FOLDER_SEQUENCE,
    INSTALLATION_SEQUENCE,
    OBJECT_SEQUENCE_START,
    OBJECT_SEQUENCE_STEP,
    PRUNE_SEQUENCE,
    REGISTRY_SEQUENCE,
    SCHEMA_SEQUENCE,
    check_sequence_headroom,
    payload_path,
    sha256_hex,
)

#: Files areas that are never folder resources, so a prune never touches them.
_RESERVED_FILES_AREAS = frozenset({REPOS_AREA, BUILD_BUNDLES_AREA})

#: Schemas a prune never touches. A schema-enabled Fabric Lakehouse has a default
#: ``dbo`` schema that cannot be dropped and that Weaver does not manage; ``_``
#: holds Weaver's own catalogue, which no application repository owns. An
#: application build normally cannot see `_` at all — it lives in the Weaver
#: Lakehouse and prune is scoped to the bound destination's own storage — but a
#: repository built *into* the Weaver Lakehouse would, and a prune that dropped
#: the catalogue would take the record of every installation with it.
_RESERVED_SCHEMAS = frozenset({"dbo", CATALOGUE_SCHEMA})
from .targets import LAKEHOUSE_TARGET, WAREHOUSE_TARGET, BoundTarget, TargetBindings

#: Which physical binding an SES target kind needs. Folders and Delta tables
#: both live in a Lakehouse; Warehouse SQL needs a Warehouse.
BINDING_FOR_TARGET_KIND = {
    FOLDER_TARGET: LAKEHOUSE_TARGET,
    DELTA_TARGET: LAKEHOUSE_TARGET,
    SQL_TARGET: WAREHOUSE_TARGET,
}


@dataclass(frozen=True)
class Projection:
    """The retained subgraph, in dependency order, plus what was left out."""

    graph: Graph
    omitted: tuple[OmittedNode, ...]

    @property
    def retained(self) -> tuple[str, ...]:
        """Retained node ids, upstream before downstream."""

        return self.graph.order()

    @property
    def is_empty(self) -> bool:
        return len(self.graph) == 0


def target_kind_of_node(node_id: str) -> str:
    """The SES target kind a node id carries, from its ``kind:Schema.Object`` head."""

    head = node_id.split(":", 1)[0]
    if head not in BINDING_FOR_TARGET_KIND:
        raise BuildError(f"node {node_id!r} has an unrecognised target kind {head!r}")
    return head


def project(
    graph: Graph,
    *,
    bound_target_kinds: frozenset[str],
    target_kind_of: Mapping[str, str] | None = None,
) -> Projection:
    """The maximal coherent subgraph for the supplied physical bindings.

    ``bound_target_kinds`` is the set of physical bindings present — some subset
    of ``{"lakehouse", "warehouse"}``. ``target_kind_of`` maps each node to its
    SES target kind; by default it is read from the node id.
    """

    def kind(node: str) -> str:
        if target_kind_of is not None:
            return target_kind_of[node]
        return target_kind_of_node(node)

    omitted: dict[str, OmittedNode] = {}

    # Step 1-3: a node is initially eligible only if its target kind is bound.
    retained: set[str] = set()
    for node in graph.nodes:
        binding = BINDING_FOR_TARGET_KIND[kind(node)]
        if binding in bound_target_kinds:
            retained.add(node)
        else:
            omitted[node] = OmittedNode(
                node_id=node, reason=OMIT_TARGET_UNBOUND, detail=f"no {binding} binding"
            )

    # Step 4: drop, to a fixpoint, any retained node standing above a dropped
    # producer — it would otherwise be planned with a missing dependency.
    changed = True
    while changed:
        changed = False
        for node in sorted(retained):
            missing = [up for up in graph.upstream_of(node) if up not in retained]
            if missing:
                retained.discard(node)
                omitted[node] = OmittedNode(
                    node_id=node,
                    reason=OMIT_DEPENDS_ON_OMITTED,
                    detail=f"depends on {missing[0]}",
                )
                changed = True
                break

    projected = graph.subgraph(retained)

    # Step 6: the retained graph must have complete internal closure.
    for node in projected.nodes:
        stranded = [up for up in projected.upstream_of(node) if up not in retained]
        if stranded:  # pragma: no cover - guaranteed by construction, guarded anyway
            raise BuildError(
                f"projection left {node!r} without its producer {stranded[0]!r}"
            )

    ordered_omitted = tuple(omitted[node] for node in sorted(omitted))
    return Projection(graph=projected, omitted=ordered_omitted)


# --- plan generation ---------------------------------------------------------

#: How an object's kind names its action, its payload directory slug, and its
#: payload filename prefix. Folders are not here — they carry no create DDL.
_OBJECT_LAYOUT = {
    TABLE: (BUILD_TABLE, "build-delta", "table"),
    VIEW: (BUILD_VIEW, "build-view", "view"),
}


def generate_build_bundle(
    *,
    weaver_lakehouse: ItemRef,
    repository_name: str,
    targets: TargetBindings,
    output: Location,
    host: Host,
    store: Store,
    prune: bool = True,
    catalogue: bool = True,
    spark: Any = None,
    sql: Any = None,
) -> BuildBundle:
    """Read a repository once, project it, and write a fully bound bundle.

    This is the whole of interpretation: repository reading, target projection,
    ordering, executable generation, and certification of the snapshot. The
    returned bundle is reloaded and validated before it is handed back.

    ``prune`` (default on) reconciles the target: the build inspects it *now* and
    freezes a concrete ``DROP`` for everything it holds that this bundle does not
    manage, so a reviewer can see exactly what an install will remove — no
    enumeration happens at install time. It requires the target to be visible;
    pass ``prune=False`` to opt out when it is not. ``spark`` lets the inspection
    see catalog views; without it, prune still reconciles tables, folders and
    schemas from storage.

    ``catalogue`` (default on) appends the central catalogue's reconciliation after
    all physical work, against the Weaver Lakehouse rather than the destination.
    Pass ``catalogue=False`` for a build that must not record itself — the tables
    have to exist for the statements to run, so a repository built before setup
    would otherwise fail at the last barrier.
    """

    binding = _single_binding(targets)

    resolver = resolver_for(host)
    repo_location = _repository_location(resolver, weaver_lakehouse, repository_name)
    repository = read_repository(repo_location, store=store, name=repository_name)

    bound_target = binding.to_bound_target()
    control_target = _control_target(weaver_lakehouse, bound_target)

    projection = project(
        repository.dependency_graph, bound_target_kinds=targets.bound_target_kinds
    )

    sequences, payloads = _plan_sequences(
        repository, projection, bound_target, resolver, store, prune, spark, sql, host
    )

    bound_targets = (bound_target,)
    if catalogue:
        catalogue_sequences = _catalogue_sequences(
            repository=repository,
            projection=projection,
            target=bound_target,
            control_target=control_target,
            payloads=payloads,
            spark=spark,
        )
        sequences = sequences + catalogue_sequences
        if control_target.id != bound_target.id:
            bound_targets = bound_targets + (control_target,)

    plan = BuildPlan(
        format_version=SUPPORTED_FORMAT_VERSION,
        bundle_id="",
        repository_name=repository_name,
        repository_signature=repository.signature,
        targets=bound_targets,
        sequences=sequences,
        omitted_nodes=projection.omitted,
    )
    plan = _with_identity(plan)

    snapshot = _snapshot(repository, repo_location, store)
    return write_bundle(output, plan=plan, payloads=payloads, snapshot=snapshot, store=store)


def _repository_location(resolver, weaver_lakehouse: ItemRef, repository_name: str) -> Location:
    # A resolver knows its own Weaver Lakehouse; a caller-named one must agree,
    # so a build cannot silently read a repository from a different Lakehouse. The
    # host carries the configured *name*; a resolved location would carry a path
    # segment (a display name locally, an item id on Fabric), which is not it.
    host = getattr(resolver, "host", None)
    configured = getattr(host, "weaver_lakehouse", None)
    if configured is not None and configured != weaver_lakehouse.name:
        raise BuildError(
            f"the host's Weaver Lakehouse {configured!r} does not match the "
            f"requested {weaver_lakehouse.name!r}"
        )
    return resolver.repository(RepositoryRef(repository_name))


def _single_binding(targets: TargetBindings):
    """The one physical side this bundle builds.

    The signature accepts every physical binding (at least one supplied), but a
    single bundle materialises a single side: folders and Delta/Spark objects
    into the Lakehouse, or T-SQL objects into the Warehouse. Crossing the boundary
    in one build — a Warehouse object reading a Lakehouse table it also builds —
    needs a SQL-endpoint refresh and lands with cross-database chaining, so until
    then the two sides are built in separate calls.
    """

    if targets.lakehouse is not None and targets.warehouse is not None:
        raise BuildError(
            "a build targets one physical side at a time: build the Lakehouse and "
            "the Warehouse in separate calls until cross-database chaining lands"
        )
    if targets.lakehouse is not None:
        return targets.lakehouse
    if targets.warehouse is not None:
        return targets.warehouse
    raise BuildError("a build requires at least one physical target binding")


def _plan_sequences(
    repository: SesRepository,
    projection: Projection,
    target: BoundTarget,
    resolver,
    store: Store,
    prune: bool,
    spark,
    sql=None,
    host: Host | None = None,
) -> tuple[tuple[BuildSequence, ...], dict[str, bytes]]:
    payloads: dict[str, bytes] = {}
    documents = {node: repository.by_id[node] for node in projection.retained}
    is_warehouse = target.kind == WAREHOUSE_TARGET
    managed = _managed_sets(documents, SQL_TARGET if is_warehouse else DELTA_TARGET)

    sequences: list[BuildSequence] = []

    if prune:
        if is_warehouse:
            prune_sequence = _warehouse_prune_sequence(target, sql, host, managed, payloads)
        else:
            prune_sequence = _prune_sequence(target, resolver, store, spark, managed, payloads)
        if prune_sequence is not None:
            sequences.append(prune_sequence)

    schema_sequence = _schema_sequence(repository, documents, target, resolver, payloads)
    if schema_sequence is not None:
        sequences.append(schema_sequence)

    folder_nodes = [n for n, d in documents.items() if d.target_kind == FOLDER_TARGET]
    if folder_nodes:
        sequences.append(_folder_sequence(folder_nodes, documents, target))

    object_nodes = [n for n in projection.retained if documents[n].target_kind != FOLDER_TARGET]
    object_graph = projection.graph.subgraph(object_nodes)
    for index, layer in enumerate(object_graph.layers()):
        number = OBJECT_SEQUENCE_START + index * OBJECT_SEQUENCE_STEP
        check_sequence_headroom(number)
        sequences.append(
            _object_layer_sequence(number, list(layer), documents, target, payloads)
        )

    return tuple(sequences), payloads


@dataclass(frozen=True)
class _Managed:
    """The keep-set the build diffs the target against, folded for comparison."""

    schemas: frozenset[str]
    folder_schemas: frozenset[str]
    folders: frozenset[str]
    tables: frozenset[str]
    views: frozenset[str]


def _managed_sets(
    documents: Mapping[str, SourceDocument], object_target_kind: str = DELTA_TARGET
) -> _Managed:
    """The keep-set for one physical side: Delta objects, or Warehouse ones."""

    tables = {d.qualified for d in documents.values() if d.target_kind == object_target_kind and d.kind == TABLE}
    views = {d.qualified for d in documents.values() if d.target_kind == object_target_kind and d.kind == VIEW}
    folders = {d.qualified for d in documents.values() if d.target_kind == FOLDER_TARGET}
    return _Managed(
        schemas=frozenset(name.split(".", 1)[0].lower() for name in tables | views),
        folder_schemas=frozenset(name.split(".", 1)[0].lower() for name in folders),
        folders=frozenset(name.lower() for name in folders),
        tables=frozenset(name.lower() for name in tables),
        views=frozenset(name.lower() for name in views),
    )


def _prune_sequence(
    target: BoundTarget,
    resolver,
    store: Store,
    spark,
    managed: _Managed,
    payloads: dict[str, bytes],
) -> BuildSequence | None:
    """Inspect the target now and freeze a concrete DROP for each unmanaged object.

    The build reads the target's own storage (and, with a session, its catalogue)
    and emits visible drops — ``DROP TABLE``/``VIEW``/``SCHEMA`` as Spark SQL
    payloads, an unmanaged folder as a directory-removing action. The installer
    runs exactly these; it never enumerates. Reconciliation is scoped to the one
    bound Lakehouse's ``Tables``/``Files`` storage, so a shared catalogue cannot
    make a build reach into another Lakehouse.

    Both halves of the inspection now name the Lakehouse being reconciled, which
    is what makes reconciling a Lakehouse other than the attached one correct
    rather than lucky.
    """

    # Store addressing, not Spark addressing: inspection *lists* the target, and
    # on Fabric that is the DFS location, while a LakehouseSparkLocation carries
    # the `abfss://` roots Spark writes through. Same Lakehouse, two transports —
    # conflating them would have prune listing a URL Spark cannot read a directory
    # from.
    #
    # Schemas come from storage on both hosts, and have to: Fabric refuses
    # `SHOW SCHEMAS IN `workspace`.`lakehouse`` — a bare `SHOW SCHEMAS` answers
    # only for the *attached* Lakehouse — so asking the catalogue would have
    # reconciled the destination against the control plane's inventory.
    lakehouse = ItemRef(target.item_id)
    tables_root = resolver.tables_root(lakehouse)
    files_root = resolver.files_root(lakehouse)
    catalogue = _catalogue_for(resolver, lakehouse, spark)

    existing_schemas = [
        entry.name
        for entry in _child_dirs(store, tables_root)
        if entry.name.lower() not in _RESERVED_SCHEMAS
    ]
    orphan_schemas = {s.lower() for s in existing_schemas if s.lower() not in managed.schemas}

    actions: list[BuildAction] = []

    # Views (catalogue only, since a view is not a directory): drop those not
    # managed, per schema that survives — an orphan schema is dropped whole below
    # and takes its views with it. Asked of the *destination's* catalogue, so a
    # build reconciling a Lakehouse the session is not attached to sees that
    # Lakehouse's views rather than the control plane's.
    if catalogue is not None:
        for schema in existing_schemas:
            if schema.lower() in orphan_schemas:
                continue
            for view in catalogue.views(schema):
                if f"{schema}.{view}".lower() in managed.views:
                    continue
                actions.append(
                    _drop_action(target, "prune_view", "view", f"{schema}.{view}",
                                 f"DROP VIEW IF EXISTS {object_token(schema, view)}", payloads)
                )

    # Tables: unmanaged ones in a schema that survives (an orphan schema is
    # dropped whole below).
    for schema_entry in _child_dirs(store, tables_root):
        schema = schema_entry.name
        if schema.lower() in orphan_schemas or schema.lower() in _RESERVED_SCHEMAS:
            continue
        for object_entry in _child_dirs(store, schema_entry.location):
            qualified = f"{schema}.{object_entry.name}"
            if qualified.lower() not in managed.tables:
                actions.append(
                    _drop_action(target, "prune_table", "table", qualified,
                                 f"DROP TABLE IF EXISTS {object_token(schema, object_entry.name)}",
                                 payloads)
                )

    # Folders: an unmanaged folder object, or a whole unmanaged folder schema.
    for schema_entry in _child_dirs(store, files_root):
        schema = schema_entry.name
        if schema in _RESERVED_FILES_AREAS:
            continue
        if schema.lower() not in managed.folder_schemas:
            actions.append(_prune_folder_action(target, f"folder:{schema}"))
            continue
        for object_entry in _child_dirs(store, schema_entry.location):
            qualified = f"{schema}.{object_entry.name}"
            if qualified.lower() not in managed.folders:
                actions.append(_prune_folder_action(target, f"folder:{qualified}"))

    # Schemas: drop the whole orphan schema, which cascades to its tables/views.
    # SCHEMA (not DATABASE) works in Fabric and its local emulator — Fabric's
    # Trident Spark refuses CREATE/DROP DATABASE on a Lakehouse, but accepts SCHEMA.
    for schema in sorted({s for s in existing_schemas if s.lower() in orphan_schemas}):
        actions.append(
            _drop_action(target, "prune_schema", "schema", schema,
                         f"DROP SCHEMA IF EXISTS {schema_token(schema)} CASCADE", payloads)
        )

    if not actions:
        return None
    batch = BuildBatch(id=f"{PRUNE_SEQUENCE:03d}-{target.id}", target_id=target.id, actions=tuple(actions))
    return BuildSequence(
        number=PRUNE_SEQUENCE, description="prune unmanaged objects", batches=(batch,)
    )


#: Warehouse schemas Weaver never manages, so a prune never drops them. The
#: schemas owned by fixed database roles are excluded separately, by ownership —
#: see :func:`_warehouse_prune_actions`.
_RESERVED_SQL_SCHEMAS = frozenset(
    {"dbo", "guest", "information_schema", "sys", "queryinsights", "_rsc"}
)


def _warehouse_prune_sequence(
    target: BoundTarget,
    sql,
    host: Host,
    managed: _Managed,
    payloads: dict[str, bytes],
) -> BuildSequence | None:
    """Inspect the Warehouse catalogue now and freeze a concrete DROP per orphan.

    The Warehouse counterpart of :func:`_prune_sequence`: reconciliation reads
    ``sys.objects``/``sys.schemas`` at *plan* time (target inspection is a
    planning concern — build-philosophy §6) and compiles each unmanaged table,
    view and schema into an explicit T-SQL drop. The installer runs exactly these
    and enumerates nothing.

    Order is dependency-safe and matters more than on the Lakehouse: T-SQL has no
    ``DROP SCHEMA … CASCADE``, so views are dropped before the tables they read,
    and a schema only after everything in it has gone.

    Reading the target is **Fabric-native by default**, like
    :func:`weaver.wipe.wipe_sql_target`: Weaver runs in Fabric, so it inspects the
    Warehouse through its own session identity. A desktop caller crossing into
    Fabric — a developer, or the CLI — injects ``desktop_sql_executor``
    explicitly. Either way the inventory is read where the build is planned, and
    the drops are frozen into the bundle from there.
    """

    owns_sql = sql is None
    if sql is None:
        from ..fabric.sql import fabric_sql_executor
        from ..targets import WarehouseTarget

        sql = fabric_sql_executor(
            WarehouseTarget(warehouse=ItemRef(target.item_id)), host
        )
    try:
        return _warehouse_prune_actions(target, sql, managed, payloads)
    finally:
        if owns_sql and hasattr(sql, "close"):
            sql.close()


def _warehouse_prune_actions(
    target: BoundTarget,
    sql,
    managed: _Managed,
    payloads: dict[str, bytes],
) -> BuildSequence | None:
    """Compile the frozen drops from one catalogue reading."""

    rows = sql.query(
        """
        select
            schema_name(objects.schema_id) as schema_name
          , objects.name                  as object_name
          , objects.type                  as object_type
        from sys.objects as objects
        where objects.is_ms_shipped = 0
          and objects.type in (N'U', N'V')
        order by schema_name(objects.schema_id), objects.name
        """
    )
    existing = [
        (str(row["schema_name"]), str(row["object_name"]), str(row["object_type"]).strip())
        for row in rows
        if str(row["schema_name"]).lower() not in _RESERVED_SQL_SCHEMAS
    ]

    # A fixed database role owns a schema of its own — `db_owner`, `db_datareader`
    # and seven more — and those are not Weaver's to drop, or anyone's: `DROP
    # SCHEMA` on one fails. They are excluded by *ownership* rather than by adding
    # nine more names to the reserved list, because the reserved list is a
    # statement about Weaver's conventions and this is a statement about SQL's.
    schema_rows = sql.query(
        """
        select schemas.name as name
        from sys.schemas as schemas
        left join sys.database_principals as owners
          on owners.principal_id = schemas.principal_id
        where owners.is_fixed_role is null
           or owners.is_fixed_role = 0
        """
    )
    existing_schemas = [
        str(row["name"])
        for row in schema_rows
        if str(row["name"]).lower() not in _RESERVED_SQL_SCHEMAS
    ]

    def unmanaged(schema: str, name: str, keep: frozenset[str]) -> bool:
        return f"{schema}.{name}".lower() not in keep

    actions: list[BuildAction] = []

    # Views first — a view may read a table this same prune drops.
    for schema, name, kind in existing:
        if kind == "V" and unmanaged(schema, name, managed.views):
            actions.append(
                _drop_action(
                    target, PRUNE_VIEW, "view", f"{schema}.{name}",
                    f"drop view if exists {_tsql_ident(schema)}.{_tsql_ident(name)};",
                    payloads, executor="tsql", extension=".sql",
                )
            )

    for schema, name, kind in existing:
        if kind == "U" and unmanaged(schema, name, managed.tables):
            actions.append(
                _drop_action(
                    target, PRUNE_TABLE, "table", f"{schema}.{name}",
                    f"drop table if exists {_tsql_ident(schema)}.{_tsql_ident(name)};",
                    payloads, executor="tsql", extension=".sql",
                )
            )

    # Schemas last, and only those the bundle does not manage: by now everything
    # inside an orphan schema has been dropped above, so the schema is empty.
    for schema in sorted({s for s in existing_schemas if s.lower() not in managed.schemas}):
        actions.append(
            _drop_action(
                target, PRUNE_SCHEMA, "schema", schema,
                f"drop schema if exists {_tsql_ident(schema)};",
                payloads, executor="tsql", extension=".sql",
            )
        )

    if not actions:
        return None
    batch = BuildBatch(
        id=f"{PRUNE_SEQUENCE:03d}-{target.id}", target_id=target.id, actions=tuple(actions)
    )
    return BuildSequence(
        number=PRUNE_SEQUENCE, description="prune unmanaged objects", batches=(batch,)
    )


def _drop_action(
    target,
    kind,
    slug,
    name,
    statement,
    payloads,
    *,
    executor: str = "spark_sql",
    extension: str = ".spark.sql",
) -> BuildAction:
    content = (statement + "\n").encode("utf-8")
    path = payload_path(PRUNE_SEQUENCE, "prune", f"{slug}-{name}{extension}")
    payloads[path] = content
    return BuildAction(
        id=f"prune-{slug}-{name}",
        kind=kind,
        resource_node_id=None,
        executor=executor,
        payload=path,
        payload_sha256=sha256_hex(content),
    )


def _prune_folder_action(target, resource: str) -> BuildAction:
    return BuildAction(
        id=f"prune-{resource}",
        kind=PRUNE_FOLDER,
        resource_node_id=resource,
        executor="folder",
        payload=None,
        payload_sha256=None,
    )


def _child_dirs(store: Store, root) -> list:
    if not store.exists(root) or not store.is_directory(root):
        return []
    return sorted(
        (entry for entry in store.list(root) if entry.is_directory), key=lambda e: e.name
    )


def _catalogue_for(resolver, lakehouse: ItemRef, spark) -> "SparkCatalogue | None":
    """Catalogue operations against the Lakehouse being reconciled.

    None without a session — prune still reconciles tables, folders and schemas
    from storage, and simply cannot see views, which is the documented cost of
    generating without one.
    """

    if spark is None:
        return None
    resolve = getattr(resolver, "spark_destination", None)
    if resolve is None:  # pragma: no cover - both shipped resolvers provide it
        return None
    return SparkCatalogue(spark, resolve(lakehouse))


def _schema_sequence(
    repository: SesRepository,
    documents: Mapping[str, SourceDocument],
    target: BoundTarget,
    resolver,
    payloads: dict[str, bytes],
) -> BuildSequence | None:
    """Create the schemas the retained tables and views need, in the destination.

    Only schemas that hold a materialised object get one. Folder-only schemas are
    directories, not catalogue objects, and get none. A schema is created only
    because a bound resource uses it; none is inferred from a ``Schema.Object``
    name.

    On the Lakehouse the action names the schema and stops there. It used to
    freeze the whole statement, and doing so froze a *resolved path*: local Spark
    needs a ``LOCATION`` for a managed table to land under the Lakehouse's
    ``Tables`` area, so a bundle generated by a test carried its own temporary
    directory in the hashed plan (build-philosophy §10), and a Fabric bundle
    carried a bare two-part name that created the schema in the attached Lakehouse
    instead of the destination. How to make a schema is the destination's
    business; which schema, and where, is the manifest's.

    On the Warehouse a T-SQL ``CREATE SCHEMA`` needs no location and stays a
    frozen script.
    """

    is_warehouse = target.kind == WAREHOUSE_TARGET
    object_target_kind = SQL_TARGET if is_warehouse else DELTA_TARGET
    schemas = sorted(
        {
            document.object_id.schema
            for document in documents.values()
            if document.target_kind == object_target_kind
        }
    )
    undeclared = [schema for schema in schemas if schema not in repository.schemas]
    if undeclared:  # pragma: no cover - the reader already rejects undeclared schemas
        raise BuildError(f"retained resource uses undeclared schema(s): {undeclared}")
    if not schemas:
        return None

    actions: list[BuildAction] = []
    for schema in schemas:
        if is_warehouse:
            # A Warehouse schema is a plain T-SQL CREATE SCHEMA — no storage path,
            # run through the SQL executor. T-SQL has no CREATE SCHEMA IF NOT
            # EXISTS, so guard it with a catalogue check.
            statement = (
                f"if not exists (select 1 from sys.schemas where name = "
                f"{_sql_literal(schema)})\n    exec('create schema {_tsql_ident(schema)}');"
            )
            content = (statement + "\n").encode("utf-8")
            executor, extension = "tsql", ".sql"
        else:
            content = (json.dumps({"schema": schema}, sort_keys=True) + "\n").encode("utf-8")
            executor, extension = "spark_schema", ".schema.json"
        path = payload_path(SCHEMA_SEQUENCE, "create-schemas", f"create-{schema}{extension}")
        payloads[path] = content
        actions.append(
            BuildAction(
                id=f"schema-{schema}",
                kind=CREATE_SCHEMA,
                resource_node_id=None,
                executor=executor,
                payload=path,
                payload_sha256=sha256_hex(content),
            )
        )

    batch = BuildBatch(id=f"{SCHEMA_SEQUENCE:03d}-{target.id}", target_id=target.id, actions=tuple(actions))
    return BuildSequence(number=SCHEMA_SEQUENCE, description="create declared schemas", batches=(batch,))


def _folder_sequence(
    nodes: list[str], documents: Mapping[str, SourceDocument], target: BoundTarget
) -> BuildSequence:
    """One directory-creating action per retained Folder — no payload, no data."""

    actions = tuple(
        BuildAction(
            id=f"folder-{documents[node].qualified}",
            kind=BUILD_FOLDER,
            resource_node_id=node,
            executor="folder",
            payload=None,
            payload_sha256=None,
        )
        for node in sorted(nodes)
    )
    batch = BuildBatch(id=f"{FOLDER_SEQUENCE:03d}-{target.id}", target_id=target.id, actions=actions)
    return BuildSequence(number=FOLDER_SEQUENCE, description="build folders", batches=(batch,))


def _object_layer_sequence(
    number: int,
    nodes: list[str],
    documents: Mapping[str, SourceDocument],
    target: BoundTarget,
    payloads: dict[str, bytes],
) -> BuildSequence:
    actions = tuple(
        _object_action(number, node, documents[node], payloads) for node in sorted(nodes)
    )
    batch = BuildBatch(id=f"{number:03d}-{target.id}", target_id=target.id, actions=actions)
    kinds = {documents[node].kind for node in nodes}
    slug = "build-view" if kinds == {VIEW} else "build-delta"
    return BuildSequence(number=number, description=slug.replace("-", " "), batches=(batch,))


def _object_action(
    number: int, node: str, document: SourceDocument, payloads: dict[str, bytes]
) -> BuildAction:
    action_kind, slug, file_prefix = _OBJECT_LAYOUT[document.kind]
    ddl = document.create_ddl()
    filename = f"{file_prefix}-{document.qualified}{ddl.extension}"
    path = payload_path(number, slug, filename)
    content = ddl.content.encode("utf-8")
    payloads[path] = content
    return BuildAction(
        id=f"{file_prefix}-{document.qualified}",
        kind=action_kind,
        resource_node_id=node,
        executor=ddl.executor,
        payload=path,
        payload_sha256=sha256_hex(content),
    )


# --- the central catalogue ---------------------------------------------------


def _control_target(weaver_lakehouse: ItemRef, bound_target: BoundTarget) -> BoundTarget:
    """The Weaver Lakehouse, as a bound target the catalogue's batches name.

    The catalogue lives in the control plane, not in the destination, so writing it
    is work against a *different* item — and a bundle must name every physical
    destination it touches (build-philosophy §9). Hence a second bound target
    rather than an implicit "wherever the installer happens to be pointed".

    When the destination *is* the Weaver Lakehouse — which is exactly the case
    when Weaver builds its own catalogue — the existing binding is reused rather
    than duplicated, so one item never appears twice in a manifest.
    """

    if bound_target.kind == LAKEHOUSE_TARGET and bound_target.name == weaver_lakehouse.name:
        return bound_target
    return BoundTarget(
        id=f"control-{LAKEHOUSE_TARGET}-{weaver_lakehouse.name}",
        kind=LAKEHOUSE_TARGET,
        item_id=weaver_lakehouse.name,
        item_name=weaver_lakehouse.name,
    )


def _catalogue_sequences(
    *,
    repository: SesRepository,
    projection: Projection,
    target: BoundTarget,
    control_target: BoundTarget,
    payloads: dict[str, bytes],
    spark,
) -> tuple[BuildSequence, ...]:
    """The catalogue's reconciliation, appended after every physical action.

    Three barriers, in one fixed order: the dictionaries describe what was built,
    Installation records which item the repository is now bound to, and Registry
    certifies. Registry is a barrier of its own so that any earlier failure — a
    physical build, a dictionary statement, the installation record — stops the
    install before anything is certified.

    Catalogue rows are projected from the *retained* subgraph. An object omitted
    because its target was not bound is outside this installation's scope, and a
    build that pruned it here would read a missing Warehouse binding as a deletion.
    """

    from ..catalogue.projection import project_installation
    from ..catalogue.reconcile import reconcile
    from ..catalogue.render import InstallationScope

    if projection.is_empty:
        # Nothing was retained, so there is nothing this build could certify. The
        # installation record is deliberately not written either: a build that
        # materialised nothing has not installed the repository.
        return ()

    # The installation's target type is the bound target's kind — the two
    # vocabularies are the same two words, deliberately. Taking it from the
    # binding rather than inferring it from a retained node means the scope comes
    # from what the caller actually bound, which is the thing that decides it.
    scope = InstallationScope(
        repository=repository.name, target_type=target.kind
    )
    installation = project_installation(
        repository,
        retained=projection.retained,
        scope=scope,
        target_name=target.name,
        weaver_version=_weaver_version(),
    )
    plan = reconcile(installation)

    # Deliberately *not* read here. The statements never depended on the existing
    # rows — see weaver.catalogue.reconcile — so the only use for a read was a row
    # count in the sequence description, and a description is part of the hashed
    # plan. That made two runs of the same repository produce different bundle
    # identities purely because the catalogue's state had changed, which breaks the
    # property review and environment comparison rest on (§10). Counting rows is a
    # report about state, not part of a frozen contract.
    #
    # `weaver.catalogue.reconcile.summarise` and the tolerant reader remain the API
    # for asking what a build would change; they are what the drop policy will run
    # its signature comparison through.

    numbers = (CATALOGUE_SEQUENCE, INSTALLATION_SEQUENCE, REGISTRY_SEQUENCE)
    kinds = (RECONCILE_CATALOGUE, RECORD_INSTALLATION, PUBLISH_REGISTRY)
    slugs = ("catalogue", "catalogue-installation", "catalogue-registry")

    sequences: list[BuildSequence] = []
    for number, kind, slug, (description, group) in zip(
        numbers, kinds, slugs, plan.groups
    ):
        actions: list[BuildAction] = []
        for reconciliation in group:
            # Named from what the statement *is*, not from its position: a table
            # whose key is the installation scope has no obsolete row to delete,
            # so its one statement is a merge and must not be labelled otherwise.
            for verb, statement in (
                ("delete", reconciliation.delete),
                ("merge", reconciliation.merge),
            ):
                if statement is None:
                    continue
                actions.append(
                    _catalogue_action(
                        number=number,
                        kind=kind,
                        slug=slug,
                        name=f"{reconciliation.table.name}-{verb}",
                        statement=statement,
                        payloads=payloads,
                    )
                )
        if not actions:  # pragma: no cover - every group renders at least a delete
            continue
        batch = BuildBatch(
            id=f"{number:03d}-{control_target.id}",
            target_id=control_target.id,
            actions=tuple(actions),
        )
        sequences.append(
            BuildSequence(number=number, description=description, batches=(batch,))
        )
    return tuple(sequences)


def _catalogue_action(
    *,
    number: int,
    kind: str,
    slug: str,
    name: str,
    statement: str,
    payloads: dict[str, bytes],
) -> BuildAction:
    content = statement.encode("utf-8")
    path = payload_path(number, slug, f"{name}.spark.sql")
    payloads[path] = content
    return BuildAction(
        id=f"catalogue-{name}",
        kind=kind,
        resource_node_id=None,
        executor="spark_sql",
        payload=path,
        payload_sha256=sha256_hex(content),
    )


def _weaver_version() -> str:
    from .. import __version__

    return __version__


def _snapshot(
    repository: SesRepository, repo_location: Location, store: Store
) -> dict[str, bytes]:
    """The certified repository snapshot: every file the reader saw, verbatim.

    Shipped as the certified record of the source a bundle was built from — the
    signature is taken over it. Build executes only the generated DDL, not the
    snapshot; a later load phase will run object code from it.
    """

    relatives = {document.relative_path for document in repository.documents}
    relatives |= set(repository.support_files)
    relatives |= {schema.relative_path for schema in repository.schemas.values()}
    return {
        relative: store.read(repo_location.join(*relative.split("/")))
        for relative in sorted(relatives)
    }


def _with_identity(plan: BuildPlan) -> BuildPlan:
    from dataclasses import replace

    return replace(plan, bundle_id=compute_bundle_id(plan))


def _sql_literal(value: str) -> str:
    """A single-quoted T-SQL string literal for a Warehouse schema statement."""

    return "'" + value.replace("'", "''") + "'"


def _tsql_ident(name: str) -> str:
    """A bracket-quoted T-SQL identifier."""

    return "[" + name.replace("]", "]]") + "]"
