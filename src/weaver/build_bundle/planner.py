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

from dataclasses import dataclass
from typing import Mapping

from ..errors import BuildError
from ..hosts import Host
from ..locations import Location
from ..resolution import resolver_for
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
    PRUNE_KINDS,
    BuildAction,
    BuildBatch,
    BuildPlan,
    BuildSequence,
    ManagedInventory,
    OmittedNode,
)
from .payloads import (
    FOLDER_SEQUENCE,
    OBJECT_SEQUENCE_START,
    OBJECT_SEQUENCE_STEP,
    PRUNE_SEQUENCE,
    SCHEMA_SEQUENCE,
    payload_path,
    sha256_hex,
)
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
) -> BuildBundle:
    """Read a repository once, project it, and write a fully bound bundle.

    This is the whole of interpretation: repository reading, target projection,
    ordering, executable generation, and certification of the snapshot. The
    returned bundle is reloaded and validated before it is handed back.
    """

    if targets.lakehouse is None:
        raise BuildError("build bundle v1 requires a Lakehouse binding")

    resolver = resolver_for(host)
    repo_location = _repository_location(resolver, weaver_lakehouse, repository_name)
    repository = read_repository(repo_location, store=store, name=repository_name)

    _reject_unsupported(repository, targets)

    projection = project(
        repository.dependency_graph, bound_target_kinds=targets.bound_target_kinds
    )

    bound_target = targets.lakehouse.to_bound_target()
    sequences, payloads = _plan_sequences(repository, projection, bound_target, resolver)

    plan = BuildPlan(
        format_version=SUPPORTED_FORMAT_VERSION,
        bundle_id="",
        repository_name=repository_name,
        repository_signature=repository.signature,
        targets=(bound_target,),
        sequences=sequences,
        omitted_nodes=projection.omitted,
    )
    plan = _with_identity(plan)

    snapshot = _snapshot(repository, repo_location, store)
    return write_bundle(output, plan=plan, payloads=payloads, snapshot=snapshot, store=store)


def _repository_location(resolver, weaver_lakehouse: ItemRef, repository_name: str) -> Location:
    # A resolver knows its own Weaver Lakehouse; a caller-named one must agree,
    # so a build cannot silently read a repository from a different Lakehouse.
    configured = getattr(resolver, "weaver_lakehouse", None)
    if configured is not None and configured.name != weaver_lakehouse.name:
        raise BuildError(
            f"the host's Weaver Lakehouse {configured.name!r} does not match the "
            f"requested {weaver_lakehouse.name!r}"
        )
    return resolver.repository(RepositoryRef(repository_name))


def _reject_unsupported(repository: SesRepository, targets: TargetBindings) -> None:
    if targets.warehouse is not None and repository.warehouse_native:
        raise NotImplementedError(
            "T-SQL and Warehouse installation are not supported by build bundle v1"
        )


def _plan_sequences(
    repository: SesRepository,
    projection: Projection,
    target: BoundTarget,
    resolver,
) -> tuple[tuple[BuildSequence, ...], dict[str, bytes]]:
    payloads: dict[str, bytes] = {}
    documents = {node: repository.by_id[node] for node in projection.retained}

    sequences: list[BuildSequence] = [_prune_sequence(target)]

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
        sequences.append(
            _object_layer_sequence(number, list(layer), documents, target, payloads)
        )

    return tuple(sequences), payloads


def _prune_sequence(target: BoundTarget) -> BuildSequence:
    """Reconcile the target to the managed set before anything is created.

    The actions carry no payload — the installer reconciles against the bundle's
    own managed inventory, and each action drops what that inventory does not
    keep. They run in one batch, in dependency order.
    """

    actions = tuple(
        BuildAction(
            id=f"{kind}-{target.id}",
            kind=kind,
            resource_node_id=None,
            executor="prune",
            payload=None,
            payload_sha256=None,
        )
        for kind in PRUNE_KINDS
    )
    batch = BuildBatch(id=f"{PRUNE_SEQUENCE:03d}-{target.id}", target_id=target.id, actions=actions)
    return BuildSequence(
        number=PRUNE_SEQUENCE, description="prune unmanaged objects", batches=(batch,)
    )


def _schema_sequence(
    repository: SesRepository,
    documents: Mapping[str, SourceDocument],
    target: BoundTarget,
    resolver,
    payloads: dict[str, bytes],
) -> BuildSequence | None:
    """Create the catalog databases the retained tables and views need.

    Only schemas that hold a Delta table or view get a database, and it is given
    an explicit ``LOCATION`` in the Lakehouse ``Tables`` area — the one physical
    path a build resolves — so a managed table created under it lands where Weaver
    addresses it. Folder-only schemas are directories, not catalog databases, and
    get none. A schema is created only because a bound resource uses it; none is
    inferred from a ``Schema.Object`` name.
    """

    schemas = sorted(
        {
            document.object_id.schema
            for document in documents.values()
            if document.target_kind == DELTA_TARGET
        }
    )
    undeclared = [schema for schema in schemas if schema not in repository.schemas]
    if undeclared:  # pragma: no cover - the reader already rejects undeclared schemas
        raise BuildError(f"retained resource uses undeclared schema(s): {undeclared}")
    if not schemas:
        return None

    lakehouse = ItemRef(target.item_id)
    actions: list[BuildAction] = []
    for schema in schemas:
        location = resolver.tables_root(lakehouse).join(schema).value
        content = (
            f"CREATE DATABASE IF NOT EXISTS {_ident(schema)} LOCATION '{location}'\n"
        ).encode("utf-8")
        path = payload_path(SCHEMA_SEQUENCE, "create-schemas", f"create-{schema}.spark.sql")
        payloads[path] = content
        actions.append(
            BuildAction(
                id=f"schema-{schema}",
                kind=CREATE_SCHEMA,
                resource_node_id=None,
                executor="spark_sql",
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


def managed_inventory(plan: BuildPlan) -> ManagedInventory:
    """The keep-set a prune reconciles to, derived from a plan's create actions.

    Read from the plan, never from the repository, so the installer needs no
    second look at the source. A schema is a database exactly when a table or
    view lives in it.
    """

    folders: set[str] = set()
    tables: set[str] = set()
    views: set[str] = set()
    for _, _, action in plan.actions():
        node = action.resource_node_id
        if node is None:
            continue
        qualified = node.split(":", 1)[1]
        if action.kind == BUILD_FOLDER:
            folders.add(qualified)
        elif action.kind == BUILD_TABLE:
            tables.add(qualified)
        elif action.kind == BUILD_VIEW:
            views.add(qualified)

    folder_schemas = {folder.split(".", 1)[0] for folder in folders}
    schemas = {name.split(".", 1)[0] for name in tables | views}
    return ManagedInventory(
        schemas=frozenset(schemas),
        folder_schemas=frozenset(folder_schemas),
        folders=frozenset(folders),
        tables=frozenset(tables),
        views=frozenset(views),
    )


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


def _ident(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"
