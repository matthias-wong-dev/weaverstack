"""Compile a resolved Mirror to one MutationPlan.

Planning reads everything the mirror needs first: source code definitions, the
case-exact source paths, the deployed load tree and the source Installation
rows. Execution then reads nothing it did not plan:

.. code-block:: text

    wipe catalogue ─→ catalogue build ─────────────────────┐
    wipe item A ─→ reconstruct item A ─→ refresh endpoint ─┤
    wipe item B ─→ reconstruct item B ─────────────────────┴→ fork, record, bind

A reconstructed Lakehouse's SQL endpoint is refreshed, and a Warehouse view
that reads it through the endpoint waits until it is current.

The fork, the record of what each item borrows and the binding of each item to
its mirror are one transaction, after every reconstruction. Until it commits
the destination catalogue records no installation: it never claims the
source's items, and an incomplete mirror is never published.
"""

from __future__ import annotations

import json
import tempfile
from dataclasses import dataclass, field, replace
from pathlib import Path

from .build_bundle.executors.sql_endpoint_refresh import (
    AWAIT_EXECUTOR,
    REFRESH_RESULT,
    START_EXECUTOR,
)
from .build_bundle.executors.sql_endpoint_refresh import (
    CONTRACTS as ENDPOINT_REFRESH_CONTRACTS,
)
from .build_bundle.payloads import sha256_hex
from .errors import CommandError
from .mutation import (
    BoundTarget,
    MutationAction,
    MutationBatch,
    MutationExecution,
    MutationPlan,
    MutationSequence,
    PhysicalScope,
)
from .mutation.bundle import compute_bundle_id
from .wipe_plan import finishing, target_wipe_actions

#: Deployed code is copied; other ``Files/_`` state remains destination-owned.
LOAD_TREE = "_/Load"
#: Statements in one Warehouse script. Each runs as its own dynamic batch.
STATEMENTS_PER_SCRIPT = 50
#: Lakehouse wrapper views created by one Spark action.
VIEWS_PER_ACTION = 10


def dynamic_script(statements) -> str:
    """Run statements in one round trip, each as its own batch.

    ``CREATE VIEW`` and ``CREATE PROCEDURE`` must start a batch, so each runs
    through ``sp_executesql``.
    """

    return "\n".join(
        "exec sp_executesql N'" + statement.replace("'", "''") + "';"
        for statement in statements
    )


@dataclass
class _Compiling:
    workspace: str
    targets: dict = field(default_factory=dict)
    sequences: list = field(default_factory=list)
    payloads: dict = field(default_factory=dict)
    protected: list = field(default_factory=list)

    def target(self, kind: str, name: str, *, prefix: str = "") -> BoundTarget:
        made = BoundTarget(
            id=f"{prefix}{kind}-{name}",
            kind=kind,
            item_id=name,
            item_name=name,
            workspace_name=self.workspace,
        )
        self.targets.setdefault(made.id, made)
        return made

    def stage(self, description: str, target: BoundTarget, actions) -> None:
        if actions:
            self.sequences.append(
                MutationSequence(
                    len(self.sequences) + 1,
                    description,
                    (
                        MutationBatch(
                            f"mirror-{len(self.sequences) + 1}",
                            target.id,
                            tuple(actions),
                        ),
                    ),
                )
            )

    def payload(self, name: str, content: bytes) -> tuple[str, str]:
        path = f"payload/mirror/{name}"
        self.payloads[path] = content
        return path, sha256_hex(content)


def _action(
    id,
    kind,
    target,
    *,
    executor,
    depends_on=(),
    payload=None,
    digest=None,
    resources=(),
    scope=None,
):
    scopes = () if scope is None else (PhysicalScope(target.id, scope),)
    return MutationAction(
        id=id,
        kind=kind,
        resource_node_id=None,
        executor=executor,
        payload=payload,
        payload_sha256=digest,
        target_id=target.id,
        depends_on=tuple(dict.fromkeys(depends_on)),
        resources=tuple(resources),
        writes=scopes,
    )


def _scripts(
    compiling, name, target, statements, *, kind, depends_on, ordered=False
) -> list:
    """Warehouse statements as scripts of at most ``STATEMENTS_PER_SCRIPT``.

    Independent scripts share the Warehouse's lanes. ``ordered`` scripts run one
    after another, in statement order.
    """

    statements = list(statements)
    actions = []
    previous = tuple(depends_on)
    for index in range(0, len(statements), STATEMENTS_PER_SCRIPT):
        chunk = statements[index : index + STATEMENTS_PER_SCRIPT]
        path, digest = compiling.payload(
            f"{name}-{index // STATEMENTS_PER_SCRIPT:03d}.sql",
            dynamic_script(chunk).encode("utf-8"),
        )
        action = _action(
            f"{name}-{index // STATEMENTS_PER_SCRIPT:03d}",
            kind,
            target,
            executor="tsql",
            payload=path,
            digest=digest,
            depends_on=previous,
            resources=(f"warehouse:{target.item_id}",),
        )
        actions.append(action)
        if ordered:
            previous = (action.id,)
    return actions


def _await_endpoint_objects(compiling, target, objects, *, after) -> list:
    """Wait until ``target`` lists each ``(item, schema, object)`` it reads."""

    if not objects:
        return []
    from .build_bundle.executors.endpoint_objects import EXECUTOR

    path, digest = compiling.payload(
        f"endpoint-objects-{target.id}.endpoint-objects.json",
        json.dumps({"objects": [list(each) for each in objects]}).encode(),
    )
    return [
        _action(
            f"mirror-await-endpoint-objects-{target.id}",
            "await_endpoint_objects",
            target,
            executor=EXECUTOR,
            payload=path,
            digest=digest,
            depends_on=after,
            resources=(f"warehouse:{target.item_id}",),
        )
    ]


def mirror_mutation_plan(resolved, *, session) -> tuple[MutationPlan, dict, dict]:
    """The mirror as one sealed plan, its payloads, and what it reconstructs."""

    from .declaration.model import LAKEHOUSE

    workspace = resolved.workspace
    compiling = _Compiling(str(workspace.workspace))
    catalogue = compiling.target("warehouse", resolved.destination.name)

    wipe, written = target_wipe_actions(
        catalogue, payload_dir="payload/mirror/wipe-catalogue"
    )
    compiling.payloads.update(written)
    compiling.stage(f"empty {resolved.plan.target}", catalogue, wipe)
    built = _compose_catalogue_build(
        compiling, resolved, catalogue=catalogue, after=finishing(wipe)
    )

    #: Lakehouses whose SQL endpoints this plan refreshes.
    endpoints = {
        other.destination.casefold()
        for other in resolved.items
        if other.kind == LAKEHOUSE
    }
    reconstructed: dict[str, tuple[str, ...]] = {}
    #: What a reader through each Lakehouse's SQL endpoint waits for.
    current: dict[str, tuple[str, ...]] = {}
    published: list[str] = []
    summary = {}
    for each in resolved.items:
        destination = compiling.target(each.kind.lower(), each.destination)
        cleared, written = target_wipe_actions(
            destination, payload_dir=f"payload/mirror/wipe-{destination.id}"
        )
        compiling.payloads.update(written)
        compiling.stage(f"empty {each.target}", destination, cleared)
        # A Lakehouse reads its producers through OneLake; a Warehouse reads
        # them through their SQL endpoints.
        produced = reconstructed if each.kind == LAKEHOUSE else current
        producers = tuple(
            action
            for item in _producers(each, resolved)
            for action in produced.get(item, ())
        )
        if each.kind == LAKEHOUSE:
            actions, summary[str(each.item)] = _lakehouse(
                compiling,
                resolved,
                each,
                destination,
                after=finishing(cleared),
                catalogue_built=built,
                producers=producers,
                session=session,
            )
        else:
            actions, summary[str(each.item)] = _warehouse(
                compiling,
                resolved,
                each,
                destination,
                after=finishing(cleared),
                catalogue_built=built,
                producers=producers,
                endpoints=endpoints,
                session=session,
            )
        reconstructed[str(each.item)] = tuple(a.id for a in actions)
        published.extend((*finishing(cleared), *(a.id for a in actions)))
        if each.kind == LAKEHOUSE:
            refreshed = _refresh_endpoint(
                compiling,
                each,
                destination,
                after=(*finishing(cleared), *(a.id for a in actions)),
            )
            current[str(each.item)] = (refreshed,)
            published.append(refreshed)
    _publish(compiling, resolved, catalogue, after=(built, *published))

    spark_home = next(
        (
            target.id
            for target in compiling.targets.values()
            if target.kind == "lakehouse"
            and not target.id.startswith("source-")
            and any(
                a.target_id == target.id and _uses_spark(a)
                for s in compiling.sequences
                for b in s.batches
                for a in b.actions
            )
        ),
        None,
    )
    every = tuple(
        a.id for s in compiling.sequences for b in s.batches for a in b.actions
    )
    plan = MutationPlan(
        targets=tuple(compiling.targets.values()),
        sequences=tuple(compiling.sequences),
        execution=MutationExecution(
            workspace_name=compiling.workspace,
            catalogue_target_id=catalogue.id,
            environment=_environment(workspace),
            spark_home_target_id=spark_home,
        ),
        required_completion=every,
        protected_scopes=tuple(compiling.protected),
        driver_contracts=ENDPOINT_REFRESH_CONTRACTS if current else (),
    )
    return replace(plan, bundle_id=compute_bundle_id(plan)), compiling.payloads, summary


def _refresh_endpoint(compiling, each, destination, *, after) -> str:
    """Refresh a reconstructed Lakehouse's SQL endpoint; return the await.

    The refresh starts once the reconstruction has a known outcome.
    """

    from .build_bundle.models import AWAIT_ENDPOINT_REFRESH, START_ENDPOINT_REFRESH
    from .mutation.models import ResultReference

    slug = destination.id
    start = replace(
        _action(
            f"mirror-start-endpoint-refresh-{slug}",
            START_ENDPOINT_REFRESH,
            destination,
            executor=START_EXECUTOR,
        ),
        settle_after=tuple(dict.fromkeys(after)),
    )
    finish = replace(
        _action(
            f"mirror-await-endpoint-refresh-{slug}",
            AWAIT_ENDPOINT_REFRESH,
            destination,
            executor=AWAIT_EXECUTOR,
            depends_on=(start.id,),
        ),
        result_from=ResultReference(start.id, REFRESH_RESULT),
    )
    compiling.stage(f"refresh {each.target} SQL endpoint", destination, [start, finish])
    return finish.id


def _uses_spark(action) -> bool:
    from .build_bundle.execution import needs_spark

    return needs_spark(action)


def _environment(workspace):
    from .mutation.execution import BundleEnvironment

    reference = workspace.environment
    if reference is None:
        return None
    return BundleEnvironment(name=reference.name, workspace=reference.workspace)


def _producers(each, resolved) -> tuple[str, ...]:
    """Mirrored items whose destinations this item's recreated shortcuts read."""

    mirrored = {str(other.item) for other in resolved.items}
    return tuple(
        sorted(
            {
                str(shortcut.target_item)
                for shortcut in each.shortcuts
                if shortcut.is_logical
                and shortcut.target_item is not None
                and str(shortcut.target_item) in mirrored
                and str(shortcut.target_item) != str(each.item)
            }
        )
    )


def _compose_catalogue_build(compiling, resolved, *, catalogue, after) -> str:
    """Add the destination catalogue's Build, planned against an empty Warehouse.

    The destination is emptied first in this same plan, so its state is known
    and nothing is read from it. Returns the action that completes the build.
    """

    from .build_bundle import (
        ItemBindings,
        WarehouseBinding,
        effective_item_bindings,
    )
    from .build_bundle.execution import ExecutionIdentity
    from .build_bundle.prune import TargetInventory
    from .build_bundle.workflow import (
        BuildState,
        build_repository_bundle,
        prepare_repository,
    )
    from .catalogue.builtin import BUILTIN_ITEM
    from .catalogue.state import Catalogue
    from .locations import Location
    from .store import FilesystemStore

    workspace = resolved.workspace
    control = WarehouseBinding(
        workspace.catalogue_item, workspace_name=workspace.workspace
    )
    bindings = effective_item_bindings(
        ItemBindings(()),
        control_item=workspace.catalogue_item,
        workspace_name=workspace.workspace,
    )
    with tempfile.TemporaryDirectory(prefix="weaver-mirror-") as scratch:
        empty = Path(scratch) / "repository"
        empty.mkdir()
        with prepare_repository(
            Location(empty.as_posix()), source_store=FilesystemStore()
        ) as ready:
            target = bindings.by_item[BUILTIN_ITEM].to_bound_target()
            bundle = build_repository_bundle(
                ready.repository,
                state=BuildState(
                    catalogue=Catalogue(rows={}),
                    target_inventories={
                        BUILTIN_ITEM: TargetInventory(
                            target_id=target.id,
                            kind=target.kind,
                            target_name=target.name,
                        )
                    },
                ),
                bindings=bindings,
                catalogue_binding=control,
                execution=ExecutionIdentity(workspace_name=str(workspace.workspace)),
                source_store=ready.store,
                output=Location((Path(scratch) / "bundle").as_posix()),
            )
            built = bundle.plan
            for _s, _b, action in built.actions():
                if action.payload is not None:
                    compiling.payloads[f"payload/mirror/catalogue/{action.payload}"] = (
                        bundle.store.read(
                            bundle.location.join(*action.payload.split("/"))
                        )
                    )
    for physical in built.targets:
        compiling.targets.setdefault(physical.id, physical)
    for sequence in built.sequences:
        compiling.sequences.append(
            MutationSequence(
                len(compiling.sequences) + 1,
                f"catalogue: {sequence.description}",
                tuple(
                    replace(
                        batch,
                        id=f"catalogue-{batch.id}",
                        actions=tuple(
                            replace(
                                action,
                                payload=None
                                if action.payload is None
                                else f"payload/mirror/catalogue/{action.payload}",
                                # Roots wait for the destination to be emptied.
                                depends_on=action.depends_on
                                or (() if action.settle_after else tuple(after)),
                                settle_after=action.settle_after,
                            )
                            for action in batch.actions
                        ),
                    )
                    for batch in sequence.batches
                ),
            )
        )
    (completed,) = built.required_completion
    return completed


def _warehouse(
    compiling,
    resolved,
    each,
    destination,
    *,
    after,
    catalogue_built,
    producers,
    endpoints,
    session,
):
    from .catalogue.borrow import (
        catalogue_schema_statements,
        missing_programmables,
        programmable_statements,
        schema_statements,
        surface_view_statements,
    )
    from .catalogue.borrow import view_statement as borrowed_view
    from .catalogue.shortcuts import schemas_of, view_source, view_statement
    from .operations.mirror import _recreatable
    from .targets import ItemRef, WarehouseTarget

    slug = destination.id
    pointers = _recreatable(each, bindings=resolved.bindings)
    rows = tuple(
        session.sql_executor(
            WarehouseTarget(ItemRef(each.source_target)), workspace=resolved.workspace
        ).query(
            "select schema_name(o.schema_id) as schema_name, o.name as object_name, "
            "m.definition as definition "
            "from sys.sql_modules as m "
            "join sys.objects as o on o.object_id = m.object_id "
            "where o.is_ms_shipped = 0 and o.type in (N'P', N'FN', N'IF', N'TF')"
        )
    )
    absent = missing_programmables(
        each.programmables,
        tuple(f"{row['schema_name']}.{row['object_name']}" for row in rows),
    )
    if absent:
        raise CommandError(
            f"The source item {each.item} is missing deployed code: "
            + ", ".join(identity.object_id.qualified for identity in absent)
            + f". Build {each.item} before mirroring it."
        )

    # Every schema is established once, before any branch writes into it, so
    # concurrent Warehouse lanes never race the same schema creation.
    schemas = _scripts(
        compiling,
        f"mirror-schemas-{slug}",
        destination,
        [
            *catalogue_schema_statements(),
            *schema_statements(
                [
                    *(borrowed.schema for borrowed in each.relations),
                    *schemas_of(pointers),
                    *(str(row["schema_name"]) for row in rows),
                ]
            ),
        ],
        kind="create_schemas",
        depends_on=after,
    )
    ready = tuple(a.id for a in schemas)
    actions = _scripts(
        compiling,
        f"mirror-relations-{slug}",
        destination,
        [
            borrowed_view(borrowed.identity, source_target=each.source_target)
            for borrowed in each.relations
        ],
        kind="borrow_relations",
        depends_on=ready,
    )
    relations_built = tuple(a.id for a in actions)
    # The ``_`` surface views read the destination catalogue's tables.
    surface = _scripts(
        compiling,
        f"mirror-surface-{slug}",
        destination,
        surface_view_statements(resolved.destination.name),
        kind="borrow_surface",
        depends_on=(*ready, catalogue_built),
    )
    # A pointer into this same destination reads a relation rebuilt above.
    reads_here = any(
        p.target_workspace is None
        and p.target_name.casefold() == each.destination.casefold()
        for p in pointers
    )
    # A refreshed endpoint can still be listing new tables, so the views over
    # it wait until this Warehouse sees every object they read.
    awaited = _await_endpoint_objects(
        compiling,
        destination,
        sorted(
            {
                view_source(p)
                for p in pointers
                if p.target_workspace is None and p.target_name.casefold() in endpoints
            }
        ),
        after=(*ready, *producers),
    )
    recreated = _scripts(
        compiling,
        f"mirror-pointers-{slug}",
        destination,
        [view_statement(p) for p in pointers],
        kind="recreate_shortcuts",
        depends_on=(
            *ready,
            *producers,
            *(a.id for a in awaited),
            *(relations_built if reads_here else ()),
        ),
    )
    # Copied code may read any local relation, pointer or surface view, and
    # Fabric refuses a definition while DDL on an object it references runs.
    # One routine may call another, so the copies keep the source's order.
    code = programmable_statements(str(row["definition"]) for row in rows)
    programmables = _scripts(
        compiling,
        f"mirror-code-{slug}",
        destination,
        code,
        kind="copy_programmables",
        depends_on=(
            *ready,
            *relations_built,
            *(a.id for a in surface),
            *(a.id for a in recreated),
        ),
        ordered=True,
    )
    every = [*schemas, *actions, *surface, *awaited, *recreated, *programmables]
    # Each stage waits on something different, so each is timed on its own.
    compiling.stage(f"reconstruct {each.target}", destination, [*schemas, *actions])
    compiling.stage(f"recreate catalogue views in {each.target}", destination, surface)
    compiling.stage(
        f"recreate views over other items in {each.target}",
        destination,
        [*awaited, *recreated],
    )
    compiling.stage(
        f"copy procedures and functions into {each.target}", destination, programmables
    )
    return every, {
        "source": each.source,
        "target": each.target,
        "relations": len(each.relations),
        "pointers": len(pointers),
        "programmables": len(code),
    }


def _lakehouse(
    compiling,
    resolved,
    each,
    destination,
    *,
    after,
    catalogue_built,
    producers,
    session,
):
    from .build_bundle.models import AWAIT_TABLE_SHORTCUTS
    from .build_bundle.shortcuts import readiness_actions
    from .catalogue.borrow import wrapper_view_statement
    from .fabric.resources import LAKEHOUSE as LAKEHOUSE_ITEM
    from .fabric.resources import WAREHOUSE as WAREHOUSE_ITEM
    from .operations.mirror import _recreatable
    from .targets import ItemRef

    resolver = session.resolver(resolved.workspace)
    store = session.store(resolved.workspace)
    names = StoredNames(store)
    slug = destination.id
    source_item = resolver.external_item(each.source_target, item_type=LAKEHOUSE_ITEM)
    source_root = resolver.external_root(source_item)

    from .catalogue.borrow import pointer_shortcuts, surface_shortcuts
    from .catalogue.shortcuts import shortcut_request

    pointers = pointer_shortcuts(
        each.relations,
        source=source_item,
        path_of=lambda borrowed: names.resolve(
            source_root,
            (borrowed.area, borrowed.schema, borrowed.name),
            what=f"mirror reads {each.source} for {borrowed.identity}",
        ),
    )
    recreatable = _recreatable(each, bindings=resolved.bindings)
    # This plan builds these destinations, under their recorded spellings.
    built_here = {other.destination.casefold() for other in resolved.items}
    recreated = []
    for pointer in recreatable:
        item = resolver.external_item(
            pointer.target_name,
            item_type=pointer.shortcut.target_item.item_type,
            workspace=pointer.target_workspace,
        )
        if (
            pointer.target_workspace is None
            and pointer.target_name.casefold() in built_here
        ):
            source_path = "/".join(pointer.source_components)
        else:
            source_path = names.resolve(
                resolver.external_root(item),
                pointer.source_components,
                what=f"mirror recreates {pointer.destination}, which reads "
                f"{pointer.target_name}",
            )
        recreated.append(
            shortcut_request(pointer, source=item, source_path=source_path)
        )
    surface = surface_shortcuts(
        each.item,
        catalogue=resolver.external_item(
            resolved.workspace.catalogue_item.name, item_type=WAREHOUSE_ITEM
        ),
    )

    actions = []
    spark = ("spark",)
    schemas = sorted({borrowed.schema for borrowed in each.relations})
    if schemas:
        from .spark import FabricSparkTarget

        target = FabricSparkTarget(
            workspace=str(resolved.workspace.workspace), lakehouse=each.destination
        )
        path, digest = compiling.payload(
            f"schemas-{slug}.spark-sql-batch.json",
            json.dumps([target.create_schema_statement(s) for s in schemas]).encode(),
        )
        actions.append(
            _action(
                f"mirror-schemas-{slug}",
                "create_schema",
                destination,
                executor="spark_sql_batch",
                payload=path,
                digest=digest,
                depends_on=after,
                resources=spark,
            )
        )
    schemas_ready = tuple(a.id for a in actions)

    def create(name, requests, depends_on):
        if not requests:
            return None
        frozen = [
            {
                "shortcut": request["shortcut"],
                "type": request["type"],
                "path": request["path"],
                "name": request["name"],
                "source": str(request["source"].name),
                "source_workspace_id": request["source"].workspace_id,
                "source_item_id": request["source"].id,
                "source_item_name": request["source"].name,
                "source_path": request["source_path"],
            }
            for request in requests
        ]
        path, digest = compiling.payload(
            f"{name}-{slug}.shortcut.json",
            json.dumps({"shortcuts": frozen}, sort_keys=True).encode(),
        )
        made = _action(
            f"mirror-{name}-{slug}",
            "create_shortcut",
            destination,
            executor="shortcut",
            payload=path,
            digest=digest,
            depends_on=depends_on,
            resources=(f"shortcuts:{destination.item_id}",),
        )
        actions.append(made)
        local: dict[str, bytes] = {}
        for waiting, _names in readiness_actions(
            frozen, local, name=f"{name}-{slug}", file=f"{name}-{slug}"
        ):
            path, digest = compiling.payload(waiting.payload, local[waiting.payload])
            actions.append(
                _action(
                    f"mirror-{waiting.id}",
                    waiting.kind,
                    destination,
                    executor="shortcut_readiness",
                    payload=path,
                    digest=digest,
                    depends_on=(made.id,),
                    resources=spark
                    if waiting.kind == AWAIT_TABLE_SHORTCUTS
                    else (f"onelake:{destination.item_id}",),
                )
            )
        return made

    created = len(actions)
    create("pointers", pointers, (*after, *schemas_ready))
    pointers_ready = tuple(a.id for a in actions[created:])
    # A recreated shortcut into this same destination reads a pointer above.
    reads_here = any(
        p.target_workspace is None
        and p.target_name.casefold() == each.destination.casefold()
        for p in recreatable
    )
    create(
        "recreated",
        recreated,
        (
            *after,
            *schemas_ready,
            *producers,
            *(pointers_ready if reads_here else ()),
        ),
    )
    # The ``_`` surface reads the destination catalogue's tables.
    create("surface", surface, (*after, catalogue_built))

    source = BoundTarget(
        id=f"source-lakehouse-{each.source_target}",
        kind="lakehouse",
        item_id=each.source_target,
        item_name=each.source_target,
        workspace_name=compiling.workspace,
    )
    destination_spark = resolver.spark_destination(ItemRef(each.destination))
    source_spark = resolver.spark_destination(ItemRef(each.source_target))
    wrapped = [
        wrapper_view_statement(
            borrowed, destination=destination_spark, source=source_spark
        )
        for borrowed in each.relations
        if not borrowed.is_pointer
    ]
    # Each action is one Spark submission, so views spread across the lanes.
    for index in range(0, len(wrapped), VIEWS_PER_ACTION):
        chunk = wrapped[index : index + VIEWS_PER_ACTION]
        number = index // VIEWS_PER_ACTION
        path, digest = compiling.payload(
            f"views-{slug}-{number:03d}.spark-sql-batch.json",
            json.dumps(chunk).encode(),
        )
        actions.append(
            _action(
                f"mirror-views-{slug}-{number:03d}",
                "build_view",
                destination,
                executor="spark_sql_batch",
                payload=path,
                digest=digest,
                depends_on=(*after, *schemas_ready),
                resources=spark,
            )
        )

    load_root = source_root.join("Files", *LOAD_TREE.split("/"))
    files = []
    if store.exists(load_root):
        prefix = load_root.value.rstrip("/") + "/"
        files = sorted(
            entry.location.value[len(prefix) :]
            for entry in store.list(load_root, recursive=True)
            if not entry.is_directory
        )
    if files:
        compiling.targets.setdefault(source.id, source)
        compiling.protected.append(PhysicalScope(source.id, ""))
        path, digest = compiling.payload(
            f"load-{slug}.copy-files.json",
            json.dumps(
                {"source_target_id": source.id, "path": LOAD_TREE, "files": files}
            ).encode(),
        )
        actions.append(
            _action(
                f"mirror-load-tree-{slug}",
                "copy_load_tree",
                destination,
                executor="copy_files",
                payload=path,
                digest=digest,
                depends_on=after,
                resources=(f"onelake:{destination.item_id}",),
            )
        )
    compiling.stage(f"reconstruct {each.target}", destination, actions)
    return actions, {
        "source": each.source,
        "target": each.target,
        "relations": len(each.relations),
        "shortcuts": len(pointers) + len(recreated) + len(surface),
        "pointers": len(recreatable),
        "views": len(wrapped),
        "files": len(files),
    }


def _publish(compiling, resolved, catalogue, *, after) -> None:
    """Fork the source's state, record what each item borrows and bind each item.

    One transaction, after every reconstruction: until it commits, the
    destination catalogue records no installation, so nothing can read it as
    owning the source's items or an incomplete destination.
    """

    from . import __version__
    from .catalogue.borrow import record_statements
    from .catalogue.fork import copied_tables, copy_statement, create_statement
    from .catalogue.render import InstallationScope, render_merge
    from .catalogue.tables import INSTALLATION, MIRROR

    recorded = [
        statement
        for each in resolved.items
        for statement in record_statements(
            each.relations,
            source_workspace=resolved.workspace.workspace,
            source_target=each.source_target,
        )
        # ``_.Mirror`` is created below, outside the transaction.
        if statement != create_statement(MIRROR)
    ]
    merges = []
    for each in resolved.items:
        item = each.item
        row = dict(resolved.installations.get(item) or {})
        row.update(
            {
                "item_type": item.item_type,
                "item_name": item.item_name,
                "target_name": each.destination,
                "weaver_version": __version__,
                "signature": str(row.get("signature") or ""),
            }
        )
        merges.append(
            render_merge(
                INSTALLATION,
                [row],
                scope=InstallationScope(item.item_type, item.item_name),
            )
        )
    forked = [
        copy_statement(table, source_catalogue=resolved.source.name)
        for table in copied_tables(borrowed=resolved.borrowed)
    ]
    created = [create_statement(MIRROR)] if resolved.borrowed or recorded else []
    path, digest = compiling.payload(
        "publish.sql",
        transaction_script(created, [*forked, *recorded, *merges]).encode("utf-8"),
    )
    published = _action(
        "mirror-publish-catalogue",
        "publish_mirror",
        catalogue,
        executor="tsql",
        payload=path,
        digest=digest,
        depends_on=tuple(dict.fromkeys(after)),
        resources=(f"warehouse:{catalogue.item_id}",),
    )
    compiling.stage("publish the mirrored catalogue", catalogue, [published])


def transaction_script(prepared, statements) -> str:
    """``prepared`` statements, then ``statements`` committed together or not at all.

    Fabric Warehouse refuses ``SET XACT_ABORT``, so a failure rolls back in
    ``CATCH`` and is raised again.
    """

    body = "\n".join("    " + line for line in "\n".join(statements).splitlines())
    return "\n".join(
        [
            *prepared,
            "begin try",
            "    begin transaction;",
            body,
            "    commit transaction;",
            "end try",
            "begin catch",
            "    if @@trancount > 0 rollback transaction;",
            "    throw;",
            "end catch;",
        ]
    )


class StoredNames:
    """Resolve case-exact storage paths with one listing per directory."""

    def __init__(self, store) -> None:
        self.store = store
        self._listed: dict[str, dict[str, list[str]]] = {}

    def resolve(self, root, components, *, what: str) -> str:
        from .errors import BuildError

        settled: list[str] = []
        for component in components:
            parent = root.join(*settled) if settled else root
            names = self._names(parent, what=what)
            if component in names.get(component.casefold(), ()):
                settled.append(component)
                continue
            matches = sorted(names.get(component.casefold(), ()))
            if len(matches) != 1:
                if not matches:
                    raise BuildError(f"{what}; {component!r} is not in {parent.value}")
                raise BuildError(
                    f"{what}; {component!r} matches more than one entry in "
                    f"{parent.value}: " + ", ".join(matches)
                )
            settled.append(matches[0])
        return "/".join(settled)

    def _names(self, parent, *, what: str) -> dict[str, list[str]]:
        from .errors import BuildError

        if parent.value not in self._listed:
            try:
                entries = self.store.list(parent)
            except Exception as exc:  # noqa: BLE001 - reported with its subject
                raise BuildError(
                    f"{what}; could not read {parent.value}: {type(exc).__name__}: {exc}"
                ) from exc
            folded: dict[str, list[str]] = {}
            for entry in entries:
                folded.setdefault(entry.location.name.casefold(), []).append(
                    entry.location.name
                )
            self._listed[parent.value] = folded
        return self._listed[parent.value]
