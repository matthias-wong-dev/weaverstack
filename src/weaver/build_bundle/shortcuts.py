"""Plan each declared shortcut as part of its destination item's work.

Shortcuts precede the documents that use their namespace. Their physical form is
fixed by the destination target:

===============================  ==============================================
Lakehouse                        a OneLake shortcut, made over REST
Warehouse                        a frozen view over the source's three-part name
===============================  ==============================================

One action carries the item's frozen destination-source pairs. Weaver owns each
shortcut root, never the data reachable through it, so no object may be declared
beneath a schema or folder shortcut. The bundle planner reports unsupported
selected declarations; installation runs only frozen actions.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Iterable, Mapping

from ..declaration.model import (
    WeaverDocumentId,
    WeaverItemId,
)
from ..targets import FILES_AREA, TABLES_AREA
from .changes import (
    FOLDER as FOLDER_KIND,
)
from .changes import RUNTIME_REFERENCE as RUNTIME_REFERENCE_KIND
from .changes import (
    SCHEMA as SCHEMA_KIND,
)
from .changes import (
    TABLE as TABLE_KIND,
)
from .changes import (
    VIEW as VIEW_KIND,
)
from .changes import (
    added,
)
from .dependencies import (
    action_key,
    dropped_key,
    endpoint_object_key,
    object_key,
    schema_key,
)
from .models import (
    AWAIT_FILE_SHORTCUTS,
    AWAIT_TABLE_SHORTCUTS,
    CREATE_SHORTCUT,
    OMIT_SHORTCUT_UNSUPPORTED,
    BuildBatch,
    InstallAction,
    OmittedNode,
)
from .payloads import sha256_hex
from .stages import SHORTCUT, PlannedStage
from .targets import WAREHOUSE_TARGET, BoundTarget


@dataclass(frozen=True)
class ResolvedShortcutSource:
    """A direct source's case-exact address, frozen during generation."""

    workspace_id: str
    item_id: str
    item_name: str
    path: str


def _slug(value) -> str:
    return str(value).replace("/", "--").replace(" ", "-")


def shortcut_node_id(destination) -> str:
    """Prefix destinations that are not repository document nodes."""

    return f"shortcut:{destination}"


@dataclass(frozen=True)
class ItemShortcutPlan:
    """One item's shortcut stage, prerequisite schemas and omissions."""

    stage: PlannedStage | None = None
    schemas: tuple[str, ...] = ()
    omitted: tuple[OmittedNode, ...] = ()
    omitted_destinations: tuple[object, ...] = ()


def _is_the_catalogue_itself(
    target: BoundTarget, catalogue: BoundTarget | None
) -> bool:
    """Compare physical kind and item id, not the logical binding id."""

    return (
        catalogue is not None
        and target.kind == catalogue.kind
        and target.item_id == catalogue.item_id
    )


def _references_the_catalogue(declaration, logical_sources) -> bool:
    from ..catalogue.builtin import BUILTIN_ITEM

    source = logical_sources.get(declaration.destination)
    return source is not None and source.item == BUILTIN_ITEM


def plan_lakehouse_shortcuts(
    repository,
    *,
    item: WeaverItemId,
    target: BoundTarget,
    target_by_item: Mapping[WeaverItemId, BoundTarget],
    selected: Iterable[WeaverDocumentId],
    sources: Mapping[str, ResolvedShortcutSource] | None = None,
    catalogue_target: BoundTarget | None = None,
) -> ItemShortcutPlan:
    return _plan_item_shortcuts(
        repository,
        item=item,
        target=target,
        target_by_item=target_by_item,
        selected=selected,
        sources=sources,
        catalogue_target=catalogue_target,
        action_renderer=_lakehouse_shortcut_action,
    )


def plan_warehouse_shortcuts(
    repository,
    *,
    item: WeaverItemId,
    target: BoundTarget,
    target_by_item: Mapping[WeaverItemId, BoundTarget],
    selected: Iterable[WeaverDocumentId],
    sources: Mapping[str, ResolvedShortcutSource] | None = None,
    catalogue_target: BoundTarget | None = None,
) -> ItemShortcutPlan:
    return _plan_item_shortcuts(
        repository,
        item=item,
        target=target,
        target_by_item=target_by_item,
        selected=selected,
        sources=sources,
        catalogue_target=catalogue_target,
        action_renderer=_warehouse_shortcut_action,
    )


def _plan_item_shortcuts(
    repository,
    *,
    item: WeaverItemId,
    target: BoundTarget,
    target_by_item: Mapping[WeaverItemId, BoundTarget],
    selected: Iterable[WeaverDocumentId],
    action_renderer,
    sources: Mapping[str, ResolvedShortcutSource] | None = None,
    catalogue_target: BoundTarget | None = None,
) -> ItemShortcutPlan:
    """Plan selected shortcuts while retaining every prerequisite schema.

    Unselected shortcuts remain in place, but their schemas are still required.
    A schema shortcut reports no prerequisite because it is the namespace.
    """

    sources = dict(sources or {})
    declarations = sorted(
        (
            declaration
            for declaration in repository.shortcuts
            if declaration.owner == item
        ),
        key=lambda declaration: str(declaration.destination),
    )
    logical_sources = {
        pair.destination: pair.source for pair in repository.logical_shortcuts
    }
    if _is_the_catalogue_itself(target, catalogue_target):
        # The catalogue Warehouse already owns `_`; a runtime view there would
        # point at itself and T-SQL would refuse it.
        declarations = [
            declaration
            for declaration in declarations
            if not _references_the_catalogue(declaration, logical_sources)
        ]
    if not declarations:
        return ItemShortcutPlan()

    chosen = set(selected)
    omitted: list[OmittedNode] = []
    omitted_destinations: list[object] = []
    supported: list[tuple] = []
    schemas: list[str] = []

    for declaration in declarations:
        if not declaration.is_schema and declaration.destination_identity is None:
            schemas.append(declaration.schema)
        if declaration.destination not in chosen:
            continue
        source_target = None
        if declaration.is_logical:
            source = logical_sources.get(declaration.destination)
            source_target = (
                target_by_item.get(source.item) if source is not None else None
            )
        reason = _unsupported(
            declaration,
            source_target=source_target,
            sources=sources,
        )
        if reason is not None:
            omitted.append(
                OmittedNode(
                    node_id=shortcut_node_id(declaration.destination),
                    reason=OMIT_SHORTCUT_UNSUPPORTED,
                    detail=reason,
                )
            )
            omitted_destinations.append(declaration.destination)
            continue
        supported.append((declaration, source_target))

    stage = None
    if supported:
        item_slug = _slug(item)
        payloads: dict[str, bytes] = {}
        action = action_renderer(
            supported,
            item=item,
            payloads=payloads,
            sources=sources,
            logical_sources=logical_sources,
        )
        awaited = _readiness_actions(action, payloads, item_slug=item_slug)
        # One action creates several destinations, so it records one change per
        # declaration in the inventory form selected by this binding.
        stage = PlannedStage(
            phase=SHORTCUT,
            slug="item-shortcuts",
            description="materialise item-owned shortcuts",
            payloads=payloads,
            changes={
                target.id: tuple(
                    added(
                        _change_kind(declaration),
                        _change_name(declaration),
                        action.id,
                    )
                    for declaration, _source_target in supported
                )
            },
            batches=(
                BuildBatch(
                    id=f"item-shortcuts-{item_slug}",
                    target_id=target.id,
                    actions=(action, *(each for each, _ in awaited)),
                ),
            ),
            requires={
                action.id: _creation_requirements(
                    supported, target=target, logical_sources=logical_sources
                ),
                **{each.id: (action_key(action.id),) for each, _ in awaited},
            },
            provides={
                action.id: tuple(
                    object_key(declaration.destination)
                    for declaration, _ in supported
                    if not any(
                        str(declaration.destination) in names for _, names in awaited
                    )
                ),
                **{
                    each.id: tuple(object_key(name) for name in names)
                    for each, names in awaited
                },
            },
        )
    return ItemShortcutPlan(
        stage=stage,
        schemas=tuple(sorted(set(schemas))),
        omitted=tuple(omitted),
        omitted_destinations=tuple(omitted_destinations),
    )


def _creation_requirements(supported, *, target, logical_sources) -> tuple[str, ...]:
    """Destination schemas, released destination names, and in-plan sources."""

    keys = []
    for declaration, source_target in supported:
        # A runtime reference lands in ``_``, which the item's schemas create.
        if not declaration.is_schema:
            keys.append(schema_key(target.id, declaration.schema))
        keys.append(dropped_key(declaration.destination))
        if not declaration.is_logical:
            continue
        source = logical_sources[declaration.destination]
        keys.append(object_key(source))
        if target.kind == WAREHOUSE_TARGET and source_target.kind != WAREHOUSE_TARGET:
            # A Warehouse view reads a Lakehouse table through its SQL endpoint.
            keys.append(endpoint_object_key(source))
    return tuple(keys)


#: Readiness kind for each shortcut type a consumer reads through.
_READINESS = {"table": AWAIT_TABLE_SHORTCUTS, "folder": AWAIT_FILE_SHORTCUTS}
_SURFACE = {AWAIT_TABLE_SHORTCUTS: "tables", AWAIT_FILE_SHORTCUTS: "files"}


def _readiness_actions(create, payloads, *, item_slug):
    """Await each consumer surface of a OneLake shortcut creation separately.

    A schema shortcut exposes a changing source namespace and is not awaited.
    Returns each readiness action with the destinations it makes readable.
    """

    if create.executor != "shortcut":
        return ()
    frozen = json.loads(payloads[create.payload].decode("utf-8"))["shortcuts"]
    return readiness_actions(
        frozen, payloads, name=f"shortcuts-{item_slug}", file=item_slug
    )


#: Shortcuts one readiness action checks. Each check is one or two Spark reads,
#: so chunks spread a large creation's wait across the Spark lanes.
READINESS_CHUNK = 25


def readiness_actions(frozen, payloads, *, name: str, file: str):
    """Readiness actions for created shortcuts, by surface and in chunks.

    ``name`` and ``file`` distinguish the action ids and payload names. Returns
    each action with the destinations it makes readable.
    """

    awaited = []
    for kind in (AWAIT_TABLE_SHORTCUTS, AWAIT_FILE_SHORTCUTS):
        members = [
            {key: each[key] for key in ("shortcut", "path", "name")}
            for each in frozen
            if _READINESS.get(each.get("type", "table")) == kind
        ]
        chunks = [
            members[start : start + READINESS_CHUNK]
            for start in range(0, len(members), READINESS_CHUNK)
        ]
        for index, chunk in enumerate(chunks):
            suffix = "" if len(chunks) == 1 else f"-{index:03d}"
            content = (
                json.dumps(
                    {"surface": _SURFACE[kind], "shortcuts": chunk},
                    indent=2,
                    sort_keys=True,
                    ensure_ascii=False,
                )
                + "\n"
            ).encode("utf-8")
            filename = f"{_SURFACE[kind]}-{file}{suffix}.shortcut-readiness.json"
            payloads[filename] = content
            awaited.append(
                (
                    InstallAction(
                        id=f"await-{_SURFACE[kind]}-{name}{suffix}",
                        kind=kind,
                        resource_node_id=None,
                        executor="shortcut_readiness",
                        payload=filename,
                        payload_sha256=sha256_hex(content),
                    ),
                    tuple(each["shortcut"] for each in chunk),
                )
            )
    return tuple(awaited)


def declaration_key(declaration) -> str:
    return f"{declaration.owner}/{declaration.name}"


def _unsupported(
    declaration,
    *,
    source_target: BoundTarget | None,
    sources: Mapping[str, ResolvedShortcutSource],
) -> str | None:
    if not declaration.is_logical:
        if declaration.is_view:
            return None
        if declaration_key(declaration) not in sources:
            return (
                "the physical target was not resolved when this bundle was "
                "generated, so there is no address to point at"
            )
        return None
    if source_target is None:
        return (
            f"target item {declaration.logical_source.item} is not bound, so "
            "there is no physical source to point at"
        )
    source = declaration.logical_source
    if not declaration.is_view and declaration.is_files != source.is_files:
        return (
            "a shortcut must stay in one namespace: a Files destination needs a "
            "Files source, and a table destination a table source"
        )
    if declaration.is_files and source_target.kind == WAREHOUSE_TARGET:
        return (
            "a Files shortcut needs a Lakehouse source, and the bound source "
            f"{source} is a Warehouse"
        )
    return None


def _change_kind(declaration) -> str:
    if declaration.is_view:
        return VIEW_KIND
    if declaration.destination_identity is not None:
        return RUNTIME_REFERENCE_KIND
    if declaration.is_schema:
        return SCHEMA_KIND
    return FOLDER_KIND if declaration.is_files else TABLE_KIND


def _change_name(declaration) -> str:
    if declaration.is_schema:
        return declaration.name
    if declaration.destination_identity is not None and not declaration.is_view:
        # Lakehouse runtime references are table names under ``Tables/_``;
        # Warehouse references are qualified views in the ordinary collection.
        return declaration.destination.object_id.object
    return declaration.destination.object_id.qualified


def _warehouse_shortcut_action(
    supported,
    *,
    item: WeaverItemId,
    payloads: dict[str, bytes],
    sources: Mapping[str, ResolvedShortcutSource],
    logical_sources: Mapping,
) -> InstallAction:
    item_slug = _slug(item)
    content = (
        json.dumps(
            [
                view_statement(declaration, source_target, logical_sources)
                for declaration, source_target in supported
            ],
            indent=2,
            ensure_ascii=False,
        )
        + "\n"
    ).encode("utf-8")
    filename = f"shortcuts-{item_slug}.tsql-batch.json"
    payloads[filename] = content
    return InstallAction(
        id=f"shortcuts-{item_slug}",
        kind=CREATE_SHORTCUT,
        # The payload names every destination; no single resource identifies it.
        resource_node_id=None,
        executor="tsql_batch",
        payload=filename,
        payload_sha256=sha256_hex(content),
    )


def _lakehouse_shortcut_action(
    supported,
    *,
    item: WeaverItemId,
    payloads: dict[str, bytes],
    sources: Mapping[str, ResolvedShortcutSource],
    logical_sources: Mapping,
) -> InstallAction:
    item_slug = _slug(item)
    content = shortcut_payload(
        supported, sources=sources, logical_sources=logical_sources
    )
    filename = f"shortcuts-{item_slug}.shortcut.json"
    payloads[filename] = content
    return InstallAction(
        id=f"shortcuts-{item_slug}",
        kind=CREATE_SHORTCUT,
        resource_node_id=None,
        executor="shortcut",
        payload=filename,
        payload_sha256=sha256_hex(content),
    )


def view_statement(declaration, source_target, logical_sources=None) -> str:
    """Render one Warehouse shortcut as a rerunnable view statement.

    The source's three-part target spelling is frozen during generation.
    ``CREATE OR ALTER`` must run in its own batch because T-SQL requires
    ``CREATE VIEW`` to be the first statement.
    """

    destination = declaration.destination
    schema = _tsql_ident(destination.object_id.schema)
    name = _tsql_ident(destination.object_id.object)
    if declaration.is_logical:
        source = (logical_sources or {})[destination]
        item_name = source_target.name
        source_object = source.object_id
    else:
        item_name = _target_item_name(declaration)
        source_object = declaration.target_object
    source_sql = ".".join(
        _tsql_ident(part)
        for part in (item_name, source_object.schema, source_object.object)
    )
    return f"create or alter view {schema}.{name} as select * from {source_sql};"


def shortcut_payload(supported, *, sources, logical_sources=None) -> bytes:
    """Freeze Lakehouse shortcut addresses for one item.

    Bound sources use a plan target id and are resolved like destinations. Direct
    sources carry the workspace, item and path resolved during generation.
    """

    frozen = []
    for declaration, source_target in supported:
        destination = declaration.destination
        entry = {
            "shortcut": str(destination),
            "type": declaration.shortcut_type,
            "path": _destination_path(declaration),
            "name": _destination_name(declaration),
        }
        if declaration.is_logical:
            source = (logical_sources or {})[destination]
            entry.update(
                {
                    "source": str(source),
                    "source_target_id": source_target.id,
                    "source_area": FILES_AREA if source.is_files else TABLES_AREA,
                    "source_schema": source.object_id.schema,
                    "source_object": source.object_id.object,
                }
            )
        else:
            resolved = sources[declaration_key(declaration)]
            entry.update(
                {
                    "source": declaration.target,
                    "source_workspace_id": resolved.workspace_id,
                    "source_item_id": resolved.item_id,
                    "source_item_name": resolved.item_name,
                    "source_path": resolved.path,
                }
            )
        frozen.append(entry)
    return (
        json.dumps({"shortcuts": frozen}, indent=2, sort_keys=True, ensure_ascii=False)
        + "\n"
    ).encode("utf-8")


def shortcut_removal_payload(destination) -> bytes:
    """Name one shortcut root for removal without naming its source."""

    area = FILES_AREA if destination.is_files else TABLES_AREA
    return (
        json.dumps(
            {
                "remove": [
                    {
                        "shortcut": str(destination),
                        "path": f"{area}/{destination.object_id.schema}",
                        "name": destination.object_id.object,
                    }
                ]
            },
            indent=2,
            sort_keys=True,
            ensure_ascii=False,
        )
        + "\n"
    ).encode("utf-8")


def _destination_path(declaration) -> str:
    """Place schema shortcuts at ``Tables`` and other shortcuts under a schema."""

    if declaration.is_schema:
        return TABLES_AREA
    area = FILES_AREA if declaration.is_files else TABLES_AREA
    return f"{area}/{declaration.destination.object_id.schema}"


def _destination_name(declaration) -> str:
    if declaration.is_schema:
        return declaration.name
    return declaration.destination.object_id.object


def _target_item_name(declaration) -> str:
    named = getattr(declaration, "target_item_name", None)
    return named if named is not None else declaration.target_item.item_name


def _tsql_ident(name: str) -> str:
    return "[" + name.replace("]", "]]") + "]"
