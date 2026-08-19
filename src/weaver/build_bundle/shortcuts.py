"""Planning each declared shortcut and external view as one physical action.

A shortcut is the consuming item's own name for something else: another Weaver
item's document, or a physical Fabric item that Weaver does not manage. It is
owned by its destination item, so it is planned as part of that item's work and
ahead of every document the item declares, because those documents are written
against the namespace it establishes.

What a declaration *becomes* depends on the target it is bound to, and that is a
planning decision because it is a decision about the target kind:

===============================  ==============================================
Lakehouse                        a OneLake shortcut, made over REST
Warehouse                        a frozen view over the source's three-part name
===============================  ==============================================

Both are one ``create_shortcut`` action, and the payload says which it is: a
Lakehouse entry carries the shortcut's type, and a Warehouse entry is a T-SQL
batch. Only the Warehouse form is spelled out in SQL, because only there is the
statement itself the decision. A shortcut carries one frozen decision: this
destination, that source.

**Weaver owns the shortcut root and nothing reachable through it.** OneLake makes
a shortcut a read-write window into the item it points at, so a write beneath one
lands in that item. Nothing is planned inside a schema or folder shortcut, and
:func:`weaver.declaration.shortcuts.validate_destinations` refuses a repository
that declares something there.

A declaration the current bindings give no physical form is left out of the plan
and recorded as an omission. That is the planner's decision: the installer may
only run an action already frozen for it.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Iterable, Mapping

from ..declaration.model import (
    WeaverDocumentId,
    WeaverItemId,
)
from .changes import (
    FOLDER as FOLDER_KIND,
)
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
from .models import (
    CREATE_SHORTCUT,
    OMIT_SHORTCUT_UNSUPPORTED,
    BuildBatch,
    InstallAction,
    OmittedNode,
)
from .payloads import sha256_hex
from .stages import SHORTCUT, PlannedStage
from .targets import WAREHOUSE_TARGET, BoundTarget

#: Where a Lakehouse shortcut is materialised, by what it points at.
TABLES_AREA = "Tables"
FILES_AREA = "Files"


@dataclass(frozen=True)
class ResolvedShortcutSource:
    """The physical address a direct shortcut points at, frozen at generation.

    Resolved where the estate can be read, and carried in the bundle from there:
    Fabric validates a shortcut's target when it is created and its paths are
    case-sensitive, so an address guessed at install time is a 400 rather than a
    wrong answer. Freezing it also keeps the installer free of cross-workspace
    resolution, which nothing else needs.
    """

    workspace_id: str
    item_id: str
    item_name: str
    path: str


def _slug(value) -> str:
    return str(value).replace("/", "--").replace(" ", "-")


def shortcut_node_id(destination) -> str:
    """How a shortcut is named in a plan.

    Prefixed, because a shortcut destination is not a repository document:
    nothing declares it, and no dependency layer contains it.
    """

    return f"shortcut:{destination}"


@dataclass(frozen=True)
class ItemShortcutPlan:
    """One item's planned shortcuts, the schemas they need, and what was left out."""

    stage: PlannedStage | None = None
    schemas: tuple[str, ...] = ()
    omitted: tuple[OmittedNode, ...] = ()
    #: The destinations behind ``omitted``, as identities rather than node ids.
    #: The caller needs them because a shortcut with no physical form must not be
    #: certified as installed.
    omitted_destinations: tuple[object, ...] = ()


def plan_item_shortcuts(
    repository,
    *,
    item: WeaverItemId,
    target: BoundTarget,
    target_by_item: Mapping[WeaverItemId, BoundTarget],
    selected: Iterable[WeaverDocumentId],
    sources: Mapping[str, ResolvedShortcutSource] | None = None,
) -> ItemShortcutPlan:
    """Plan the shortcuts and external views this build selected.

    ``selected`` is what incremental selection chose to rebuild. A declaration
    absent from it is current: unchanged, its destination there, and its source
    not rebuilt since. So it is left alone rather than replaced, exactly as an
    unchanged document is.

    Its *schema* is still reported. A retained shortcut lives in a namespace the
    item must have, and a build that created only the schemas its rebuilt
    shortcuts needed would leave the others homeless. A schema shortcut reports
    none, because it *is* the namespace and the item does not own it.
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
    if not declarations:
        return ItemShortcutPlan()

    chosen = set(selected)
    omitted: list[OmittedNode] = []
    omitted_destinations: list[object] = []
    supported: list[tuple] = []
    schemas: list[str] = []

    logical_sources = {
        pair.destination: pair.source for pair in repository.logical_shortcuts
    }

    for declaration in declarations:
        source_target = None
        if declaration.is_logical:
            source = logical_sources.get(declaration.destination)
            source_target = (
                target_by_item.get(source.item) if source is not None else None
            )
        reason = _unsupported(
            declaration,
            target=target,
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
        if not declaration.is_schema:
            schemas.append(declaration.schema)
        if declaration.destination in chosen:
            supported.append((declaration, source_target))

    stage = None
    if supported:
        item_slug = _slug(item)
        payloads: dict[str, bytes] = {}
        action = _shortcut_action(
            supported,
            item=item,
            target=target,
            payloads=payloads,
            sources=sources,
            logical_sources=logical_sources,
        )
        # One action stands for every declaration the item consumes, so it
        # produces several changes. Each names what the destination physically
        # *is* at this binding, which is the same question the Registry row
        # answers.
        stage = PlannedStage(
            phase=SHORTCUT,
            slug="item-shortcuts",
            description="materialise item-owned shortcuts",
            payloads=payloads,
            changes={
                target.id: tuple(
                    added(
                        _change_kind(declaration, target),
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
                    actions=(action,),
                ),
            ),
        )
    return ItemShortcutPlan(
        stage=stage,
        schemas=tuple(sorted(set(schemas))),
        omitted=tuple(omitted),
        omitted_destinations=tuple(omitted_destinations),
    )


def _key(declaration) -> str:
    """How a declaration names its resolved source, where one is frozen."""

    return f"{declaration.owner}/{declaration.name}"


def _unsupported(
    declaration,
    *,
    target: BoundTarget,
    source_target: BoundTarget | None,
    sources: Mapping[str, ResolvedShortcutSource],
) -> str | None:
    """Why this declaration has no physical form here, or None when it has one."""

    if not declaration.is_logical:
        if declaration.is_view:
            return None
        if _key(declaration) not in sources:
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
    if not declaration.is_view and source_target.kind == WAREHOUSE_TARGET:
        return (
            "a Lakehouse shortcut is a OneLake shortcut, and there is no "
            f"shortcut form for the Warehouse source {source}"
        )
    return None


def _change_kind(declaration, target) -> str:
    """What a destination will look like to an inventory.

    The same rule :func:`weaver.build_bundle.prune.managed_sets` applies for the
    keep-set, and for the same reason: a destination is a schema, a folder, a
    view or a table according to what was declared and what it was bound to.
    """

    if declaration.is_view:
        return VIEW_KIND
    if declaration.is_schema:
        return SCHEMA_KIND
    return FOLDER_KIND if declaration.is_files else TABLE_KIND


def _change_name(declaration) -> str:
    if declaration.is_schema:
        return declaration.name
    return declaration.destination.object_id.qualified


def _shortcut_action(
    supported,
    *,
    item: WeaverItemId,
    target: BoundTarget,
    payloads: dict[str, bytes],
    sources: Mapping[str, ResolvedShortcutSource],
    logical_sources: Mapping,
) -> InstallAction:
    """One action for all of this item's declarations.

    One rather than one-per-declaration, because materialising a shortcut is not
    instantaneous and the cost is per *wait*, not per create. A Lakehouse
    shortcut becomes usable some seconds after it exists (measured at 6-31s), so
    N actions run serially pay N waits while one action that creates everything
    and then waits pays roughly one. A Warehouse view is a script, and the
    executor already runs a multi-statement script as one unit.

    The action reports each one it made in its details, so the manifest loses no
    traceability by grouping them.
    """

    item_slug = _slug(item)
    if target.kind == WAREHOUSE_TARGET:
        content = (
            json.dumps(
                [
                    _view_statement(declaration, source_target, logical_sources)
                    for declaration, source_target in supported
                ],
                indent=2,
                ensure_ascii=False,
            )
            + "\n"
        ).encode("utf-8")
        filename = f"shortcuts-{item_slug}.tsql-batch.json"
        executor = "tsql_batch"
    else:
        content = _shortcut_payload(
            supported, sources=sources, logical_sources=logical_sources
        )
        filename = f"shortcuts-{item_slug}.shortcut.json"
        executor = "shortcut"
    payloads[filename] = content
    return InstallAction(
        id=f"shortcuts-{item_slug}",
        kind=CREATE_SHORTCUT,
        # No single resource: this action stands for every declaration the item
        # consumes, and the payload names them.
        resource_node_id=None,
        executor=executor,
        payload=filename,
        payload_sha256=sha256_hex(content),
    )


def _view_statement(declaration, source_target, logical_sources) -> str:
    """An external view, as the one statement that makes it exist.

    The source is named by its three-part spelling, which is how a Fabric
    Warehouse reaches another item in the same workspace, and is frozen here for
    the same reason an authored three-part reference is: it is the semantic
    decision, not transport. A bound reference is spelled with the item its
    binding resolved to; a direct one with the physical item the author named.

    ``CREATE OR ALTER`` rather than a drop and a create. A view over another
    item holds no data, so replacing one is not a destructive transition needing
    proof of prior state, and a build that could not run twice over its own
    views would not be re-runnable at all. It is also the only *single-statement*
    way to say that, and T-SQL requires ``CREATE VIEW`` to be the first statement
    in its batch.
    """

    destination = declaration.destination
    schema = _tsql_ident(destination.object_id.schema)
    name = _tsql_ident(destination.object_id.object)
    if declaration.is_logical:
        source = logical_sources[destination]
        item_name = source_target.name
        source_object = source.object_id
    else:
        item_name = declaration.target_item.item_name
        source_object = declaration.target_object
    source_sql = ".".join(
        _tsql_ident(part)
        for part in (item_name, source_object.schema, source_object.object)
    )
    return f"create or alter view {schema}.{name} as select * from {source_sql};"


def _shortcut_payload(supported, *, sources, logical_sources) -> bytes:
    """This item's Lakehouse shortcuts, as the frozen addresses they stand for.

    A bound source is named by target id: the installer already resolves every
    target the plan declares through its own environment, so it is addressed
    exactly as the destination is and the bundle carries no path from the machine
    that wrote it. A direct source is named by the workspace and item it was
    resolved to, because it is not a target of this build and nothing at install
    time would know where to look.
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
            source = logical_sources[destination]
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
            resolved = sources[_key(declaration)]
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


def _destination_path(declaration) -> str:
    """Where the shortcut is created, as Fabric addresses it.

    A schema shortcut sits directly under ``Tables`` and is named for the schema
    it presents; everything else sits under its schema. Measured against Fabric:
    a schema shortcut is ``path=Tables, name=<Schema>``.
    """

    if declaration.is_schema:
        return TABLES_AREA
    area = FILES_AREA if declaration.is_files else TABLES_AREA
    return f"{area}/{declaration.destination.object_id.schema}"


def _destination_name(declaration) -> str:
    if declaration.is_schema:
        return declaration.name
    return declaration.destination.object_id.object


def _tsql_ident(name: str) -> str:
    return "[" + name.replace("]", "]]") + "]"
