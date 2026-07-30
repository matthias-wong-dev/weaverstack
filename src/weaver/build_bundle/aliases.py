"""Planning each repository alias as one physical action.

An alias is the consuming item's own name for something another item produces.
It is owned by its destination item — the source stays the canonical producer —
so it is planned as part of that item's work, ahead of every document the item
declares, because those documents are written against the namespace it
establishes.

What an alias *becomes* depends on the destination it is bound to, and that is a
planning decision because it is a decision about the target kind:

===============================  ==============================================
Fabric or local Lakehouse        a ``create_alias`` action — a OneLake shortcut
                                 in Fabric, a filesystem link in the emulator
Fabric Warehouse                 a frozen view over the bound source
===============================  ==============================================

Only the last of the two is spelled out in SQL, because only there is the
statement itself the semantic decision. A shortcut and a link are two transports
for one frozen decision — this destination, that source — which is the same split
:mod:`weaver.build_bundle.executors.spark_schema` documents for ``LOCATION``.

Where the current bindings give an alias no physical form at all, it is left out
of the plan and recorded as an omission. That is deliberately the planner's
decision and never the installer's: the installer may only run an alias action
already frozen for it.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Mapping

from ..declaration.model import WeaverDocumentId, WeaverItemId
from .models import (
    CREATE_ALIAS,
    OMIT_ALIAS_UNSUPPORTED,
    BuildAction,
    BuildBatch,
    OmittedNode,
)
from .payloads import sha256_hex
from .stages import ALIAS, PlannedStage
from .targets import BoundTarget, WAREHOUSE_TARGET

#: Where a Lakehouse alias is materialised, by whether it names a Files document.
TABLES_AREA = "Tables"
FILES_AREA = "Files"


def _slug(value) -> str:
    return str(value).replace("/", "--").replace(" ", "-")


def alias_node_id(destination: WeaverDocumentId) -> str:
    """How an alias is named in a plan.

    Prefixed, because an alias destination is not a repository document: nothing
    declares it, and no dependency layer contains it.
    """

    return f"alias:{destination}"


@dataclass(frozen=True)
class ItemAliasPlan:
    """One item's planned aliases, the schemas they need, and what was left out."""

    stage: PlannedStage | None = None
    schemas: tuple[str, ...] = ()
    omitted: tuple[OmittedNode, ...] = ()


def plan_item_aliases(
    repository,
    *,
    item: WeaverItemId,
    target: BoundTarget,
    target_by_item: Mapping[WeaverItemId, BoundTarget],
) -> ItemAliasPlan:
    """Plan every alias this item consumes, against the current bindings."""

    aliases = sorted(
        (alias for alias in repository.aliases if alias.destination.item == item),
        key=lambda alias: str(alias.destination),
    )
    if not aliases:
        return ItemAliasPlan()

    payloads: dict[str, bytes] = {}
    actions: list[BuildAction] = []
    omitted: list[OmittedNode] = []
    schemas: list[str] = []

    for alias in aliases:
        source_target = target_by_item.get(alias.source.item)
        reason = _unsupported(alias, target=target, source_target=source_target)
        if reason is not None:
            omitted.append(
                OmittedNode(
                    node_id=alias_node_id(alias.destination),
                    reason=OMIT_ALIAS_UNSUPPORTED,
                    detail=reason,
                )
            )
            continue
        action = _alias_action(alias, target=target, source_target=source_target, payloads=payloads)
        actions.append(action)
        schemas.append(alias.destination.object_id.schema)

    stage = None
    if actions:
        stage = PlannedStage(
            phase=ALIAS,
            slug="item-aliases",
            description="materialise item-owned aliases",
            payloads=payloads,
            batches=(
                BuildBatch(
                    id=f"item-aliases-{_slug(item)}",
                    target_id=target.id,
                    actions=tuple(actions),
                ),
            ),
        )
    return ItemAliasPlan(
        stage=stage,
        schemas=tuple(sorted(set(schemas))),
        omitted=tuple(omitted),
    )


def _unsupported(alias, *, target: BoundTarget, source_target: BoundTarget | None) -> str | None:
    """Why this alias has no physical form here, or None when it has one."""

    if source_target is None:
        return (
            f"source item {alias.source.item} is not bound, so the alias has no "
            "physical source to point at"
        )
    if alias.destination.is_files != alias.source.is_files:
        return (
            "an alias must stay in one namespace — a Files destination needs a "
            "Files source, and a table destination a table source"
        )
    if target.kind != WAREHOUSE_TARGET and source_target.kind == WAREHOUSE_TARGET:
        return (
            "a Lakehouse alias is a OneLake shortcut, and there is no shortcut "
            f"form for the Warehouse source {alias.source}"
        )
    return None


def _alias_action(
    alias,
    *,
    target: BoundTarget,
    source_target: BoundTarget,
    payloads: dict[str, bytes],
) -> BuildAction:
    action_slug = _slug(alias.destination)
    if target.kind == WAREHOUSE_TARGET:
        content = _view_script(alias, source_target).encode("utf-8")
        filename = f"alias-{action_slug}.sql"
        executor = "tsql"
    else:
        content = _shortcut_payload(alias, target=target, source_target=source_target)
        filename = f"alias-{action_slug}.alias.json"
        executor = "alias"
    payloads[filename] = content
    return BuildAction(
        id=f"alias-{action_slug}",
        kind=CREATE_ALIAS,
        resource_node_id=alias_node_id(alias.destination),
        executor=executor,
        payload=filename,
        payload_sha256=sha256_hex(content),
    )


def _view_script(alias, source_target: BoundTarget) -> str:
    """A Warehouse alias, as the one statement that makes it exist.

    The source is named by its bound item's three-part spelling, which is how a
    Fabric Warehouse reaches another item in the same workspace, and is frozen
    here for the same reason an authored three-part reference is: it is the
    semantic decision, not transport.

    Dropped first rather than created strictly. An alias holds no data — it is a
    pointer — so replacing one is not a destructive transition needing proof of
    prior state, and a build that could not re-run over its own aliases would not
    be re-runnable at all.
    """

    schema = _tsql_ident(alias.destination.object_id.schema)
    name = _tsql_ident(alias.destination.object_id.object)
    source = ".".join(
        _tsql_ident(part)
        for part in (
            source_target.name,
            alias.source.object_id.schema,
            alias.source.object_id.object,
        )
    )
    return (
        f"drop view if exists {schema}.{name};\n"
        f"create view {schema}.{name} as select * from {source};\n"
    )


def _shortcut_payload(alias, *, target: BoundTarget, source_target: BoundTarget) -> bytes:
    """A Lakehouse alias, as the frozen pair of addresses it stands for.

    Named by target id rather than by resolved path: the installer already
    resolves every target the plan declares through its own environment, so the
    source is addressed exactly as the destination is, and a bundle carries no
    path from the machine that wrote it.
    """

    area = FILES_AREA if alias.destination.is_files else TABLES_AREA
    mapping = {
        "alias": str(alias.destination),
        "source": str(alias.source),
        "source_target_id": source_target.id,
        "area": area,
        "schema": alias.destination.object_id.schema,
        "object": alias.destination.object_id.object,
        "source_area": FILES_AREA if alias.source.is_files else TABLES_AREA,
        "source_schema": alias.source.object_id.schema,
        "source_object": alias.source.object_id.object,
    }
    return (json.dumps(mapping, indent=2, sort_keys=True, ensure_ascii=False) + "\n").encode(
        "utf-8"
    )


def _tsql_ident(name: str) -> str:
    return "[" + name.replace("]", "]]") + "]"
