"""Planning each repository alias as one physical action.

An alias is the consuming item's own name for something another item produces.
It is owned by its destination item — the source stays the canonical producer —
so it is planned as part of that item's work, ahead of every document the item
declares, because those documents are written against the namespace it
establishes.

What an alias *becomes* depends on the destination it is bound to, and that is a
planning decision because it is a decision about the target kind:

===============================  ==============================================
Lakehouse                        a ``create_alias`` action — a OneLake shortcut
Warehouse                        a frozen view over the bound source
===============================  ==============================================

Only the Warehouse form is spelled out in SQL, because only there is the
statement itself the decision. A shortcut carries one frozen decision — this
destination, that source.

An alias the current bindings give no physical form is left out of the plan and
recorded as an omission. That is the planner's decision: the installer may only
run an alias action already frozen for it.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Iterable, Mapping

from ..declaration.model import WeaverDocumentId, WeaverItemId
from .changes import (
    FOLDER as FOLDER_KIND,
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
    CREATE_ALIAS,
    OMIT_ALIAS_UNSUPPORTED,
    BuildBatch,
    InstallAction,
    OmittedNode,
)
from .payloads import sha256_hex
from .stages import ALIAS, PlannedStage
from .targets import WAREHOUSE_TARGET, BoundTarget

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
    #: The destinations behind ``omitted``, as identities rather than node ids.
    #: The caller needs them because an alias with no physical form must not be
    #: certified as installed — see :attr:`omitted`.
    omitted_destinations: tuple[WeaverDocumentId, ...] = ()


def plan_item_aliases(
    repository,
    *,
    item: WeaverItemId,
    target: BoundTarget,
    target_by_item: Mapping[WeaverItemId, BoundTarget],
    selected: Iterable[WeaverDocumentId],
) -> ItemAliasPlan:
    """Plan the aliases this build selected, against the current bindings.

    ``selected`` is what incremental selection chose to rebuild. An alias absent
    from it is current — its declaration is unchanged, its destination is there,
    and its source has not been rebuilt since — so it is left alone rather than
    replaced, exactly as an unchanged document is.

    Its *schema* is still reported. A retained alias lives in a namespace the
    item must have, and a build that created only the schemas its rebuilt aliases
    needed would leave the others homeless.
    """

    aliases = sorted(
        (alias for alias in repository.aliases if alias.destination.item == item),
        key=lambda alias: str(alias.destination),
    )
    if not aliases:
        return ItemAliasPlan()

    chosen = set(selected)
    omitted: list[OmittedNode] = []
    omitted_destinations: list[WeaverDocumentId] = []
    supported: list[tuple] = []
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
            omitted_destinations.append(alias.destination)
            continue
        schemas.append(alias.destination.object_id.schema)
        if alias.destination in chosen:
            supported.append((alias, source_target))

    stage = None
    if supported:
        item_slug = _slug(item)
        payloads: dict[str, bytes] = {}
        action = _alias_action(
            supported, item=item, target=target, payloads=payloads
        )
        # One action stands for every alias the item consumes, so it produces
        # several changes. Each names what the alias physically *is* at this
        # binding — a folder under Files, a view in a Warehouse, a table in a
        # Lakehouse — which is the same question the Registry row answers.
        stage = PlannedStage(
            phase=ALIAS,
            slug="item-aliases",
            description="materialise item-owned aliases",
            payloads=payloads,
            changes={
                target.id: tuple(
                    added(
                        _alias_change_kind(alias.destination, target),
                        alias.destination.object_id.qualified,
                        action.id,
                    )
                    for alias, _source_target in supported
                )
            },
            batches=(
                BuildBatch(
                    id=f"item-aliases-{item_slug}",
                    target_id=target.id,
                    actions=(action,),
                ),
            ),
        )
    return ItemAliasPlan(
        stage=stage,
        schemas=tuple(sorted(set(schemas))),
        omitted=tuple(omitted),
        omitted_destinations=tuple(omitted_destinations),
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


def _alias_change_kind(destination, target) -> str:
    """What an alias will look like to an inventory, which depends on binding.

    The same rule :func:`weaver.build_bundle.prune.managed_sets` applies for the
    keep-set, and for the same reason: an alias is a folder, a view or a table
    according to what it was bound to, and nothing about the declaration says
    which.
    """

    if destination.is_files:
        return FOLDER_KIND
    return VIEW_KIND if target.kind == WAREHOUSE_TARGET else TABLE_KIND


def _alias_action(
    supported,
    *,
    item: WeaverItemId,
    target: BoundTarget,
    payloads: dict[str, bytes],
) -> InstallAction:
    """One action for all of this item's aliases.

    One rather than one-per-alias, because materialising an alias is not
    instantaneous and the cost is per *wait*, not per create. A Lakehouse alias
    becomes usable some seconds after the shortcut exists (measured at 6–31s), so
    N actions run serially pay N waits while one action that creates everything and
    then waits pays roughly one. A Warehouse alias is a script, and the executor
    already runs a multi-statement script as one unit.

    The action reports each alias it made in its details, so the manifest loses no
    traceability by grouping them.
    """

    item_slug = _slug(item)
    if target.kind == WAREHOUSE_TARGET:
        content = (
            json.dumps(
                [
                    _view_statement(alias, source_target)
                    for alias, source_target in supported
                ],
                indent=2,
                ensure_ascii=False,
            )
            + "\n"
        ).encode("utf-8")
        filename = f"aliases-{item_slug}.tsql-batch.json"
        executor = "tsql_batch"
    else:
        content = _shortcut_payload(supported, target=target)
        filename = f"aliases-{item_slug}.alias.json"
        executor = "alias"
    payloads[filename] = content
    return InstallAction(
        id=f"aliases-{item_slug}",
        kind=CREATE_ALIAS,
        # No single resource: this action stands for every alias the item
        # consumes, and the payload names them.
        resource_node_id=None,
        executor=executor,
        payload=filename,
        payload_sha256=sha256_hex(content),
    )


def _view_statement(alias, source_target: BoundTarget) -> str:
    """A Warehouse alias, as the one statement that makes it exist.

    The source is named by its bound item's three-part spelling, which is how a
    Fabric Warehouse reaches another item in the same workspace, and is frozen
    here for the same reason an authored three-part reference is: it is the
    semantic decision, not transport.

    ``CREATE OR ALTER`` rather than a drop and a create. An alias holds no data —
    it is a pointer — so replacing one is not a destructive transition needing
    proof of prior state, and a build that could not run twice over its own
    aliases would not be re-runnable at all. It is also the only *single-statement*
    way to say that, and T-SQL requires ``CREATE VIEW`` to be the first statement
    in its batch: ``drop …; create view …;`` in one batch is rejected outright,
    which is exactly how this was found.
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
    return f"create or alter view {schema}.{name} as select * from {source};"


def _shortcut_payload(supported, *, target: BoundTarget) -> bytes:
    """This item's Lakehouse aliases, as the frozen pairs of addresses they stand for.

    Each is named by target id rather than by resolved path: the installer already
    resolves every target the plan declares through its own environment, so a
    source is addressed exactly as the destination is, and a bundle carries no
    path from the machine that wrote it.
    """

    mapping = {
        "aliases": [
            {
                "alias": str(alias.destination),
                "source": str(alias.source),
                "source_target_id": source_target.id,
                "area": FILES_AREA if alias.destination.is_files else TABLES_AREA,
                "schema": alias.destination.object_id.schema,
                "object": alias.destination.object_id.object,
                "source_area": FILES_AREA if alias.source.is_files else TABLES_AREA,
                "source_schema": alias.source.object_id.schema,
                "source_object": alias.source.object_id.object,
            }
            for alias, source_target in supported
        ]
    }
    return (json.dumps(mapping, indent=2, sort_keys=True, ensure_ascii=False) + "\n").encode(
        "utf-8"
    )


def _tsql_ident(name: str) -> str:
    return "[" + name.replace("]", "]]") + "]"
