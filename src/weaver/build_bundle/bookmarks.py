"""Bring ``_.Bookmark`` into line with what a build will leave installed.

Two statements, both against the catalogue Warehouse:

* a scoped delete of rows whose object this build no longer loads;
* a scoped merge resetting to the sentinel every loadable object it rebuilds.

Both run **before** any physical work, and the ordering is the safety property. A
bookmark at the sentinel makes the next load read the whole source; a bookmark
left advanced over a table that was dropped and recreated makes it read almost
nothing. So the reset is written first and the rebuild follows it: a build that
fails in between leaves work to repeat rather than rows that will never arrive.

The scope is the items this build reconciles, and nothing wider. Rows belonging
to an item the build was not pointed at are another build's to maintain.
"""

from __future__ import annotations

import json
from typing import Iterable, Sequence

from ..catalogue.render import (
    InstallationScope,
    InstallationScopes,
    Row,
    render_delete_obsolete,
    render_merge,
)
from ..catalogue.claims import catalogue_schema
from ..catalogue.tables import BOOKMARK, BOOKMARK_SENTINEL_TEXT
from ..declaration.model import WeaverDocumentId, WeaverItemId
from ..etl import item_bookmarkable_objects
from .models import RECONCILE_BOOKMARKS, BuildBatch, InstallAction
from .payloads import sha256_hex
from .stages import CATALOGUE, PlannedStage

#: The error a build raises when the catalogue holds no ``_.Bookmark`` table.
#: Checked in the Warehouse rather than during planning, because the Warehouse is
#: where the answer is, and raised rather than skipped: a build that quietly did
#: no bookmark work would leave the next load reading from a bookmark nothing
#: maintains.
MISSING_TABLE_ERROR = 51030

MISSING_TABLE_MESSAGE = (
    "weaver: the Weaver catalogue has no _.Bookmark table. Build "
    "Warehouse/_weaver to create it, then build again."
)


def _precondition() -> str:
    """Refuse the batch unless the table the rest of it maintains is there."""

    return (
        "if object_id(N'[_].[Bookmark]', N'U') is null\n"
        f"    throw {MISSING_TABLE_ERROR}, '{MISSING_TABLE_MESSAGE}', 1;\n"
    )


def _row(identity: WeaverDocumentId, *, bookmark: str | None = None) -> dict:
    """One bookmark row's identity, spelled as the Registry spells it.

    Through :func:`~weaver.catalogue.claims.catalogue_schema`, so a Folder keeps
    its ``Files/`` prefix. Without it a Folder and a Table of the same name are
    one key, and one bookmark would stand for both.
    """

    row = {
        "item_type": identity.item.item_type,
        "item_name": identity.item.item_name,
        "schema_name": catalogue_schema(identity),
        "object_name": identity.object_id.object,
    }
    if bookmark is not None:
        row["bookmark_datetime"] = bookmark
    return row


def bookmark_statements(
    repository,
    *,
    items: Sequence[WeaverItemId],
    selected_for_build: Iterable[WeaverDocumentId],
    removed: Iterable[WeaverDocumentId] = (),
) -> tuple[str, ...]:
    """The bookmark reconciliation statements for one build, in execution order.

    Empty in two cases, and both matter. A build of the built-in catalogue item
    reconciles nothing that can hold a bookmark. And a build with nothing to
    build and nothing to remove has nothing to say about bookmarks: an
    unchanged repository produces an empty bundle, so the statements are issued
    when the build acts and not merely because it ran.

    When they are issued the prune is a full reconciliation of the scope rather
    than a delete of the objects this build noticed, so a row left behind by an
    earlier failure goes too.
    """

    scoped = tuple(item for item in items if not _is_builtin(item))
    selected = set(selected_for_build)
    if not scoped or not (selected or set(removed)):
        return ()

    declared: dict[WeaverItemId, tuple[WeaverDocumentId, ...]] = {
        item: item_bookmarkable_objects(repository, item=item) for item in scoped
    }
    keep: list[Row] = [
        _row(identity) for objects in declared.values() for identity in objects
    ]
    reset: list[Row] = [
        _row(identity, bookmark=BOOKMARK_SENTINEL_TEXT)
        for objects in declared.values()
        for identity in objects
        if identity in selected
    ]

    scopes = InstallationScopes(
        tuple(InstallationScope(item.item_type, item.item_name) for item in scoped)
    )
    statements = [_precondition()]
    prune = render_delete_obsolete(BOOKMARK, keep, scope=scopes)
    if prune is not None:
        statements.append(prune)
    merge = render_merge(BOOKMARK, reset, scope=scopes)
    if merge is not None:
        statements.append(merge)
    return tuple(statements)


def render_bookmark_reconciliation(
    repository,
    *,
    items: Sequence[WeaverItemId],
    selected_for_build: Iterable[WeaverDocumentId],
    removed: Iterable[WeaverDocumentId] = (),
    catalogue_target,
) -> PlannedStage | None:
    """The one stage that reconciles ``_.Bookmark`` ahead of physical work."""

    statements = bookmark_statements(
        repository,
        items=items,
        selected_for_build=selected_for_build,
        removed=removed,
    )
    if not statements:
        return None

    slug = "bookmark-reconciliation"
    filename = f"{slug}.tsql-batch.json"
    content = (
        json.dumps(list(statements), indent=2, ensure_ascii=False) + "\n"
    ).encode("utf-8")
    action = InstallAction(
        id=slug,
        kind=RECONCILE_BOOKMARKS,
        resource_node_id=None,
        executor="tsql_batch",
        payload=filename,
        payload_sha256=sha256_hex(content),
    )
    return PlannedStage(
        phase=CATALOGUE,
        index=0,
        slug=slug,
        description="reset and prune bookmarks before physical work",
        payloads={filename: content},
        batches=(
            BuildBatch(id=slug, target_id=catalogue_target.id, actions=(action,)),
        ),
    )


def _is_builtin(item: WeaverItemId) -> bool:
    from ..catalogue.builtin import BUILTIN_ITEM

    return item == BUILTIN_ITEM


__all__ = [
    "MISSING_TABLE_ERROR",
    "MISSING_TABLE_MESSAGE",
    "bookmark_statements",
    "render_bookmark_reconciliation",
]
