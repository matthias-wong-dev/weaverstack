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
from ..catalogue.tables import BOOKMARK, BOOKMARK_SENTINEL_TEXT, CATALOGUE_SCHEMA
from ..catalogue.tsql import identifier
from ..declaration.model import WeaverDocumentId, WeaverItemId
from ..etl import item_bookmarkable_objects
from .changes import VIEW as VIEW_KIND
from .changes import added
from .models import (
    CREATE_BOOKMARK_REFERENCE,
    RECONCILE_BOOKMARKS,
    BuildBatch,
    InstallAction,
)
from .payloads import sha256_hex
from .stages import CATALOGUE, SHORTCUT, PlannedStage
from .targets import WAREHOUSE_TARGET

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


# --- the local name a generated statement uses --------------------------------


def bookmark_reference_views(repository, *, item: WeaverItemId, target) -> tuple[str, ...]:
    """``_.Bookmark`` where this item's target presents it as a view.

    For the keep-set, so prune spares the reference this build creates. It goes
    when the item's last loadable object goes, exactly as the ``_`` schema holding
    the load procedures does.
    """

    if target.kind != WAREHOUSE_TARGET:
        return ()
    if not item_bookmarkable_objects(repository, item=item):
        return ()
    return (f"{CATALOGUE_SCHEMA}.{BOOKMARK.name}",)


def render_bookmark_reference(
    repository,
    *,
    item: WeaverItemId,
    target,
    inventory,
    catalogue_target,
) -> PlannedStage | None:
    """Give a built Warehouse the catalogue's ``_.Bookmark`` under that name.

    A generated load procedure reads and writes its own bookmark, and it says
    ``[_].[Bookmark]`` to do it. In the Warehouse the catalogue lives in that is
    already the table; anywhere else it is a view over the catalogue's three-part
    name, which is how a Fabric Warehouse reaches another item in the workspace.

    Weaver runtime infrastructure rather than a declared shortcut: nothing
    authored it and it is not published as a ``_.Shortcut`` row. It is created in
    the shortcut phase for the reason declared shortcuts are — before the
    documents written against the namespace it completes.

    Created when the Warehouse does not already hold it, so an unchanged
    repository plans nothing and a view somebody removed comes back.
    """

    if target.kind != WAREHOUSE_TARGET:
        return None
    if not item_bookmarkable_objects(repository, item=item):
        return None
    if target.item_id == catalogue_target.item_id:
        # The catalogue's own Warehouse: the table is right there, and a view of
        # that name would be a view over itself.
        return None
    if inventory.has_object(CATALOGUE_SCHEMA, BOOKMARK.name, "view"):
        return None

    statement = (
        f"create or alter view {identifier(CATALOGUE_SCHEMA)}."
        f"{identifier(BOOKMARK.name)} as select * from "
        f"{identifier(catalogue_target.name)}.{identifier(CATALOGUE_SCHEMA)}."
        f"{identifier(BOOKMARK.name)};"
    )
    slug = "bookmark-reference"
    item_slug = str(item).replace("/", "--")
    filename = f"{slug}-{item_slug}.tsql-batch.json"
    content = (json.dumps([statement], indent=2, ensure_ascii=False) + "\n").encode(
        "utf-8"
    )
    action = InstallAction(
        id=f"{slug}-{item_slug}",
        kind=CREATE_BOOKMARK_REFERENCE,
        resource_node_id=None,
        executor="tsql_batch",
        payload=filename,
        payload_sha256=sha256_hex(content),
    )
    return PlannedStage(
        phase=SHORTCUT,
        index=1,
        slug=slug,
        description="present the catalogue's _.Bookmark in this Warehouse",
        payloads={filename: content},
        batches=(
            BuildBatch(id=f"{slug}-{item_slug}", target_id=target.id, actions=(action,)),
        ),
        # Declared alongside the action, as every physical change is: the view
        # this leaves is part of what the Warehouse holds afterwards.
        changes={
            target.id: (
                added(VIEW_KIND, f"{CATALOGUE_SCHEMA}.{BOOKMARK.name}", action.id),
            )
        },
    )


def _is_builtin(item: WeaverItemId) -> bool:
    from ..catalogue.builtin import BUILTIN_ITEM

    return item == BUILTIN_ITEM


__all__ = [
    "MISSING_TABLE_ERROR",
    "MISSING_TABLE_MESSAGE",
    "bookmark_reference_views",
    "bookmark_statements",
    "render_bookmark_reconciliation",
    "render_bookmark_reference",
]
