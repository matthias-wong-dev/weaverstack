"""What a build does about bookmarks: invalidate rows, present the table.

**Invalidating rows.** A bookmark row means "a clean load has run for this
object's current physical incarnation". A build ends that for two kinds of object,
and both lose their row:

.. code-block:: text

    no longer declared, or no longer loaded    the object is going
    dropped and rebuilt                        the incarnation is going

One scoped delete does both, keeping the rows of objects this build still loads
and is not replacing. It runs **before** any physical work, and the ordering is
the safety property: an absent bookmark makes the next load read the whole
source, while one left in place over a table that was dropped and recreated makes
it read almost nothing. So the row goes first and the rebuild follows it, and a
build that fails in between leaves work to repeat rather than rows that will
never arrive.

Deleted rather than reset to a stored sentinel, because absence already means
what a sentinel would: nothing has been loaded since this incarnation began.

The scope is the items this build reconciles, and nothing wider. Rows belonging
to an item the build was not pointed at are another build's to maintain.

**Presenting the table.** Every target the build installs a load into gets the
catalogue's ``_.Bookmark`` under that name — a view in a Warehouse, a OneLake
shortcut in a Lakehouse — because a generated procedure says ``[_].[Bookmark]``
and authored Spark SQL may too.
"""

from __future__ import annotations

import json
from typing import Iterable, Sequence

from ..catalogue.claims import bookmark_row
from ..catalogue.render import Row, sorted_rows
from ..catalogue.runtime_state import (
    RuntimeStateInvalidation,
    invalidation_payload,
)
from ..catalogue.tables import (
    BOOKMARK,
    CATALOGUE_SCHEMA,
    SCOPE_ITEM_NAME,
    SCOPE_ITEM_TYPE,
)
from ..declaration.model import WAREHOUSE, WeaverDocumentId, WeaverItemId
from ..etl import item_bookmarkable_objects
from .changes import TABLE as TABLE_KIND
from .changes import VIEW as VIEW_KIND
from .changes import added
from .models import (
    CREATE_BOOKMARK_REFERENCE,
    RECONCILE_BOOKMARKS,
    BuildBatch,
    InstallAction,
)
from .payloads import sha256_hex
from .shortcuts import Reference, declaration_key, shortcut_payload, view_statement
from .stages import CATALOGUE, LOAD, PlannedStage
from .targets import LAKEHOUSE_TARGET, WAREHOUSE_TARGET


#: The error a build raises when the catalogue holds no ``_.Bookmark`` table.
def _row(identity: WeaverDocumentId) -> dict:
    """One bookmark row's identity, spelled as the Registry spells it.

    A Folder keeps its ``Files/`` prefix: without it a Folder and a Table of the
    same name are one key, and one bookmark would stand for both.
    """

    return bookmark_row(identity)


def bookmark_invalidation(
    repository,
    *,
    items: Sequence[WeaverItemId],
    selected_for_build: Iterable[WeaverDocumentId],
    catalogue,
) -> tuple[RuntimeStateInvalidation, ...]:
    """The ``_.Bookmark`` rows this build ends the life of, as structured intent.

    The build has read ``_.Bookmark``, so which rows are obsolete is arithmetic
    over rows it holds: every row in the scope whose object this build is about
    to replace, and every row whose object the repository no longer declares as
    something Weaver loads. What is left keeps its history.

    Empty whenever that set is empty, which is most builds — an unchanged
    repository has nothing to invalidate, and a build creating the table has read
    no rows because there were none.

    Keyed rows rather than rendered SQL. The installer renders one scoped DELETE
    from this, and a Catalogue applies the same intent in memory, so what a build
    decided about an object's operational state can be read without parsing DML.
    """

    scoped = {item for item in items if not _is_builtin(item)}
    if not scoped:
        return ()
    selected = set(selected_for_build)
    # What keeps its bookmark: an object this build still loads and is *not*
    # replacing. Everything else loses its row.
    keep = {
        _key(_row(identity))
        for item in scoped
        for identity in item_bookmarkable_objects(repository, item=item)
        if identity not in selected
    }
    obsolete = tuple(
        {name: row.get(name) for name in BOOKMARK.key}
        for row in sorted_rows(
            BOOKMARK,
            [
                row
                for row in catalogue.table_rows(BOOKMARK)
                if _item_of(row) in scoped and _key(row) not in keep
            ],
        )
    )
    if not obsolete:
        return ()
    return (RuntimeStateInvalidation(table=BOOKMARK.name, rows=obsolete),)


def _key(row: Row) -> tuple:
    """One bookmark row's identity, as the table keys it."""

    return tuple(row.get(name) for name in BOOKMARK.key)


def _item_of(row: Row) -> WeaverItemId:
    """Which logical item a bookmark row belongs to."""

    return WeaverItemId(
        str(row.get(SCOPE_ITEM_TYPE) or ""), str(row.get(SCOPE_ITEM_NAME) or "")
    )


def render_bookmark_reconciliation(
    invalidation: Sequence[RuntimeStateInvalidation],
    *,
    catalogue_target,
) -> PlannedStage | None:
    """The one stage that invalidates ``_.Bookmark`` rows ahead of physical work.

    One action carrying the intent, not one action per table or per row: the
    invalidation is one lifecycle decision, taken once and reported once.
    """

    if not any(one.rows for one in invalidation):
        return None

    slug = "bookmark-reconciliation"
    filename = f"{slug}.runtime-state.json"
    content = invalidation_payload(tuple(invalidation))
    action = InstallAction(
        id=slug,
        kind=RECONCILE_BOOKMARKS,
        resource_node_id=None,
        executor="runtime_state",
        payload=filename,
        payload_sha256=sha256_hex(content),
    )
    return PlannedStage(
        phase=CATALOGUE,
        index=0,
        slug=slug,
        description="invalidate bookmarks before physical work",
        payloads={filename: content},
        batches=(
            BuildBatch(id=slug, target_id=catalogue_target.id, actions=(action,)),
        ),
    )


# --- the local name a generated statement uses --------------------------------


#: What the reference's action and payload are named after.
SLUG = "bookmark-reference"


def _reference(item: WeaverItemId, catalogue: str) -> Reference:
    """``_.Bookmark`` as the reference it is: a shortcut nobody authored.

    Weaver's own infrastructure, so it is not published as a ``_.Shortcut`` row —
    but *what it becomes* is exactly what a declared shortcut becomes, so it is
    expressed as one and rendered by the same code. A Warehouse gets the view, a
    Lakehouse the OneLake shortcut.
    """

    from ..declaration.metadata import ObjectId
    from ..declaration.model import TABLE_SHORTCUT, VIEW_SHORTCUT

    qualified = f"{CATALOGUE_SCHEMA}.{BOOKMARK.name}"
    return Reference(
        owner=item,
        name=qualified,
        destination=WeaverDocumentId(item, ObjectId(CATALOGUE_SCHEMA, BOOKMARK.name)),
        shortcut_type=(
            VIEW_SHORTCUT if item.item_type == WAREHOUSE else TABLE_SHORTCUT
        ),
        target=f"{WAREHOUSE}/{catalogue}/{qualified}",
        target_item_name=catalogue,
        target_object=ObjectId(CATALOGUE_SCHEMA, BOOKMARK.name),
    )


def bookmark_reference_views(
    repository, *, item: WeaverItemId, target
) -> tuple[str, ...]:
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
    bookmark_source=None,
) -> PlannedStage | None:
    """Give a built target the catalogue's ``_.Bookmark`` under that name.

    A generated Warehouse load procedure says ``[_].[Bookmark]``, and authored
    Spark SQL may too, so every target holding loadable objects presents it.

    Created in the load phase, with the artefacts it exists for — not with the
    declared shortcuts, which precede an item's documents. Nothing built reads a
    bookmark, and on the build that creates the catalogue the table this points at
    is built in the same bundle and is not there yet when the shortcut phase runs.
    """

    if not item_bookmarkable_objects(repository, item=item):
        return None
    reference = _reference(item, catalogue_target.name)
    if target.kind == WAREHOUSE_TARGET:
        return _warehouse_view(reference, item, target, catalogue_target, inventory)
    return _lakehouse_shortcut(
        reference,
        item=item,
        target=target,
        inventory=inventory,
        source=bookmark_source,
    )


def _warehouse_view(
    reference, item, target, catalogue_target, inventory
) -> PlannedStage | None:
    """The view, created when the Warehouse does not already hold it.

    Gated on the inventory, so an unchanged repository plans nothing and a view
    somebody removed comes back.
    """

    if target.name.casefold() == catalogue_target.name.casefold():
        # The catalogue's own Warehouse: the table is right there, and a view of
        # that name would be a view over itself.
        return None
    if inventory.has_object(CATALOGUE_SCHEMA, BOOKMARK.name, "view"):
        return None

    statement = view_statement(reference, catalogue_target)
    item_slug = str(item).replace("/", "--")
    filename = f"{SLUG}-{item_slug}.tsql-batch.json"
    content = (json.dumps([statement], indent=2, ensure_ascii=False) + "\n").encode(
        "utf-8"
    )
    return _stage(
        item_slug,
        target=target,
        executor="tsql_batch",
        filename=filename,
        content=content,
        description="present the catalogue's _.Bookmark in this Warehouse",
    )


def _lakehouse_shortcut(
    reference, *, item, target, inventory, source
) -> PlannedStage | None:
    """The OneLake shortcut, created when the Lakehouse does not already hold it.

    Gated the way the Warehouse's view is: on what is physically there, so an
    unchanged repository plans nothing and a reference somebody removed comes
    back. ``Tables/_`` is Weaver's own rather than the item's, so the inventory
    reports it as a fact of its own instead of as one of the item's schemas.
    """

    if source is None or inventory.bookmark_reference:
        return None

    item_slug = str(item).replace("/", "--")
    content = shortcut_payload(
        [(reference, target)], sources={declaration_key(reference): source}
    )
    return _stage(
        item_slug,
        target=target,
        executor="shortcut",
        filename=f"{SLUG}-{item_slug}.shortcut.json",
        content=content,
        description="present the catalogue's _.Bookmark in this Lakehouse",
    )


def _stage(
    item_slug: str,
    *,
    target,
    executor: str,
    filename: str,
    content: bytes,
    description: str,
) -> PlannedStage:
    action = InstallAction(
        id=f"{SLUG}-{item_slug}",
        kind=CREATE_BOOKMARK_REFERENCE,
        resource_node_id=None,
        executor=executor,
        payload=filename,
        payload_sha256=sha256_hex(content),
    )
    return PlannedStage(
        phase=LOAD,
        # Behind the artefacts, which share this phase at index 0. Order within
        # it does not matter: nothing here runs, and a procedure's reference to
        # ``[_].[Bookmark]`` resolves when it is called rather than installed.
        index=1,
        slug=SLUG,
        description=description,
        payloads={filename: content},
        batches=(
            BuildBatch(
                id=f"{SLUG}-{item_slug}", target_id=target.id, actions=(action,)
            ),
        ),
        # Declared alongside the action, as every physical change is: what this
        # leaves is part of what the target holds afterwards.
        changes={
            target.id: (
                added(
                    _CHANGE_KIND[target.kind],
                    f"{CATALOGUE_SCHEMA}.{BOOKMARK.name}",
                    action.id,
                ),
            )
        },
    )


#: What the reference physically is, by target kind. A Warehouse view and a
#: Lakehouse table shortcut both answer to ``_.Bookmark``.
_CHANGE_KIND = {WAREHOUSE_TARGET: VIEW_KIND, LAKEHOUSE_TARGET: TABLE_KIND}


def _is_builtin(item: WeaverItemId) -> bool:
    from ..catalogue.builtin import BUILTIN_ITEM

    return item == BUILTIN_ITEM


__all__ = [
    "bookmark_invalidation",
    "bookmark_reference_views",
    "render_bookmark_reconciliation",
    "render_bookmark_reference",
]
