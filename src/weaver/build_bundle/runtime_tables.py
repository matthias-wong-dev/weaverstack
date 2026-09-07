"""What a build writes into the catalogue's runtime tables.

Runtime-table references themselves are ordinary shortcut declarations, composed
into the repository and planned by the shortcut planner.

Before any physical work:

.. code-block:: text

    a rebuilt table or folder      LoadStatus Pending, Bookmark at the sentinel
    a rebuilt Test or Assumption   TestStatus Pending
    no longer declared             the row goes

After the physical work succeeds:

.. code-block:: text

    a rebuilt View                 LoadStatus Succeeded, dated by the build

A View is Succeeded once its DDL has run: the build is what establishes it, and
there is nothing further to run. A View has no bookmark.

The reset runs before physical work, and that ordering is the safety property. A
bookmark at the sentinel makes the next load read the whole source, while a
cursor left in place over a recreated table makes it read almost nothing. So a
build that fails in between leaves work to repeat.

The scope is the items this build reconciles and nothing wider, because the
tables are shared across the estate.

See ``design/how-does-build-work.md`` for where this sits in a build, and
``design/catalogue.md`` for the model.
"""

from __future__ import annotations

from typing import Iterable, Sequence

from ..catalogue.claims import bookmark_row
from ..catalogue.render import Row, sorted_rows
from ..catalogue.runtime_state import (
    RuntimeStateEstablishment,
    RuntimeStateInvalidation,
    invalidation_payload,
)
from ..catalogue.tables import (
    BOOKMARK,
    BOOKMARK_SENTINEL_TEXT,
    BY_DATA_NODE,
    BY_LOADABLE,
    BY_VALIDATION,
    CURRENT_STATE_TABLES,
    LOAD_STATUS,
    PENDING,
    SCOPE_ITEM_NAME,
    SCOPE_ITEM_TYPE,
    SUCCEEDED,
    TEST_STATUS,
)
from ..declaration.model import WeaverDocumentId, WeaverItemId
from ..etl import (
    item_bookmarkable_objects,
    item_data_nodes,
    item_validated_objects,
    item_view_objects,
)
from ..tokens import BUILD_DATETIME_TOKEN
from .models import (
    RECONCILE_RUNTIME_STATE,
    BuildBatch,
    InstallAction,
)
from .payloads import sha256_hex
from .stages import CATALOGUE, PlannedStage

#: What the reconciliation stage's action and payload are named after.
RECONCILE_SLUG = "runtime-state-reconciliation"

#: The stage that records the Views a build created.
VIEW_STATE_SLUG = "view-state"

# --- which rows a build ends the life of --------------------------------------


def _identity_row(identity: WeaverDocumentId) -> dict:
    """One current-state row's identity, spelled as the Registry spells it.

    A Folder keeps its ``Files/`` prefix, so a Folder and a Table of the same
    name are not one key.
    """

    return bookmark_row(identity)


#: How each population is read from the repository. The table declares which one
#: invalidates it, so nothing here decides that twice.
#:
#: ``_.LoadStatus`` covers every data node, because a View carries lifecycle
#: state too. ``_.Bookmark`` covers loadables alone: a cursor belongs to
#: something that reads a source.
_POPULATIONS = {
    BY_DATA_NODE: item_data_nodes,
    BY_LOADABLE: item_bookmarkable_objects,
    BY_VALIDATION: item_validated_objects,
}


def runtime_state_invalidation(
    repository,
    *,
    items: Sequence[WeaverItemId],
    selected_for_build: Iterable[WeaverDocumentId],
    catalogue,
) -> tuple[RuntimeStateInvalidation, ...]:
    """The current-state rows this build removes.

    Only objects the repository no longer declares. A rebuilt object keeps its
    row and has new state written over it by
    :func:`runtime_state_establishment`.
    """

    scoped = {item for item in items if not _is_builtin(item)}
    if not scoped:
        return ()
    invalidation = []
    for table in CURRENT_STATE_TABLES:
        population = _POPULATIONS[table.invalidated_by]
        keep = {
            _key(table, _identity_row(identity))
            for item in scoped
            for identity in population(repository, item=item)
        }
        gone = tuple(
            {name: row.get(name) for name in table.key}
            for row in sorted_rows(
                table,
                [
                    row
                    for row in catalogue.table_rows(table)
                    if _item_of(row) in scoped and _key(table, row) not in keep
                ],
            )
        )
        if gone:
            invalidation.append(RuntimeStateInvalidation(table=table.name, rows=gone))
    return tuple(invalidation)


def runtime_state_establishment(
    repository,
    *,
    items: Sequence[WeaverItemId],
    selected_for_build: Iterable[WeaverDocumentId],
    holds_table=None,
) -> tuple[RuntimeStateEstablishment, ...]:
    """The state this build writes before it touches anything physical.

    A rebuilt table or folder is ``Pending`` with its bookmark at the sentinel,
    and a rebuilt validation is ``Pending``. Bounded by ``selected_for_build``,
    so an object left alone keeps the state it had.

    ``holds_table`` says whether the catalogue already has a table. This runs
    before the physical work that creates them, so the build that creates the
    ``_`` schema writes into none.
    """

    scoped = sorted({item for item in items if not _is_builtin(item)}, key=str)
    selected = set(selected_for_build)
    established = []
    for table in CURRENT_STATE_TABLES:
        if holds_table is not None and not holds_table(table):
            continue
        rows = [
            row
            for item in scoped
            for row in _ESTABLISHED[table.name](
                repository, item=item, selected=selected
            )
        ]
        if rows:
            established.append(
                RuntimeStateEstablishment(
                    table=table.name, rows=tuple(sorted_rows(table, rows))
                )
            )
    return tuple(established)


def _load_status_state(repository, *, item, selected) -> list[dict]:
    """A rebuilt table or folder holds nothing until it loads."""

    return [
        {**_identity_row(identity), "result": PENDING}
        for identity in item_bookmarkable_objects(repository, item=item)
        if identity in selected
    ]


def _bookmark_state(repository, *, item, selected) -> list[dict]:
    """A rebuilt loadable has read nothing, so its cursor is the sentinel."""

    return [
        bookmark_row(identity, BOOKMARK_SENTINEL_TEXT)
        for identity in item_bookmarkable_objects(repository, item=item)
        if identity in selected
    ]


def _test_status_state(repository, *, item, selected) -> list[dict]:
    """A rebuilt validation has not run against what this build installed."""

    from ..catalogue.projection import TEST_TYPE_FOR_KIND

    return [
        {
            **_identity_row(identity),
            "test_type": TEST_TYPE_FOR_KIND[repository.source_documents[identity].kind],
            "result": PENDING,
        }
        for identity in item_validated_objects(repository, item=item)
        if identity in selected
    ]


#: What each current-state table holds once a build has reset the object.
_ESTABLISHED = {
    LOAD_STATUS.name: _load_status_state,
    BOOKMARK.name: _bookmark_state,
    TEST_STATUS.name: _test_status_state,
}


def view_state_establishment(
    repository,
    *,
    items: Sequence[WeaverItemId],
    selected_for_build: Iterable[WeaverDocumentId],
) -> tuple[RuntimeStateEstablishment, ...]:
    """The Views this build rebuilt, as the state a successful build leaves.

    Rendered into a stage that follows the physical work, so a View that was
    never created carries no Succeeded row.
    """

    scoped = sorted({item for item in items if not _is_builtin(item)}, key=str)
    selected = set(selected_for_build)
    rows = [
        {
            **_identity_row(identity),
            "result": SUCCEEDED,
            "started_datetime": BUILD_DATETIME_TOKEN,
            "completed_datetime": BUILD_DATETIME_TOKEN,
        }
        for item in scoped
        for identity in item_view_objects(repository, item=item)
        if identity in selected
    ]
    if not rows:
        return ()
    return (
        RuntimeStateEstablishment(
            table=LOAD_STATUS.name, rows=tuple(sorted_rows(LOAD_STATUS, rows))
        ),
    )


def _key(table, row: Row) -> tuple:
    """One row's identity, as its table keys it."""

    return tuple(row.get(name) for name in table.key)


def _item_of(row: Row) -> WeaverItemId:
    """Which logical item a current-state row belongs to."""

    return WeaverItemId(
        str(row.get(SCOPE_ITEM_TYPE) or ""), str(row.get(SCOPE_ITEM_NAME) or "")
    )


def render_runtime_state_reconciliation(
    invalidation: Sequence[RuntimeStateInvalidation],
    *,
    catalogue_target,
    establishment: Sequence[RuntimeStateEstablishment] = (),
    slug: str = RECONCILE_SLUG,
    description: str = "reconcile runtime state before physical work",
    index: int = 0,
) -> PlannedStage | None:
    """One stage carrying a runtime-state decision, as one action."""

    if not any(one.rows for one in (*invalidation, *establishment)):
        return None

    filename = f"{slug}.runtime-state.json"
    content = invalidation_payload(tuple(invalidation), tuple(establishment))
    action = InstallAction(
        id=slug,
        kind=RECONCILE_RUNTIME_STATE,
        resource_node_id=None,
        executor="runtime_state",
        payload=filename,
        payload_sha256=sha256_hex(content),
    )
    return PlannedStage(
        phase=CATALOGUE,
        index=index,
        slug=slug,
        description=description,
        payloads={filename: content},
        batches=(
            BuildBatch(id=slug, target_id=catalogue_target.id, actions=(action,)),
        ),
    )


def _is_builtin(item: WeaverItemId) -> bool:
    from ..catalogue.builtin import BUILTIN_ITEM

    return item == BUILTIN_ITEM


__all__ = [
    "RECONCILE_SLUG",
    "VIEW_STATE_SLUG",
    "render_runtime_state_reconciliation",
    "runtime_state_establishment",
    "runtime_state_invalidation",
    "view_state_establishment",
]
