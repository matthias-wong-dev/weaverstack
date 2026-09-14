"""Plan runtime-state writes around physical build work.

Before physical work, rebuilt loadables become Pending with sentinel bookmarks,
rebuilt validations become Pending, and undeclared rows are removed. Resetting
bookmarks first makes a failed build repeat the next load in full. Successful
View DDL records Succeeded afterwards. All writes are scoped to the items being
reconciled because these tables are shared across installations.
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

RECONCILE_SLUG = "runtime-state-reconciliation"

VIEW_STATE_SLUG = "view-state"


def _identity_row(identity: WeaverDocumentId) -> dict:
    """Preserve the ``Files/`` prefix that distinguishes Folders from Tables."""

    return bookmark_row(identity)


# Views have lifecycle state but no cursor; bookmarks cover loadables only.
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
    """Return rows for undeclared objects; rebuilt rows are overwritten instead."""

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
    """Return state written before physical work for selected objects only.

    ``holds_table`` says whether the catalogue already has a table. This runs
    before catalogue tables are created, so an initial build writes only to
    tables already present.
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
    return [
        {**_identity_row(identity), "result": PENDING}
        for identity in item_bookmarkable_objects(repository, item=item)
        if identity in selected
    ]


def _bookmark_state(repository, *, item, selected) -> list[dict]:
    return [
        bookmark_row(identity, BOOKMARK_SENTINEL_TEXT)
        for identity in item_bookmarkable_objects(repository, item=item)
        if identity in selected
    ]


def _test_status_state(repository, *, item, selected) -> list[dict]:
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
    """Return Succeeded rows for Views whose physical work has completed."""

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
    return tuple(row.get(name) for name in table.key)


def _item_of(row: Row) -> WeaverItemId:
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
