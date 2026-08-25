"""What a build does about the catalogue's runtime tables.

This module invalidates the current-state rows whose incarnation a build is
ending. Runtime-table references themselves are ordinary shortcut declarations,
injected while the repository is prepared and planned by the shortcut planner.

.. code-block:: text

    no longer declared, or no longer run       the object is going
    dropped and rebuilt                        the incarnation is going

The invalidation runs **before** any physical work, and that ordering is the
safety property: an absent bookmark makes the next load read the whole source,
while one left in place over a recreated table makes it read almost nothing. So a
build that fails in between leaves work to repeat rather than rows that will
never arrive.

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
    RuntimeStateInvalidation,
    invalidation_payload,
)
from ..catalogue.tables import (
    BY_LOADABLE,
    BY_VALIDATION,
    CURRENT_STATE_TABLES,
    SCOPE_ITEM_NAME,
    SCOPE_ITEM_TYPE,
)
from ..declaration.model import WeaverDocumentId, WeaverItemId
from ..etl import (
    item_bookmarkable_objects,
    item_validated_objects,
)
from .models import (
    RECONCILE_RUNTIME_STATE,
    BuildBatch,
    InstallAction,
)
from .payloads import sha256_hex
from .stages import CATALOGUE, PlannedStage

#: What the reconciliation stage's action and payload are named after.
RECONCILE_SLUG = "runtime-state-reconciliation"

# --- which rows a build ends the life of --------------------------------------


def _identity_row(identity: WeaverDocumentId) -> dict:
    """One current-state row's identity, spelled as the Registry spells it.

    A Folder keeps its ``Files/`` prefix, so a Folder and a Table of the same
    name are not one key.
    """

    return bookmark_row(identity)


#: How each population is read from the repository. The table declares which one
#: invalidates it, so nothing here decides that twice.
_POPULATIONS = {
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
    """The current-state rows this build ends the life of, as structured intent.

    Arithmetic over the rows the build read: every row in scope whose object it
    is about to replace, and every row whose object the repository no longer
    declares as something Weaver runs. Empty for most builds, because an
    unchanged repository invalidates nothing.

    Keyed rows rather than rendered SQL, so the installer and a ``Catalogue``
    read one decision two ways.
    """

    scoped = {item for item in items if not _is_builtin(item)}
    if not scoped:
        return ()
    selected = set(selected_for_build)
    invalidation = []
    for table in CURRENT_STATE_TABLES:
        population = _POPULATIONS[table.invalidated_by]
        # What keeps its row: an object this build still runs and is not
        # replacing. Everything else loses its row.
        keep = {
            _key(table, _identity_row(identity))
            for item in scoped
            for identity in population(repository, item=item)
            if identity not in selected
        }
        obsolete = tuple(
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
        if obsolete:
            invalidation.append(
                RuntimeStateInvalidation(table=table.name, rows=obsolete)
            )
    return tuple(invalidation)


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
) -> PlannedStage | None:
    """The one stage that invalidates current state ahead of physical work.

    One action carrying the whole intent: it is one lifecycle decision.
    """

    if not any(one.rows for one in invalidation):
        return None

    filename = f"{RECONCILE_SLUG}.runtime-state.json"
    content = invalidation_payload(tuple(invalidation))
    action = InstallAction(
        id=RECONCILE_SLUG,
        kind=RECONCILE_RUNTIME_STATE,
        resource_node_id=None,
        executor="runtime_state",
        payload=filename,
        payload_sha256=sha256_hex(content),
    )
    return PlannedStage(
        phase=CATALOGUE,
        index=0,
        slug=RECONCILE_SLUG,
        description="invalidate runtime state before physical work",
        payloads={filename: content},
        batches=(
            BuildBatch(
                id=RECONCILE_SLUG, target_id=catalogue_target.id, actions=(action,)
            ),
        ),
    )


def _is_builtin(item: WeaverItemId) -> bool:
    from ..catalogue.builtin import BUILTIN_ITEM

    return item == BUILTIN_ITEM


__all__ = [
    "RECONCILE_SLUG",
    "render_runtime_state_reconciliation",
    "runtime_state_invalidation",
]
