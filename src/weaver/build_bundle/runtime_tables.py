"""What a build does about the catalogue's runtime tables.

Two separate things: it invalidates the current-state rows whose incarnation it
is ending, and it gives every target it installs something runnable into the
runtime tables under their own names — views in a Warehouse, OneLake shortcuts in
a Lakehouse.

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

import json
from typing import Iterable, Mapping, Sequence

from ..catalogue.claims import bookmark_row
from ..catalogue.render import Row, sorted_rows
from ..catalogue.runtime_state import (
    RuntimeStateInvalidation,
    invalidation_payload,
)
from ..catalogue.tables import (
    BY_LOADABLE,
    BY_VALIDATION,
    CATALOGUE_SCHEMA,
    CURRENT_STATE_TABLES,
    SCOPE_ITEM_NAME,
    SCOPE_ITEM_TYPE,
)
from ..declaration.model import WAREHOUSE, WeaverDocumentId, WeaverItemId
from ..etl import (
    item_bookmarkable_objects,
    item_validated_objects,
)
from .changes import TABLE as TABLE_KIND
from .changes import VIEW as VIEW_KIND
from .changes import added
from .models import (
    CREATE_RUNTIME_REFERENCE,
    RECONCILE_RUNTIME_STATE,
    BuildBatch,
    InstallAction,
)
from .payloads import sha256_hex
from .shortcuts import (
    Reference,
    declaration_key,
    shortcut_payload,
    view_statement,
)
from .stages import CATALOGUE, SHORTCUT, PlannedStage
from .targets import LAKEHOUSE_TARGET, WAREHOUSE_TARGET

#: What the reconciliation stage's action and payload are named after.
RECONCILE_SLUG = "runtime-state-reconciliation"

#: What the reference stage's action and payload are named after.
REFERENCE_SLUG = "runtime-reference"


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
        # What keeps its row: an object this build still runs and is *not*
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


# --- the local names a generated statement uses -------------------------------


def _reference(pair, catalogue: str) -> Reference:
    """One runtime table as the reference it is: a shortcut nobody authored.

    Published through the ordinary installed shortcut contract and physically
    rendered by the same code as an authored declaration.
    """

    from ..declaration.model import TABLE_SHORTCUT, VIEW_SHORTCUT

    destination = pair.destination
    source = pair.source
    qualified = destination.object_id.qualified
    return Reference(
        owner=destination.item,
        name=qualified,
        destination=destination,
        shortcut_type=(
            VIEW_SHORTCUT if destination.item.item_type == WAREHOUSE else TABLE_SHORTCUT
        ),
        target=f"{WAREHOUSE}/{catalogue}/{qualified}",
        target_item_name=catalogue,
        target_object=source.object_id,
    )


def _runtime_references(repository, item: WeaverItemId):
    """The injected logical relations this item presents physically."""

    from ..catalogue.builtin import BUILTIN_ITEM

    return tuple(
        pair
        for pair in repository.logical_shortcuts
        if pair.destination.item == item
        and pair.source.item == BUILTIN_ITEM
        and pair.destination.object_id == pair.source.object_id
        and pair.destination.object_id.schema == CATALOGUE_SCHEMA
    )


def render_runtime_references(
    repository,
    *,
    item: WeaverItemId,
    target,
    catalogue_target,
    selected: Iterable[WeaverDocumentId],
    runtime_sources: Mapping[str, object] | None = None,
) -> PlannedStage | None:
    """Give a built target the catalogue's runtime tables under their own names.

    After schemas and authored shortcuts, but before documents: building a table
    may execute authored SQL to discover its shape, and that SQL can read one of
    these local names. The injected dependency on the built-in catalogue item
    puts its source tables in an earlier item layer when both arrive in one
    bundle.

    One action per target carries every selected reference. Selection already
    compared Registry with physical inventory, so an unchanged target reaches
    this function with no work and a missing or uncertified reference is remade.
    """

    selected = set(selected)
    references = tuple(
        pair
        for pair in _runtime_references(repository, item)
        if pair.destination in selected
    )
    if not references:
        return None
    if target.kind == WAREHOUSE_TARGET:
        return _warehouse_views(references, target, catalogue_target)
    return _lakehouse_shortcuts(
        references=references,
        target=target,
        sources=runtime_sources or {},
    )


def _wanted_views(references, catalogue_target, target) -> tuple:
    """The selected runtime-table views this Warehouse can present."""

    if target.name.casefold() == catalogue_target.name.casefold():
        # The catalogue's own Warehouse: the tables are right there, and a view
        # of that name would be a view over itself.
        return ()
    return tuple(references)


def _warehouse_views(references, target, catalogue_target) -> PlannedStage | None:
    """Create or replace the selected runtime-table views."""

    wanted = _wanted_views(references, catalogue_target, target)
    if not wanted:
        return None

    statements = [
        view_statement(_reference(pair, catalogue_target.name), catalogue_target)
        for pair in wanted
    ]
    item = wanted[0].destination.item
    item_slug = str(item).replace("/", "--")
    filename = f"{REFERENCE_SLUG}-{item_slug}.tsql-batch.json"
    content = (json.dumps(statements, indent=2, ensure_ascii=False) + "\n").encode(
        "utf-8"
    )
    return _stage(
        item_slug,
        target=target,
        executor="tsql_batch",
        filename=filename,
        content=content,
        description="create views over the catalogue's runtime tables",
        presented=wanted,
    )


def _lakehouse_shortcuts(*, references, target, sources) -> PlannedStage | None:
    """Create or replace the selected OneLake runtime-table shortcuts."""

    wanted = tuple(
        pair for pair in references if pair.source.object_id.object in sources
    )
    if not wanted:
        return None

    item = wanted[0].destination.item
    item_slug = str(item).replace("/", "--")
    rendered = [
        (
            _reference(pair, sources[pair.source.object_id.object].item_name),
            target,
        )
        for pair in wanted
    ]
    content = shortcut_payload(
        rendered,
        sources={
            declaration_key(reference): sources[pair.source.object_id.object]
            for pair, (reference, _target) in zip(wanted, rendered)
        },
    )
    return _stage(
        item_slug,
        target=target,
        executor="shortcut",
        filename=f"{REFERENCE_SLUG}-{item_slug}.shortcut.json",
        content=content,
        description="create shortcuts to the catalogue's runtime tables",
        presented=wanted,
    )


def _stage(
    item_slug: str,
    *,
    target,
    executor: str,
    filename: str,
    content: bytes,
    description: str,
    presented,
) -> PlannedStage:
    action = InstallAction(
        id=f"{REFERENCE_SLUG}-{item_slug}",
        kind=CREATE_RUNTIME_REFERENCE,
        resource_node_id=None,
        executor=executor,
        payload=filename,
        payload_sha256=sha256_hex(content),
    )
    return PlannedStage(
        phase=SHORTCUT,
        # Authored shortcuts occupy index 0. Runtime references follow them in
        # the same phase and still precede every document build.
        index=1,
        slug=REFERENCE_SLUG,
        description=description,
        payloads={filename: content},
        batches=(
            BuildBatch(
                id=f"{REFERENCE_SLUG}-{item_slug}",
                target_id=target.id,
                actions=(action,),
            ),
        ),
        # Declared alongside the action, as every physical change is.
        changes={
            target.id: tuple(
                added(
                    _CHANGE_KIND[target.kind],
                    pair.destination.object_id.qualified,
                    action.id,
                )
                for pair in presented
            )
        },
    )


#: What a reference physically is, by target kind. A Warehouse view and a
#: Lakehouse table shortcut both answer to ``_.Bookmark``.
_CHANGE_KIND = {WAREHOUSE_TARGET: VIEW_KIND, LAKEHOUSE_TARGET: TABLE_KIND}


def _is_builtin(item: WeaverItemId) -> bool:
    from ..catalogue.builtin import BUILTIN_ITEM

    return item == BUILTIN_ITEM


__all__ = [
    "RECONCILE_SLUG",
    "REFERENCE_SLUG",
    "render_runtime_references",
    "render_runtime_state_reconciliation",
    "runtime_state_invalidation",
]
