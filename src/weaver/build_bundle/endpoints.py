"""Refresh a mutated Lakehouse's SQL analytics endpoint once, without a barrier.

Endpoint metadata lags Delta mutations, and Fabric syncs each changed table in
turn. A refresh syncs the tables the plan reads through the endpoint, and starts
once their mutations have known outcomes. Only work that reads through the
endpoint waits for it to finish. The Build completes only once it is current.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Iterable, Sequence

from ..declaration.model import OBJECT_SHAPE, WeaverDocumentId, WeaverItemId
from ..errors import IdentityError
from .dependencies import OBJECT, action_key, endpoint_object_key
from .executors.sql_endpoint_refresh import (
    AWAIT_EXECUTOR,
    REFRESH_RESULT,
    START_EXECUTOR,
    START_TABLES_EXECUTOR,
    TABLES_EXTENSION,
    tables_payload,
)
from .models import (
    AWAIT_ENDPOINT_REFRESH,
    AWAIT_FILE_SHORTCUTS,
    AWAIT_TABLE_SHORTCUTS,
    BUILD_TABLE,
    BUILD_VIEW,
    CREATE_SHORTCUT,
    DROP_SHORTCUT,
    DROP_TABLE,
    DROP_VIEW,
    START_ENDPOINT_REFRESH,
    BuildBatch,
    InstallAction,
)
from .payloads import sha256_hex
from .stages import REFRESH, PlannedStage
from .targets import BoundTarget

#: OneLake shortcuts appear as tables and therefore also stale endpoint metadata.
_ENDPOINT_MUTATING_KINDS = frozenset(
    {
        BUILD_TABLE,
        BUILD_VIEW,
        DROP_TABLE,
        DROP_VIEW,
        "prune_table",
        "prune_view",
        "prune_schema",
        CREATE_SHORTCUT,
        DROP_SHORTCUT,
        AWAIT_TABLE_SHORTCUTS,
        AWAIT_FILE_SHORTCUTS,
    }
)


def lakehouse_endpoint_refresh_stage(
    stages: Iterable[PlannedStage],
    *,
    item: WeaverItemId,
    target: BoundTarget,
) -> PlannedStage | None:
    stages = tuple(stages)
    mutations = [
        action
        for stage in stages
        for batch in stage.batches
        for action in batch.actions
        if action.kind in _ENDPOINT_MUTATING_KINDS
    ]
    if not mutations:
        return None
    mutated = {action.id for action in mutations}
    # Every object these actions establish or remove, including shortcut
    # destinations, is current in the endpoint once the refresh completes.
    current = sorted(
        {
            endpoint_object_key(key.removeprefix(OBJECT))
            for stage in stages
            for action_id, keys in stage.provides.items()
            if action_id in mutated
            for key in keys
            if key.startswith(OBJECT)
        }
        | {
            endpoint_object_key(action.resource_node_id)
            for action in mutations
            if action.resource_node_id is not None
        }
    )
    slug = str(item).replace("/", "--").replace(" ", "-")
    start = f"start-sql-endpoint-refresh-{slug}"
    finish = f"await-sql-endpoint-refresh-{slug}"
    return PlannedStage(
        phase=REFRESH,
        slug="refresh-endpoints",
        description="refresh mutated Lakehouse SQL endpoints",
        batches=(
            BuildBatch(
                id=f"refresh-endpoint-{slug}",
                target_id=target.id,
                actions=(
                    InstallAction(
                        id=start,
                        kind=START_ENDPOINT_REFRESH,
                        resource_node_id=None,
                        executor=START_EXECUTOR,
                        payload=None,
                        payload_sha256=None,
                    ),
                    InstallAction(
                        id=finish,
                        kind=AWAIT_ENDPOINT_REFRESH,
                        resource_node_id=None,
                        executor=AWAIT_EXECUTOR,
                        payload=None,
                        payload_sha256=None,
                    ),
                ),
            ),
        ),
        follows={start: tuple(action_key(action.id) for action in mutations)},
        requires={finish: (action_key(start),)},
        results={finish: (start, REFRESH_RESULT)},
        provides={finish: tuple(current)},
    )


_ENDPOINT = endpoint_object_key("")
_ACTION = action_key("")


def narrow_endpoint_refreshes(
    stages: Sequence[PlannedStage],
) -> tuple[PlannedStage, ...]:
    """Sync only the objects something in the plan reads through an endpoint.

    A refresh nothing reads through is dropped. One whose readers read tables
    syncs those tables and follows only the mutations that make them; one read
    by a schema keeps syncing every table.
    """

    read = {
        key
        for stage in stages
        for keys in stage.requires.values()
        for key in keys
        if key.startswith(_ENDPOINT)
    }
    makes: dict[str, set[str]] = {}
    for stage in stages:
        for batch in stage.batches:
            for action in batch.actions:
                keys = {
                    endpoint_object_key(key.removeprefix(OBJECT))
                    for key in stage.provides.get(action.id, ())
                    if key.startswith(OBJECT)
                }
                if action.resource_node_id is not None:
                    keys.add(endpoint_object_key(action.resource_node_id))
                makes[action.id] = keys
    return tuple(
        _narrowed(stage, read, makes) if stage.phase == REFRESH else stage
        for stage in stages
    )


def _narrowed(stage: PlannedStage, read, makes) -> PlannedStage:
    batches = []
    payloads = dict(stage.payloads)
    provides = dict(stage.provides)
    requires = dict(stage.requires)
    follows = dict(stage.follows)
    results = dict(stage.results)
    for batch in stage.batches:
        start = next(a for a in batch.actions if a.kind == START_ENDPOINT_REFRESH)
        finish = next(a for a in batch.actions if a.kind == AWAIT_ENDPOINT_REFRESH)
        needed = set(provides.get(finish.id, ())) & read
        if not needed:
            for keys in (provides, requires, follows, results):
                keys.pop(start.id, None)
                keys.pop(finish.id, None)
            continue
        tables = _tables(needed)
        if tables is None:
            batches.append(batch)
            continue
        content = tables_payload(tables)
        filename = f"{start.id}{TABLES_EXTENSION}"
        payloads[filename] = content
        scoped = replace(
            start,
            executor=START_TABLES_EXECUTOR,
            payload=filename,
            payload_sha256=sha256_hex(content),
        )
        batches.append(
            replace(
                batch,
                actions=tuple(
                    scoped if action.id == start.id else action
                    for action in batch.actions
                ),
            )
        )
        follows[start.id] = tuple(
            key
            for key in follows.get(start.id, ())
            if makes.get(key.removeprefix(_ACTION), set()) & needed
        )
        provides[finish.id] = tuple(sorted(needed))
    return replace(
        stage,
        batches=tuple(batches),
        payloads=payloads,
        provides=provides,
        requires=requires,
        follows=follows,
        results=results,
    )


def _tables(keys) -> list[tuple[str, str]] | None:
    """The ``(schema, table)`` each key names, or ``None`` where one is a schema."""

    tables = []
    for key in keys:
        try:
            identity = WeaverDocumentId.parse(key.removeprefix(_ENDPOINT))
        except IdentityError:
            return None
        if identity.is_files or identity.shape != OBJECT_SHAPE:
            return None
        tables.append((identity.object_id.schema, identity.object_id.object))
    return sorted(tables)
