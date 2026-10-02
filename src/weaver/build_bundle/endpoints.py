"""Refresh a mutated Lakehouse's SQL analytics endpoint once, without a barrier.

Endpoint metadata lags Delta mutations. The refresh starts after every mutation
of its Lakehouse has a known outcome. A plan waits for it to finish only where
an action reads through the endpoint; otherwise the refresh is requested and the
endpoint catches up after the Build.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Iterable

from ..declaration.model import WeaverItemId
from .dependencies import OBJECT, action_key, endpoint_object_key
from .executors.sql_endpoint_refresh import (
    AWAIT_EXECUTOR,
    REFRESH_RESULT,
    REQUEST_EXECUTOR,
    START_EXECUTOR,
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
    REQUEST_ENDPOINT_REFRESH,
    START_ENDPOINT_REFRESH,
    BuildBatch,
    InstallAction,
)
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


def unawaited_refreshes(stages) -> list:
    """Replace each refresh nothing in the plan reads through with a request."""

    required = {
        key for stage in stages for keys in stage.requires.values() for key in keys
    }
    result = []
    for stage in stages:
        batches = []
        follows = dict(stage.follows)
        requires = dict(stage.requires)
        results = dict(stage.results)
        provides = dict(stage.provides)
        for batch in stage.batches:
            actions = {action.executor: action for action in batch.actions}
            finish = actions.get(AWAIT_EXECUTOR)
            if finish is None or required.intersection(provides.get(finish.id, ())):
                batches.append(batch)
                continue
            start = actions[START_EXECUTOR]
            request = replace(
                start,
                id=start.id.replace("start-", "request-", 1),
                kind=REQUEST_ENDPOINT_REFRESH,
                executor=REQUEST_EXECUTOR,
            )
            batches.append(replace(batch, actions=(request,)))
            follows[request.id] = follows.pop(start.id)
            for mapping in (requires, results, provides):
                mapping.pop(finish.id, None)
        result.append(
            replace(
                stage,
                batches=tuple(batches),
                follows=follows,
                requires=requires,
                results=results,
                provides=provides,
            )
        )
    return result
