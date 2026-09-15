"""Place Lakehouse SQL endpoint refreshes at item boundaries.

Endpoint metadata lags Delta mutations, so refresh must complete before a later
item layer builds against it. The executor owns the refresh mechanism.
"""

from __future__ import annotations

from typing import Iterable

from ..declaration.model import WeaverItemId
from .models import (
    BUILD_TABLE,
    BUILD_VIEW,
    CREATE_SHORTCUT,
    DROP_SHORTCUT,
    DROP_TABLE,
    DROP_VIEW,
    REFRESH_SQL_ENDPOINT,
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
    }
)


def lakehouse_endpoint_refresh_stage(
    stages: Iterable[PlannedStage],
    *,
    item: WeaverItemId,
    target: BoundTarget,
) -> PlannedStage | None:
    if not any(
        action.kind in _ENDPOINT_MUTATING_KINDS
        for stage in stages
        for batch in stage.batches
        for action in batch.actions
    ):
        return None
    slug = str(item).replace("/", "--").replace(" ", "-")
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
                        id=f"refresh-sql-endpoint-{slug}",
                        kind=REFRESH_SQL_ENDPOINT,
                        resource_node_id=None,
                        executor="sql_endpoint_refresh",
                        payload=None,
                        payload_sha256=None,
                    ),
                ),
            ),
        ),
    )
