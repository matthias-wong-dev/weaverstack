"""The refresh that closes one Lakehouse item's physical work.

A Fabric Lakehouse presents its Delta tables twice: natively to Spark, and
through a SQL analytics endpoint whose metadata is synchronised behind the
mutation rather than with it. Everything that reads a Lakehouse as SQL reads
that endpoint, a Warehouse view over another item, a report, a downstream
shortcut, so a build that created a table and immediately built a dependent view
over it could and did see the previous shape.

The refresh therefore sits at the item boundary rather than in a tail after all
physical work: it is the completion barrier for the item that mutated Delta, and
it has to be behind that item and ahead of any later item layer. A global tail
would leave a consumer's Warehouse view created against metadata that had not
caught up.

Placement is this module's business; the refresh itself belongs to
:mod:`weaver.build_bundle.executors.sql_endpoint_refresh`.
"""

from __future__ import annotations

from typing import Iterable

from ..declaration.model import WeaverItemId
from .models import (
    CREATE_SHORTCUT,
    DROP_SHORTCUT,
    REFRESH_SQL_ENDPOINT,
    BuildBatch,
    InstallAction,
)
from .physical import DELTA_MUTATING_KINDS
from .stages import REFRESH, PlannedStage
from .targets import WAREHOUSE_TARGET, BoundTarget

#: Everything that leaves a Lakehouse's endpoint metadata stale. Shortcut creation
#: and removal are here because a OneLake shortcut appears in the destination as a
#: table.
_MUTATING = DELTA_MUTATING_KINDS | {CREATE_SHORTCUT, DROP_SHORTCUT}


def item_refresh_stage(
    stages: Iterable[PlannedStage],
    *,
    item: WeaverItemId,
    target: BoundTarget,
) -> PlannedStage | None:
    """One refresh for this item, when its planned work mutated Delta.

    A Warehouse item has no endpoint of its own to refresh. It is reached over
    SQL, and an item whose only work was a folder or a schema has changed
    nothing the endpoint describes.
    """

    if target.kind == WAREHOUSE_TARGET:
        return None
    if not any(
        action.kind in _MUTATING
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
