"""The refresh that closes one Lakehouse item's physical work.

A Fabric Lakehouse presents its Delta tables twice: natively to Spark, and
through a SQL analytics endpoint whose metadata is synchronised behind the
mutation rather than with it. Everything that reads a Lakehouse *as SQL* reads
that endpoint — a Warehouse view over another item, a report, a downstream
shortcut — so a build that created a table and immediately built a dependent view
over it could and did see the previous shape.

The refresh therefore sits at the item boundary, not in a tail after every item:
it is the completion barrier for the item that mutated Delta, and it has to be
behind that item and ahead of anything in a later item layer.

It is planned host-independently, exactly like the rest of the bundle. The
emulator has no SQL analytics endpoint at all, and the executor says so and skips
rather than inventing a local equivalent that would keep no promise.

The *placement* is this module's business; the refresh itself belongs to
:mod:`weaver.build_bundle.executors.sql_endpoint_refresh`. An earlier design put
one refresh in a global tail after all physical work, which is correct for a
single item and wrong the moment a second item reads the first: the consumer's
Warehouse view would be created against endpoint metadata that had not caught up.
"""

from __future__ import annotations

from typing import Iterable

from ..declaration.model import WeaverItemId
from .models import CREATE_ALIAS, REFRESH_SQL_ENDPOINT, InstallAction, BuildBatch
from .physical import DELTA_MUTATING_KINDS
from .stages import REFRESH, PlannedStage
from .targets import BoundTarget, WAREHOUSE_TARGET

#: Everything that leaves a Lakehouse's endpoint metadata stale. Alias creation
#: is here because a OneLake shortcut *is* a new table in the destination.
_MUTATING = DELTA_MUTATING_KINDS | {CREATE_ALIAS}


def item_refresh_stage(
    stages: Iterable[PlannedStage],
    *,
    item: WeaverItemId,
    target: BoundTarget,
) -> PlannedStage | None:
    """One refresh for this item, when its planned work mutated Delta.

    A Warehouse item has no endpoint of its own to refresh — it *is* reached over
    SQL — and an item whose only work was a folder or a schema has changed
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
