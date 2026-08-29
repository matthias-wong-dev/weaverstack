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

#: Everything that leaves a Lakehouse's endpoint metadata stale. Shortcut creation
#: and removal are here because a OneLake shortcut appears in the destination as a
#: table.
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
    """Plan one refresh when this Lakehouse item's work mutated Delta."""

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
