"""Render target-bound SQL analytics endpoint refresh barriers."""

from __future__ import annotations

from .models import (
    BUILD_TABLE,
    DROP_TABLE,
    PRUNE_TABLE,
    REFRESH_SQL_ENDPOINT,
    BuildAction,
    BuildBatch,
    BuildSequence,
)
from .payloads import (
    APPLICATION_ENDPOINT_REFRESH_SEQUENCE,
    CONTROL_ENDPOINT_REFRESH_SEQUENCE,
)
from .targets import LAKEHOUSE_TARGET

_DELTA_MUTATIONS = frozenset({BUILD_TABLE, DROP_TABLE, PRUNE_TABLE})


def _slug(value: str) -> str:
    return value.replace("/", "--").replace(" ", "-")


def _refresh_action(target, *, role: str) -> BuildAction:
    return BuildAction(
        id=f"refresh-{role}-sql-endpoint-{_slug(target.id)}",
        kind=REFRESH_SQL_ENDPOINT,
        resource_node_id=None,
        executor="sql_endpoint_refresh",
        payload=None,
        payload_sha256=None,
    )


def render_application_endpoint_refresh(
    physical_sequences,
    *,
    targets,
    control_target,
) -> BuildSequence | None:
    """Refresh each non-control Lakehouse whose planned actions mutate Delta."""

    targets_by_id = {target.id: target for target in targets}
    affected_ids = {
        batch.target_id
        for sequence in physical_sequences
        for batch in sequence.batches
        if batch.target_id != control_target.id
        and targets_by_id[batch.target_id].kind == LAKEHOUSE_TARGET
        and any(action.kind in _DELTA_MUTATIONS for action in batch.actions)
    }
    if not affected_ids:
        return None

    number = APPLICATION_ENDPOINT_REFRESH_SEQUENCE
    batches = tuple(
        BuildBatch(
            id=f"{number:03d}-refresh-application-{_slug(target_id)}",
            target_id=target_id,
            actions=(
                _refresh_action(targets_by_id[target_id], role="application"),
            ),
        )
        for target_id in sorted(affected_ids)
    )
    return BuildSequence(
        number=number,
        description="refresh affected application Lakehouse SQL endpoints",
        batches=batches,
    )


def render_control_endpoint_refresh(control_target) -> BuildSequence:
    """Always refresh the Weaver Lakehouse endpoint after catalogue DML."""

    number = CONTROL_ENDPOINT_REFRESH_SEQUENCE
    return BuildSequence(
        number=number,
        description="refresh Weaver Lakehouse SQL endpoint after catalogue publication",
        batches=(
            BuildBatch(
                id=f"{number:03d}-refresh-control-{_slug(control_target.id)}",
                target_id=control_target.id,
                actions=(_refresh_action(control_target, role="control"),),
            ),
        ),
    )
