"""Installation reports built from action statuses."""

from __future__ import annotations

from datetime import datetime, timezone

from weaver.build_bundle.report import (
    ActionResult,
    InstallationReport,
    SequenceResult,
)


def report_of(
    *statuses: str, status: str = "succeeded", bundle_id: str = "bundle"
) -> InstallationReport:
    """One sequence holding one action per status, in order."""

    actions = tuple(
        ActionResult(
            action_id=f"action-{index}",
            resource_node_id=None,
            target_id="target",
            executor="tsql",
            status=each,
        )
        for index, each in enumerate(statuses)
    )
    return InstallationReport(
        bundle_id=bundle_id,
        status=status,
        started_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        finished_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        sequences=(SequenceResult(1, "actions", status, actions),),
    )
