"""Refresh a target Lakehouse's SQL analytics endpoint where supported."""

from __future__ import annotations

from ...errors import InstallError
from ..models import REFRESH_SQL_ENDPOINT
from .base import InstallationContext, SkippedExecution


class SqlEndpointRefreshExecutor:
    """Perform the planned refresh, or explicitly skip an unsupported host."""

    name = "sql_endpoint_refresh"

    def execute(self, action, payload, context: InstallationContext):
        if action.kind != REFRESH_SQL_ENDPOINT:
            raise InstallError(
                f"SQL endpoint refresh action {action.id!r} has unknown kind "
                f"{action.kind!r}"
            )
        if payload is not None:
            raise InstallError(
                f"SQL endpoint refresh action {action.id!r} must not carry a payload"
            )

        refresh = getattr(context.resolver, "refresh_sql_endpoint", None)
        if refresh is None:
            return SkippedExecution(
                {
                    "reason": "SQL endpoint refresh is unsupported in this environment",
                }
            )
        details = refresh(context.target.lakehouse)
        return details or {"lakehouse": context.target.bound.name}
