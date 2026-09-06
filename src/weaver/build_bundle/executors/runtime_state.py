"""Reconcile the catalogue's current-state rows to what one build installs.

The payload is structured intent naming the table and the rows, so the lifecycle
decision survives as something that can be inspected. This writes the rows the
build establishes, then removes the rows it ended.

See :mod:`weaver.catalogue.runtime_state` for what the intent holds.
"""

from __future__ import annotations

from typing import Any

from ...catalogue.runtime_state import (
    read_invalidation,
    render_establishment,
    render_invalidation,
)
from ...errors import InstallError
from ...tokens import substitute_build_datetime
from ..models import InstallAction
from .base import InstallationContext


class RuntimeStateExecutor:
    name = "runtime_state"

    def execute(
        self,
        action: InstallAction,
        payload: bytes | None,
        context: InstallationContext,
    ) -> dict[str, Any] | None:
        if payload is None:
            raise InstallError(f"runtime_state action {action.id!r} has no payload")
        if context.sql is None:
            raise InstallError(
                f"runtime_state action {action.id!r} needs a SQL executor but none "
                "was provided, the catalogue is a Warehouse"
            )
        establishment, invalidation = read_invalidation(payload)
        statements = (
            *render_establishment(establishment),
            *render_invalidation(invalidation),
        )
        for statement in statements:
            context.sql.execute_script(
                substitute_build_datetime(statement, context.build_datetime)
            )
        return {
            "tables": [
                one.table for one in (*establishment, *invalidation) if one.rows
            ],
            "established": sum(len(one.rows) for one in establishment),
            "rows": sum(len(one.rows) for one in invalidation),
        }
