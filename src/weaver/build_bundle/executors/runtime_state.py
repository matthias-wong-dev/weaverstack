"""Invalidate the catalogue's current-state rows one build has ended.

The payload is structured intent — which table, and which keyed rows — rather
than a statement, so the lifecycle decision survives as something a reader can
inspect. This renders one scoped DELETE per table and runs it.

See :mod:`weaver.catalogue.runtime_state` for what the intent holds and
:mod:`weaver.build_bundle.bookmarks` for why it runs ahead of physical work.
"""

from __future__ import annotations

from typing import Any

from ...catalogue.runtime_state import read_invalidation, render_invalidation
from ...errors import InstallError
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
                "was provided — the catalogue is a Warehouse"
            )
        invalidation = read_invalidation(payload)
        statements = render_invalidation(invalidation)
        for statement in statements:
            context.sql.execute_script(statement)
        return {
            "tables": [one.table for one in invalidation if one.rows],
            "rows": sum(len(one.rows) for one in invalidation),
        }
