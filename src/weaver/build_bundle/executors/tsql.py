"""Run finished Warehouse T-SQL payloads through the supplied SQL executor."""

from __future__ import annotations

import json
from typing import Any

from ...errors import InstallError
from ...tokens import substitute_build_datetime
from ..models import InstallAction
from .base import InstallationContext


class TSqlExecutor:
    name = "tsql"

    def execute(
        self,
        action: InstallAction,
        payload: bytes | None,
        context: InstallationContext,
    ) -> dict[str, Any] | None:
        if payload is None:
            raise InstallError(f"tsql action {action.id!r} has no payload")
        if context.sql is None:
            raise InstallError(
                f"tsql action {action.id!r} needs a SQL executor but none was "
                "provided. A Warehouse install must supply one"
            )
        script = payload.decode("utf-8")
        context.sql.execute_script(script)
        return {
            "statement_first_line": script.splitlines()[0] if script.strip() else ""
        }


class TSqlBatchExecutor:
    """Run an ordered array as separate T-SQL batches within one action.

    ``CREATE VIEW`` must be the first statement in a batch, so view statements
    cannot share one ``execute_script`` call.
    """

    name = "tsql_batch"

    def execute(
        self,
        action: InstallAction,
        payload: bytes | None,
        context: InstallationContext,
    ) -> dict[str, Any] | None:
        if payload is None:
            raise InstallError(f"tsql_batch action {action.id!r} has no payload")
        if context.sql is None:
            raise InstallError(
                f"tsql_batch action {action.id!r} needs a SQL executor but none was "
                "provided. A Warehouse install must supply one"
            )
        statements = json.loads(payload.decode("utf-8"))
        if not isinstance(statements, list):
            raise InstallError(
                f"tsql_batch action {action.id!r} payload must be an array of statements"
            )
        # The publication instant is installation-scoped and cannot be frozen
        # into the payload generated earlier.
        for statement in statements:
            context.sql.execute_script(
                substitute_build_datetime(statement, context.build_datetime)
            )
        return {"statements": len(statements)}
