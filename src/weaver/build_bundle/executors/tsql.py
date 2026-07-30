"""T-SQL execution — run a generated Warehouse script through the SQL stack.

The payload is a finished, self-contained T-SQL script (built by
:mod:`weaver.declaration.tsql_ddl`): a table build materialises and inspects its own
query shape server-side and creates only its main table; a view is a
strict ``CREATE VIEW``. The executor runs it as one multi-statement script
through the pooled SQL executor the environment supplies — it adds no logic of
its own, exactly the mechanical executor the build philosophy calls for.
"""

from __future__ import annotations

import json
from typing import Any

from ...errors import InstallError
from ..models import BuildAction
from .base import InstallationContext


class TSqlExecutor:
    name = "tsql"

    def execute(
        self,
        action: BuildAction,
        payload: bytes | None,
        context: InstallationContext,
    ) -> dict[str, Any] | None:
        if payload is None:
            raise InstallError(f"tsql action {action.id!r} has no payload")
        if context.sql is None:
            raise InstallError(
                f"tsql action {action.id!r} needs a SQL executor but none was "
                "provided — a Warehouse install must supply one"
            )
        script = payload.decode("utf-8")
        context.sql.execute_script(script)
        return {
            "statement_first_line": script.splitlines()[0] if script.strip() else ""
        }


class TSqlBatchExecutor:
    """Ordered T-SQL statements, each run as its own batch, as one action.

    The T-SQL twin of ``spark_sql_batch``, and it exists for a reason the Spark
    side does not have: several statements cannot share a batch when any of them
    is a ``CREATE VIEW``, because T-SQL requires that to be the first statement in
    its batch. ``execute_script`` sends what it is given as one batch, so a script
    holding two ``CREATE OR ALTER VIEW`` statements is rejected outright —
    *Incorrect syntax near the keyword 'create'*.

    So the payload is an ordered array rather than one script, and each element is
    submitted on its own. The action stays one action: it is one decision, reported
    once, and the batching is transport.
    """

    name = "tsql_batch"

    def execute(
        self,
        action: BuildAction,
        payload: bytes | None,
        context: InstallationContext,
    ) -> dict[str, Any] | None:
        if payload is None:
            raise InstallError(f"tsql_batch action {action.id!r} has no payload")
        if context.sql is None:
            raise InstallError(
                f"tsql_batch action {action.id!r} needs a SQL executor but none was "
                "provided — a Warehouse install must supply one"
            )
        statements = json.loads(payload.decode("utf-8"))
        if not isinstance(statements, list):
            raise InstallError(
                f"tsql_batch action {action.id!r} payload must be an array of statements"
            )
        for statement in statements:
            context.sql.execute_script(statement)
        return {"statements": len(statements)}
