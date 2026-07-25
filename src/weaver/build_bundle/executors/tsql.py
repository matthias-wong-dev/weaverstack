"""T-SQL execution — run a generated Warehouse script through the SQL stack.

The payload is a finished, self-contained T-SQL script (built by
:mod:`weaver.ses.tsql_ddl`): a table build materialises and inspects its own
query shape server-side and creates only its main table; a view is a
``CREATE OR ALTER VIEW``. The executor runs it as one multi-statement script
through the pooled SQL executor the environment supplies — it adds no logic of
its own, exactly the mechanical executor the build philosophy calls for.
"""

from __future__ import annotations

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
