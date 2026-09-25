"""A Session that records requested execution without interpreting it.

It implements the same
:class:`~weaver.sessions.base.Session` contract the console and the notebook do.
Tests run production operation code and record external calls without making them.

It does not parse SQL, evaluate statements, model a catalogue, or stand in for
Spark. Tests configure responses and inspect recorded statements.

.. code-block:: python

    session = TestSession(workspace=Workspace(workspace="Demo"))
    session.answer_spark_sql("SHOW TABLES IN ...", [{"tableName": "customer"}])
    weaver.build(".", session=session)
    assert "CREATE SCHEMA" in session.spark_sql[0]
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Sequence

from ..errors import CommandError
from ..workspaces import Workspace
from .base import Session, WorkspaceScope


@dataclass
class RecordedCall:
    kind: str
    body: Any
    workspace: str | None = None
    detail: dict[str, Any] = field(default_factory=dict)


class TestSession(Session):
    """A Session that records execution and returns configured results.

    Tests supply ``resolver`` and ``store``. Everything else is recorded.
    """

    #: Its name begins with ``Test``, so pytest would otherwise try to collect
    #: it as a test class wherever it is imported.
    __test__ = False

    def __init__(
        self,
        *,
        workspace: Workspace | None = None,
        resolver: Any = None,
        store: Any = None,
        executes_here: bool = False,
        telemetry=None,
        executor=None,
    ) -> None:
        super().__init__(workspace=workspace, telemetry=telemetry, executor=executor)
        self._resolver = resolver
        self._store = store
        self._executes_here = executes_here
        self.calls: list[RecordedCall] = []
        self._spark_answers: dict[str, Any] = {}
        self._tsql_answers: dict[str, Any] = {}
        self._python_answers: list[Any] = []
        self._default_rows: list[dict] = []

    # --- what a test configures ---------------------------------------------

    def answer_spark_sql(self, statement: str, rows) -> None:
        self._spark_answers[statement.strip()] = rows

    def answer_tsql(self, statement: str, rows) -> None:
        self._tsql_answers[statement.strip()] = rows

    def answer_python(self, value) -> None:
        self._python_answers.append(value)

    # --- what a test reads ---------------------------------------------------

    @property
    def spark_sql(self) -> tuple[str, ...]:
        statements: list[str] = []
        for call in self.calls:
            if call.kind == "spark_sql":
                statements.extend(call.body)
            elif call.kind == "spark_sql_actions":
                statements.extend(statement for _label, statement in call.body)
        return tuple(statements)

    @property
    def tsql(self) -> tuple[str, ...]:
        return tuple(
            statement
            for call in self.calls
            if call.kind == "tsql"
            for statement in call.body
        )

    @property
    def python(self) -> tuple[str, ...]:
        return tuple(call.body for call in self.calls if call.kind == "python")

    # --- the Session contract ------------------------------------------------

    def executes_here(self, workspace: Workspace | None = None) -> bool:
        return self._executes_here

    def _new_scope(self, workspace: Workspace) -> WorkspaceScope:
        return WorkspaceScope(
            workspace,
            telemetry=self.telemetry,
            executor=self._executor,
            resolver=self._resolver,
            store=self._store,
        )

    def create_delta_table(
        self,
        qualified_name: str,
        columns: Sequence[Sequence[Any]],
        *,
        identity_column: str | None = None,
        column_mapping: bool = True,
        workspace: Workspace | None = None,
        timeout: float | None = None,
    ) -> Any:
        self._record(
            "delta_table",
            {
                "object": qualified_name,
                "columns": [list(column) for column in columns],
                "identity_column": identity_column,
                "column_mapping": column_mapping,
            },
            workspace,
            timeout=timeout,
        )
        return None

    def execute_python(
        self,
        program: str,
        *,
        workspace: Workspace | None = None,
        timeout: float | None = None,
    ) -> Any:
        self._record("python", program, workspace, timeout=timeout)
        if not self._python_answers:
            raise CommandError(
                "No Python result is configured for this TestSession. Call "
                "answer_python() first."
            )
        return self._python_answers.pop(0)

    def execute_spark_sql_batch(
        self,
        statements: Sequence[str],
        *,
        exact_case: bool = False,
        workspace: Workspace | None = None,
        timeout: float | None = None,
    ) -> Any:
        body = [str(statement) for statement in statements]
        self._record(
            "spark_sql", body, workspace, exact_case=exact_case, timeout=timeout
        )
        return self._answer(self._spark_answers, body)

    def execute_spark_sql_actions(
        self,
        actions: Sequence[tuple[str, str]],
        *,
        exact_case: bool = False,
        workspace: Workspace | None = None,
        timeout: float | None = None,
    ) -> list[dict[str, Any]]:
        body = [(str(label), str(statement)) for label, statement in actions]
        self._record(
            "spark_sql_actions",
            body,
            workspace,
            exact_case=exact_case,
            timeout=timeout,
        )
        return [
            {
                "label": label,
                "succeeded": True,
                "started_after_seconds": 0.0,
                "duration_seconds": 0.0,
            }
            for label, _statement in body
        ]

    def execute_tsql(
        self,
        statement: str,
        *,
        target: Any,
        workspace: Workspace | None = None,
        parameters: Sequence[Any] | None = None,
    ) -> None:
        self._record(
            "tsql",
            [statement],
            workspace,
            target=target,
            parameters=None if parameters is None else list(parameters),
        )
        return None

    def query_tsql(
        self,
        statement: str,
        *,
        target: Any,
        workspace: Workspace | None = None,
        parameters: Sequence[Any] | None = None,
    ) -> Any:
        self._record(
            "tsql",
            [statement],
            workspace,
            target=target,
            parameters=None if parameters is None else list(parameters),
        )
        return self._answer(self._tsql_answers, [statement])

    # --- recording -----------------------------------------------------------

    def _record(self, kind: str, body, workspace, **detail) -> None:
        named = getattr(self.workspace_or_default(workspace), "workspace", None)
        self.calls.append(
            RecordedCall(kind=kind, body=body, workspace=named, detail=detail)
        )

    def _answer(self, configured: dict, statements: list[str]):
        if not statements:
            return []
        return configured.get(statements[-1].strip(), self._default_rows)


__all__ = ["RecordedCall", "TestSession"]
