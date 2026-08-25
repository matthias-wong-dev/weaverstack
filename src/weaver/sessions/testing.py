"""A Session that records what a host would have been asked to do.

The substrate the fast suite decides against. It implements the same
:class:`~weaver.sessions.base.Session` contract the console and the notebook do,
so a test drives the real operations rather than a copy of them, and every
crossing is captured instead of made.

What it must never become is an engine. It does not parse SQL, evaluate a
statement, model a catalogue or stand in for Spark: it records the call and
answers with whatever the test configured. A question about what a statement
means is a question for a Fabric test, and one about what Weaver renders is
answered by reading the recorded statement.

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
    """One crossing a host was asked to make."""

    kind: str
    body: Any
    workspace: str | None = None
    detail: dict[str, Any] = field(default_factory=dict)


class TestSession(Session):
    """A Session that records crossings and answers from configured results.

    ``resolver`` and ``store`` are supplied by the test, because they are the
    two seams a doubled host still has to have. Everything else is recorded.
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
        #: Every crossing, in order, whatever kind it was.
        self.calls: list[RecordedCall] = []
        self._spark_answers: dict[str, Any] = {}
        self._tsql_answers: dict[str, Any] = {}
        self._python_answers: list[Any] = []
        self._default_rows: list[dict] = []

    # --- what a test configures ---------------------------------------------

    def answer_spark_sql(self, statement: str, rows) -> None:
        """The rows one Spark SQL statement returns, matched exactly."""

        self._spark_answers[statement.strip()] = rows

    def answer_tsql(self, statement: str, rows) -> None:
        """The rows one T-SQL statement returns, matched exactly."""

        self._tsql_answers[statement.strip()] = rows

    def answer_python(self, value) -> None:
        """The next value a crossed Python program emits."""

        self._python_answers.append(value)

    # --- what a test reads ---------------------------------------------------

    @property
    def spark_sql(self) -> tuple[str, ...]:
        """Every Spark SQL statement submitted, in order, batches flattened."""

        return tuple(
            statement
            for call in self.calls
            if call.kind == "spark_sql"
            for statement in call.body
        )

    @property
    def tsql(self) -> tuple[str, ...]:
        """Every T-SQL statement submitted, in order."""

        return tuple(
            statement
            for call in self.calls
            if call.kind == "tsql"
            for statement in call.body
        )

    @property
    def python(self) -> tuple[str, ...]:
        """Every Python program crossed, in order."""

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
                "this TestSession was asked to run a Python program and no "
                "result was configured for it, call answer_python() first"
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
        """The configured rows for the last statement, or none at all.

        Only the last statement in a batch returns rows, which is what the
        hosts do: everything before it is setup.
        """

        if not statements:
            return []
        return configured.get(statements[-1].strip(), self._default_rows)


__all__ = ["RecordedCall", "TestSession"]
