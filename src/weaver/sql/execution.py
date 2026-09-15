"""Shared SQL statement, script, query, and transaction handling."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from .errors import SqlError, SqlExecutionError
from .pool import SqlConnectionPool

SqlRow = dict[str, Any]


@dataclass(frozen=True)
class ProcedureResult:
    """A procedure's result sets and output parameters."""

    outputs: "SqlRow"
    result_sets: tuple[tuple["SqlRow", ...], ...]


class SqlExecutor(Protocol):
    """The SQL surface used by Weaver operations."""

    def execute(
        self, statement: str, parameters: Sequence[object] | None = None
    ) -> None: ...

    def execute_script(self, script: str) -> None: ...

    def query(
        self, statement: str, parameters: Sequence[object] | None = None
    ) -> Sequence[SqlRow]: ...

    def query_result_sets(
        self, statement: str, parameters: Sequence[object] | None = None
    ) -> tuple[tuple[SqlRow, ...], ...]: ...

    def call_procedure(
        self,
        procedure: str,
        *,
        inputs: Sequence[tuple[str, object]] = (),
        outputs: Sequence[tuple[str, str]] = (),
    ) -> SqlRow: ...

    def call_procedure_with_results(
        self,
        procedure: str,
        *,
        inputs: Sequence[tuple[str, object]] = (),
        outputs: Sequence[tuple[str, str]] = (),
    ) -> ProcedureResult: ...


class PooledSqlExecutor:
    """Execute through one owned or injected bounded connection pool."""

    def __init__(self, pool: SqlConnectionPool, *, owns_pool: bool = False) -> None:
        self.pool = pool
        self.owns_pool = owns_pool

    def execute(
        self, statement: str, parameters: Sequence[object] | None = None
    ) -> None:
        self._run(statement, parameters=parameters, query=False, drain=True)

    def execute_script(self, script: str) -> None:
        self._run(script, parameters=None, query=False, drain=True)

    def query(
        self, statement: str, parameters: Sequence[object] | None = None
    ) -> Sequence[SqlRow]:
        return self._run(statement, parameters=parameters, query=True, drain=False)

    def query_result_sets(
        self, statement: str, parameters: Sequence[object] | None = None
    ) -> tuple[tuple[SqlRow, ...], ...]:
        """Return every result set in order; :meth:`query` returns only the first."""

        sets = self._run(
            statement,
            parameters=parameters,
            query=True,
            drain=False,
            all_result_sets=True,
        )
        return tuple(tuple(rows) for rows in sets)

    def call_procedure(
        self,
        procedure: str,
        *,
        inputs: Sequence[tuple[str, object]] = (),
        outputs: Sequence[tuple[str, str]] = (),
    ) -> SqlRow:
        """Return a procedure's outputs from Weaver's final result set.

        ``mssql-python`` cannot bind output parameters, so Weaver declares and
        projects them in T-SQL. Earlier result sets are ignored.
        """

        if not outputs:
            raise SqlExecutionError(
                f"{procedure} was called for its outputs and none were named"
            )
        row = self._run(
            _output_parameter_batch(procedure, inputs, outputs),
            parameters=[value for _name, value in inputs],
            query=True,
            drain=False,
            last_result_set=True,
        )
        if not row:
            raise SqlExecutionError(
                f"{procedure} returned no output row. Rebuild its item to restore "
                "the Weaver procedure"
            )
        return row[0]

    def call_procedure_with_results(
        self,
        procedure: str,
        *,
        inputs: Sequence[tuple[str, object]] = (),
        outputs: Sequence[tuple[str, str]] = (),
    ) -> "ProcedureResult":
        """Return a procedure's result sets and final output projection together.

        Keeping both from one execution ensures Test rows and counts describe
        the same data.
        """

        if not outputs:
            raise SqlExecutionError(
                f"{procedure} was called for its outputs and none were named"
            )
        sets = self._run(
            _output_parameter_batch(procedure, inputs, outputs),
            parameters=[value for _name, value in inputs],
            query=True,
            drain=False,
            all_result_sets=True,
        )
        if not sets or not sets[-1]:
            raise SqlExecutionError(
                f"{procedure} returned no output row. Rebuild its item to restore "
                "the Weaver procedure"
            )
        return ProcedureResult(
            outputs=sets[-1][0],
            result_sets=tuple(tuple(rows) for rows in sets[:-1]),
        )

    def _run(
        self,
        statement: str,
        *,
        parameters: Sequence[object] | None,
        query: bool,
        drain: bool,
        last_result_set: bool = False,
        all_result_sets: bool = False,
    ):
        with self.pool.lease() as lease:
            connection = lease.connection
            cursor = None
            try:
                cursor = connection.cursor()
                if parameters is None:
                    cursor.execute(statement)
                else:
                    cursor.execute(statement, tuple(parameters))

                if query:
                    if all_result_sets:
                        rows = _every_result_set(cursor)
                    elif last_result_set:
                        rows = _final_rows(cursor)
                    else:
                        rows = _rows(cursor)
                    connection.commit()
                    return rows

                if drain:
                    _drain(cursor)
                connection.commit()
                return None
            except SqlError:
                lease.discard()
                _rollback(connection)
                raise
            except Exception as exc:
                lease.discard()
                _rollback(connection)
                operation = "query" if query else "SQL execution"
                raise SqlExecutionError(
                    f"{operation} failed on {self.pool.endpoint}: {exc}"
                ) from exc
            finally:
                if cursor is not None:
                    try:
                        cursor.close()
                    except Exception:
                        lease.discard()

    def close(self) -> None:
        if self.owns_pool:
            self.pool.close()

    def __enter__(self) -> "PooledSqlExecutor":
        return self

    def __exit__(self, *exc) -> bool:
        self.close()
        return False


def _output_parameter_batch(
    procedure: str,
    inputs: Sequence[tuple[str, object]],
    outputs: Sequence[tuple[str, str]],
) -> str:
    """Build a parameterised batch whose final row contains procedure outputs.

    Output locals are prefixed to avoid parameter-name collisions.
    """

    declares = "\n".join(
        f"declare @weaver_out_{name} {type_name};" for name, type_name in outputs
    )
    arguments = [f"@{name} = ?" for name, _value in inputs] + [
        f"@{name} = @weaver_out_{name} output" for name, _type in outputs
    ]
    projection = ", ".join(f"@weaver_out_{name} as {name}" for name, _type in outputs)
    call = f"exec {procedure}\n    " + "\n  , ".join(arguments) + ";"
    return f"{declares}\n\n{call}\n\nselect {projection};"


def _rows(cursor) -> list[SqlRow]:
    if cursor.description is None:
        return []
    columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _final_rows(cursor) -> list[SqlRow]:
    """Consume preceding result sets and return only the last."""

    latest: list[SqlRow] = []
    while True:
        if cursor.description is not None:
            latest = _rows(cursor)
        if not cursor.nextset():
            return latest


def _every_result_set(cursor) -> list[list[SqlRow]]:
    """Every result set the batch produced, in order.

    Statements without columns do not produce result sets.
    """

    sets: list[list[SqlRow]] = []
    while True:
        if cursor.description is not None:
            sets.append(_rows(cursor))
        if not cursor.nextset():
            return sets


def _drain(cursor) -> None:
    """Consume all result sets so multi-statement T-SQL can commit reliably."""

    while True:
        if cursor.description is not None:
            cursor.fetchall()
        if not cursor.nextset():
            return


def _rollback(connection) -> None:
    try:
        connection.rollback()
    except Exception:
        pass
