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
    """What one procedure execution produced: its rows, and its outputs.

    Apart because they are transported differently: the rows come back as
    result sets, the outputs as a projection Weaver appended. A caller wanting
    only the numbers asks :meth:`PooledSqlExecutor.call_procedure`.
    """

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
        """Every result set a batch produced, in order.

        :meth:`query` reads the first and stops, which is right for a statement
        that answers one question. A batch that returns *evidence and then a
        projection* — which is what a validation run directly from source is —
        needs both, and reading only the first silently answers with the wrong
        one: a diagnostic row has no count column, so the counts read as zero
        and a failing Test reports as passed.
        """

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
        """Call a procedure and read back the values it set on its outputs.

        ``mssql-python`` does not bind output parameters — ``callproc`` is
        declared and raises ``NotSupportedError`` — so they are marshalled in
        T-SQL instead: locals are declared, passed as ``output``, and projected
        by a ``select`` this method writes.

        That last detail is the point. The projection is the final statement of
        a batch *Weaver* composed, so the row read back is Weaver's own however
        many result sets the procedure emitted on the way — which is the whole
        reason the load result stopped being one of them. Anything the
        procedure's authored setup returned is passed over, not parsed.
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
                f"{procedure} returned no output row — it may have been altered "
                "outside Weaver, or replaced by a version without these outputs"
            )
        return row[0]

    def call_procedure_with_results(
        self,
        procedure: str,
        *,
        inputs: Sequence[tuple[str, object]] = (),
        outputs: Sequence[tuple[str, str]] = (),
    ) -> "ProcedureResult":
        """Call a procedure and keep *both* its result sets and its outputs.

        :meth:`call_procedure` reads the last result set — Weaver's own output
        projection — and passes over everything the procedure emitted on the
        way. That is right for a load, whose evidence is entirely in its counts.
        It is wrong for a Test: the counts say how much disagreed and the rows
        say what, and a caller wanting both must not run the Test twice, because
        the data could change in between and the cost could be large.

        So this keeps everything, splits the last set off as the outputs, and
        hands back the rest in the order the procedure produced them.
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
                f"{procedure} returned no output row — it may have been altered "
                "outside Weaver, or replaced by a version without these outputs"
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
    """A batch that calls ``procedure`` and hands its outputs back as a row.

    The locals are prefixed so they cannot collide with a parameter name, since
    ``@rows_read = @rows_read output`` would be legal and unreadable.

    Input *values* are placeholders rather than literals — the values come from
    a caller and are the one part of this text that is not Weaver's own.
    """

    declares = "\n".join(
        f"declare @weaver_out_{name} {type_name};" for name, type_name in outputs
    )
    arguments = [f"@{name} = ?" for name, _value in inputs] + [
        f"@{name} = @weaver_out_{name} output" for name, _type in outputs
    ]
    projection = ", ".join(
        f"@weaver_out_{name} as {name}" for name, _type in outputs
    )
    call = f"exec {procedure}\n    " + "\n  , ".join(arguments) + ";"
    return f"{declares}\n\n{call}\n\nselect {projection};"


def _rows(cursor) -> list[SqlRow]:
    if cursor.description is None:
        return []
    columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _final_rows(cursor) -> list[SqlRow]:
    """The last result set the batch produced, and only it.

    For a batch whose *own* trailing ``select`` is the answer: everything before
    it belongs to whatever the batch called, and has to be consumed to be got
    past rather than interpreted.
    """

    latest: list[SqlRow] = []
    while True:
        if cursor.description is not None:
            latest = _rows(cursor)
        if not cursor.nextset():
            return latest


def _every_result_set(cursor) -> list[list[SqlRow]]:
    """Every result set the batch produced, in order.

    A set with no description is one a statement produced without returning
    columns, and is skipped rather than recorded as empty — otherwise a
    procedure's internal work would appear as result sets a caller has to know
    to ignore.
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
