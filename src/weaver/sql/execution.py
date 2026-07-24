"""Shared SQL statement, script, query, and transaction handling."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Protocol

from .errors import SqlError, SqlExecutionError
from .pool import SqlConnectionPool

SqlRow = dict[str, Any]


class SqlExecutor(Protocol):
    """The SQL surface used by Weaver operations."""

    def execute(
        self, statement: str, parameters: Sequence[object] | None = None
    ) -> None: ...

    def execute_script(self, script: str) -> None: ...

    def query(
        self, statement: str, parameters: Sequence[object] | None = None
    ) -> Sequence[SqlRow]: ...


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

    def _run(
        self,
        statement: str,
        *,
        parameters: Sequence[object] | None,
        query: bool,
        drain: bool,
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
                    if cursor.description is None:
                        return []
                    columns = [column[0] for column in cursor.description]
                    return [dict(zip(columns, row)) for row in cursor.fetchall()]

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
