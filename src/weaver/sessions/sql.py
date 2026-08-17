"""Session-owned Warehouse SQL execution with resource telemetry."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from ..sql import ProcedureResult, SqlExecutor, SqlRow
from .telemetry import SessionTelemetry


class SessionSqlExecutor:
    """Record every TDS operation made through a Session capability."""

    def __init__(self, executor: SqlExecutor, telemetry: SessionTelemetry) -> None:
        self._executor = executor
        self._telemetry = telemetry

    def execute(
        self, statement: str, parameters: Sequence[object] | None = None
    ) -> None:
        with self._telemetry.external("tds", "execute"):
            self._executor.execute(statement, parameters)

    def execute_script(self, script: str) -> None:
        with self._telemetry.external("tds", "execute_script"):
            self._executor.execute_script(script)

    def query(
        self, statement: str, parameters: Sequence[object] | None = None
    ) -> Sequence[SqlRow]:
        with self._telemetry.external("tds", "query"):
            return self._executor.query(statement, parameters)

    def query_result_sets(
        self, statement: str, parameters: Sequence[object] | None = None
    ) -> tuple[tuple[SqlRow, ...], ...]:
        with self._telemetry.external("tds", "query_result_sets"):
            return self._executor.query_result_sets(statement, parameters)

    def call_procedure(
        self,
        procedure: str,
        *,
        inputs: Sequence[tuple[str, object]] = (),
        outputs: Sequence[tuple[str, str]] = (),
    ) -> SqlRow:
        with self._telemetry.external("tds", "call_procedure"):
            return self._executor.call_procedure(
                procedure, inputs=inputs, outputs=outputs
            )

    def call_procedure_with_results(
        self,
        procedure: str,
        *,
        inputs: Sequence[tuple[str, object]] = (),
        outputs: Sequence[tuple[str, str]] = (),
    ) -> ProcedureResult:
        with self._telemetry.external("tds", "call_procedure_with_results"):
            return self._executor.call_procedure_with_results(
                procedure, inputs=inputs, outputs=outputs
            )

    def close(self) -> None:
        close = getattr(self._executor, "close", None)
        if close is not None:
            close()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._executor, name)


__all__ = ["SessionSqlExecutor"]
