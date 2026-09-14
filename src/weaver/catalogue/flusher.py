"""Batch asynchronous writes to one Warehouse table.

``submit`` appends evidence; ``update`` upserts current state by key. They use
separate INSERT and MERGE batches. Session close is the durability barrier.
"""

from __future__ import annotations

import queue
import threading
import time
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping

from ..errors import WeaverError
from .render import render_keyed_merge
from .tables import TIMESTAMP, RuntimeTable
from .tsql import identifier, literal, qualified_name, typed_literal

# Keep INSERT batches within Warehouse limits without frequent round trips.
BATCH_ROWS = 50

# A wedged connection must not hang a completed run indefinitely.
DRAIN_TIMEOUT = 60.0


class FlushError(WeaverError):
    """A queued row could not be written."""


@dataclass(frozen=True)
class FlusherKey:
    workspace: str
    warehouse: str
    schema: str
    table: str


class WarehouseFlusher:
    """Queue one table's rows for a single worker and connection."""

    def __init__(
        self,
        table: RuntimeTable,
        *,
        execute,
        key: FlusherKey,
        batch_rows: int = BATCH_ROWS,
        capture_context=None,
        use_context=None,
    ) -> None:
        self.table = table
        self.key = key
        self._execute = execute
        self._batch_rows = batch_rows
        self._capture_context = capture_context
        self._use_context = use_context
        self._queue: queue.Queue = queue.Queue()
        self._worker: threading.Thread | None = None
        self._lock = threading.Lock()
        self._accepting = True
        self._failure: BaseException | None = None
        self._pending = 0

    # --- the contract ---------------------------------------------------------

    def submit(self, row: Mapping[str, Any], *, keyed: bool = False) -> None:
        """Queue a row without waiting for the Warehouse.

        Acceptance and queueing share a lock so ``close`` cannot place its stop
        sentinel before an accepted row.
        """

        with self._lock:
            if not self._accepting:
                raise FlushError(
                    f"{self.table.qualified} is closed and accepts no more rows"
                )
            self._pending += 1
            self._ensure_worker()
            context = (
                self._capture_context() if self._capture_context is not None else None
            )
            self._queue.put(_QueuedRow(row, context, keyed=keyed))

    def update(self, row: Mapping[str, Any]) -> None:
        if not getattr(self.table, "is_current_state", bool(self.table.key)):
            raise FlushError(
                f"Cannot merge a row into history table {self.table.qualified}; "
                "append it with submit()"
            )
        self.submit(row, keyed=True)

    def flush(self, *, timeout: float = DRAIN_TIMEOUT) -> None:
        """Wait for accepted rows and surface any write failure."""

        if self._worker is None:
            self._raise_any_failure()
            return
        self._wait_for_empty(timeout)
        self._raise_any_failure()

    def _wait_for_empty(self, timeout: float) -> None:
        deadline = time.monotonic() + timeout
        while self._pending > 0:
            if time.monotonic() >= deadline:
                raise FlushError(
                    f"{self.table.qualified} still had {self._pending} row(s) "
                    f"unwritten after {timeout:g}s"
                )
            time.sleep(0.01)

    def close(self, *, timeout: float = DRAIN_TIMEOUT) -> None:
        """Stop after writing every accepted row."""

        with self._lock:
            was_accepting = self._accepting
            self._accepting = False
            worker = self._worker
            # Queue the sentinel under the lock, but join outside it because the
            # worker acquires the same lock when settling a batch.
            if worker is not None and was_accepting:
                self._queue.put(_STOP)
        if worker is None:
            self._raise_any_failure()
            return
        worker.join(timeout=timeout)
        if worker.is_alive():
            raise FlushError(
                f"{self.table.qualified} worker did not stop after {timeout:g}s; "
                f"{self._pending} row(s) remain pending"
            )
        self._worker = None
        self._raise_any_failure()

    # --- the worker -----------------------------------------------------------

    def _ensure_worker(self) -> None:
        """Start the worker lazily so idle Sessions open no connection."""
        if self._worker is not None:
            return
        self._worker = threading.Thread(
            target=self._drain,
            name=f"weaver-flusher-{self.table.name}",
            daemon=True,
        )
        self._worker.start()

    def _drain(self) -> None:
        """Settle rows only after their batch has been written."""

        batch: list[dict] = []
        context = None
        keyed = False
        while True:
            item = self._queue.get()
            if item is _STOP:
                self._write(batch, context, keyed=keyed)
                self._settle(len(batch))
                return
            row, item_context = item, item.context
            # One batch cannot mix statement kinds or reporting contexts.
            if batch and (item_context != context or item.keyed != keyed):
                self._write(batch, context, keyed=keyed)
                self._settle(len(batch))
                batch = []
            context = item_context
            keyed = item.keyed
            batch.append(row)
            if len(batch) >= self._batch_rows or self._queue.empty():
                self._write(batch, context, keyed=keyed)
                self._settle(len(batch))
                batch = []
                context = None
                keyed = False

    def _settle(self, count: int) -> None:
        if not count:
            return
        with self._lock:
            self._pending -= count

    def _write(self, rows: list[dict], context=None, *, keyed: bool = False) -> None:
        """Write one ordered batch, recording failure without retrying it."""

        if not rows:
            return
        try:
            activation = (
                self._use_context(context)
                if self._use_context is not None and context is not None
                else nullcontext()
            )
            with activation:
                statement = self._merge(rows) if keyed else self._insert(rows)
                if statement is not None:
                    self._execute(statement)
        except BaseException as exc:  # noqa: BLE001 - re-raised from flush/close
            if self._failure is None:
                self._failure = exc

    def _merge(self, rows: list[dict]) -> str | None:
        return render_keyed_merge(self.table, rows)

    def _insert(self, rows: list[dict]) -> str:
        """Render an INSERT with all three non-null audit values."""

        columns = [self.table.column(name) for name in self.table.column_names]
        names = ", ".join(
            identifier(self.table.public_name_of(name))
            for name in self.table.physical_columns
        )
        values = ",\n       ".join(
            "("
            + ", ".join(
                [typed_literal(row.get(column.name), column) for column in columns]
                + [_audit_values(row)]
            )
            + ")"
            for row in rows
        )
        return f"INSERT INTO {qualified_name(self.table)} ({names})\nVALUES {values}\n"

    def _raise_any_failure(self) -> None:
        failure = self._failure
        if failure is None:
            return
        self._failure = None
        raise FlushError(
            f"{self.table.qualified} did not accept every row: {failure}"
        ) from failure


def _audit_values(row: Mapping[str, Any]) -> str:
    from ..declaration.metadata import AUDIT_LIVE_DELETE_DATETIME

    written = row.get("row_insert_datetime") or datetime.now(timezone.utc)
    return ", ".join(
        (
            literal(written),
            literal(written),
            literal(AUDIT_LIVE_DELETE_DATETIME, TIMESTAMP),
        )
    )


class _Stop:
    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return "<stop>"


_STOP = _Stop()


class _QueuedRow(dict):
    def __init__(self, row: Mapping[str, Any], context, *, keyed: bool = False) -> None:
        super().__init__(row)
        self.context = context
        self.keyed = keyed


__all__ = ["BATCH_ROWS", "FlushError", "FlusherKey", "WarehouseFlusher"]
