"""Asynchronous batch append to a Warehouse table.

Rows are queued to one worker. Session close is the durability barrier.
"""

from __future__ import annotations

import queue
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping

from ..errors import WeaverError
from .tables import TIMESTAMP, EvidenceTable
from .tsql import identifier, literal, qualified_name, typed_literal

#: How many rows one INSERT carries. Large enough that a busy run writes a
#: handful of statements rather than hundreds; small enough to stay well inside
#: what a Warehouse accepts in one batch.
BATCH_ROWS = 50

#: How long ``flush`` and ``close`` wait for the worker before giving up. A
#: bounded wait, so a wedged connection cannot hang a run that has finished.
DRAIN_TIMEOUT = 60.0


class FlushError(WeaverError):
    """A queued row could not be written."""


@dataclass(frozen=True)
class FlusherKey:
    """Identity of one Warehouse write stream."""

    workspace: str
    warehouse: str
    schema: str
    table: str


class WarehouseFlusher:
    """Queues rows for one table and writes them on a worker thread.

    One thread and one connection per flusher, never per row. ``submit`` costs
    a queue put; everything else happens behind it.
    """

    def __init__(
        self,
        table: EvidenceTable,
        *,
        execute,
        key: FlusherKey,
        batch_rows: int = BATCH_ROWS,
    ) -> None:
        self.table = table
        self.key = key
        self._execute = execute
        self._batch_rows = batch_rows
        self._queue: queue.Queue = queue.Queue()
        self._worker: threading.Thread | None = None
        self._lock = threading.Lock()
        self._accepting = True
        self._failure: BaseException | None = None
        self._pending = 0

    # --- the contract ---------------------------------------------------------

    def submit(self, row: Mapping[str, Any]) -> None:
        """Accept one row for writing. Does not wait for the Warehouse.

        Accepting and queueing happen under one lock, so ``close`` cannot put
        the stop sentinel between them: the worker would stop before reaching
        the row, and close would return reporting nothing wrong. Queueing costs
        nothing to hold the lock for — the queue is unbounded and the worker
        never blocks a put.
        """

        with self._lock:
            if not self._accepting:
                raise FlushError(
                    f"{self.table.qualified} is closed and accepts no more rows"
                )
            self._pending += 1
            self._ensure_worker()
            self._queue.put(dict(row))

    def flush(self, *, timeout: float = DRAIN_TIMEOUT) -> None:
        """Wait for every accepted row to be written, and surface any failure."""

        if self._worker is None:
            self._raise_any_failure()
            return
        self._wait_for_empty(timeout)
        self._raise_any_failure()

    def _wait_for_empty(self, timeout: float) -> None:
        """Block until the worker has written everything accepted so far.

        Bounded, unlike ``Queue.join``: a wedged connection must not hang a run
        that has otherwise finished.
        """

        deadline = time.monotonic() + timeout
        while self._pending > 0:
            if time.monotonic() >= deadline:
                raise FlushError(
                    f"{self.table.qualified} still had {self._pending} row(s) "
                    f"unwritten after {timeout:g}s"
                )
            time.sleep(0.01)

    def close(self, *, timeout: float = DRAIN_TIMEOUT) -> None:
        """Stop accepting, write what was accepted, and stop the worker."""

        with self._lock:
            was_accepting = self._accepting
            self._accepting = False
            worker = self._worker
            # Under the lock, so the sentinel is behind every row already
            # accepted. Joining is not: the worker settles a batch under the
            # same lock, and waiting for it while holding one would deadlock.
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
        """Start the thread on the first row, never when the flusher is made.

        Opening a Session must not start a worker or a connection: most
        Sessions never log anything.
        """

        if self._worker is not None:
            return
        self._worker = threading.Thread(
            target=self._drain,
            name=f"weaver-flusher-{self.table.name}",
            daemon=True,
        )
        self._worker.start()

    def _drain(self) -> None:
        """Accumulate into a batch, and settle a row only once it is written.

        ``_pending`` drops after the write, never before, so a caller that
        waited on it has the rows in the Warehouse rather than in this queue.
        """

        batch: list[dict] = []
        while True:
            item = self._queue.get()
            if item is _STOP:
                self._write(batch)
                self._settle(len(batch))
                return
            batch.append(item)
            if len(batch) >= self._batch_rows or self._queue.empty():
                self._write(batch)
                self._settle(len(batch))
                batch = []

    def _settle(self, count: int) -> None:
        if not count:
            return
        with self._lock:
            self._pending -= count

    def _write(self, rows: list[dict]) -> None:
        """One INSERT for a batch, in the order the rows were submitted.

        A failure is remembered and the rows are dropped rather than retried:
        retrying a batch whose statement the engine refused would fail the same
        way for as long as the run lasted.
        """

        if not rows:
            return
        try:
            self._execute(self._insert(rows))
        except BaseException as exc:  # noqa: BLE001 - re-raised from flush/close
            if self._failure is None:
                self._failure = exc

    def _insert(self, rows: list[dict]) -> str:
        """One INSERT carrying the batch, audit columns supplied here.

        The audit trio is physically not null on every table Weaver builds, so
        an append supplies all three: the insert datetime is the row's own, and
        the other two take the same live values an unmodified row carries
        anywhere else in the catalogue.
        """

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
    """Weaver's audit trio for one appended row, as rendered literals."""

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
    """The sentinel that ends the worker loop."""

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return "<stop>"


_STOP = _Stop()


__all__ = ["BATCH_ROWS", "FlushError", "FlusherKey", "WarehouseFlusher"]
