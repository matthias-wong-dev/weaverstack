"""Where a catalogue's runtime writes go.

The ``_`` schema's runtime tables are written as work happens: ``_.Log`` appended
as each unit settles, ``_.Bookmark`` merged as each clean load finishes. Both go
through one boundary, so a caller says what it recorded and never how.

Underneath is :class:`~weaver.catalogue.flusher.WarehouseFlusher`, one per table:
rows are queued, batched and written on a worker, and a failure is surfaced by
:meth:`CatalogueWriter.flush`. Whether a lost row matters is the caller's
judgement, a lost ``_.Log`` row loses evidence, a lost bookmark makes the next
load read a window it has already read, so this raises and lets them decide.

:meth:`CatalogueWriter.delete` removes named rows. It runs its statement and
waits, because a caller removing rows is about to do the work that removal
precedes.
"""

from __future__ import annotations

from typing import Any, Callable, Mapping, Sequence


class CatalogueWriter:
    """One catalogue's runtime writes, by table.

    ``flusher_for`` is asked for a table's write stream on first use, so a
    catalogue nothing writes to opens no connection and starts no worker.
    ``execute`` runs one statement against the same Warehouse and waits.
    """

    def __init__(
        self,
        flusher_for: Callable[[Any], Any],
        *,
        execute: Callable[[str], None] | None = None,
    ) -> None:
        self._flusher_for = flusher_for
        self._execute = execute
        self._flushers: dict[str, Any] = {}

    def submit(self, table, row: Mapping[str, Any]) -> None:
        """Append one row."""

        self._flusher(table).submit(row)

    def update(self, table, row: Mapping[str, Any]) -> None:
        """Merge one row on the table's own key."""

        self._flusher(table).update(row)

    def delete(self, table, rows: Sequence[Mapping[str, Any]]) -> None:
        """Remove the named rows, and wait for the removal.

        Each row carries the table's key. Queued rows are drained first, so a
        merge already in flight cannot land behind the removal.
        """

        from ..errors import CommandError
        from .render import render_delete_rows

        statement = render_delete_rows(table, list(rows))
        if statement is None:
            return
        if self._execute is None:
            raise CommandError(
                f"{table.qualified} cannot be deleted from here: this catalogue "
                "was built without a connection to run a statement through"
            )
        self.flush()
        self._execute(statement)

    def flush(self) -> None:
        """Wait for every queued row, and surface the first failure."""

        for flusher in list(self._flushers.values()):
            flusher.flush()

    def _flusher(self, table):
        known = self._flushers.get(table.name)
        if known is None:
            known = self._flusher_for(table)
            self._flushers[table.name] = known
        return known


class RefusingWriter:
    """A catalogue that can be read but not written, saying so when asked.

    What a catalogue reconstructed from a payload has: it crossed a boundary as
    data, and the connection it was read through did not come with it.
    """

    def __init__(self, why: str) -> None:
        self._why = why

    def submit(self, table, row) -> None:
        self._refuse(table)

    def update(self, table, row) -> None:
        self._refuse(table)

    def delete(self, table, rows) -> None:
        self._refuse(table)

    def flush(self) -> None:
        return None

    def _refuse(self, table) -> None:
        from ..errors import CommandError

        raise CommandError(f"{table.qualified} cannot be written here: {self._why}")


def writer_for(session, workspace=None) -> CatalogueWriter:
    """Where a Session sends the catalogue's runtime writes.

    One flusher per table, opened on first use, so a catalogue nothing writes to
    starts no worker and opens no connection. A removal runs over the Session's
    TDS against the same Warehouse.
    """

    from ..targets import WarehouseTarget

    resolved = session.workspace_or_default(workspace)
    target = WarehouseTarget(warehouse=resolved.catalogue_item)
    return CatalogueWriter(
        lambda table: session.flusher(table, warehouse=target, workspace=resolved),
        execute=lambda statement: session.execute_tsql(
            statement, target=target, workspace=resolved
        ),
    )


__all__ = ["CatalogueWriter", "RefusingWriter", "writer_for"]
