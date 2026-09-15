"""Route runtime catalogue writes through per-table Warehouse flushers.

``_.Log`` rows append and ``_.Bookmark`` rows merge. ``flush`` surfaces worker
failures. Deletes drain queued writes first and complete synchronously because
they precede the caller's next state transition.
"""

from __future__ import annotations

from typing import Any, Callable, Mapping, Sequence


class CatalogueWriter:
    """One catalogue's lazy per-table runtime write streams."""

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
        self._flusher(table).submit(row)

    def update(self, table, row: Mapping[str, Any]) -> None:
        self._flusher(table).update(row)

    def delete(self, table, rows: Sequence[Mapping[str, Any]]) -> None:
        """Drain queued writes, then synchronously remove rows by key."""

        from ..errors import CommandError
        from .render import render_delete_rows

        statement = render_delete_rows(table, list(rows))
        if statement is None:
            return
        if self._execute is None:
            raise CommandError(
                f"Cannot delete from {table.qualified}: this catalogue has no "
                "connection for executing statements"
            )
        self.flush()
        self._execute(statement)

    def flush(self) -> None:
        """Wait for queued writes and surface the first failure."""

        for flusher in list(self._flushers.values()):
            flusher.flush()

    def _flusher(self, table):
        known = self._flushers.get(table.name)
        if known is None:
            known = self._flusher_for(table)
            self._flushers[table.name] = known
        return known


class RefusingWriter:
    """Reject writes to a catalogue reconstructed without its connection."""

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
    """Create lazy runtime writers for the Session's catalogue Warehouse."""

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
