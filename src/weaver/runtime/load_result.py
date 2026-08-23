"""What a load reports when it finishes, in one shape for every primitive.

A Warehouse procedure, a Python table, a compiled Spark SQL table and a Python
folder run on different engines and return through different transports — a
T-SQL result set, a Spark ``DataFrame``, a Python object. What they *mean* is
the same, and this module is where that meaning is written down once.

The field names are the contract. :data:`RESULT_COLUMNS` names them in order
and the generated Warehouse procedure declares its output parameters from the
same list (:data:`weaver.declaration.tsql_load.RESULT_PARAMETERS`), so a field
added here reaches every transport.

Success is not "nothing raised": a load that rejected rows reports
``succeeded=False`` even when it was asked to tolerate them and did.
``fault_tolerant`` changes whether the valid rows are still written, never
whether the run is reported as good (see
:mod:`weaver.runtime.load_contract`).
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone

#: The result's columns, in order. The generated T-SQL procedure projects
#: exactly these names, so a transport's final row can be read straight into
#: :class:`LoadResult` and any mismatch is a generation bug rather than a
#: silently misread column.
RESULT_COLUMNS = (
    "succeeded",
    "rows_read",
    "rows_inserted",
    "rows_updated",
    "rows_deleted",
    "rows_rejected",
    "error_message",
    "bookmark_datetime",
)


@dataclass(frozen=True)
class LoadResult:
    """One object's load outcome: what happened, and whether it was acceptable.

    The counts describe the *target*, not the source. ``rows_read`` is what the
    source produced, and the rest are what the load did with it, so
    ``rows_read`` need not equal the sum of the others: an unchanged row is read
    and neither inserted nor updated, which is the ordinary state of most rows in
    most loads.
    """

    succeeded: bool
    rows_read: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_deleted: int = 0
    rows_rejected: int = 0
    error_message: str | None = None
    #: The UTC instant this load began, captured by the engine that ran it
    #: immediately before it read anything. A clean load's bookmark advances to
    #: it, so it comes from the clock the load's own reads were timed by rather
    #: than from whichever machine happened to be orchestrating.
    bookmark_datetime: datetime | None = None

    @classmethod
    def failure(cls, message: str, **counts: int) -> "LoadResult":
        """A failed load, carrying whatever it managed to do before failing.

        The counts are kept rather than zeroed because a partial load is exactly
        the case where they matter: "failed having written nothing" and "failed
        having written four hundred rows" are different situations to recover
        from, and a result that reported neither would send the reader to the
        target to find out.
        """

        return cls(succeeded=False, error_message=message, **counts)

    def rejected(self, message: str) -> "LoadResult":
        """This result, marked failed for row rejections it already counted."""

        return replace(self, succeeded=False, error_message=message)

    def as_row(self) -> dict:
        """The result as a mapping keyed by :data:`RESULT_COLUMNS`.

        The bookmark instant crosses as text: the row travels through a Livy
        submission's JSON, which has no datetime.
        """

        row = {name: getattr(self, name) for name in RESULT_COLUMNS}
        if self.bookmark_datetime is not None:
            row["bookmark_datetime"] = self.bookmark_datetime.isoformat()
        return row

    @classmethod
    def from_row(cls, row) -> "LoadResult":
        """Read a transport's final result row back into a result.

        Takes anything indexable by column name — a ``dict``, a pyodbc row
        mapping, a Spark ``Row`` — because the three transports each hand back
        their own type and none of them is worth converting twice.
        """

        values = {name: row[name] for name in RESULT_COLUMNS}
        return cls(
            succeeded=bool(values["succeeded"]),
            rows_read=int(values["rows_read"]),
            rows_inserted=int(values["rows_inserted"]),
            rows_updated=int(values["rows_updated"]),
            rows_deleted=int(values["rows_deleted"]),
            rows_rejected=int(values["rows_rejected"]),
            error_message=values["error_message"],
            bookmark_datetime=_instant(values["bookmark_datetime"]),
        )


def _instant(value) -> datetime | None:
    """One reported bookmark instant, always aware and always UTC.

    A T-SQL ``datetime2`` and a Livy payload's ISO text arrive differently and
    neither carries a zone: the procedure took ``sysutcdatetime()`` and Python
    took ``datetime.now(timezone.utc)``, so an instant with no zone is UTC.
    """

    if value is None or value == "":
        return None
    at = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    return at if at.tzinfo is not None else at.replace(tzinfo=timezone.utc)


__all__ = ["RESULT_COLUMNS", "LoadResult"]
