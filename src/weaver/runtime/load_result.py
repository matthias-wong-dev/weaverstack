"""What a load reports when it finishes, in one shape for all four primitives.

A Warehouse procedure, a Spark SQL program, a Python table and a Python folder
run on different engines and return through different transports — a T-SQL
result set, a Spark ``DataFrame``, a Python object. What they *mean* is the same,
and this module is where that meaning is written down once.

The field names are the contract, not just the dataclass. :data:`RESULT_COLUMNS`
names them in order, and the SQL generators build their final result row from it,
so a column added here reaches every transport instead of three spellings
drifting apart. That is the whole reason the names live beside the dataclass
rather than inside each generator.

**Success is not "nothing raised".** A load that rejected rows reports
``succeeded=False`` even when it was asked to tolerate them and did — the rows
did not arrive, and a caller that only checked for an exception would call that a
clean load. What ``fault_tolerant`` changes is whether the valid rows are still
written, never whether the run is reported as good (see
:mod:`weaver.runtime.load_contract`).
"""

from __future__ import annotations

from dataclasses import dataclass, replace

#: The result's columns, in order. The generated T-SQL and Spark SQL programs
#: project exactly these names, so a transport's final row can be read straight
#: into :class:`LoadResult` and any mismatch is a generation bug rather than a
#: silently misread column.
RESULT_COLUMNS = (
    "succeeded",
    "rows_read",
    "rows_inserted",
    "rows_updated",
    "rows_deleted",
    "rows_rejected",
    "error_message",
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
        """The result as a mapping keyed by :data:`RESULT_COLUMNS`."""

        return {name: getattr(self, name) for name in RESULT_COLUMNS}

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
        )


__all__ = ["RESULT_COLUMNS", "LoadResult"]
