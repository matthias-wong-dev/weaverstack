"""The common result of every load primitive."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone

#: Serialized result columns, in transport order.
RESULT_COLUMNS = (
    "succeeded",
    "rows_read",
    "rows_inserted",
    "rows_updated",
    "rows_deleted",
    "rows_rejected",
    "error_message",
    "bookmark_datetime",
    "is_static_skip",
)


@dataclass(frozen=True)
class LoadResult:
    """One object's load outcome and row counts."""

    succeeded: bool
    rows_read: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_deleted: int = 0
    rows_rejected: int = 0
    error_message: str | None = None
    #: The UTC instant the executing engine captured before the load read anything.
    bookmark_datetime: datetime | None = None
    #: Explicit because zero counts cannot distinguish a static skip from an empty read.
    is_static_skip: bool = False

    @classmethod
    def failure(cls, message: str, **counts: int) -> "LoadResult":
        """Preserve counts from work completed before failure."""
        return cls(succeeded=False, error_message=message, **counts)

    def rejected(self, message: str) -> "LoadResult":
        return replace(self, succeeded=False, error_message=message)

    def as_row(self) -> dict:
        """Serialize the bookmark as ISO text for JSON transport."""
        row = {name: getattr(self, name) for name in RESULT_COLUMNS}
        if self.bookmark_datetime is not None:
            row["bookmark_datetime"] = self.bookmark_datetime.isoformat()
        return row

    @classmethod
    def from_row(cls, row) -> "LoadResult":
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
            is_static_skip=bool(values["is_static_skip"]),
        )


def _instant(value) -> datetime | None:
    """Treat zone-less engine timestamps as UTC."""

    if value is None or value == "":
        return None
    at = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    return at if at.tzinfo is not None else at.replace(tzinfo=timezone.utc)


__all__ = ["RESULT_COLUMNS", "LoadResult"]
